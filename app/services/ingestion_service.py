"""Durable ingestion jobs.

API requests insert a pending job and return. The worker, or an Airflow task, calls
`run_job`. A Postgres advisory lock prevents two runners from processing the same
job. Document row locks prevent two jobs from publishing the same version.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.connectors.factory import build_connector, validate_source_config
from app.core.exceptions import (
    ConflictError,
    DatabaseFailure,
    NotFoundError,
    PayloadTooLargeError,
    UnsupportedMediaError,
)
from app.core.logging import document_id_var, get_logger, job_id_var, log_extra, operation_var
from app.core.security import resolve_allowed_path, sanitize_filename
from app.database.models import (
    DataQualityResult,
    Document,
    DocumentChunk,
    DocumentVersion,
    IngestionJob,
    JobDocument,
    ProcessingAttempt,
    SchemaContract,
    SchemaDriftEvent,
    Source,
)
from app.database.repositories import (
    DocumentRepository,
    DriftRepository,
    JobRepository,
    QualityRepository,
    SchemaContractRepository,
    SourceRepository,
)
from app.database.repositories.jobs import stale_cutoff
from app.database.session import session_scope
from app.domain.enums import DocumentType, SourceType
from app.domain.models import RejectedItem, SourceRecord
from app.observability.metrics import (
    DISCOVERED,
    DOCUMENTS,
    EXTRACTION_FAILURES,
    INGESTION_DURATION,
    QUALITY_FAILURES,
)
from app.parsers.common import EXTENSIONS
from app.processing.hashing import document_uuid, drift_signature, sha256_bytes, sha256_text
from app.processing.planning import DocumentPlan, ExistingDocument, build_plan
from app.processing.state_machine import advance_status, is_retryable_exception
from app.services.context import AppContext
from app.services.indexing_service import IndexingService

logger = get_logger(__name__)
UPLOAD_SOURCE_NAME = "uploads"


class IngestionService:
    def __init__(self, context: AppContext) -> None:
        self.context = context
        self.indexing = IndexingService(context)

    def register_source(
        self,
        *,
        name: str,
        source_type: str,
        config: dict,
        contract: dict | None = None,
        contract_name: str | None = None,
    ) -> dict:
        normalized = validate_source_config(source_type, config, self.context.settings)
        contract_body = normalized.pop("schema_contract", None) or contract
        with session_scope(self.context.settings) as session:
            repo = SourceRepository(session)
            if repo.get_by_name(name):
                raise ConflictError(f"Source {name} already exists")
            source = repo.add(
                Source(name=name, source_type=source_type, config=normalized, status="active")
            )
            if contract_body:
                SchemaContractRepository(session).add(
                    SchemaContract(
                        source_id=source.id,
                        name=contract_name or f"{name}-contract",
                        version=str(contract_body.get("version") or "1"),
                        contract=contract_body.get("contract") or contract_body,
                        active=True,
                    )
                )
            return _source_dict(source)

    def list_sources(
        self, *, source_type: str | None, limit: int, offset: int
    ) -> tuple[list[dict], int]:
        with session_scope(self.context.settings) as session:
            rows, total = SourceRepository(session).list(
                source_type=source_type, limit=limit, offset=offset
            )
            return [_source_dict(row) for row in rows], total

    def get_source(self, source_id: UUID) -> dict:
        with session_scope(self.context.settings) as session:
            source = SourceRepository(session).get(source_id)
            if source is None:
                raise NotFoundError("Source not found")
            return _source_dict(source)

    def enqueue_source(
        self,
        source_id: UUID,
        *,
        trigger: str,
        force: bool = False,
        idempotency_key: str | None = None,
        logical_keys: list[str] | None = None,
        start_immediately: bool = False,
        retry_of_job_id: UUID | None = None,
    ) -> dict:
        self.get_source(source_id)
        payload = {
            "mode": "source",
            "force": force,
            "logical_keys": logical_keys or [],
            "source_id": str(source_id),
        }
        return self._create_job(
            source_id=source_id,
            trigger=trigger,
            force=force,
            idempotency_key=idempotency_key,
            config=payload,
            start_immediately=start_immediately,
            retry_of_job_id=retry_of_job_id,
            payload_hash=sha256_text(repr(sorted(payload.items()))),
        )

    def enqueue_upload(self, *, filename: str, content: bytes, idempotency_key: str | None) -> dict:
        safe = sanitize_filename(filename)
        extension = Path(safe).suffix.lower()
        if extension not in EXTENSIONS:
            raise UnsupportedMediaError(f"Unsupported file type {extension or 'unknown'}")
        if len(content) > self.context.settings.max_upload_bytes:
            raise PayloadTooLargeError("Upload exceeds MAX_UPLOAD_SIZE_MB")
        source_id = self._ensure_upload_source()
        logical_key = safe
        document_id = document_uuid(source_id, logical_key)
        content_hash = sha256_bytes(content)
        existing = self._existing_idempotent(idempotency_key, content_hash)
        if existing is not None:
            existing["document_id"] = document_id
            return existing
        destination = self.context.settings.data_path / "raw" / "uploads" / str(document_id) / safe
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        payload = {
            "mode": "upload",
            "stored_path": str(destination),
            "filename": safe,
            "logical_key": logical_key,
            "content_hash": content_hash,
            "force": False,
        }
        return self._create_job(
            source_id=source_id,
            trigger="api",
            force=False,
            idempotency_key=idempotency_key,
            config=payload,
            start_immediately=False,
            retry_of_job_id=None,
            payload_hash=payload["content_hash"],
            document_id=document_id,
        )

    def retry_job(self, job_id: UUID, *, include_quarantined: bool) -> dict:
        with session_scope(self.context.settings) as session:
            repo = JobRepository(session)
            job = repo.get(job_id)
            if job is None:
                raise NotFoundError("Job not found")
            outcomes = {"failed"}
            if include_quarantined:
                outcomes.add("quarantined")
            rows = repo.list_document_results(job_id, outcomes=outcomes)
            logical_keys = [row.logical_key for row in rows]
            source_id = job.source_id
            mode = (job.config_snapshot or {}).get("mode", "source")
            config = dict(job.config_snapshot or {})
        if not logical_keys:
            raise ConflictError("Job has no failed documents eligible for retry")
        if mode == "upload":
            config["mode"] = "upload"
            config["force"] = True
            return self._create_job(
                source_id=source_id,
                trigger="retry",
                force=True,
                idempotency_key=None,
                config=config,
                start_immediately=False,
                retry_of_job_id=job_id,
                payload_hash=sha256_text("retry-upload:" + ",".join(logical_keys)),
            )
        return self.enqueue_source(
            source_id,
            trigger="retry",
            force=True,
            logical_keys=logical_keys,
            retry_of_job_id=job_id,
        )

    def run_job(self, job_id: UUID) -> dict:
        started = time.perf_counter()
        token = job_id_var.set(str(job_id))
        operation = operation_var.set("run_job")
        connection = self.context.settings and None
        engine_connection = None
        locked = False
        key = _advisory_key(job_id)
        try:
            from app.database.session import get_engine

            engine_connection = get_engine(self.context.settings).connect()
            locked = bool(
                engine_connection.execute(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
                ).scalar()
            )
            engine_connection.commit()
            if not locked:
                logger.info(
                    "ingestion job is already running", extra=log_extra(operation="run_job")
                )
                return self.get_job(job_id)
            self._mark_running(job_id)
            note = None
            try:
                note = self._execute(job_id)
            except (DatabaseFailure, OperationalError) as exc:
                note = "database failure"
                logger.exception(
                    "database failure during ingestion",
                    extra=log_extra(operation="run_job", error_category="database"),
                )
                self._finalize(job_id, note=note, failed=True)
                raise DatabaseFailure("Database failure during ingestion") from exc
            except Exception as exc:
                note = str(exc)[:500]
                logger.exception(
                    "ingestion job failed",
                    extra=log_extra(operation="run_job", error_category=type(exc).__name__),
                )
                self._finalize(job_id, note=note, failed=True)
                if is_retryable_exception(exc):
                    raise
            else:
                self._finalize(job_id, note=note, failed=bool(note))
            INGESTION_DURATION.observe(time.perf_counter() - started)
            return self.get_job(job_id)
        finally:
            if engine_connection is not None:
                if locked:
                    engine_connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                    engine_connection.commit()
                engine_connection.close()
            job_id_var.reset(token)
            operation_var.reset(operation)
            del connection

    def get_job(self, job_id: UUID) -> dict:
        with session_scope(self.context.settings) as session:
            job = JobRepository(session).get(job_id)
            if job is None:
                raise NotFoundError("Job not found")
            return _job_dict(job)

    def list_jobs(
        self, *, status: str | None, source_id: UUID | None, limit: int, offset: int
    ) -> tuple[list[dict], int]:
        with session_scope(self.context.settings) as session:
            rows, total = JobRepository(session).list(
                status=status, source_id=source_id, limit=limit, offset=offset
            )
            return [_job_dict(row) for row in rows], total

    def requeue_stale_jobs(self) -> dict[str, int]:
        cutoff = stale_cutoff(self.context.settings.job_stale_seconds)
        with session_scope(self.context.settings) as session:
            requeued, failed = JobRepository(session).requeue_stale(
                cutoff, self.context.settings.max_job_retries
            )
        return {"requeued": requeued, "failed": failed}

    def _execute(self, job_id: UUID) -> str | None:
        with session_scope(self.context.settings) as session:
            job = JobRepository(session).get(job_id)
            if job is None:
                raise NotFoundError("Job not found")
            config = dict(job.config_snapshot or {})
            source_id = job.source_id
            force = bool(job.force or config.get("force"))
        if config.get("mode") == "upload":
            self._run_upload(job_id, source_id, config, force)
            return None
        if source_id is None:
            return "Job has no source"
        with session_scope(self.context.settings) as session:
            source = SourceRepository(session).get(source_id)
            if source is None:
                return "Source no longer exists"
            source_type = source.source_type
            source_config = dict(source.config or {})
        if source_type == SourceType.UPLOAD.value:
            self._run_upload(job_id, source_id, config, force)
            return None
        return self._run_connector(job_id, source_id, source_type, source_config, config, force)

    def _run_upload(self, job_id: UUID, source_id: UUID | None, config: dict, force: bool) -> None:
        if source_id is None:
            raise NotFoundError("Upload source is missing")
        path = resolve_allowed_path(config["stored_path"], self.context.settings)
        content = path.read_bytes()
        extension = Path(config["filename"]).suffix.lower()
        record = SourceRecord(
            logical_key=config["logical_key"],
            source_uri=str(path),
            filename=config["filename"],
            content_type=EXTENSIONS[extension].value,
            content=content,
            document_type=EXTENSIONS[extension],
            metadata={"upload": True},
        )
        self.process_record(job_id, source_id, SourceType.UPLOAD.value, {}, record, force=force)

    def _run_connector(
        self, job_id, source_id, source_type, source_config, job_config, force: bool
    ) -> str | None:
        connector = build_connector(
            source_id=source_id,
            source_type=source_type,
            config=source_config,
            settings=self.context.settings,
        )
        wanted = set(job_config.get("logical_keys") or [])
        seen: list[str] = []
        processed = 0
        complete = True
        note = None
        cap = self.context.settings.max_documents_per_run
        try:
            for item in connector.iter_records():
                if processed >= cap:
                    complete = False
                    note = "Stopped at MAX_DOCUMENTS_PER_RUN before deletion detection"
                    break
                processed += 1
                DISCOVERED.inc()
                if isinstance(item, RejectedItem):
                    if wanted and item.logical_key not in wanted:
                        continue
                    self._record_rejection(job_id, source_id, item)
                    continue
                if wanted and item.logical_key not in wanted:
                    continue
                seen.append(item.logical_key)
                try:
                    self.process_record(
                        job_id, source_id, source_type, source_config, item, force=force
                    )
                except (DatabaseFailure, OperationalError):
                    raise
                except Exception as exc:
                    logger.exception(
                        "document failed inside batch",
                        extra=log_extra(
                            operation="process_record", error_category=type(exc).__name__
                        ),
                    )
                    self._record_rejection(
                        job_id,
                        source_id,
                        RejectedItem(
                            source_uri=item.source_uri,
                            filename=item.filename,
                            logical_key=item.logical_key,
                            reason=str(exc)[:500],
                            category=getattr(exc, "category", None) or "unexpected",
                        ),
                        outcome="failed",
                        retryable=is_retryable_exception(exc),
                    )
                self._heartbeat(job_id)
        except (DatabaseFailure, OperationalError):
            raise
        except Exception as exc:
            complete = False
            note = str(exc)[:500]
            logger.exception(
                "source scan failed",
                extra=log_extra(operation="discover", error_category=type(exc).__name__),
            )
        if wanted:
            for missing in wanted.difference(seen):
                self._record_rejection(
                    job_id,
                    source_id,
                    RejectedItem(
                        source_uri=missing,
                        filename=None,
                        logical_key=missing,
                        reason="Record was not returned by the source during retry",
                        category="missing",
                    ),
                    outcome="failed",
                )
        if complete and source_config.get("detect_deletions") and not wanted:
            self._apply_deletions(job_id, source_id, set(seen))
        return note

    def process_record(
        self,
        job_id: UUID,
        source_id: UUID,
        source_type: str,
        source_config: dict,
        record: SourceRecord,
        *,
        force: bool,
    ) -> str:
        document_id = document_uuid(source_id, record.logical_key)
        doc_token = document_id_var.set(str(document_id))
        try:
            state_and_flag, contract = self._lock_document(
                job_id, source_id, source_type, record, document_id
            )
            state, flag = state_and_flag
            if flag == "locked":
                self._finish_locked(job_id, document_id, record)
                return "skipped"
            try:
                plan = build_plan(
                    record,
                    source_id=source_id,
                    source_type=source_type,
                    settings=self.context.settings,
                    existing=state,
                    contract=contract[0] if contract else None,
                    schema_name=contract[1] if contract else None,
                    schema_version=contract[2] if contract else None,
                    force=force,
                    embedding_model=self.context.embeddings.model,
                    text_fields=source_config.get("text_fields"),
                    title_field=source_config.get("title_field"),
                )
                return self._apply_plan(
                    job_id,
                    source_id,
                    source_type,
                    record,
                    plan,
                    state,
                    force=force,
                )
            finally:
                self._unlock(document_id, job_id)
        finally:
            document_id_var.reset(doc_token)

    def _lock_document(
        self, job_id, source_id, source_type, record: SourceRecord, document_id: UUID
    ):
        with session_scope(self.context.settings) as session:
            repo = DocumentRepository(session)
            contracts = SchemaContractRepository(session)
            contract_row = contracts.get_active_for_source(source_id)
            contract = None
            if contract_row:
                contract = (contract_row.contract, contract_row.name, contract_row.version)
            row = repo.get(document_id)
            now = datetime.now(UTC)
            if row is None:
                row = repo.add(
                    Document(
                        id=document_id,
                        source_id=source_id,
                        logical_key=record.logical_key,
                        source_type=source_type,
                        source_uri=record.source_uri,
                        original_filename=record.filename,
                        document_type=(record.document_type or DocumentType.TXT).value,
                        title=record.filename or record.logical_key,
                        processing_status="pending",
                        raw_uri=record.source_uri,
                        byte_size=len(record.content),
                        media_type=record.content_type,
                        metadata_schema_version=self.context.settings.metadata_schema_version,
                        custom_metadata={},
                    )
                )
            else:
                repo.lock(document_id)
                row = repo.get(document_id)
            assert row is not None
            if row.lock_job_id and row.lock_job_id != job_id and row.lock_acquired_at:
                stale_at = now - timedelta(seconds=self.context.settings.job_stale_seconds)
                if row.lock_acquired_at >= stale_at:
                    return (None, "locked"), contract
            row.lock_job_id = job_id
            row.lock_acquired_at = now
            row.deleted_at = None
            if row.processing_status == "tombstoned":
                row.processing_status = "pending"
            state = ExistingDocument(
                document_id=row.id,
                current_version=row.current_version,
                content_hash=row.content_hash,
                fingerprint=row.processing_fingerprint,
                indexing_status=row.indexing_status,
                processing_status=row.processing_status,
                chunks_persisted=row.current_version > 0
                and bool(repo.list_chunks(row.id, row.current_version)),
            )
            return (state, "ok"), contract

    def _finish_locked(self, job_id: UUID, document_id: UUID, record: SourceRecord) -> None:
        self._write_outcome(
            job_id,
            document_id,
            record.logical_key,
            outcome="skipped",
            stage="pending",
            message="Document is locked by another job",
            category="locked",
        )

    def _unlock(self, document_id: UUID, job_id: UUID) -> None:
        with session_scope(self.context.settings) as session:
            document = session.get(Document, document_id)
            if document is not None and document.lock_job_id == job_id:
                document.lock_job_id = None
                document.lock_acquired_at = None

    def _apply_plan(
        self,
        job_id,
        source_id,
        source_type,
        record: SourceRecord,
        plan: DocumentPlan,
        existing: ExistingDocument | None,
        *,
        force: bool,
    ) -> str:
        self._persist_quality_and_drift(job_id, source_id, plan)
        if plan.action == "skip":
            published = existing is not None and existing.current_version > 0
            self._mark_document_status(
                plan,
                record,
                status="completed" if published else "skipped",
                indexing_status=existing.indexing_status if published and existing else None,
            )
            self._write_outcome(
                job_id,
                plan.document_id,
                plan.logical_key,
                "skipped",
                "validating",
                plan.message,
                None,
            )
            self._attempt(job_id, plan.document_id, "validating", "skipped", None, plan.message)
            DOCUMENTS.labels(status="skipped").inc()
            return "skipped"
        if plan.action == "quarantine":
            keep_published = (
                existing is not None
                and existing.indexing_status == "indexed"
                and existing.current_version > 0
            )
            status = "completed" if keep_published else "quarantined"
            self._mark_document_status(
                plan,
                record,
                status=status,
                extraction_status=plan.extraction_status,
                indexing_status=existing.indexing_status
                if keep_published and existing
                else "not_indexed",
                error=None if keep_published else plan.message,
                category=None if keep_published else plan.error_category,
            )
            self._write_outcome(
                job_id,
                plan.document_id,
                plan.logical_key,
                "quarantined",
                "validating",
                plan.message,
                plan.error_category,
            )
            self._attempt(
                job_id,
                plan.document_id,
                "validating",
                "quarantined",
                plan.error_category,
                plan.message,
            )
            DOCUMENTS.labels(status="quarantined").inc()
            if (
                plan.error_category
                and "parse" in plan.error_category
                or plan.extraction_status == "failed"
            ):
                EXTRACTION_FAILURES.inc()
            return "quarantined"
        self._persist_version(plan, record, source_type, existing)
        try:
            self.indexing.embed_pending(
                plan.document_id, plan.version, force=force and plan.action == "reindex"
            )
            stats = self.indexing.flush(document_id=plan.document_id, raise_on_error=False)
        except Exception as exc:
            self._mark_document_status(
                plan,
                record,
                status="failed",
                extraction_status=plan.extraction_status,
                indexing_status="failed",
                error=str(exc)[:500],
                category=getattr(exc, "category", None) or "embedding",
                version=plan.version,
                chunk_count=len(plan.chunks),
            )
            self._write_outcome(
                job_id,
                plan.document_id,
                plan.logical_key,
                "failed",
                "embedding",
                str(exc)[:500],
                "embedding",
            )
            self._attempt(
                job_id, plan.document_id, "embedding", "failed", "embedding", str(exc)[:500]
            )
            DOCUMENTS.labels(status="failed").inc()
            return "failed"
        if stats["failed"]:
            self._mark_document_status(
                plan,
                record,
                status="failed",
                extraction_status=plan.extraction_status,
                indexing_status="failed",
                error="Vector indexing failed",
                category="vector_store",
                version=plan.version,
                chunk_count=len(plan.chunks),
            )
            self._write_outcome(
                job_id,
                plan.document_id,
                plan.logical_key,
                "failed",
                "indexing",
                "Vector indexing failed",
                "vector_store",
            )
            self._attempt(
                job_id,
                plan.document_id,
                "indexing",
                "failed",
                "vector_store",
                "Vector indexing failed",
            )
            DOCUMENTS.labels(status="failed").inc()
            return "failed"
        if plan.action == "new_version" and existing and existing.current_version > 0:
            with session_scope(self.context.settings) as session:
                previous = DocumentRepository(session).vector_ids_for_version(
                    plan.document_id, existing.current_version
                )
            self.indexing.enqueue_deletes(
                document_id=plan.document_id, version=existing.current_version, vector_ids=previous
            )
            self.indexing.flush(document_id=plan.document_id, raise_on_error=False)
        self._mark_document_status(
            plan,
            record,
            status="completed",
            extraction_status=plan.extraction_status,
            indexing_status="indexed",
            error=None,
            category=None,
            version=plan.version,
            chunk_count=len(plan.chunks),
            fingerprint=plan.fingerprint,
            content_hash=plan.content_hash,
        )
        self._write_outcome(
            job_id, plan.document_id, plan.logical_key, "processed", "indexing", None, None
        )
        self._attempt(job_id, plan.document_id, "indexing", "completed", None, None)
        DOCUMENTS.labels(status="processed").inc()
        return "processed"

    def _persist_version(
        self,
        plan: DocumentPlan,
        record: SourceRecord,
        source_type: str,
        existing: ExistingDocument | None,
    ) -> None:
        settings = self.context.settings
        with session_scope(settings) as session:
            repo = DocumentRepository(session)
            document = repo.get(plan.document_id)
            if document is None:
                raise NotFoundError("Document disappeared before version persistence")
            document.title = plan.title
            document.document_type = plan.document_type
            document.source_type = source_type
            document.source_uri = record.source_uri
            document.original_filename = record.filename
            document.custom_metadata = plan.custom_metadata
            document.source_created_at = plan.source_created_at
            document.source_modified_at = plan.source_modified_at
            document.byte_size = len(record.content)
            document.media_type = record.content_type
            document.raw_uri = record.source_uri
            document.metadata_schema_version = settings.metadata_schema_version
            document.extraction_status = plan.extraction_status
            document.processing_status = advance_status(
                document.processing_status, "indexing"
            ).value
            version = repo.get_version(plan.document_id, plan.version)
            if version is None:
                version = repo.add_version(
                    DocumentVersion(
                        document_id=plan.document_id,
                        version=plan.version,
                        content_hash=plan.content_hash or "",
                        fingerprint=plan.fingerprint or "",
                        title=plan.title,
                        normalized_text=plan.normalized_text,
                        parser_version=settings.parser_version,
                        chunker_version=settings.chunker_version,
                        embedding_model=self.context.embeddings.model,
                        embedding_version=self.context.embeddings.version,
                        metadata_schema_version=settings.metadata_schema_version,
                        source_payload=plan.payload,
                        warnings=plan.warnings,
                    )
                )
            existing_chunks = repo.list_chunks(plan.document_id, plan.version)
            replacement_ids = {str(chunk.chunk_id) for chunk in plan.chunks}
            stale_vectors = [
                chunk.vector_id
                for chunk in existing_chunks
                if chunk.vector_id not in replacement_ids
            ]
            if not _same_chunks(existing_chunks, plan):
                if existing_chunks:
                    repo.delete_version_chunks(plan.document_id, plan.version)
                repo.add_chunks(
                    [
                        DocumentChunk(
                            id=chunk.chunk_id,
                            document_id=plan.document_id,
                            version_id=version.id,
                            document_version=plan.version,
                            chunk_index=chunk.chunk_index,
                            chunk_text=chunk.text,
                            section_title=chunk.section_title,
                            page_number=chunk.page_number,
                            start_offset=chunk.start_offset,
                            end_offset=chunk.end_offset,
                            token_count=chunk.token_count,
                            chunk_hash=chunk.chunk_hash,
                            embedding_model=self.context.embeddings.model,
                            embedding_version=self.context.embeddings.version,
                            vector_id=str(chunk.chunk_id),
                        )
                        for chunk in plan.chunks
                    ]
                )
            document.current_version = plan.version
            document.content_hash = plan.content_hash
            document.processing_fingerprint = plan.fingerprint
            document.chunk_count = len(plan.chunks)
            document.indexing_status = "pending"
            document.last_processed_at = datetime.now(UTC)
        if stale_vectors:
            self.indexing.enqueue_deletes(
                document_id=plan.document_id,
                version=plan.version,
                vector_ids=stale_vectors,
            )

    def _mark_document_status(
        self,
        plan: DocumentPlan,
        record: SourceRecord,
        *,
        status: str,
        extraction_status: str | None = None,
        indexing_status: str | None = None,
        error: str | None = None,
        category: str | None = None,
        version: int | None = None,
        chunk_count: int | None = None,
        fingerprint: str | None = None,
        content_hash: str | None = None,
    ) -> None:
        with session_scope(self.context.settings) as session:
            document = session.get(Document, plan.document_id)
            if document is None:
                return
            document.processing_status = advance_status(document.processing_status, status).value
            document.title = plan.title or document.title
            document.document_type = plan.document_type or document.document_type
            document.source_uri = record.source_uri
            document.original_filename = record.filename
            if plan.custom_metadata:
                document.custom_metadata = plan.custom_metadata
            if extraction_status:
                document.extraction_status = extraction_status
            if indexing_status:
                document.indexing_status = indexing_status
            document.last_error = error
            document.error_category = category
            document.last_processed_at = datetime.now(UTC)
            if version is not None:
                document.current_version = version
            if chunk_count is not None:
                document.chunk_count = chunk_count
            if fingerprint is not None:
                document.processing_fingerprint = fingerprint
            if content_hash is not None:
                document.content_hash = content_hash
            if status == "failed":
                document.retry_count += 1

    def _persist_quality_and_drift(self, job_id: UUID, source_id: UUID, plan: DocumentPlan) -> None:
        with session_scope(self.context.settings) as session:
            quality = QualityRepository(session)
            drift = DriftRepository(session)
            rows = []
            for finding in plan.findings:
                if not finding.passed:
                    QUALITY_FAILURES.inc()
                rows.append(
                    DataQualityResult(
                        job_id=job_id,
                        document_id=plan.document_id,
                        source_id=source_id,
                        check_name=finding.check_name,
                        expected=finding.expected,
                        actual=finding.actual,
                        severity="error" if not finding.passed else finding.severity,
                        passed=finding.passed,
                        details=finding.details,
                    )
                )
            quality.add_many(rows)
            for finding in plan.drift:
                signature = drift_signature(
                    str(source_id),
                    finding.field_path,
                    finding.drift_type,
                    finding.expected,
                    finding.actual,
                )
                inserted = drift.add_if_new(
                    SchemaDriftEvent(
                        source_id=source_id,
                        job_id=job_id,
                        schema_name="active",
                        schema_version=self.context.settings.metadata_schema_version,
                        severity=finding.severity,
                        drift_type=finding.drift_type,
                        field_path=finding.field_path,
                        expected=finding.expected,
                        actual=finding.actual,
                        record_key=plan.logical_key,
                        signature=signature,
                        message=finding.message,
                        details={},
                    )
                )
                if inserted is not None:
                    from app.observability.metrics import DRIFT_EVENTS

                    DRIFT_EVENTS.labels(severity=finding.severity).inc()

    def _apply_deletions(self, job_id: UUID, source_id: UUID, seen: set[str]) -> None:
        with session_scope(self.context.settings) as session:
            repo = DocumentRepository(session)
            active = repo.active_logical_keys(source_id)
            missing = [
                (document_id, logical_key)
                for document_id, logical_key in active
                if logical_key not in seen
            ]
        for document_id, logical_key in missing:
            with session_scope(self.context.settings) as session:
                repo = DocumentRepository(session)
                document = repo.lock(document_id)
                if document is None or document.deleted_at is not None:
                    continue
                vector_ids = repo.vector_ids_for_version(document_id, document.current_version)
                repo.tombstone(document)
                version = document.current_version
            self.indexing.enqueue_deletes(
                document_id=document_id, version=version, vector_ids=vector_ids
            )
            self.indexing.flush(document_id=document_id, raise_on_error=False)
            self._write_outcome(
                job_id,
                document_id,
                logical_key,
                "processed",
                "indexing",
                "Source record was deleted",
                None,
            )
            DOCUMENTS.labels(status="deleted").inc()

    def _record_rejection(
        self,
        job_id,
        source_id,
        item: RejectedItem,
        *,
        outcome: str = "skipped",
        retryable: bool = False,
    ) -> None:
        document_id = None
        if outcome == "failed":
            document_id = document_uuid(source_id, item.logical_key)
        self._write_outcome(
            job_id,
            document_id,
            item.logical_key,
            outcome,
            "fetching",
            item.reason,
            item.category,
        )
        with session_scope(self.context.settings) as session:
            QualityRepository(session).add_many(
                [
                    DataQualityResult(
                        job_id=job_id,
                        document_id=document_id,
                        source_id=source_id,
                        check_name="source_record_accepted",
                        expected="supported source record",
                        actual=item.reason,
                        severity="error" if outcome == "failed" else "warning",
                        passed=False,
                        details={"category": item.category, "retryable": retryable},
                    )
                ]
            )
        DOCUMENTS.labels(status=outcome).inc()
        if not retryable:
            QUALITY_FAILURES.inc()

    def _write_outcome(
        self, job_id, document_id, logical_key, outcome, stage, message, category
    ) -> None:
        with session_scope(self.context.settings) as session:
            JobRepository(session).add_document_result(
                JobDocument(
                    job_id=job_id,
                    document_id=document_id,
                    logical_key=logical_key[:1024],
                    outcome=outcome,
                    stage=stage,
                    error_category=category,
                    error_message=None if not message else message[:2000],
                )
            )

    def _attempt(self, job_id, document_id, stage, status, category, message) -> None:
        with session_scope(self.context.settings) as session:
            JobRepository(session).add_attempt(
                ProcessingAttempt(
                    job_id=job_id,
                    document_id=document_id,
                    stage=stage,
                    status=status,
                    error_category=category,
                    error_message=None if not message else message[:2000],
                    completed_at=datetime.now(UTC),
                )
            )

    def _heartbeat(self, job_id: UUID) -> None:
        with session_scope(self.context.settings) as session:
            job = session.get(IngestionJob, job_id)
            if job is not None:
                JobRepository(session).touch(job)

    def _mark_running(self, job_id: UUID) -> None:
        with session_scope(self.context.settings) as session:
            job = session.get(IngestionJob, job_id)
            if job is None or job.status in {"completed", "completed_with_errors"}:
                return
            now = datetime.now(UTC)
            job.status = "running"
            job.stage = "fetching"
            job.started_at = job.started_at or now
            job.heartbeat_at = now

    def _finalize(self, job_id: UUID, *, note: str | None, failed: bool) -> None:
        with session_scope(self.context.settings) as session:
            job = session.get(IngestionJob, job_id)
            if job is None or job.status in {"completed", "completed_with_errors"}:
                return
            results = JobRepository(session).list_document_results(job_id)
            counts = {"processed": 0, "skipped": 0, "failed": 0, "quarantined": 0}
            messages = []
            for row in results:
                counts[row.outcome] = counts.get(row.outcome, 0) + 1
                if row.error_message:
                    messages.append(f"{row.logical_key}: {row.error_message}")
            job.total_documents = len(results)
            job.processed_documents = counts.get("processed", 0)
            job.skipped_documents = counts.get("skipped", 0)
            job.failed_documents = counts.get("failed", 0)
            job.quarantined_documents = counts.get("quarantined", 0)
            summary = "\n".join(messages[:20])[:4000]
            job.error_summary = summary or note
            if failed and not results:
                job.status = "failed"
            elif counts.get("failed") or counts.get("quarantined") or (failed and note):
                job.status = "completed_with_errors" if results else "failed"
            else:
                job.status = "completed"
            job.stage = job.status
            job.completed_at = datetime.now(UTC)
            job.heartbeat_at = job.completed_at

    def _existing_idempotent(self, idempotency_key: str | None, payload_hash: str) -> dict | None:
        if not idempotency_key:
            return None
        with session_scope(self.context.settings) as session:
            existing = JobRepository(session).find_by_idempotency(idempotency_key)
            if existing is None:
                return None
            if (existing.config_snapshot or {}).get("idempotency_payload") != payload_hash:
                raise ConflictError("Idempotency key was already used for a different request")
            return _job_dict(existing)

    def _create_job(
        self,
        *,
        source_id: UUID | None,
        trigger: str,
        force: bool,
        idempotency_key: str | None,
        config: dict,
        start_immediately: bool,
        retry_of_job_id: UUID | None,
        payload_hash: str,
        document_id: UUID | None = None,
    ) -> dict:
        config = {**config, "idempotency_payload": payload_hash}
        with session_scope(self.context.settings) as session:
            repo = JobRepository(session)
            if idempotency_key:
                existing = repo.find_by_idempotency(idempotency_key)
                if existing is not None:
                    if (existing.config_snapshot or {}).get("idempotency_payload") != payload_hash:
                        raise ConflictError(
                            "Idempotency key was already used for a different request"
                        )
                    payload = _job_dict(existing)
                    payload["document_id"] = document_id
                    return payload
            now = datetime.now(UTC)
            job = repo.add(
                IngestionJob(
                    source_id=source_id,
                    retry_of_job_id=retry_of_job_id,
                    trigger_type=trigger,
                    idempotency_key=idempotency_key,
                    status="running" if start_immediately else "pending",
                    stage="fetching" if start_immediately else "pending",
                    heartbeat_at=now if start_immediately else None,
                    config_snapshot=config,
                    force=force,
                )
            )
            payload = _job_dict(job)
            payload["document_id"] = document_id
            return payload

    def _ensure_upload_source(self) -> UUID:
        with session_scope(self.context.settings) as session:
            repo = SourceRepository(session)
            existing = repo.get_by_name(UPLOAD_SOURCE_NAME)
            if existing:
                return existing.id
            source = repo.add(
                Source(
                    name=UPLOAD_SOURCE_NAME,
                    source_type=SourceType.UPLOAD.value,
                    config={"detect_deletions": False},
                    status="active",
                )
            )
            return source.id


def _same_chunks(existing: list[DocumentChunk], plan: DocumentPlan) -> bool:
    if len(existing) != len(plan.chunks):
        return False
    for row, draft in zip(
        sorted(existing, key=lambda item: item.chunk_index), plan.chunks, strict=False
    ):
        if (
            row.chunk_index != draft.chunk_index
            or row.chunk_hash != draft.chunk_hash
            or row.id != draft.chunk_id
        ):
            return False
    return True


def _advisory_key(job_id: UUID) -> int:
    value = int.from_bytes(job_id.bytes[:8], byteorder="big", signed=False)
    if value >= 2**63:
        value -= 2**64
    return value


def _source_dict(source: Source) -> dict:
    return {
        "source_id": source.id,
        "name": source.name,
        "source_type": source.source_type,
        "status": source.status,
        "config": source.config,
        "created_at": source.created_at,
        "updated_at": source.updated_at,
    }


def _job_dict(job: IngestionJob) -> dict:
    return {
        "job_id": job.id,
        "source_id": job.source_id,
        "retry_of_job_id": job.retry_of_job_id,
        "trigger_type": job.trigger_type,
        "status": job.status,
        "stage": job.stage,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "total_documents": job.total_documents,
        "processed_documents": job.processed_documents,
        "skipped_documents": job.skipped_documents,
        "failed_documents": job.failed_documents,
        "quarantined_documents": job.quarantined_documents,
        "error_summary": job.error_summary,
        "retry_count": job.retry_count,
        "force": job.force,
        "created_at": job.created_at,
        "idempotency_key": job.idempotency_key,
    }
