"""Corpus quality audits and schema probes that do not run the full indexer."""

from __future__ import annotations

from uuid import UUID

from app.connectors.factory import build_connector
from app.database.models import DataQualityResult
from app.database.repositories import (
    DocumentRepository,
    DriftRepository,
    QualityRepository,
    SchemaContractRepository,
    SourceRepository,
)
from app.database.session import session_scope
from app.domain.models import QualityFinding
from app.processing.schema_drift import detect_drift, has_breaking
from app.services.context import AppContext


class QualityService:
    def __init__(self, context: AppContext) -> None:
        self.context = context

    def list_results(
        self,
        *,
        job_id: UUID | None = None,
        document_id: UUID | None = None,
        source_id: UUID | None = None,
        severity: str | None = None,
        passed: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        with session_scope(self.context.settings) as session:
            repo = QualityRepository(session)
            rows, total = repo.list(
                job_id=job_id,
                document_id=document_id,
                source_id=source_id,
                severity=severity,
                passed=passed,
                limit=limit,
                offset=offset,
            )
            summary = repo.summary(job_id=job_id, source_id=source_id)
            return {
                "items": [_quality_dict(row) for row in rows],
                "total": total,
                "summary": summary,
                "limit": limit,
                "offset": offset,
            }

    def audit(self, *, limit: int = 200) -> dict:
        findings: list[QualityFinding] = []
        with session_scope(self.context.settings) as session:
            documents = DocumentRepository(session).corpus(limit=limit)
            for document in documents:
                chunks = DocumentRepository(session).list_chunks(
                    document.id, document.current_version
                )
                count_ok = document.chunk_count == len(chunks)
                findings.append(
                    QualityFinding(
                        check_name="chunk_count_consistency",
                        expected="chunk_count equals stored chunks for the current version",
                        actual=f"declared {document.chunk_count}, stored {len(chunks)}",
                        severity="error" if not count_ok else "info",
                        passed=count_ok,
                        document_id=document.id,
                        source_id=document.source_id,
                    )
                )
                empty = [
                    chunk.chunk_index for chunk in chunks if not (chunk.chunk_text or "").strip()
                ]
                findings.append(
                    QualityFinding(
                        check_name="chunk_text_non_empty",
                        expected="current chunks contain text",
                        actual="empty" if empty else "non-empty",
                        severity="error" if empty else "info",
                        passed=not empty,
                        document_id=document.id,
                        source_id=document.source_id,
                    )
                )
                if document.indexing_status == "indexed":
                    missing = [chunk.chunk_index for chunk in chunks if chunk.indexed_at is None]
                    findings.append(
                        QualityFinding(
                            check_name="indexing_completeness",
                            expected="every current chunk is indexed",
                            actual=f"missing {missing}" if missing else "complete",
                            severity="error" if missing else "info",
                            passed=not missing,
                            document_id=document.id,
                            source_id=document.source_id,
                        )
                    )
                source_ok = document.source_id is not None
                findings.append(
                    QualityFinding(
                        check_name="source_to_document_consistency",
                        expected="document references its source",
                        actual=str(document.source_id),
                        severity="info" if source_ok else "error",
                        passed=source_ok,
                        document_id=document.id,
                        source_id=document.source_id,
                    )
                )
        with session_scope(self.context.settings) as session:
            QualityRepository(session).add_many(
                [
                    DataQualityResult(
                        document_id=finding.document_id,
                        source_id=finding.source_id,
                        check_name=finding.check_name,
                        expected=finding.expected,
                        actual=finding.actual,
                        severity="error" if not finding.passed else "info",
                        passed=finding.passed,
                        details=finding.details,
                    )
                    for finding in findings
                ]
            )
        failed = len([finding for finding in findings if not finding.passed])
        return {"checked": len(findings), "failed": failed}

    def list_drift(
        self, *, source_id: UUID | None, severity: str | None, limit: int, offset: int
    ) -> dict:
        with session_scope(self.context.settings) as session:
            rows, total = DriftRepository(session).list(
                source_id=source_id, severity=severity, limit=limit, offset=offset
            )
            return {
                "items": [
                    {
                        "id": row.id,
                        "source_id": row.source_id,
                        "job_id": row.job_id,
                        "schema_name": row.schema_name,
                        "schema_version": row.schema_version,
                        "severity": row.severity,
                        "drift_type": row.drift_type,
                        "field_path": row.field_path,
                        "expected": row.expected,
                        "actual": row.actual,
                        "record_key": row.record_key,
                        "message": row.message,
                        "created_at": row.created_at,
                    }
                    for row in rows
                ],
                "total": total,
                "limit": limit,
                "offset": offset,
            }

    def probe_contracts(self) -> dict:
        """Fetch a single page from each REST source and record drift without indexing."""
        summary = {"sources": 0, "breaking": 0, "events": 0}
        with session_scope(self.context.settings) as session:
            sources = SourceRepository(session).list_active(source_type="rest_api")
            paired = []
            for source in sources:
                contract = SchemaContractRepository(session).get_active_for_source(source.id)
                if contract is None:
                    continue
                paired.append(
                    (
                        source.id,
                        dict(source.config or {}),
                        contract.contract,
                        contract.name,
                        contract.version,
                    )
                )
        for source_id, config, contract, name, version in paired:
            summary["sources"] += 1
            probe_config = dict(config)
            pagination = dict(probe_config.get("pagination") or {})
            pagination["max_pages"] = 1
            probe_config["pagination"] = pagination
            connector = build_connector(
                source_id=source_id,
                source_type="rest_api",
                config=probe_config,
                settings=self.context.settings,
            )
            breaking = False
            try:
                records = list(connector.iter_records())
            except Exception as exc:
                summary["events"] += 1
                breaking = True
                self._record_probe_error(source_id, name, version, str(exc)[:500])
                summary["breaking"] += 1
                continue
            for record in records:
                if record.payload is None:
                    continue
                findings = detect_drift(record.payload, contract)
                if has_breaking(findings):
                    breaking = True
                # Reuse the ingestion persistence path by building a quarantining plan only when needed.
                # Events are written through a direct repository call below.
                self._record_findings(source_id, name, version, record.logical_key, findings)
                summary["events"] += len(findings)
            if breaking:
                summary["breaking"] += 1
        return summary

    def _record_findings(self, source_id, name, version, logical_key, findings) -> None:
        from app.database.models import SchemaDriftEvent
        from app.processing.hashing import drift_signature

        with session_scope(self.context.settings) as session:
            repo = DriftRepository(session)
            for finding in findings:
                repo.add_if_new(
                    SchemaDriftEvent(
                        source_id=source_id,
                        schema_name=name,
                        schema_version=version,
                        severity=finding.severity,
                        drift_type=finding.drift_type,
                        field_path=finding.field_path,
                        expected=finding.expected,
                        actual=finding.actual,
                        record_key=logical_key,
                        signature=drift_signature(
                            str(source_id),
                            finding.field_path,
                            finding.drift_type,
                            finding.expected,
                            finding.actual,
                        ),
                        message=finding.message,
                        details={"probe": True},
                    )
                )

    def _record_probe_error(self, source_id, name, version, message: str) -> None:
        from app.database.models import SchemaDriftEvent
        from app.processing.hashing import drift_signature

        with session_scope(self.context.settings) as session:
            DriftRepository(session).add_if_new(
                SchemaDriftEvent(
                    source_id=source_id,
                    schema_name=name,
                    schema_version=version,
                    severity="breaking",
                    drift_type="probe_failed",
                    field_path="$",
                    expected="source responds with the contracted schema",
                    actual=message,
                    signature=drift_signature(str(source_id), "probe", message),
                    message=message,
                    details={"probe": True},
                )
            )


def _quality_dict(row: DataQualityResult) -> dict:
    return {
        "id": row.id,
        "job_id": row.job_id,
        "document_id": row.document_id,
        "source_id": row.source_id,
        "check_name": row.check_name,
        "expected": row.expected,
        "actual": row.actual,
        "severity": row.severity,
        "passed": row.passed,
        "details": row.details or {},
        "created_at": row.created_at,
    }
