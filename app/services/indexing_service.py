"""Embed chunks and flush the vector outbox.

Embeddings are generated after the chunk rows commit. Vector payloads live in the
outbox until Pinecone (or the fake store) accepts them, then the payload is cleared.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from app.core.exceptions import PermanentProcessingError, TransientProcessingError
from app.core.logging import get_logger, log_extra
from app.database.models import Document, DocumentChunk, IndexingOutbox
from app.database.repositories import DocumentRepository, OutboxRepository
from app.database.session import session_scope
from app.observability.metrics import VECTOR_FAILURES
from app.processing.quality import check_embedding_dimensions
from app.services.context import AppContext

logger = get_logger(__name__)


class IndexingService:
    def __init__(self, context: AppContext) -> None:
        self.context = context

    def enqueue_deletes(self, *, document_id: UUID, version: int, vector_ids: list[str]) -> None:
        if not vector_ids:
            return
        with session_scope(self.context.settings) as session:
            outbox = OutboxRepository(session)
            for vector_id in vector_ids:
                outbox.enqueue(
                    document_id=document_id,
                    chunk_id=None,
                    document_version=version,
                    vector_id=vector_id,
                    operation="delete",
                    payload=None,
                )

    def embed_pending(self, document_id: UUID, version: int, *, force: bool = False) -> int:
        """Create upsert outbox rows for chunks that are not indexed yet."""
        with session_scope(self.context.settings) as session:
            document = session.get(Document, document_id)
            if document is None:
                return 0
            chunks = DocumentRepository(session).list_chunks(document_id, version)
            if not force:
                chunks = [chunk for chunk in chunks if chunk.indexed_at is None]
            pending = []
            for chunk in chunks:
                pending.append(
                    {
                        "chunk_id": chunk.id,
                        "vector_id": chunk.vector_id,
                        "text": chunk.chunk_text,
                        "metadata": _metadata(document, chunk),
                    }
                )
        if not pending:
            return 0
        vectors = self.context.embeddings.embed_texts([item["text"] for item in pending])
        dimension_check = check_embedding_dimensions(vectors, self.context.embeddings.dimension)
        if not dimension_check.passed:
            raise PermanentProcessingError("Embedding dimension mismatch", category="embedding")
        with session_scope(self.context.settings) as session:
            outbox = OutboxRepository(session)
            for item, vector in zip(pending, vectors, strict=True):
                outbox.enqueue(
                    document_id=document_id,
                    chunk_id=item["chunk_id"],
                    document_version=version,
                    vector_id=item["vector_id"],
                    operation="upsert",
                    payload={"values": vector, "metadata": item["metadata"]},
                )
        return len(pending)

    def flush(
        self,
        *,
        document_id: UUID | None = None,
        limit: int = 200,
        raise_on_error: bool = False,
    ) -> dict[str, int]:
        with session_scope(self.context.settings) as session:
            rows = OutboxRepository(session).list_actionable(limit)
            if document_id is not None:
                rows = [row for row in rows if row.document_id == document_id]
            snapshot = [
                {
                    "id": row.id,
                    "operation": row.operation,
                    "vector_id": row.vector_id,
                    "payload": row.payload,
                    "chunk_id": row.chunk_id,
                    "document_id": row.document_id,
                }
                for row in rows
            ]
        upserted = 0
        deleted = 0
        failed = 0
        deletes = [row for row in snapshot if row["operation"] == "delete"]
        upserts = [row for row in snapshot if row["operation"] == "upsert"]
        if deletes:
            try:
                self.context.vector_store.delete(ids=[row["vector_id"] for row in deletes])
            except Exception as exc:
                failed += len(deletes)
                VECTOR_FAILURES.inc(len(deletes))
                self._mark(deletes, error=str(exc)[:500])
                logger.warning(
                    "vector delete failed",
                    extra=log_extra(operation="vector_delete", error_category="vector_store"),
                )
                if raise_on_error:
                    raise
            else:
                self._mark(deletes, error=None)
                deleted += len(deletes)
        batch_size = self.context.settings.vector_upsert_batch_size
        for start in range(0, len(upserts), batch_size):
            batch = upserts[start : start + batch_size]
            try:
                prepared = [self._prepare_upsert(item) for item in batch]
                self.context.vector_store.upsert(prepared)
            except Exception as exc:
                failed += len(batch)
                VECTOR_FAILURES.inc(len(batch))
                self._mark(batch, error=str(exc)[:500])
                logger.warning(
                    "vector upsert failed",
                    extra=log_extra(operation="vector_upsert", error_category="vector_store"),
                )
                if raise_on_error:
                    raise TransientProcessingError(
                        "Vector upsert failed", category="vector_store"
                    ) from exc
                continue
            self._mark(batch, error=None)
            upserted += len(batch)
        touched = {row["document_id"] for row in snapshot}
        for touched_id in touched:
            self.refresh_document(touched_id)
        return {"upserted": upserted, "deleted": deleted, "failed": failed}

    def refresh_document(self, document_id: UUID) -> None:
        with session_scope(self.context.settings) as session:
            document = session.get(Document, document_id)
            if document is None or document.deleted_at is not None:
                return
            pending = session.scalar(
                select(IndexingOutbox.id)
                .where(
                    IndexingOutbox.document_id == document_id,
                    IndexingOutbox.status.in_(("pending", "failed")),
                )
                .limit(1)
            )
            chunks = DocumentRepository(session).list_chunks(document_id, document.current_version)
            fully_indexed = bool(chunks) and all(chunk.indexed_at is not None for chunk in chunks)
            if fully_indexed and pending is None:
                document.indexing_status = "indexed"
                if document.processing_status not in {"tombstoned", "quarantined"}:
                    document.processing_status = "completed"
                document.last_error = None
                document.error_category = None
            elif (
                document.deleted_at is None
                and document.indexing_status == "deleting"
                and pending is None
            ):
                document.indexing_status = "deleted"

    def _prepare_upsert(self, item: dict) -> dict:
        payload = item["payload"] or {}
        values = payload.get("values")
        metadata = payload.get("metadata") or {}
        if not values:
            text = self._chunk_text(item["chunk_id"])
            if text is None:
                raise TransientProcessingError(
                    "Chunk text is unavailable for re-embedding", category="vector_store"
                )
            values = self.context.embeddings.embed_texts([text])[0]
            metadata = metadata or self._metadata_for_chunk(item["chunk_id"])
        return {"id": item["vector_id"], "values": values, "metadata": metadata}

    def _chunk_text(self, chunk_id: UUID | None) -> str | None:
        if chunk_id is None:
            return None
        with session_scope(self.context.settings) as session:
            chunk = session.get(DocumentChunk, chunk_id)
            return None if chunk is None else chunk.chunk_text

    def _metadata_for_chunk(self, chunk_id: UUID | None) -> dict:
        if chunk_id is None:
            return {}
        with session_scope(self.context.settings) as session:
            chunk = session.get(DocumentChunk, chunk_id)
            if chunk is None:
                return {}
            document = session.get(Document, chunk.document_id)
            if document is None:
                return {}
            return _metadata(document, chunk)

    def _mark(self, rows: list[dict], *, error: str | None) -> None:
        now = datetime.now(UTC)
        with session_scope(self.context.settings) as session:
            outbox = OutboxRepository(session)
            for item in rows:
                stored = session.get(IndexingOutbox, item["id"])
                if stored is None:
                    continue
                if error:
                    outbox.mark_failed(stored, error)
                    continue
                outbox.mark_completed(stored)
                if item["chunk_id"] is not None and item["operation"] == "upsert":
                    chunk = session.get(DocumentChunk, item["chunk_id"])
                    if chunk is not None:
                        chunk.indexed_at = now
                        chunk.embedding_model = self.context.embeddings.model
                        chunk.embedding_version = self.context.embeddings.version


def _metadata(document: Document, chunk: DocumentChunk) -> dict:
    metadata = {
        "document_id": str(document.id),
        "chunk_id": str(chunk.id),
        "source_id": str(document.source_id),
        "source_type": document.source_type,
        "document_type": document.document_type,
        "document_version": chunk.document_version,
        "chunk_index": chunk.chunk_index,
        "title": document.title[:200],
    }
    if document.source_modified_at is not None:
        metadata["source_modified_ts"] = document.source_modified_at.timestamp()
    return metadata
