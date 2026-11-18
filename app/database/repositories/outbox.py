"""Vector indexing outbox.

PostgreSQL and Pinecone cannot commit together. Rows in this table are the durable
intent to upsert or delete a vector. Reconciliation retries rows that are still
pending or failed. Successful payloads are cleared so embedding values are not kept
after the index accepts them.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.database.models import IndexingOutbox


class OutboxRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def enqueue(
        self,
        *,
        document_id: UUID,
        chunk_id: UUID | None,
        document_version: int,
        vector_id: str,
        operation: str,
        payload: dict | None,
    ) -> IndexingOutbox:
        existing = self.session.scalar(
            select(IndexingOutbox).where(
                IndexingOutbox.vector_id == vector_id,
                IndexingOutbox.operation == operation,
                IndexingOutbox.document_version == document_version,
            )
        )
        if existing:
            existing.document_id = document_id
            existing.chunk_id = chunk_id
            existing.status = "pending"
            existing.payload = payload
            existing.error_message = None
            existing.completed_at = None
            self.session.flush()
            return existing
        row = IndexingOutbox(
            document_id=document_id,
            chunk_id=chunk_id,
            document_version=document_version,
            vector_id=vector_id,
            operation=operation,
            status="pending",
            payload=payload,
        )
        self.session.add(row)
        self.session.flush()
        return row

    def list_actionable(self, limit: int = 100) -> list[IndexingOutbox]:
        return list(
            self.session.scalars(
                select(IndexingOutbox)
                .where(IndexingOutbox.status.in_(("pending", "failed")))
                .order_by(IndexingOutbox.created_at.asc())
                .limit(limit)
            ).all()
        )

    def pending_count(self) -> int:
        return int(
            self.session.scalar(
                select(func.count())
                .select_from(IndexingOutbox)
                .where(IndexingOutbox.status.in_(("pending", "failed")))
            )
            or 0
        )

    def mark_completed(self, row: IndexingOutbox) -> None:
        row.status = "completed"
        row.payload = None
        row.error_message = None
        row.completed_at = datetime.now(UTC)
        row.updated_at = datetime.now(UTC)
        self.session.flush()

    def mark_failed(self, row: IndexingOutbox, message: str) -> None:
        row.status = "failed"
        row.attempts += 1
        row.error_message = message[:2000]
        row.updated_at = datetime.now(UTC)
        self.session.flush()

    def for_document(self, document_id: UUID) -> list[IndexingOutbox]:
        return list(
            self.session.scalars(
                select(IndexingOutbox)
                .where(IndexingOutbox.document_id == document_id)
                .order_by(IndexingOutbox.created_at.asc())
            ).all()
        )
