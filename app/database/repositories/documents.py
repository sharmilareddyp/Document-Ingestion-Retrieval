"""Document, version, and chunk persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import Select, delete, func, select
from sqlalchemy.orm import Session

from app.database.models import Document, DocumentChunk, DocumentVersion


class DocumentRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, document_id: UUID) -> Document | None:
        return self.session.get(Document, document_id)

    def get_by_logical(self, source_id: UUID, logical_key: str) -> Document | None:
        return self.session.scalar(
            select(Document).where(
                Document.source_id == source_id, Document.logical_key == logical_key
            )
        )

    def lock(self, document_id: UUID) -> Document | None:
        return self.session.scalar(
            select(Document).where(Document.id == document_id).with_for_update()
        )

    def add(self, document: Document) -> Document:
        self.session.add(document)
        self.session.flush()
        return document

    def add_version(self, version: DocumentVersion) -> DocumentVersion:
        self.session.add(version)
        self.session.flush()
        return version

    def add_chunks(self, chunks: list[DocumentChunk]) -> None:
        self.session.add_all(chunks)
        self.session.flush()

    def delete_version_chunks(self, document_id: UUID, version: int) -> None:
        self.session.execute(
            delete(DocumentChunk).where(
                DocumentChunk.document_id == document_id,
                DocumentChunk.document_version == version,
            )
        )
        self.session.flush()

    def vector_ids_for_version(self, document_id: UUID, version: int) -> list[str]:
        rows = self.session.scalars(
            select(DocumentChunk.vector_id).where(
                DocumentChunk.document_id == document_id,
                DocumentChunk.document_version == version,
            )
        ).all()
        return list(rows)

    def list_versions(self, document_id: UUID) -> list[DocumentVersion]:
        return list(
            self.session.scalars(
                select(DocumentVersion)
                .where(DocumentVersion.document_id == document_id)
                .order_by(DocumentVersion.version.asc())
            ).all()
        )

    def get_version(self, document_id: UUID, version: int) -> DocumentVersion | None:
        return self.session.scalar(
            select(DocumentVersion).where(
                DocumentVersion.document_id == document_id,
                DocumentVersion.version == version,
            )
        )

    def list_chunks(self, document_id: UUID, version: int | None = None) -> list[DocumentChunk]:
        stmt = select(DocumentChunk).where(DocumentChunk.document_id == document_id)
        if version is not None:
            stmt = stmt.where(DocumentChunk.document_version == version)
        stmt = stmt.order_by(DocumentChunk.chunk_index.asc())
        return list(self.session.scalars(stmt).all())

    def get_chunks_by_ids(self, chunk_ids: list[UUID]) -> list[DocumentChunk]:
        if not chunk_ids:
            return []
        return list(
            self.session.scalars(select(DocumentChunk).where(DocumentChunk.id.in_(chunk_ids))).all()
        )

    def ids_for_metadata(
        self,
        *,
        source_id: UUID | None = None,
        document_type: str | None = None,
        title: str | None = None,
        metadata_filters: dict | None = None,
        limit: int = 200,
    ) -> list[UUID]:
        stmt: Select[tuple[Document]] = select(Document.id).where(Document.deleted_at.is_(None))
        if source_id:
            stmt = stmt.where(Document.source_id == source_id)
        if document_type:
            stmt = stmt.where(Document.document_type == document_type)
        if title:
            escaped = title.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            stmt = stmt.where(Document.title.ilike(f"%{escaped}%", escape="\\"))
        for key, value in (metadata_filters or {}).items():
            stmt = stmt.where(Document.custom_metadata.contains({key: value}))
        stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt).all())

    def list_filtered(
        self,
        *,
        source_id: UUID | None = None,
        document_type: str | None = None,
        processing_status: str | None = None,
        title: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        include_deleted: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Document], int]:
        stmt = select(Document)
        if not include_deleted:
            stmt = stmt.where(Document.deleted_at.is_(None))
        if source_id:
            stmt = stmt.where(Document.source_id == source_id)
        if document_type:
            stmt = stmt.where(Document.document_type == document_type)
        if processing_status:
            stmt = stmt.where(Document.processing_status == processing_status)
        if title:
            escaped = title.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            stmt = stmt.where(Document.title.ilike(f"%{escaped}%", escape="\\"))
        if date_from:
            stmt = stmt.where(Document.ingested_at >= date_from)
        if date_to:
            stmt = stmt.where(Document.ingested_at <= date_to)
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = list(
            self.session.scalars(
                stmt.order_by(Document.ingested_at.desc()).limit(limit).offset(offset)
            ).all()
        )
        return rows, int(total)

    def active_logical_keys(self, source_id: UUID) -> list[tuple[UUID, str]]:
        rows = self.session.execute(
            select(Document.id, Document.logical_key).where(
                Document.source_id == source_id,
                Document.deleted_at.is_(None),
            )
        ).all()
        return [(row[0], row[1]) for row in rows]

    def tombstone(self, document: Document) -> None:
        document.deleted_at = datetime.now(UTC)
        document.processing_status = "tombstoned"
        document.indexing_status = "deleting"
        document.lock_job_id = None
        document.lock_acquired_at = None
        self.session.flush()

    def keyword_search(
        self,
        query: str,
        *,
        limit: int,
        source_id: UUID | None = None,
        document_type: str | None = None,
        document_ids: list[UUID] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[tuple[DocumentChunk, Document, float]]:
        from sqlalchemy import func

        tsquery = func.plainto_tsquery("english", query)
        tsv = func.to_tsvector("english", DocumentChunk.chunk_text)
        rank = func.ts_rank(tsv, tsquery)
        stmt = (
            select(DocumentChunk, Document, rank)
            .join(Document, Document.id == DocumentChunk.document_id)
            .where(Document.deleted_at.is_(None))
            .where(DocumentChunk.document_version == Document.current_version)
            .where(DocumentChunk.indexed_at.is_not(None))
            .where(tsv.op("@@")(tsquery))
        )
        if source_id:
            stmt = stmt.where(Document.source_id == source_id)
        if document_type:
            stmt = stmt.where(Document.document_type == document_type)
        if document_ids is not None:
            if not document_ids:
                return []
            stmt = stmt.where(Document.id.in_(document_ids))
        if date_from:
            stmt = stmt.where(Document.source_modified_at >= date_from)
        if date_to:
            stmt = stmt.where(Document.source_modified_at <= date_to)
        stmt = stmt.order_by(rank.desc(), DocumentChunk.id.asc()).limit(limit)
        rows = []
        for chunk, document, score in self.session.execute(stmt).all():
            rows.append((chunk, document, float(score or 0)))
        return rows

    def corpus(self, *, limit: int = 500) -> list[Document]:
        return list(
            self.session.scalars(
                select(Document)
                .where(Document.deleted_at.is_(None))
                .order_by(Document.ingested_at.desc())
                .limit(limit)
            ).all()
        )
