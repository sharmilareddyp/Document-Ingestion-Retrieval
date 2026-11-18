"""Read models and document lifecycle operations that do not parse source files."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from app.core.exceptions import NotFoundError
from app.database.models import Document
from app.database.repositories import (
    DocumentRepository,
    JobRepository,
    OutboxRepository,
    SourceRepository,
)
from app.database.session import session_scope
from app.services.context import AppContext
from app.services.indexing_service import IndexingService


class DocumentService:
    def __init__(self, context: AppContext) -> None:
        self.context = context

    def list_documents(
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
    ) -> tuple[list[dict], int]:
        with session_scope(self.context.settings) as session:
            rows, total = DocumentRepository(session).list_filtered(
                source_id=source_id,
                document_type=document_type,
                processing_status=processing_status,
                title=title,
                date_from=date_from,
                date_to=date_to,
                include_deleted=include_deleted,
                limit=limit,
                offset=offset,
            )
            return [_document_dict(row) for row in rows], total

    def get_document(self, document_id: UUID) -> dict:
        with session_scope(self.context.settings) as session:
            document = DocumentRepository(session).get(document_id)
            if document is None:
                raise NotFoundError("Document not found")
            return _document_dict(document)

    def list_chunks(self, document_id: UUID, version: int | None = None) -> list[dict]:
        with session_scope(self.context.settings) as session:
            repo = DocumentRepository(session)
            document = repo.get(document_id)
            if document is None:
                raise NotFoundError("Document not found")
            selected = version or document.current_version
            return [_chunk_dict(chunk) for chunk in repo.list_chunks(document_id, selected)]

    def list_versions(self, document_id: UUID, *, include_text: bool = False) -> list[dict]:
        with session_scope(self.context.settings) as session:
            repo = DocumentRepository(session)
            if repo.get(document_id) is None:
                raise NotFoundError("Document not found")
            versions = []
            for version in repo.list_versions(document_id):
                item = {
                    "version": version.version,
                    "content_hash": version.content_hash,
                    "fingerprint": version.fingerprint,
                    "title": version.title,
                    "parser_version": version.parser_version,
                    "chunker_version": version.chunker_version,
                    "embedding_model": version.embedding_model,
                    "embedding_version": version.embedding_version,
                    "metadata_schema_version": version.metadata_schema_version,
                    "warnings": version.warnings,
                    "text_characters": len(version.normalized_text or ""),
                    "created_at": version.created_at,
                }
                if include_text:
                    item["normalized_text"] = version.normalized_text
                versions.append(item)
            return versions

    def lineage(self, document_id: UUID) -> dict:
        with session_scope(self.context.settings) as session:
            documents = DocumentRepository(session)
            document = documents.get(document_id)
            if document is None:
                raise NotFoundError("Document not found")
            source = SourceRepository(session).get(document.source_id)
            attempts = [
                {
                    "stage": attempt.stage,
                    "status": attempt.status,
                    "error_category": attempt.error_category,
                    "error_message": attempt.error_message,
                    "retry_count": attempt.retry_count,
                    "created_at": attempt.created_at,
                }
                for attempt in JobRepository(session).list_attempts(document_id)
            ]
            outbox = [
                {
                    "operation": row.operation,
                    "status": row.status,
                    "vector_id": row.vector_id,
                    "document_version": row.document_version,
                    "attempts": row.attempts,
                    "error_message": row.error_message,
                    "completed_at": row.completed_at,
                }
                for row in OutboxRepository(session).for_document(document_id)
            ]
            return {
                "document": _document_dict(document),
                "source": None
                if source is None
                else {
                    "source_id": source.id,
                    "name": source.name,
                    "source_type": source.source_type,
                    "config": {
                        key: value
                        for key, value in (source.config or {}).items()
                        if "token" not in key.lower()
                    },
                },
                "versions": [
                    {
                        "version": version.version,
                        "content_hash": version.content_hash,
                        "created_at": version.created_at,
                    }
                    for version in documents.list_versions(document_id)
                ],
                "attempts": attempts,
                "indexing_outbox": outbox,
                "raw_uri": document.raw_uri,
            }

    def delete_document(self, document_id: UUID) -> dict:
        vector_ids: list[tuple[int, str]] = []
        with session_scope(self.context.settings) as session:
            repo = DocumentRepository(session)
            document = repo.lock(document_id)
            if document is None:
                raise NotFoundError("Document not found")
            for version in repo.list_versions(document_id):
                for vector_id in repo.vector_ids_for_version(document_id, version.version):
                    vector_ids.append((version.version, vector_id))
            repo.tombstone(document)
        indexing = IndexingService(self.context)
        for version, vector_id in vector_ids:
            indexing.enqueue_deletes(
                document_id=document_id, version=version, vector_ids=[vector_id]
            )
        indexing.flush(document_id=document_id, raise_on_error=False)
        return self.get_document(document_id)


def _document_dict(document: Document) -> dict:
    return {
        "document_id": document.id,
        "source_id": document.source_id,
        "source_type": document.source_type,
        "source_uri": document.source_uri,
        "original_filename": document.original_filename,
        "document_type": document.document_type,
        "title": document.title,
        "content_hash": document.content_hash,
        "document_version": document.current_version,
        "source_created_at": document.source_created_at,
        "source_modified_at": document.source_modified_at,
        "ingested_at": document.ingested_at,
        "last_processed_at": document.last_processed_at,
        "processing_status": document.processing_status,
        "extraction_status": document.extraction_status,
        "indexing_status": document.indexing_status,
        "chunk_count": document.chunk_count,
        "metadata_schema_version": document.metadata_schema_version,
        "custom_metadata": document.custom_metadata,
        "raw_uri": document.raw_uri,
        "logical_key": document.logical_key,
        "last_error": document.last_error,
        "error_category": document.error_category,
        "retry_count": document.retry_count,
        "deleted_at": document.deleted_at,
    }


def _chunk_dict(chunk) -> dict:
    return {
        "chunk_id": chunk.id,
        "document_id": chunk.document_id,
        "document_version": chunk.document_version,
        "chunk_index": chunk.chunk_index,
        "chunk_text": chunk.chunk_text,
        "section_title": chunk.section_title,
        "page_number": chunk.page_number,
        "start_offset": chunk.start_offset,
        "end_offset": chunk.end_offset,
        "token_count": chunk.token_count,
        "chunk_hash": chunk.chunk_hash,
        "embedding_model": chunk.embedding_model,
        "embedding_version": chunk.embedding_version,
        "indexed_at": chunk.indexed_at,
        "vector_id": chunk.vector_id,
    }
