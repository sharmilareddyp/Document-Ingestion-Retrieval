"""Canonical in-memory models shared by connectors, the pipeline, and the API.

These are not SQLAlchemy rows. Persistence models live in `app.database.models`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.domain.enums import DocumentType, ExtractionStatus, IndexingStatus, ProcessingStatus


class DiscoveredItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    logical_key: str
    source_uri: str
    filename: str | None = None
    content_type: str | None = None
    size_bytes: int | None = None
    modified_at: datetime | None = None
    locator: str


class RejectedItem(BaseModel):
    source_uri: str
    filename: str | None = None
    logical_key: str
    reason: str
    category: str


class SourceRecord(BaseModel):
    """One fetched source object. Bytes stay here until a parser reads them."""

    logical_key: str
    source_uri: str
    filename: str | None = None
    content_type: str
    content: bytes = Field(default=b"", repr=False)
    metadata: dict[str, Any] = Field(default_factory=dict)
    source_created_at: datetime | None = None
    source_modified_at: datetime | None = None
    payload: dict[str, Any] | None = None
    document_type: DocumentType | None = None
    prefetched_error: str | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ContentBlock(BaseModel):
    text: str
    block_type: str
    page_number: int | None = None
    section_title: str | None = None
    level: int | None = None
    start_offset: int | None = None
    end_offset: int | None = None


class ParsedDocument(BaseModel):
    document_type: DocumentType
    title: str | None
    blocks: list[ContentBlock]
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    source_created_at: datetime | None = None
    source_modified_at: datetime | None = None
    payload: dict[str, Any] | None = None


class ChunkDraft(BaseModel):
    chunk_index: int
    text: str
    section_title: str | None = None
    page_number: int | None = None
    start_offset: int | None = None
    end_offset: int | None = None
    token_count: int
    chunk_hash: str
    chunk_id: UUID | None = None


class CanonicalMetadata(BaseModel):
    """Version 1.0.0 document metadata contract validated before persistence."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0.0"
    document_id: UUID
    source_id: UUID
    source_type: str
    source_uri: str
    original_filename: str | None = None
    document_type: DocumentType
    title: str = Field(min_length=1, max_length=500)
    content_hash: str = Field(min_length=64, max_length=64)
    document_version: int = Field(ge=1)
    source_created_at: datetime | None = None
    source_modified_at: datetime | None = None
    custom_metadata: dict[str, Any] = Field(default_factory=dict)


class QualityFinding(BaseModel):
    check_name: str
    expected: str
    actual: str
    severity: str
    passed: bool
    details: dict[str, Any] = Field(default_factory=dict)
    document_id: UUID | None = None
    source_id: UUID | None = None


class DriftFinding(BaseModel):
    severity: str
    drift_type: str
    field_path: str
    expected: str
    actual: str
    message: str


class PipelineOutcome(BaseModel):
    logical_key: str
    document_id: UUID | None = None
    outcome: str
    processing_status: ProcessingStatus
    extraction_status: ExtractionStatus = ExtractionStatus.PENDING
    indexing_status: IndexingStatus = IndexingStatus.NOT_INDEXED
    version: int | None = None
    chunk_count: int = 0
    message: str | None = None
    error_category: str | None = None
    retryable: bool = False


class SearchHit(BaseModel):
    chunk_id: UUID
    document_id: UUID
    document_title: str
    source_type: str
    source_uri: str
    document_version: int
    section_title: str | None = None
    page_number: int | None = None
    chunk_text: str
    similarity_score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    source_id: UUID | None = None
    document_type: str | None = None
