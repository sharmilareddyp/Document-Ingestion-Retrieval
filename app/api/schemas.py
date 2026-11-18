"""Pydantic request and response models for the public API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class ErrorBody(BaseModel):
    error: str
    message: str
    request_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class Page(BaseModel):
    items: list[Any]
    total: int
    limit: int
    offset: int


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


class JobAccepted(BaseModel):
    job_id: UUID
    status: str
    document_id: UUID | None = None


class SourceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    source_type: str
    config: dict[str, Any]
    schema_contract: dict[str, Any] | None = None
    contract_name: str | None = None


class SourceResponse(BaseModel):
    source_id: UUID
    name: str
    source_type: str
    status: str
    config: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DocumentResponse(BaseModel):
    document_id: UUID
    source_id: UUID
    source_type: str
    source_uri: str
    original_filename: str | None = None
    document_type: str
    title: str
    content_hash: str | None = None
    document_version: int
    source_created_at: datetime | None = None
    source_modified_at: datetime | None = None
    ingested_at: datetime | None = None
    last_processed_at: datetime | None = None
    processing_status: str
    extraction_status: str
    indexing_status: str
    chunk_count: int
    metadata_schema_version: str
    custom_metadata: dict[str, Any] = Field(default_factory=dict)
    raw_uri: str | None = None
    logical_key: str | None = None
    last_error: str | None = None
    error_category: str | None = None
    retry_count: int = 0
    deleted_at: datetime | None = None


class ChunkResponse(BaseModel):
    chunk_id: UUID
    document_id: UUID
    document_version: int
    chunk_index: int
    chunk_text: str
    section_title: str | None = None
    page_number: int | None = None
    start_offset: int | None = None
    end_offset: int | None = None
    token_count: int
    chunk_hash: str
    embedding_model: str | None = None
    embedding_version: str | None = None
    indexed_at: datetime | None = None
    vector_id: str


class JobResponse(BaseModel):
    job_id: UUID
    source_id: UUID | None = None
    retry_of_job_id: UUID | None = None
    trigger_type: str
    status: str
    stage: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    total_documents: int
    processed_documents: int
    skipped_documents: int
    failed_documents: int
    quarantined_documents: int
    error_summary: str | None = None
    retry_count: int
    force: bool
    created_at: datetime | None = None
    idempotency_key: str | None = None


class RetryRequest(BaseModel):
    include_quarantined: bool = False


class SearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2000)
    top_k: int = Field(default=5, ge=1, le=50)
    offset: int = Field(default=0, ge=0)
    mode: str = "semantic"
    source_id: UUID | None = None
    source_type: str | None = None
    document_type: str | None = None
    metadata_filters: dict[str, Any] | None = None
    title: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    dedupe_by_document: bool = False


class SearchHitResponse(BaseModel):
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


class SearchResponse(BaseModel):
    items: list[SearchHitResponse]
    limit: int
    offset: int
    mode: str
    score_type: str
    """score_type is cosine, ts_rank, or rrf. Those scales are not interchangeable."""


class QualityResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    summary: dict[str, int]
    limit: int
    offset: int


class DriftResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int
