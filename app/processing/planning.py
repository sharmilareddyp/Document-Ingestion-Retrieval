"""Pure planning for one source record.

This module does not touch PostgreSQL or a vector index. The ingestion service
persists the plan and decides how to resume after a failure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from app.core.config import Settings
from app.core.exceptions import ParseError
from app.domain.enums import DocumentType, ExtractionStatus
from app.domain.models import (
    CanonicalMetadata,
    ChunkDraft,
    DriftFinding,
    QualityFinding,
    SourceRecord,
)
from app.parsers.registry import SUPPORTED_TYPES, parser_for
from app.processing.chunking import assign_chunk_ids, chunk_blocks, normalized_document_text
from app.processing.hashing import document_uuid, processing_fingerprint, sha256_text
from app.processing.normalization import fallback_title, normalize_whitespace, sanitize_metadata
from app.processing.quality import (
    check_chunks,
    check_duplicate_state,
    check_required_metadata,
    check_supported_type,
    check_text,
    check_timestamp,
    failing,
)
from app.processing.schema_drift import detect_drift, has_breaking


@dataclass
class ExistingDocument:
    document_id: UUID
    current_version: int
    content_hash: str | None
    fingerprint: str | None
    indexing_status: str
    processing_status: str
    chunks_persisted: bool


@dataclass
class DocumentPlan:
    action: str
    document_id: UUID
    logical_key: str
    version: int
    title: str
    normalized_text: str = ""
    content_hash: str | None = None
    fingerprint: str | None = None
    chunks: list[ChunkDraft] = field(default_factory=list)
    findings: list[QualityFinding] = field(default_factory=list)
    drift: list[DriftFinding] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    extraction_status: str = ExtractionStatus.PENDING.value
    message: str | None = None
    error_category: str | None = None
    custom_metadata: dict[str, Any] = field(default_factory=dict)
    payload: dict[str, Any] | None = None
    document_type: str = DocumentType.TXT.value
    source_created_at: Any = None
    source_modified_at: Any = None
    duplicate_state: str = "new"


def decide_action(
    existing: ExistingDocument | None, *, fingerprint: str, force: bool
) -> tuple[str, int]:
    """Choose skip, reindex, or new_version for an already extracted document."""
    if existing is None or existing.current_version <= 0:
        return "new_version", 1
    unchanged = existing.fingerprint == fingerprint
    if unchanged and not force and existing.indexing_status == "indexed":
        return "skip", existing.current_version
    if unchanged:
        return "reindex", existing.current_version
    return "new_version", existing.current_version + 1


def build_plan(
    record: SourceRecord,
    *,
    source_id: UUID,
    source_type: str,
    settings: Settings,
    existing: ExistingDocument | None,
    contract: dict[str, Any] | None,
    schema_name: str | None,
    schema_version: str | None,
    force: bool,
    embedding_model: str,
    text_fields: list[str] | None = None,
    title_field: str | None = None,
) -> DocumentPlan:
    document_id = document_uuid(source_id, record.logical_key)
    plan = DocumentPlan(
        action="quarantine",
        document_id=document_id,
        logical_key=record.logical_key,
        version=1
        if existing is None or existing.current_version <= 0
        else existing.current_version,
        title=fallback_title(record.filename, record.logical_key),
        document_type=(record.document_type or DocumentType.TXT).value,
        source_created_at=record.source_created_at,
        source_modified_at=record.source_modified_at,
    )
    if record.prefetched_error:
        plan.message = record.prefetched_error
        plan.error_category = "malformed_record"
        plan.extraction_status = ExtractionStatus.FAILED.value
        plan.findings.append(
            QualityFinding(
                check_name="record_shape",
                expected="source record can be parsed",
                actual=record.prefetched_error,
                severity="error",
                passed=False,
            )
        )
        return plan

    document_type = record.document_type.value if record.document_type else None
    if document_type is None:
        plan.message = "Unsupported or unknown document type"
        plan.error_category = "unsupported"
        plan.action = "skip"
        plan.findings.append(check_supported_type("unknown", SUPPORTED_TYPES))
        return plan
    plan.document_type = document_type
    plan.findings.append(check_supported_type(document_type, SUPPORTED_TYPES))

    if contract is not None:
        payload = record.payload if record.payload is not None else _json_payload(record.content)
        plan.drift = detect_drift(payload, contract)
        if has_breaking(plan.drift):
            plan.payload = payload if isinstance(payload, dict) else None
            plan.message = "Breaking schema drift"
            plan.error_category = "schema_drift"
            plan.extraction_status = ExtractionStatus.FAILED.value
            plan.findings.append(
                QualityFinding(
                    check_name="schema_contract",
                    expected=f"payload matches {schema_name or 'contract'} {schema_version or ''}".strip(),
                    actual="breaking drift",
                    severity="error",
                    passed=False,
                    details={"count": len(plan.drift)},
                )
            )
            return plan
        plan.findings.append(
            QualityFinding(
                check_name="schema_contract",
                expected="payload matches the active contract",
                actual=f"{len(plan.drift)} non-breaking differences" if plan.drift else "match",
                severity="info",
                passed=True,
            )
        )

    try:
        parser = parser_for(document_type, text_fields=text_fields, title_field=title_field)
        if document_type == DocumentType.API_RECORD.value:
            parsed = parser.parse(
                record.content, filename=record.filename, payload=record.payload or {}
            )
        else:
            parsed = parser.parse(record.content, filename=record.filename, payload=record.payload)
    except ParseError as exc:
        plan.message = exc.message
        plan.error_category = exc.category
        plan.extraction_status = ExtractionStatus.FAILED.value
        plan.findings.append(
            QualityFinding(
                check_name="extraction",
                expected="text can be extracted",
                actual=exc.message,
                severity="error",
                passed=False,
            )
        )
        return plan
    except Exception as exc:
        plan.message = "Document extraction failed"
        plan.error_category = "parse_error"
        plan.extraction_status = ExtractionStatus.FAILED.value
        plan.findings.append(
            QualityFinding(
                check_name="extraction",
                expected="text can be extracted",
                actual=type(exc).__name__,
                severity="error",
                passed=False,
            )
        )
        return plan

    plan.warnings = list(parsed.warnings)
    plan.extraction_status = (
        ExtractionStatus.PARTIAL.value if parsed.warnings else ExtractionStatus.SUCCESS.value
    )
    blocks = []
    for block in parsed.blocks:
        text = normalize_whitespace(block.text)
        if text:
            copied = block.model_copy(update={"text": text})
            blocks.append(copied)
    plan.normalized_text = normalized_document_text(blocks)
    plan.title = fallback_title(parsed.title, plan.title)
    plan.content_hash = sha256_text(plan.normalized_text)
    plan.custom_metadata = sanitize_metadata({**record.metadata, **parsed.metadata})
    plan.payload = _bounded_payload(
        parsed.payload if parsed.payload is not None else record.payload
    )
    plan.source_created_at = parsed.source_created_at or record.source_created_at
    plan.source_modified_at = parsed.source_modified_at or record.source_modified_at
    plan.document_type = parsed.document_type.value

    plan.findings.extend(check_text(plan.normalized_text, settings))
    plan.findings.append(
        check_required_metadata(
            title=plan.title,
            source_uri=record.source_uri,
            document_type=plan.document_type,
            content_hash=plan.content_hash,
        )
    )
    if plan.source_modified_at is not None:
        plan.findings.append(
            check_timestamp(plan.source_modified_at.isoformat(), field_name="source_modified_at")
        )

    try:
        CanonicalMetadata(
            schema_version=settings.metadata_schema_version,
            document_id=document_id,
            source_id=source_id,
            source_type=source_type,
            source_uri=record.source_uri,
            original_filename=record.filename,
            document_type=DocumentType(plan.document_type),
            title=plan.title,
            content_hash=plan.content_hash,
            document_version=max(plan.version, 1),
            source_created_at=plan.source_created_at,
            source_modified_at=plan.source_modified_at,
            custom_metadata=plan.custom_metadata,
        )
    except ValidationError as exc:
        plan.message = "Canonical metadata failed validation"
        plan.error_category = "invalid_metadata"
        plan.findings.append(
            QualityFinding(
                check_name="canonical_metadata",
                expected="metadata matches schema 1.0.0",
                actual=str(exc.errors()[0]["msg"]) if exc.errors() else "invalid",
                severity="error",
                passed=False,
            )
        )
        return plan

    if failing(plan.findings):
        plan.message = failing(plan.findings)[0].check_name
        plan.error_category = "quality"
        return plan

    plan.fingerprint = processing_fingerprint(
        plan.content_hash,
        parser_version=settings.parser_version,
        chunker_version=settings.chunker_version,
        chunk_size=settings.chunk_size,
        chunk_overlap=settings.chunk_overlap,
        embedding_model=embedding_model,
        embedding_version=settings.embedding_version,
        schema_version=settings.metadata_schema_version,
    )
    action, version = decide_action(existing, fingerprint=plan.fingerprint, force=force)
    plan.version = version
    if existing is None or existing.content_hash is None:
        plan.duplicate_state = "new"
    elif existing.content_hash == plan.content_hash:
        plan.duplicate_state = "unchanged"
    else:
        plan.duplicate_state = "changed"
    plan.findings.append(check_duplicate_state(plan.duplicate_state))
    if action == "skip":
        plan.action = "skip"
        plan.message = "Document content and processing configuration are unchanged"
        return plan

    chunks = assign_chunk_ids(
        chunk_blocks(blocks, chunk_size=settings.chunk_size, chunk_overlap=settings.chunk_overlap),
        document_id=document_id,
        version=version,
    )
    plan.chunks = chunks
    plan.findings.extend(check_chunks(chunks, limit=settings.max_chunks_per_document))
    if failing(plan.findings):
        plan.action = "quarantine"
        plan.message = failing(plan.findings)[0].check_name
        plan.error_category = "quality"
        return plan
    plan.action = action
    return plan


def _json_payload(content: bytes) -> Any:
    import json

    try:
        return json.loads(content.decode("utf-8"))
    except Exception:
        return None


def _bounded_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    import json

    encoded = json.dumps(payload, default=str)
    if len(encoded) > 65536:
        return {"truncated": True, "sha256": sha256_text(encoded)}
    return payload
