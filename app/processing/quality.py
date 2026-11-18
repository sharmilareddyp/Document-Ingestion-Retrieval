"""Pure data-quality checks.

Each check records the expected condition and the observed result. Callers decide
whether an error quarantines the document. Warnings do not stop indexing.
"""

from __future__ import annotations

from app.core.config import Settings
from app.domain.models import ChunkDraft, QualityFinding
from app.processing.normalization import is_timestamp


def _finding(
    name: str,
    *,
    expected: str,
    actual: str,
    passed: bool,
    severity: str,
    details: dict | None = None,
) -> QualityFinding:
    return QualityFinding(
        check_name=name,
        expected=expected,
        actual=actual,
        passed=passed,
        severity=severity if not passed else "info",
        details=details or {},
    )


def check_required_metadata(
    *, title: str, source_uri: str, document_type: str, content_hash: str | None
) -> QualityFinding:
    missing = [
        name
        for name, value in (
            ("title", title),
            ("source_uri", source_uri),
            ("document_type", document_type),
            ("content_hash", content_hash),
        )
        if not value
    ]
    return _finding(
        "required_metadata",
        expected="title, source_uri, document_type, and content_hash are present",
        actual="missing " + ", ".join(missing) if missing else "complete",
        passed=not missing,
        severity="error",
        details={"missing": missing},
    )


def check_text(text: str, settings: Settings) -> list[QualityFinding]:
    length = len(text.strip())
    findings = [
        _finding(
            "document_text_non_empty",
            expected=f"at least {settings.quality_min_chars} characters",
            actual=f"{length} characters",
            passed=length >= settings.quality_min_chars,
            severity="error",
        )
    ]
    too_long = length > settings.quality_max_chars
    findings.append(
        _finding(
            "document_length_bounds",
            expected=f"length <= {settings.quality_max_chars}",
            actual=f"{length} characters",
            passed=not too_long,
            severity="error",
        )
    )
    return findings


def check_supported_type(document_type: str, supported: set[str]) -> QualityFinding:
    return _finding(
        "supported_content_type",
        expected="one of " + ", ".join(sorted(supported)),
        actual=document_type,
        passed=document_type in supported,
        severity="error",
    )


def check_timestamp(value: str | None, *, field_name: str) -> QualityFinding:
    if value is None:
        return _finding(
            "valid_timestamps",
            expected=f"{field_name} is absent or a supported timestamp",
            actual="absent",
            passed=True,
            severity="warning",
        )
    valid = is_timestamp(value)
    return _finding(
        "valid_timestamps",
        expected=f"{field_name} is an ISO-8601, date, or unix timestamp",
        actual="valid" if valid else "invalid",
        passed=valid,
        severity="warning",
    )


def check_duplicate_state(state: str) -> QualityFinding:
    return _finding(
        "duplicate_document",
        expected="new, unchanged, or changed content is classified",
        actual=state,
        passed=True,
        severity="info",
        details={"state": state},
    )


def check_chunks(chunks: list[ChunkDraft], *, limit: int) -> list[QualityFinding]:
    empty = [chunk.chunk_index for chunk in chunks if not chunk.text.strip()]
    findings = [
        _finding(
            "chunk_text_non_empty",
            expected="every chunk contains text",
            actual=f"empty indexes {empty}" if empty else f"{len(chunks)} non-empty chunks",
            passed=not empty and len(chunks) > 0,
            severity="error",
        ),
        _finding(
            "chunk_count_consistency",
            expected=f"1..{limit} chunks",
            actual=str(len(chunks)),
            passed=0 < len(chunks) <= limit,
            severity="error",
        ),
    ]
    return findings


def check_embedding_dimensions(vectors: list[list[float]], expected: int) -> QualityFinding:
    bad = [index for index, vector in enumerate(vectors) if len(vector) != expected]
    return _finding(
        "embedding_dimensions",
        expected=f"dimension {expected}",
        actual="mismatch at " + ", ".join(map(str, bad)) if bad else f"{len(vectors)} vectors",
        passed=not bad and len(vectors) > 0,
        severity="error",
    )


def failing(findings: list[QualityFinding]) -> list[QualityFinding]:
    return [finding for finding in findings if not finding.passed and finding.severity == "error"]
