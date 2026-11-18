"""Text and timestamp normalization.

Normalization is deterministic: the same extracted blocks always produce the same
canonical text, which keeps content hashes stable across re-ingestion.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

_WS = re.compile(r"[ \t\f\v]+")
_NEWLINES = re.compile(r"\n{3,}")
_METADATA_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


def normalize_whitespace(text: str) -> str:
    cleaned = text.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
    cleaned = _WS.sub(" ", cleaned)
    lines = [line.strip() for line in cleaned.split("\n")]
    cleaned = "\n".join(lines)
    cleaned = _NEWLINES.sub("\n\n", cleaned)
    return cleaned.strip()


def parse_timestamp(value: Any) -> datetime:
    """Parse an ISO-8601 string, a date, or a unix timestamp into UTC."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, bool):
        raise ValueError("boolean is not a timestamp")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if not isinstance(value, str):
        raise ValueError("unsupported timestamp type")
    text = value.strip()
    if not text:
        raise ValueError("empty timestamp")
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=UTC)
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("unsupported timestamp format") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def is_timestamp(value: Any) -> bool:
    try:
        parse_timestamp(value)
    except (ValueError, OverflowError, OSError):
        return False
    return True


def sanitize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Keep a bounded JSON object of scalar metadata. Drop everything else."""
    clean: dict[str, Any] = {}
    if not metadata:
        return clean
    for key, value in list(metadata.items())[:50]:
        if not isinstance(key, str) or not _METADATA_KEY.match(key):
            continue
        if isinstance(value, bool):
            clean[key] = value
        elif isinstance(value, int):
            clean[key] = value
        elif isinstance(value, float):
            clean[key] = value
        elif isinstance(value, str):
            clean[key] = value[:500]
        elif isinstance(value, list) and all(isinstance(item, str) for item in value[:20]):
            clean[key] = [item[:200] for item in value[:20]]
    return clean


def fallback_title(*candidates: str | None) -> str:
    for candidate in candidates:
        if candidate and candidate.strip():
            collapsed = normalize_whitespace(candidate)
            if collapsed:
                return collapsed[:500]
    return "Untitled"
