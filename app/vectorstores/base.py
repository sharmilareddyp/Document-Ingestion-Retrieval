"""Vector store types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class VectorHit:
    vector_id: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


PINECONE_METADATA_KEYS = {
    "document_id",
    "chunk_id",
    "source_id",
    "source_type",
    "document_type",
    "document_version",
    "chunk_index",
    "source_modified_ts",
    "title",
}


def slim_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Keep Pinecone metadata small and restricted to filterable fields."""
    slim: dict[str, Any] = {}
    for key, value in metadata.items():
        if key not in PINECONE_METADATA_KEYS or value is None:
            continue
        if isinstance(value, str):
            slim[key] = value[:200]
        elif isinstance(value, bool):
            slim[key] = value
        elif isinstance(value, int):
            slim[key] = value
        elif isinstance(value, float):
            slim[key] = value
    return slim
