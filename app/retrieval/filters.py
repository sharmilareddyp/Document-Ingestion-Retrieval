"""Retrieval filters.

Pinecone receives only a whitelisted filter: source, document type, document ids,
and a numeric source-modified timestamp. Title and custom metadata filters are
resolved in PostgreSQL first, because those fields are not stored in the vector
index. Keyword search is PostgreSQL full text and is not vector similarity.
"""

from __future__ import annotations

import re
from datetime import datetime
from uuid import UUID

from app.core.exceptions import UnprocessableError

_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
MAX_PREFILTER_IDS = 200
MAX_SEARCH_WINDOW = 100


def validate_metadata_filters(filters: dict | None) -> dict:
    if not filters:
        return {}
    if len(filters) > 20:
        raise UnprocessableError("At most 20 metadata filters are allowed")
    clean: dict = {}
    for key, value in filters.items():
        if not isinstance(key, str) or not _KEY.match(key):
            raise UnprocessableError("Metadata filter keys must be identifiers")
        if isinstance(value, bool) or isinstance(value, (int, float, str)):
            if isinstance(value, str) and len(value) > 200:
                raise UnprocessableError("Metadata filter values are limited to 200 characters")
            clean[key] = value
            continue
        raise UnprocessableError("Metadata filters accept only scalar equality matches")
    return clean


def pinecone_filter(
    *,
    source_id: UUID | None = None,
    source_type: str | None = None,
    document_type: str | None = None,
    document_ids: list[UUID] | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> dict | None:
    filt: dict = {}
    if source_id:
        filt["source_id"] = {"$eq": str(source_id)}
    if source_type:
        filt["source_type"] = {"$eq": source_type}
    if document_type:
        filt["document_type"] = {"$eq": document_type}
    if document_ids is not None:
        filt["document_id"] = {"$in": [str(item) for item in document_ids] or ["__none__"]}
    timestamp_range: dict = {}
    if date_from is not None:
        timestamp_range["$gte"] = date_from.timestamp()
    if date_to is not None:
        timestamp_range["$lte"] = date_to.timestamp()
    if timestamp_range:
        filt["source_modified_ts"] = timestamp_range
    return filt or None


def reciprocal_rank_fusion(rankings: list[list[str]], *, k: int = 60) -> list[tuple[str, float]]:
    """Fuse ranked id lists. The score is an RRF rank score, not a cosine similarity."""
    scores: dict[str, float] = {}
    for ranking in rankings:
        for index, item_id in enumerate(ranking, start=1):
            scores[item_id] = scores.get(item_id, 0.0) + 1.0 / (k + index)
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))
