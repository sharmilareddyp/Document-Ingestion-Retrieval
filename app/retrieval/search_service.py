"""Semantic, keyword, and hybrid retrieval.

Semantic scores are cosine similarities from the vector index. Keyword scores are
PostgreSQL `ts_rank` values. Hybrid scores are reciprocal-rank fusion of those two
lists and are not probabilities. Vector search is not exact-match or full-text search.

Pagination over-fetches `offset + top_k` and slices the window. Pinecone does not
provide offset pagination. The window is capped.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from app.core.exceptions import UnprocessableError
from app.database.repositories import DocumentRepository
from app.database.session import session_scope
from app.domain.models import SearchHit
from app.observability.metrics import SEARCH_LATENCY, SEARCH_REQUESTS
from app.retrieval.filters import (
    MAX_PREFILTER_IDS,
    MAX_SEARCH_WINDOW,
    pinecone_filter,
    reciprocal_rank_fusion,
    validate_metadata_filters,
)
from app.services.context import AppContext


@dataclass
class SearchRequest:
    query: str
    top_k: int = 5
    offset: int = 0
    mode: str = "semantic"
    source_id: UUID | None = None
    source_type: str | None = None
    document_type: str | None = None
    metadata_filters: dict | None = None
    title: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    dedupe_by_document: bool = False


class SearchService:
    def __init__(self, context: AppContext) -> None:
        self.context = context

    def search(self, request: SearchRequest) -> dict:
        started = time.perf_counter()
        if request.mode not in {"semantic", "keyword", "hybrid"}:
            raise UnprocessableError("Search mode must be semantic, keyword, or hybrid")
        window = request.offset + request.top_k
        if window > MAX_SEARCH_WINDOW or request.top_k < 1:
            raise UnprocessableError(f"offset + top_k must be between 1 and {MAX_SEARCH_WINDOW}")
        filters = validate_metadata_filters(request.metadata_filters)
        document_ids = self._prefilter(request, filters)
        try:
            if request.mode == "semantic":
                hits, score_type = self._semantic(request, document_ids, window)
            elif request.mode == "keyword":
                hits, score_type = self._keyword(request, document_ids, window)
            else:
                semantic_hits, _ = self._semantic(request, document_ids, window)
                keyword_hits, _ = self._keyword(request, document_ids, window)
                fused = reciprocal_rank_fusion(
                    [
                        [str(hit.chunk_id) for hit in semantic_hits],
                        [str(hit.chunk_id) for hit in keyword_hits],
                    ]
                )
                by_id = {str(hit.chunk_id): hit for hit in semantic_hits}
                for hit in keyword_hits:
                    by_id.setdefault(str(hit.chunk_id), hit)
                hits = []
                for chunk_id, score in fused:
                    hit = by_id.get(chunk_id)
                    if hit is None:
                        continue
                    copied = hit.model_copy(update={"similarity_score": score})
                    copied.metadata = {**copied.metadata, "score_type": "rrf"}
                    hits.append(copied)
                score_type = "rrf"
            if request.dedupe_by_document:
                hits = _dedupe(hits)
            selected = hits[request.offset : request.offset + request.top_k]
            return {
                "items": selected,
                "limit": request.top_k,
                "offset": request.offset,
                "mode": request.mode,
                "score_type": score_type,
            }
        finally:
            SEARCH_REQUESTS.labels(mode=request.mode).inc()
            SEARCH_LATENCY.observe(time.perf_counter() - started)

    def _prefilter(self, request: SearchRequest, filters: dict) -> list[UUID] | None:
        if not filters and not request.title:
            return None
        with session_scope(self.context.settings) as session:
            ids = DocumentRepository(session).ids_for_metadata(
                source_id=request.source_id,
                document_type=request.document_type,
                title=request.title,
                metadata_filters=filters,
                limit=MAX_PREFILTER_IDS + 1,
            )
        if len(ids) > MAX_PREFILTER_IDS:
            raise UnprocessableError(
                "Filter matched too many documents. Narrow title or metadata filters."
            )
        return ids

    def _semantic(
        self, request: SearchRequest, document_ids: list[UUID] | None, window: int
    ) -> tuple[list[SearchHit], str]:
        vector = self.context.embeddings.embed_query(request.query)
        metadata_filter = pinecone_filter(
            source_id=request.source_id,
            source_type=request.source_type,
            document_type=request.document_type,
            document_ids=document_ids,
            date_from=request.date_from,
            date_to=request.date_to,
        )
        matches = self.context.vector_store.query(
            vector, top_k=window, metadata_filter=metadata_filter
        )
        chunk_ids: list[UUID] = []
        scores: dict[str, float] = {}
        for match in matches:
            try:
                chunk_id = UUID(match.vector_id)
            except ValueError:
                continue
            chunk_ids.append(chunk_id)
            scores[match.vector_id] = match.score
        hydrated = self._hydrate(chunk_ids, scores, score_type="cosine")
        return hydrated, "cosine"

    def _keyword(
        self, request: SearchRequest, document_ids: list[UUID] | None, window: int
    ) -> tuple[list[SearchHit], str]:
        with session_scope(self.context.settings) as session:
            rows = DocumentRepository(session).keyword_search(
                request.query,
                limit=window,
                source_id=request.source_id,
                document_type=request.document_type,
                document_ids=document_ids,
                date_from=request.date_from,
                date_to=request.date_to,
            )
            hits = []
            for chunk, document, score in rows:
                if request.source_type and document.source_type != request.source_type:
                    continue
                hits.append(_hit(chunk, document, score, "ts_rank"))
        return hits, "ts_rank"

    def _hydrate(
        self, chunk_ids: list[UUID], scores: dict[str, float], *, score_type: str
    ) -> list[SearchHit]:
        if not chunk_ids:
            return []
        with session_scope(self.context.settings) as session:
            repo = DocumentRepository(session)
            chunks = {chunk.id: chunk for chunk in repo.get_chunks_by_ids(chunk_ids)}
            hits: list[SearchHit] = []
            for chunk_id in chunk_ids:
                chunk = chunks.get(chunk_id)
                if chunk is None:
                    continue
                document = repo.get(chunk.document_id)
                if document is None or document.deleted_at is not None:
                    continue
                if chunk.document_version != document.current_version:
                    continue
                hits.append(_hit(chunk, document, scores.get(str(chunk_id), 0.0), score_type))
            return hits


def _hit(chunk, document, score: float, score_type: str) -> SearchHit:
    return SearchHit(
        chunk_id=chunk.id,
        document_id=document.id,
        document_title=document.title,
        source_type=document.source_type,
        source_uri=document.source_uri,
        document_version=chunk.document_version,
        section_title=chunk.section_title,
        page_number=chunk.page_number,
        chunk_text=chunk.chunk_text,
        similarity_score=score,
        source_id=document.source_id,
        document_type=document.document_type,
        metadata={
            "score_type": score_type,
            "custom_metadata": document.custom_metadata or {},
            "content_hash": document.content_hash,
            "chunk_index": chunk.chunk_index,
        },
    )


def _dedupe(hits: list[SearchHit]) -> list[SearchHit]:
    best: dict[UUID, SearchHit] = {}
    for hit in hits:
        current = best.get(hit.document_id)
        if current is None or (hit.similarity_score or 0) > (current.similarity_score or 0):
            best[hit.document_id] = hit
    return sorted(
        best.values(), key=lambda item: (-(item.similarity_score or 0), str(item.chunk_id))
    )
