"""Offline ingestion benchmark.

Uses the fake embedding provider and in-memory vector store. The numbers are local
measurements of this process. They are not OpenAI or Pinecone throughput and they
are not evidence the system scales to millions of documents.
"""

from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path

from app.core.config import Settings, reset_settings_cache
from app.database.session import reset_engine_cache
from app.services.context import build_context, reset_context_cache
from app.services.ingestion_service import IngestionService


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100) * (len(ordered) - 1)))))
    return ordered[index]


def main() -> None:
    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("DATABASE_URL is required. This script does not invent timings.")
    root = Path(tempfile.mkdtemp(prefix="bench-docs-"))
    count = int(os.environ.get("BENCH_DOCS", "20"))
    for index in range(count):
        (root / f"doc-{index}.txt").write_text(
            f"Benchmark document {index}\n\n"
            f"The synthetic record discusses control {index} and evidence collection.\n" * 8,
            encoding="utf-8",
        )
    os.environ["EMBEDDING_BACKEND"] = "fake"
    os.environ["VECTOR_BACKEND"] = "memory"
    os.environ["ALLOWED_FILE_ROOTS"] = str(root)
    reset_settings_cache()
    reset_engine_cache()
    reset_context_cache()
    settings = Settings()
    context = build_context(settings)
    service = IngestionService(context)
    source = service.register_source(
        name=f"bench-{uuid.uuid4().hex[:8]}",
        source_type="filesystem",
        config={"root_path": str(root), "detect_deletions": False},
    )
    started = time.perf_counter()
    job = service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)
    result = service.run_job(job["job_id"])
    elapsed = time.perf_counter() - started
    processed = result["processed_documents"] or 1
    from app.database.repositories import DocumentRepository
    from app.database.session import session_scope

    chunks = 0
    with session_scope(settings) as session:
        rows, _ = DocumentRepository(session).list_filtered(
            source_id=source["source_id"], limit=count
        )
        for row in rows:
            chunks += row.chunk_count
    search_latencies = []
    from app.retrieval.search_service import SearchRequest, SearchService

    search = SearchService(context)
    for index in range(10):
        tick = time.perf_counter()
        search.search(SearchRequest(query=f"control {index}", top_k=5, mode="semantic"))
        search_latencies.append(time.perf_counter() - tick)
    print(
        {
            "documents": count,
            "processed": result["processed_documents"],
            "elapsed_seconds": round(elapsed, 4),
            "documents_per_minute": round(processed / elapsed * 60, 2),
            "chunks_total": chunks,
            "chunks_per_document": round(chunks / processed, 2),
            "embedding_backend": "fake-hash",
            "vector_backend": "memory",
            "search_p50_seconds": round(percentile(search_latencies, 50), 6),
            "search_p95_seconds": round(percentile(search_latencies, 95), 6),
            "note": "Fake embeddings. Not an OpenAI or Pinecone measurement.",
        }
    )


if __name__ == "__main__":
    main()
