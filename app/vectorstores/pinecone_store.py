"""Pinecone adapter.

The official `pinecone` SDK is used. Upserts are batched. Callers must not assume a
successful upsert is immediately returned by a query; Pinecone is eventually
consistent for serverless indexes. PostgreSQL remains the system of record, and the
indexing outbox is replayed until the vector id is marked complete.
"""

from __future__ import annotations

import time
from typing import Any

from app.core.config import Settings
from app.core.exceptions import (
    ConfigurationError,
    PermanentProcessingError,
    TransientProcessingError,
)
from app.vectorstores.base import VectorHit, slim_metadata
from app.vectorstores.fake_store import FakeVectorStore


class PineconeVectorStore:
    def __init__(
        self, index: Any, *, namespace: str, dimension: int, batch_size: int = 100
    ) -> None:
        self.index = index
        self.namespace = namespace
        self.dimension = dimension
        self.batch_size = max(1, batch_size)

    def upsert(self, vectors: list[dict[str, Any]]) -> int:
        written = 0
        for start in range(0, len(vectors), self.batch_size):
            batch = []
            for item in vectors[start : start + self.batch_size]:
                values = list(item["values"])
                if len(values) != self.dimension:
                    raise PermanentProcessingError(
                        "Vector dimension does not match the Pinecone index",
                        category="vector_store",
                    )
                batch.append(
                    {
                        "id": str(item["id"]),
                        "values": values,
                        "metadata": slim_metadata(item.get("metadata") or {}),
                    }
                )
            self._call(
                lambda batch=batch: self.index.upsert(vectors=batch, namespace=self.namespace)
            )
            written += len(batch)
        return written

    def query(
        self,
        vector: list[float],
        *,
        top_k: int,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[VectorHit]:
        kwargs: dict[str, Any] = {
            "vector": vector,
            "top_k": top_k,
            "include_metadata": True,
            "namespace": self.namespace,
        }
        if metadata_filter:
            kwargs["filter"] = metadata_filter
        result = self._call(lambda: self.index.query(**kwargs))
        matches = getattr(result, "matches", None)
        if matches is None and isinstance(result, dict):
            matches = result.get("matches", [])
        hits: list[VectorHit] = []
        for match in matches or []:
            if isinstance(match, dict):
                vector_id = str(match.get("id"))
                score = float(match.get("score") or 0)
                metadata = dict(match.get("metadata") or {})
            else:
                vector_id = str(match.id)
                score = float(getattr(match, "score", 0) or 0)
                metadata = dict(getattr(match, "metadata", None) or {})
            hits.append(VectorHit(vector_id=vector_id, score=score, metadata=metadata))
        return hits

    def delete(
        self, *, ids: list[str] | None = None, metadata_filter: dict[str, Any] | None = None
    ) -> int:
        if ids:
            for start in range(0, len(ids), self.batch_size):
                batch = ids[start : start + self.batch_size]
                self._call(
                    lambda batch=batch: self.index.delete(ids=batch, namespace=self.namespace)
                )
            return len(ids)
        if metadata_filter:
            self._call(lambda: self.index.delete(filter=metadata_filter, namespace=self.namespace))
            return 0
        return 0

    def stats(self) -> dict[str, Any]:
        described = self._call(lambda: self.index.describe_index_stats())
        namespaces = getattr(described, "namespaces", None) or {}
        if not isinstance(namespaces, dict) and hasattr(described, "namespaces"):
            namespaces = dict(described.namespaces or {})
        namespace_stats = namespaces.get(self.namespace) if isinstance(namespaces, dict) else None
        count = None
        if namespace_stats is not None:
            count = getattr(namespace_stats, "vector_count", None)
            if count is None and isinstance(namespace_stats, dict):
                count = namespace_stats.get("vector_count")
        return {
            "backend": "pinecone",
            "namespace": self.namespace,
            "vector_count": count,
            "dimension": self.dimension,
        }

    def _call(self, operation):
        delay = 0.25
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                return operation()
            except PermanentProcessingError:
                raise
            except Exception as exc:
                last_error = exc
                status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
                if status and 400 <= int(status) < 500 and int(status) != 429:
                    raise PermanentProcessingError(
                        "Pinecone rejected the vector operation", category="vector_store"
                    ) from exc
                if attempt == 3:
                    break
                time.sleep(delay)
                delay = min(delay * 2, 8)
        raise TransientProcessingError(
            "Pinecone request failed after retries", category="vector_store"
        ) from last_error


def setup_pinecone_index(settings: Settings) -> dict[str, Any]:
    """Create the configured index when it is missing and check its dimension."""
    if not settings.pinecone_api_key:
        raise ConfigurationError("PINECONE_API_KEY is not configured")
    from pinecone import Pinecone, ServerlessSpec

    client = Pinecone(api_key=settings.pinecone_api_key)
    names = _index_names(client.list_indexes())
    if settings.pinecone_index_name not in names:
        client.create_index(
            name=settings.pinecone_index_name,
            dimension=settings.embedding_dimensions,
            metric=settings.pinecone_metric,
            spec=ServerlessSpec(cloud=settings.pinecone_cloud, region=settings.pinecone_region),
        )
    description = client.describe_index(settings.pinecone_index_name)
    dimension = getattr(description, "dimension", None)
    if dimension != settings.embedding_dimensions:
        raise ConfigurationError(
            f"Pinecone index dimension is {dimension}; EMBEDDING_DIMENSIONS is {settings.embedding_dimensions}"
        )
    return {"name": settings.pinecone_index_name, "dimension": dimension}


def build_pinecone_store(settings: Settings) -> PineconeVectorStore:
    if not settings.pinecone_api_key:
        raise ConfigurationError("PINECONE_API_KEY is not configured")
    from pinecone import Pinecone

    client = Pinecone(api_key=settings.pinecone_api_key)
    try:
        description = client.describe_index(settings.pinecone_index_name)
    except Exception as exc:
        raise ConfigurationError(
            "Pinecone index is not available. Run `python -m app.cli setup-pinecone`."
        ) from exc
    dimension = getattr(description, "dimension", None)
    if dimension != settings.embedding_dimensions:
        raise ConfigurationError("Pinecone index dimension does not match EMBEDDING_DIMENSIONS")
    return PineconeVectorStore(
        client.Index(settings.pinecone_index_name),
        namespace=settings.pinecone_namespace,
        dimension=dimension,
        batch_size=settings.vector_upsert_batch_size,
    )


def build_vector_store(settings: Settings) -> FakeVectorStore | PineconeVectorStore:
    backend = settings.resolved_vector_backend
    if backend == "memory":
        return FakeVectorStore(
            dimension=settings.embedding_dimensions, namespace=settings.pinecone_namespace
        )
    if backend == "fake":
        from pathlib import Path

        return FakeVectorStore(
            dimension=settings.embedding_dimensions,
            namespace=settings.pinecone_namespace,
            path=Path(settings.vector_store_path).expanduser(),
        )
    if backend == "pinecone":
        return build_pinecone_store(settings)
    raise ConfigurationError(f"Unknown vector backend {backend}")


def _index_names(listed: Any) -> set[str]:
    names: set[str] = set()
    indexes = getattr(listed, "indexes", None) or listed
    for item in indexes or []:
        name = getattr(item, "name", None)
        if name is None and isinstance(item, dict):
            name = item.get("name")
        if name:
            names.add(name)
    return names
