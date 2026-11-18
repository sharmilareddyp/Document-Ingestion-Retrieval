"""Shared application context."""

from __future__ import annotations

from dataclasses import dataclass

from app.core.config import Settings, get_settings
from app.domain.interfaces import EmbeddingProvider, VectorStore
from app.embeddings import build_embedding_provider
from app.vectorstores.pinecone_store import build_vector_store

_CACHE: dict[tuple, AppContext] = {}


@dataclass
class AppContext:
    settings: Settings
    embeddings: EmbeddingProvider
    vector_store: VectorStore


def build_context(settings: Settings | None = None) -> AppContext:
    settings = settings or get_settings()
    key = (
        settings.database_url,
        settings.resolved_embedding_backend,
        settings.resolved_vector_backend,
        settings.embedding_model,
        settings.embedding_dimensions,
        settings.embedding_version,
        settings.vector_store_path,
        settings.pinecone_index_name,
        settings.pinecone_namespace,
        id(settings) if settings.resolved_vector_backend == "memory" else 0,
    )
    # Memory stores must not be shared across separately constructed settings objects
    # used by isolated tests. File-backed and Pinecone stores are safe to reuse.
    if settings.resolved_vector_backend == "memory":
        key = (*key, id(settings))
    cached = _CACHE.get(key)
    if cached is None:
        cached = AppContext(
            settings=settings,
            embeddings=build_embedding_provider(settings),
            vector_store=build_vector_store(settings),
        )
        _CACHE[key] = cached
    return cached


def reset_context_cache() -> None:
    _CACHE.clear()
