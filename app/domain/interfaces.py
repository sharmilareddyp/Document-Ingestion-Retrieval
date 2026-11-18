"""Protocols that keep connectors, embeddings, and vector stores replaceable."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol
from uuid import UUID

from app.domain.models import DiscoveredItem, ParsedDocument, RejectedItem, SourceRecord


class DocumentParser(Protocol):
    def parse(
        self, content: bytes, *, filename: str | None = None, payload: dict[str, Any] | None = None
    ) -> ParsedDocument:
        """Extract structured text. Must not execute document content."""


class BaseConnector(Protocol):
    """Discover and fetch source records. Parsing is intentionally not included."""

    source_id: UUID
    source_type: str

    def discover(self) -> Iterator[DiscoveredItem | RejectedItem]:
        """Yield items to fetch and items rejected before fetch."""

    def fetch(self, item: DiscoveredItem) -> SourceRecord:
        """Load one discovered item."""

    def identify(self, item: DiscoveredItem) -> str:
        """Return the stable logical key for an item."""


class EmbeddingProvider(Protocol):
    model: str
    dimension: int
    version: str

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed document chunks. Implementations must batch and retry transient failures."""

    def embed_query(self, text: str) -> list[float]:
        """Embed a search query with the same model used at index time."""


class VectorMatch(Protocol):
    vector_id: str
    score: float
    metadata: dict[str, Any]


class VectorStore(Protocol):
    namespace: str
    dimension: int

    def upsert(self, vectors: list[dict[str, Any]]) -> int:
        """Upsert vectors. Each item has id, values, and metadata. Returns the count written."""

    def query(
        self,
        vector: list[float],
        *,
        top_k: int,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[Any]:
        """Return matches ordered by descending score."""

    def delete(
        self,
        *,
        ids: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        """Delete by id or metadata filter. Returns the number of vectors removed when known."""

    def stats(self) -> dict[str, Any]:
        """Return lightweight index statistics."""
