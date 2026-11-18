"""Embedding providers."""

from __future__ import annotations

import hashlib
import math
import re
import time

from app.core.exceptions import PermanentProcessingError, TransientProcessingError
from app.observability.metrics import EMBEDDING_ERRORS, EMBEDDING_REQUESTS, EMBEDDING_TOKENS


class FakeEmbeddingProvider:
    """Deterministic hashed-bag embeddings for tests and offline demos.

    These vectors are not a substitute for a production embedding model. They exist
    so the pipeline, index, and retrieval code can run without OpenAI credentials.
    """

    def __init__(
        self, *, dimension: int = 1536, model: str = "fake-hash", version: str = "1"
    ) -> None:
        self.dimension = dimension
        self.model = model
        self.version = version

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        EMBEDDING_REQUESTS.inc()
        EMBEDDING_TOKENS.inc(sum(max(len(text) // 4, 1) for text in texts))
        return [self._vector(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self.embed_texts([text])[0]

    def _vector(self, text: str) -> list[float]:
        values = [0.0] * self.dimension
        tokens = re.findall(r"[a-z0-9]+", text.lower()) or ["empty"]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimension
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            values[index] += sign
        norm = math.sqrt(sum(value * value for value in values)) or 1.0
        return [value / norm for value in values]


class OpenAIEmbeddingProvider:
    """OpenAI embeddings adapter. Credentials come from configuration, never from code."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        dimensions: int,
        batch_size: int,
        max_retries: int,
        version: str,
    ) -> None:
        if not api_key:
            raise PermanentProcessingError(
                "OPENAI_API_KEY is not configured", category="configuration"
            )
        from openai import OpenAI

        self.client = OpenAI(api_key=api_key, max_retries=0)
        self.model = model
        self.dimension = dimensions
        self.version = version
        self.batch_size = max(1, batch_size)
        self.max_retries = max_retries

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), self.batch_size):
            batch = texts[start : start + self.batch_size]
            vectors.extend(self._embed_batch(batch))
        if any(len(vector) != self.dimension for vector in vectors):
            raise PermanentProcessingError(
                "Embedding provider returned an unexpected dimension", category="embedding"
            )
        return vectors

    def embed_query(self, text: str) -> list[float]:
        return self.embed_texts([text])[0]

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        delay = 0.25
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                kwargs: dict = {"model": self.model, "input": texts}
                if self.model.startswith("text-embedding-3"):
                    kwargs["dimensions"] = self.dimension
                response = self.client.embeddings.create(**kwargs)
                ordered = sorted(response.data, key=lambda item: item.index)
                EMBEDDING_REQUESTS.inc()
                EMBEDDING_TOKENS.inc(sum(max(len(text) // 4, 1) for text in texts))
                return [list(item.embedding) for item in ordered]
            except Exception as exc:
                last_error = exc
                EMBEDDING_ERRORS.inc()
                if not _retryable(exc) or attempt >= self.max_retries:
                    break
                time.sleep(delay)
                delay = min(delay * 2, 8)
        if _retryable(last_error):
            raise TransientProcessingError(
                "Embedding request failed after retries", category="embedding"
            ) from last_error
        raise PermanentProcessingError(
            "Embedding request was rejected", category="embedding"
        ) from last_error


def _retryable(exc: Exception | None) -> bool:
    if exc is None:
        return False
    status = getattr(exc, "status_code", None)
    if status in {408, 409, 429} or (isinstance(status, int) and status >= 500):
        return True
    name = type(exc).__name__.lower()
    return any(
        token in name
        for token in ("timeout", "connection", "ratelimit", "rate_limit", "internalserver")
    )


def build_embedding_provider(settings) -> FakeEmbeddingProvider | OpenAIEmbeddingProvider:
    backend = settings.resolved_embedding_backend
    if backend == "fake":
        return FakeEmbeddingProvider(
            dimension=settings.embedding_dimensions, version=settings.embedding_version
        )
    if backend == "openai":
        return OpenAIEmbeddingProvider(
            api_key=settings.openai_api_key,
            model=settings.embedding_model,
            dimensions=settings.embedding_dimensions,
            batch_size=settings.embedding_batch_size,
            max_retries=settings.embedding_max_retries,
            version=settings.embedding_version,
        )
    raise PermanentProcessingError(f"Unknown embedding backend {backend}", category="configuration")
