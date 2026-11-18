"""In-memory vector store with optional JSON persistence for multi-process local demos.

Writes are immediately visible to later queries in this process. A file-backed store
reloads before each call so the API and worker can share it. That is not how Pinecone
behaves: Pinecone upserts are not a transactional commit and may be briefly invisible
to queries.
"""

from __future__ import annotations

import json
import math
import os
import threading
from pathlib import Path
from typing import Any

from app.core.exceptions import PermanentProcessingError
from app.vectorstores.base import VectorHit, slim_metadata

_LOCK = threading.Lock()


class FakeVectorStore:
    def __init__(self, *, dimension: int, namespace: str, path: Path | None = None) -> None:
        self.dimension = dimension
        self.namespace = namespace
        self.path = path
        self._items: dict[str, dict[str, Any]] = {}
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def upsert(self, vectors: list[dict[str, Any]]) -> int:
        with _LOCK:
            self._load()
            for item in vectors:
                values = [float(value) for value in item["values"]]
                if len(values) != self.dimension:
                    raise PermanentProcessingError(
                        "Vector dimension does not match the configured embedding size",
                        category="vector_store",
                    )
                self._items[str(item["id"])] = {
                    "values": values,
                    "metadata": slim_metadata(item.get("metadata") or {}),
                    "namespace": self.namespace,
                }
            self._save()
            return len(vectors)

    def query(
        self,
        vector: list[float],
        *,
        top_k: int,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[VectorHit]:
        with _LOCK:
            self._load()
            scored: list[VectorHit] = []
            for vector_id, item in self._items.items():
                if item.get("namespace") != self.namespace:
                    continue
                metadata = item.get("metadata") or {}
                if not metadata_matches(metadata, metadata_filter):
                    continue
                scored.append(
                    VectorHit(
                        vector_id=vector_id,
                        score=cosine(vector, item["values"]),
                        metadata=dict(metadata),
                    )
                )
        scored.sort(key=lambda hit: (-hit.score, hit.vector_id))
        return scored[:top_k]

    def delete(
        self, *, ids: list[str] | None = None, metadata_filter: dict[str, Any] | None = None
    ) -> int:
        with _LOCK:
            self._load()
            removed = 0
            for vector_id in list(self._items):
                item = self._items[vector_id]
                if item.get("namespace") != self.namespace:
                    continue
                if ids is not None and vector_id not in ids:
                    continue
                if metadata_filter and not metadata_matches(
                    item.get("metadata") or {}, metadata_filter
                ):
                    continue
                if ids is None and not metadata_filter:
                    continue
                del self._items[vector_id]
                removed += 1
            self._save()
            return removed

    def stats(self) -> dict[str, Any]:
        with _LOCK:
            self._load()
            count = sum(
                1 for item in self._items.values() if item.get("namespace") == self.namespace
            )
        return {
            "backend": "fake",
            "namespace": self.namespace,
            "vector_count": count,
            "dimension": self.dimension,
        }

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError:
            loaded = {}
        if isinstance(loaded, dict):
            self._items = loaded

    def _save(self) -> None:
        if self.path is None:
            return
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self._items), encoding="utf-8")
        os.replace(temporary, self.path)


def cosine(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or not left:
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


def metadata_matches(metadata: dict[str, Any], metadata_filter: dict[str, Any] | None) -> bool:
    if not metadata_filter:
        return True
    for key, expected in metadata_filter.items():
        value = metadata.get(key)
        if isinstance(expected, dict):
            if "$eq" in expected and value != expected["$eq"]:
                return False
            if "$in" in expected and value not in expected["$in"]:
                return False
            if "$gte" in expected and (
                not isinstance(value, (int, float)) or value < expected["$gte"]
            ):
                return False
            if "$lte" in expected and (
                not isinstance(value, (int, float)) or value > expected["$lte"]
            ):
                return False
            if "$gt" in expected and (
                not isinstance(value, (int, float)) or value <= expected["$gt"]
            ):
                return False
            if "$lt" in expected and (
                not isinstance(value, (int, float)) or value >= expected["$lt"]
            ):
                return False
        elif value != expected:
            return False
    return True
