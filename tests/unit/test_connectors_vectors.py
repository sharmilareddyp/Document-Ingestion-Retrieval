from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from app.connectors.http import request_json
from app.core.config import Settings
from app.core.exceptions import (
    DatabaseFailure,
    PermanentProcessingError,
    TransientProcessingError,
    UnprocessableError,
    UnsafePathError,
    UnsafeURLError,
)
from app.core.security import assert_url_allowed, resolve_allowed_path, sanitize_filename
from app.embeddings.providers import FakeEmbeddingProvider, OpenAIEmbeddingProvider
from app.processing.batch import ItemFailure, process_each
from app.retrieval.filters import pinecone_filter, reciprocal_rank_fusion, validate_metadata_filters
from app.vectorstores.base import slim_metadata
from app.vectorstores.fake_store import FakeVectorStore
from app.vectorstores.pinecone_store import PineconeVectorStore


def test_path_and_url_guards(tmp_path: Path):
    settings = Settings(
        allowed_file_roots=str(tmp_path),
        data_dir=str(tmp_path / "data"),
        allowed_source_hosts="localhost",
        allow_private_source_hosts=True,
    )
    allowed = tmp_path / "notes.txt"
    allowed.write_text("ok", encoding="utf-8")
    assert resolve_allowed_path(allowed, settings) == allowed.resolve()
    with pytest.raises(UnsafePathError):
        resolve_allowed_path(Path("/etc/passwd"), settings)
    assert sanitize_filename("../secret.pdf") == "secret.pdf"
    with pytest.raises(UnsafeURLError):
        assert_url_allowed("file:///etc/passwd", settings)
    with pytest.raises(UnsafeURLError):
        assert_url_allowed("http://example.com/records", settings)
    blocked = Settings(
        allowed_source_hosts="169.254.169.254",
        allow_private_source_hosts=True,
        allowed_file_roots=str(tmp_path),
        data_dir=str(tmp_path / "data"),
    )
    with pytest.raises(UnsafeURLError):
        assert_url_allowed("http://169.254.169.254/latest/meta-data", blocked)


def test_http_retries_timeouts_and_redirects(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("app.connectors.http.time.sleep", lambda *_: None)
    settings = Settings(
        allowed_source_hosts="localhost",
        allow_private_source_hosts=True,
        http_max_retries=2,
        http_retry_backoff_seconds=0,
        allowed_file_roots=str(tmp_path),
        data_dir=str(tmp_path / "data"),
    )
    calls = {"count": 0}

    def rate_limit(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(429, headers={"retry-after": "0"})
        return httpx.Response(200, json={"ok": True})

    with httpx.Client(transport=httpx.MockTransport(rate_limit)) as client:
        assert request_json(client, "GET", "http://localhost/records", settings=settings) == {
            "ok": True
        }

    def timeout(_request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out")

    with httpx.Client(transport=httpx.MockTransport(timeout)) as client:
        with pytest.raises(TransientProcessingError):
            request_json(client, "GET", "http://localhost/records", settings=settings)

    def redirect(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "http://evil.example/steal"})

    with httpx.Client(transport=httpx.MockTransport(redirect)) as client:
        with pytest.raises(UnsafeURLError):
            request_json(client, "GET", "http://localhost/records", settings=settings)


def test_fake_embeddings_and_vector_filters():
    provider = FakeEmbeddingProvider(dimension=16)
    first = provider.embed_texts(["retention policy", "incident response"])
    assert len(first[0]) == 16
    assert first[0] == provider.embed_query("retention policy")
    assert first[0] != first[1]
    store = FakeVectorStore(dimension=16, namespace="default")
    store.upsert(
        [
            {
                "id": "a",
                "values": first[0],
                "metadata": {"document_type": "pdf", "source_modified_ts": 10, "title": "A" * 500},
            },
            {
                "id": "b",
                "values": first[1],
                "metadata": {"document_type": "txt", "source_modified_ts": 30},
            },
        ]
    )
    hits = store.query(
        first[0],
        top_k=2,
        metadata_filter={
            "document_type": {"$eq": "pdf"},
            "source_modified_ts": {"$gte": 5, "$lte": 20},
        },
    )
    assert [hit.vector_id for hit in hits] == ["a"]
    assert len(hits[0].metadata["title"]) == 200
    assert store.delete(ids=["a"]) == 1
    assert store.stats()["vector_count"] == 1
    slim = slim_metadata({"document_id": "x", "chunk_text": "secret body", "title": "kept"})
    assert "chunk_text" not in slim
    assert slim["title"] == "kept"


def test_pinecone_adapter_batches_and_rejects_bad_dimensions():
    class Index:
        def __init__(self):
            self.upserts = []
            self.deleted = []

        def upsert(self, vectors, namespace):
            self.upserts.append((namespace, vectors))

        def query(self, **kwargs):
            return {"matches": [{"id": "a", "score": 0.4, "metadata": {"document_type": "pdf"}}]}

        def delete(self, ids=None, namespace=None, filter=None):
            self.deleted.extend(ids or [])

        def describe_index_stats(self):
            return {"namespaces": {"default": {"vector_count": 1}}}

    index = Index()
    store = PineconeVectorStore(index, namespace="default", dimension=2, batch_size=1)
    assert (
        store.upsert(
            [{"id": "a", "values": [0.1, 0.2], "metadata": {"title": "A", "chunk_text": "nope"}}]
        )
        == 1
    )
    assert store.upsert([{"id": "b", "values": [0.2, 0.3], "metadata": {}}]) == 1
    assert len(index.upserts) == 2
    assert "chunk_text" not in index.upserts[0][1][0]["metadata"]
    hits = store.query([0.1, 0.2], top_k=1, metadata_filter={"document_type": {"$eq": "pdf"}})
    assert hits[0].vector_id == "a"
    assert store.delete(ids=["a", "b"]) == 2
    with pytest.raises(PermanentProcessingError):
        store.upsert([{"id": "c", "values": [1.0], "metadata": {}}])


def test_openai_provider_orders_batches_and_retries(monkeypatch):
    monkeypatch.setattr("app.embeddings.providers.time.sleep", lambda *_: None)

    class Item:
        def __init__(self, index, embedding):
            self.index = index
            self.embedding = embedding

    class Response:
        def __init__(self, vectors):
            self.data = [Item(index, vector) for index, vector in enumerate(vectors)]

    class Embeddings:
        def __init__(self):
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                error = RuntimeError("rate limit")
                error.status_code = 429
                raise error
            assert kwargs["dimensions"] == 4
            return Response([[0.1, 0.2, 0.3, 0.4], [0.4, 0.3, 0.2, 0.1]])

    provider = OpenAIEmbeddingProvider(
        api_key="test-key",
        model="text-embedding-3-small",
        dimensions=4,
        batch_size=8,
        max_retries=2,
        version="1",
    )
    provider.client.embeddings = Embeddings()
    vectors = provider.embed_texts(["one", "two"])
    assert len(vectors) == 2
    assert len(vectors[0]) == 4


def test_batch_isolation_and_filters():
    def handle(item: int) -> int:
        if item == 2:
            raise ValueError("bad document")
        return item

    results = list(process_each([1, 2, 3], handle))
    assert results[0] == 1
    assert isinstance(results[1], ItemFailure)
    assert results[2] == 3
    with pytest.raises(DatabaseFailure):
        list(process_each([1], lambda _item: (_ for _ in ()).throw(DatabaseFailure("down"))))
    assert validate_metadata_filters({"department": "legal"}) == {"department": "legal"}
    with pytest.raises(UnprocessableError):
        validate_metadata_filters({"bad-key": {"$gt": 1}})
    fused = reciprocal_rank_fusion([["a", "b"], ["b", "c"]])
    assert fused[0][0] == "b"
    assert pinecone_filter(document_type="pdf") == {"document_type": {"$eq": "pdf"}}
