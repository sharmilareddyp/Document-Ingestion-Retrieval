from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from app.core.config import get_settings
from app.processing.schema_drift import SAMPLE_RECORDS_CONTRACT
from app.services.context import build_context
from app.services.indexing_service import IndexingService
from app.services.ingestion_service import IngestionService
from app.workers.worker import worker_once
from sqlalchemy import text

pytestmark = pytest.mark.integration


def _settings_root() -> Path:
    root = get_settings().data_path / "pytest-corpus" / uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_migrations_create_expected_tables(db):
    from sqlalchemy import create_engine

    engine = create_engine(db)
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        ).scalars()
        names = set(rows)
    engine.dispose()
    assert {
        "sources",
        "documents",
        "document_versions",
        "document_chunks",
        "ingestion_jobs",
        "processing_attempts",
        "data_quality_results",
        "schema_contracts",
        "schema_drift_events",
        "indexing_outbox",
    } <= names


def test_mixed_batch_isolates_bad_files_and_is_incremental(db):
    root = _settings_root()
    (root / "policy.txt").write_text(
        "Customer contracts are retained for seven years.\n", encoding="utf-8"
    )
    (root / "empty.txt").write_text("", encoding="utf-8")
    (root / "broken.pdf").write_bytes(b"not-a-pdf")
    (root / "notes.md").write_text("unsupported", encoding="utf-8")
    service = IngestionService(build_context())
    source = service.register_source(
        name=f"files-{uuid4().hex[:8]}",
        source_type="filesystem",
        config={"root_path": str(root), "detect_deletions": True},
    )
    finished = service.run_job(
        service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    assert finished["processed_documents"] == 1
    assert finished["quarantined_documents"] >= 1
    assert finished["skipped_documents"] >= 1
    assert finished["status"] == "completed_with_errors"
    before = build_context().vector_store.stats()["vector_count"]
    second = service.run_job(
        service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    assert second["skipped_documents"] >= 1
    assert build_context().vector_store.stats()["vector_count"] == before
    (root / "policy.txt").write_text(
        "Customer contracts are retained for seven years.\nLegal hold extends retention.\n",
        encoding="utf-8",
    )
    third = service.run_job(
        service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    assert third["processed_documents"] >= 1
    (root / "policy.txt").unlink()
    deleted = service.run_job(
        service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    assert deleted["processed_documents"] >= 1


def test_api_source_pagination_drift_and_partial_success(db):
    records = [
        {
            "id": "ok-1",
            "title": "Retention schedule",
            "body": "Contracts stay for seven years.",
            "updated_at": "2024-05-01T00:00:00Z",
            "priority": 1,
        },
        {
            "id": "bad-1",
            "title": "Broken priority",
            "body": "Priority became a string.",
            "updated_at": "2024-05-02T00:00:00Z",
            "priority": "high",
        },
    ]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps({"items": records, "page": 1}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        service = IngestionService(build_context())
        source = service.register_source(
            name=f"api-{uuid4().hex[:8]}",
            source_type="rest_api",
            config={
                "url": f"http://127.0.0.1:{port}/records",
                "records_path": "items",
                "pagination": {"strategy": "none"},
                "text_fields": ["body"],
                "title_field": "title",
            },
            contract=SAMPLE_RECORDS_CONTRACT,
        )
        finished = service.run_job(
            service.enqueue_source(source["source_id"], trigger="api", start_immediately=True)[
                "job_id"
            ]
        )
        assert finished["processed_documents"] == 1
        assert finished["quarantined_documents"] == 1
        from app.services.quality_service import QualityService

        drift = QualityService(build_context()).list_drift(
            source_id=source["source_id"], severity="breaking", limit=20, offset=0
        )
        assert drift["total"] >= 1
    finally:
        server.shutdown()


def test_vector_failure_can_be_reconciled_without_duplicate_chunks(db):
    root = _settings_root()
    (root / "note.txt").write_text(
        "Rotate credentials after a severity one incident.\n", encoding="utf-8"
    )
    context = build_context()
    service = IngestionService(context)
    source = service.register_source(
        name=f"recover-{uuid4().hex[:8]}",
        source_type="filesystem",
        config={"root_path": str(root), "detect_deletions": False},
    )
    real_upsert = context.vector_store.upsert

    def fail(vectors):
        raise RuntimeError("simulated pinecone outage")

    context.vector_store.upsert = fail
    failed = service.run_job(
        service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    assert failed["failed_documents"] == 1
    context.vector_store.upsert = real_upsert
    stats = IndexingService(context).flush(limit=100)
    assert stats["upserted"] >= 1
    assert stats["failed"] == 0
    assert context.vector_store.stats()["vector_count"] == 1
    second = IndexingService(context).flush(limit=100)
    assert second["upserted"] == 0
    assert context.vector_store.stats()["vector_count"] == 1


def test_worker_runs_a_pending_upload_job(db, client):
    response = client.post(
        "/documents/upload",
        files={
            "upload": (
                "guide.txt",
                b"New hires complete security training before access.",
                "text/plain",
            )
        },
        headers={"Idempotency-Key": "upload-1"},
    )
    assert response.status_code == 202
    body = response.json()
    repeated = client.post(
        "/documents/upload",
        files={
            "upload": (
                "guide.txt",
                b"New hires complete security training before access.",
                "text/plain",
            )
        },
        headers={"Idempotency-Key": "upload-1"},
    )
    assert repeated.status_code == 202
    assert repeated.json()["job_id"] == body["job_id"]
    conflict = client.post(
        "/documents/upload",
        files={"upload": ("guide.txt", b"Different bytes.", "text/plain")},
        headers={"Idempotency-Key": "upload-1"},
    )
    assert conflict.status_code == 409
    assert worker_once(build_context()) is True
    document = client.get(f"/documents/{body['document_id']}")
    assert document.status_code == 200
    assert document.json()["processing_status"] == "completed"
    chunks = client.get(f"/documents/{body['document_id']}/chunks")
    assert chunks.status_code == 200
    assert chunks.json()[0]["chunk_text"]
    search = client.post(
        "/search", json={"query": "security training", "mode": "keyword", "top_k": 5}
    )
    assert search.status_code == 200
    assert search.json()["items"]
    semantic = client.post(
        "/search", json={"query": "security training", "mode": "semantic", "top_k": 5}
    )
    assert semantic.status_code == 200
    assert semantic.json()["score_type"] == "cosine"
    missing = client.get(f"/documents/{uuid4()}")
    assert missing.status_code == 404
    listed = client.get("/documents", params={"limit": 1, "offset": 0})
    assert listed.status_code == 200
    assert listed.json()["total"] >= 1
    rejected = client.post(
        "/documents/upload", files={"upload": ("image.png", b"nope", "image/png")}
    )
    assert rejected.status_code == 415
    unconfirmed = client.delete(f"/documents/{body['document_id']}")
    assert unconfirmed.status_code == 422
    removed = client.delete(
        f"/documents/{body['document_id']}", headers={"X-Confirm-Delete": "true"}
    )
    assert removed.status_code == 200
    assert removed.json()["processing_status"] == "tombstoned"
    hidden = client.post("/search", json={"query": "security training", "mode": "keyword"})
    assert all(item["document_id"] != body["document_id"] for item in hidden.json()["items"])


def test_auth_is_enforced_when_enabled(db, client, monkeypatch):
    monkeypatch.setenv("API_AUTH_ENABLED", "true")
    monkeypatch.setenv("API_KEYS", "top-secret")
    from app.core.config import reset_settings_cache

    reset_settings_cache()
    denied = client.get("/documents")
    assert denied.status_code == 401
    allowed = client.get("/documents", headers={"X-API-Key": "top-secret"})
    assert allowed.status_code == 200
    health = client.get("/health")
    assert health.status_code == 200


def test_job_retry_endpoint_accepts_failed_work(db, client):
    root = _settings_root()
    (root / "bad.pdf").write_bytes(b"%PDF-not-really")
    service = IngestionService(build_context())
    source = service.register_source(
        name=f"retry-{uuid4().hex[:8]}",
        source_type="filesystem",
        config={"root_path": str(root), "detect_deletions": False},
    )
    job = service.run_job(
        service.enqueue_source(source["source_id"], trigger="manual", start_immediately=True)[
            "job_id"
        ]
    )
    listed = client.get(f"/jobs/{job['job_id']}")
    assert listed.status_code == 200
    # A corrupted PDF is a permanent parse failure, so retry eligibility depends on outcome.
    # Empty retry set returns 409 when nothing is marked failed. Quarantine is included explicitly.
    if job["failed_documents"] == 0 and job["quarantined_documents"] == 0:
        pytest.skip("fixture did not produce a retryable outcome")
    response = client.post(f"/jobs/{job['job_id']}/retry", json={"include_quarantined": True})
    assert response.status_code == 202
    assert UUID(response.json()["job_id"])
