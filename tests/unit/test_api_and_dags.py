from pathlib import Path


def test_dags_are_thin_and_do_not_carry_document_bodies():
    root = Path("airflow/dags")
    names = {path.name for path in root.glob("*.py")}
    expected = {
        "document_ingestion.py",
        "api_sync.py",
        "incremental_processing.py",
        "full_backfill.py",
        "data_quality.py",
        "schema_drift.py",
        "reprocessing.py",
        "reconciliation.py",
    }
    assert expected <= names
    for path in root.glob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "DAG(" in source
        assert "python_callable" in source
        assert "normalized_text" not in source
        assert "chunk_text" not in source
    ingestion = (root / "document_ingestion.py").read_text(encoding="utf-8")
    assert ">>" in ingestion
    incremental = (root / "incremental_processing.py").read_text(encoding="utf-8")
    assert ">>" in incremental


def test_health_and_openapi_do_not_require_a_database():
    from app.main import app
    from fastapi.testclient import TestClient

    client = TestClient(app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert client.get("/openapi.json").status_code == 200
    assert "X-Request-ID" in health.headers
