"""Shared fixtures. Integration tests skip cleanly when PostgreSQL is absent."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]

TABLES = """
TRUNCATE TABLE
    indexing_outbox,
    document_chunks,
    document_versions,
    job_documents,
    processing_attempts,
    data_quality_results,
    schema_drift_events,
    schema_contracts,
    documents,
    ingestion_jobs,
    sources
RESTART IDENTITY CASCADE
"""


@pytest.fixture(autouse=True)
def _reset_runtime():
    yield
    from app.core.config import reset_settings_cache
    from app.database.session import reset_engine_cache
    from app.services.context import reset_context_cache

    reset_settings_cache()
    reset_context_cache()
    reset_engine_cache()


@pytest.fixture(scope="session")
def migrated_database():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL is not set; PostgreSQL integration tests are skipped")
    os.environ["DATABASE_URL"] = url
    os.environ["EMBEDDING_BACKEND"] = "fake"
    os.environ["VECTOR_BACKEND"] = "memory"
    os.environ["API_AUTH_ENABLED"] = "false"
    os.environ.setdefault("ALLOWED_SOURCE_HOSTS", "localhost,127.0.0.1")
    os.environ.setdefault("ALLOW_PRIVATE_SOURCE_HOSTS", "true")
    from alembic import command
    from alembic.config import Config
    from app.core.config import reset_settings_cache

    reset_settings_cache()
    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")
    yield url


@pytest.fixture
def db(migrated_database):
    from app.core.config import reset_settings_cache
    from app.database.session import reset_engine_cache
    from app.services.context import reset_context_cache

    os.environ["EMBEDDING_BACKEND"] = "fake"
    os.environ["VECTOR_BACKEND"] = "memory"
    os.environ["API_AUTH_ENABLED"] = "false"
    reset_settings_cache()
    reset_context_cache()
    reset_engine_cache()
    engine = create_engine(migrated_database)
    with engine.begin() as connection:
        connection.execute(text(TABLES))
    engine.dispose()
    yield migrated_database


@pytest.fixture
def client(db):
    from app.main import app
    from fastapi.testclient import TestClient

    with TestClient(app) as test_client:
        yield test_client
