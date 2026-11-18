"""Application configuration.

Credentials are read only from the environment or a local .env file.
Nothing in this module prints secret values.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for the API, worker, and Airflow tasks."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    service_name: str = "document-ingestion-retrieval"
    environment: str = "local"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    database_url: str = "postgresql+psycopg://ingestion:ingestion@localhost:5432/ingestion"
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_wait_timeout_seconds: float = 60

    pinecone_api_key: str = ""
    pinecone_index_name: str = "documents"
    pinecone_namespace: str = "default"
    pinecone_cloud: str = "aws"
    pinecone_region: str = "us-east-1"
    pinecone_metric: str = "cosine"

    openai_api_key: str = ""
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    embedding_batch_size: int = 64
    embedding_version: str = "1"
    embedding_backend: str = ""
    vector_backend: str = ""
    vector_store_path: str = "./data/vector_store.json"

    max_upload_size_mb: int = 25
    allowed_source_hosts: str = "localhost,127.0.0.1"
    allowed_file_roots: str = "./sample_data,./data"
    allow_private_source_hosts: bool = True
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 3
    http_retry_backoff_seconds: float = 0.5
    embedding_max_retries: int = 3
    max_redirects: int = 3
    max_response_bytes: int = 10_485_760

    api_auth_enabled: bool = False
    api_keys: str = ""

    chunk_size: int = 1500
    chunk_overlap: int = 150
    chunker_version: str = "section-window-v1"
    parser_version: str = "1"
    metadata_schema_version: str = "1.0.0"
    max_chunks_per_document: int = 2000
    min_document_chars: int = 1
    max_document_chars: int = 2_000_000

    worker_poll_interval_seconds: float = 2.0
    job_stale_seconds: int = 900
    max_job_retries: int = 3
    max_documents_per_run: int = 1000
    vector_upsert_batch_size: int = 100
    data_dir: str = "./data"

    ingestion_cron: str = "0 * * * *"
    api_sync_cron: str = "15 * * * *"
    quality_cron: str = "30 * * * *"
    drift_cron: str = "45 * * * *"
    reconciliation_cron: str = "*/15 * * * *"

    otel_exporter_otlp_endpoint: str = ""
    quality_min_chars: int = 1
    quality_max_chars: int = 2_000_000

    demo_api_token_env: str = "SAMPLE_API_TOKEN"

    @field_validator("log_level")
    @classmethod
    def _upper_level(cls, value: str) -> str:
        return value.upper()

    @property
    def max_upload_bytes(self) -> int:
        return self.max_upload_size_mb * 1024 * 1024

    @property
    def allowed_source_host_set(self) -> set[str]:
        return {
            item.strip().lower() for item in self.allowed_source_hosts.split(",") if item.strip()
        }

    @property
    def allowed_file_root_list(self) -> list[Path]:
        roots: list[Path] = []
        for item in self.allowed_file_roots.split(","):
            text = item.strip()
            if text:
                roots.append(Path(text).expanduser().resolve())
        return roots

    @property
    def api_key_set(self) -> set[str]:
        return {item.strip() for item in self.api_keys.split(",") if item.strip()}

    @property
    def resolved_embedding_backend(self) -> str:
        if self.embedding_backend:
            return self.embedding_backend.lower()
        return "openai" if self.openai_api_key else "fake"

    @property
    def resolved_vector_backend(self) -> str:
        if self.vector_backend:
            return self.vector_backend.lower()
        return "pinecone" if self.pinecone_api_key else "fake"

    @property
    def data_path(self) -> Path:
        path = Path(self.data_dir).expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path


@lru_cache
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
