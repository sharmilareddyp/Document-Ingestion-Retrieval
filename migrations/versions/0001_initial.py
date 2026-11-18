"""Initial relational schema for ingestion metadata and the vector outbox.

Revision ID: 0001_initial
Revises:
Create Date: 2026-03-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSONB = postgresql.JSONB
UUID = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("name", sa.String(200), nullable=False, unique=True),
        sa.Column("source_type", sa.String(32), nullable=False),
        sa.Column("config", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("status", sa.String(32), nullable=False, server_default="active"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_table(
        "documents",
        sa.Column("id", UUID, primary_key=True),
        sa.Column(
            "source_id", UUID, sa.ForeignKey("sources.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column("logical_key", sa.String(1024), nullable=False),
        sa.Column("source_type", sa.String(32), nullable=False),
        sa.Column("source_uri", sa.Text(), nullable=False),
        sa.Column("original_filename", sa.String(500)),
        sa.Column("document_type", sa.String(32), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("content_hash", sa.String(64)),
        sa.Column("processing_fingerprint", sa.String(64)),
        sa.Column("current_version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source_created_at", sa.DateTime(timezone=True)),
        sa.Column("source_modified_at", sa.DateTime(timezone=True)),
        sa.Column(
            "ingested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("last_processed_at", sa.DateTime(timezone=True)),
        sa.Column("processing_status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("extraction_status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("indexing_status", sa.String(32), nullable=False, server_default="not_indexed"),
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata_schema_version", sa.String(32), nullable=False, server_default="1.0.0"),
        sa.Column("custom_metadata", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("raw_uri", sa.Text()),
        sa.Column("media_type", sa.String(200)),
        sa.Column("byte_size", sa.Integer()),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text()),
        sa.Column("error_category", sa.String(64)),
        sa.Column("lock_job_id", UUID),
        sa.Column("lock_acquired_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("source_id", "logical_key", name="uq_documents_source_logical_key"),
    )
    op.create_index("ix_documents_processing_status", "documents", ["processing_status"])
    op.create_index("ix_documents_document_type", "documents", ["document_type"])
    op.create_index("ix_documents_content_hash", "documents", ["content_hash"])
    op.create_index("ix_documents_ingested_at", "documents", ["ingested_at"])
    op.create_index("ix_documents_source_modified_at", "documents", ["source_modified_at"])
    op.create_index("ix_documents_source_id", "documents", ["source_id"])
    op.create_index(
        "ix_documents_custom_metadata",
        "documents",
        ["custom_metadata"],
        postgresql_using="gin",
    )
    op.create_table(
        "document_versions",
        sa.Column("id", UUID, primary_key=True),
        sa.Column(
            "document_id", UUID, sa.ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False, server_default=""),
        sa.Column("parser_version", sa.String(64), nullable=False),
        sa.Column("chunker_version", sa.String(64), nullable=False),
        sa.Column("embedding_model", sa.String(128), nullable=False),
        sa.Column("embedding_version", sa.String(64), nullable=False),
        sa.Column("metadata_schema_version", sa.String(32), nullable=False),
        sa.Column("source_payload", JSONB),
        sa.Column("warnings", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("document_id", "version", name="uq_document_version"),
    )
    op.create_index("ix_versions_document", "document_versions", ["document_id"])
    op.create_table(
        "document_chunks",
        sa.Column("id", UUID, primary_key=True),
        sa.Column(
            "document_id", UUID, sa.ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "version_id",
            UUID,
            sa.ForeignKey("document_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("document_version", sa.Integer(), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("chunk_text", sa.Text(), nullable=False),
        sa.Column("section_title", sa.String(500)),
        sa.Column("page_number", sa.Integer()),
        sa.Column("start_offset", sa.Integer()),
        sa.Column("end_offset", sa.Integer()),
        sa.Column("token_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("chunk_hash", sa.String(64), nullable=False),
        sa.Column("embedding_model", sa.String(128)),
        sa.Column("embedding_version", sa.String(64)),
        sa.Column("vector_id", sa.String(64), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "document_id", "document_version", "chunk_index", name="uq_chunk_position"
        ),
    )
    op.create_index(
        "ix_chunks_document_version", "document_chunks", ["document_id", "document_version"]
    )
    op.create_index("ix_chunks_chunk_hash", "document_chunks", ["chunk_hash"])
    op.execute(
        "CREATE INDEX ix_chunks_text_search ON document_chunks "
        "USING GIN (to_tsvector('english', chunk_text))"
    )
    op.create_table(
        "ingestion_jobs",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("source_id", UUID, sa.ForeignKey("sources.id", ondelete="SET NULL")),
        sa.Column("retry_of_job_id", UUID, sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL")),
        sa.Column("trigger_type", sa.String(32), nullable=False),
        sa.Column("idempotency_key", sa.String(200)),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("stage", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("total_documents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processed_documents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_documents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_documents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quarantined_documents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_summary", sa.Text()),
        sa.Column("config_snapshot", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("force", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_jobs_status_created", "ingestion_jobs", ["status", "created_at"])
    op.create_index("ix_jobs_idempotency_key", "ingestion_jobs", ["idempotency_key"], unique=True)
    op.create_table(
        "job_documents",
        sa.Column("id", UUID, primary_key=True),
        sa.Column(
            "job_id", UUID, sa.ForeignKey("ingestion_jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("document_id", UUID, sa.ForeignKey("documents.id", ondelete="SET NULL")),
        sa.Column("logical_key", sa.String(1024), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("stage", sa.String(32)),
        sa.Column("error_category", sa.String(64)),
        sa.Column("error_message", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("job_id", "logical_key", name="uq_job_document_key"),
    )
    op.create_index("ix_job_documents_job", "job_documents", ["job_id"])
    op.create_table(
        "processing_attempts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("job_id", UUID, sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL")),
        sa.Column("document_id", UUID, sa.ForeignKey("documents.id", ondelete="CASCADE")),
        sa.Column("stage", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("error_category", sa.String(64)),
        sa.Column("error_message", sa.Text()),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_attempts_document", "processing_attempts", ["document_id", "created_at"])
    op.create_table(
        "data_quality_results",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("job_id", UUID, sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL")),
        sa.Column("document_id", UUID, sa.ForeignKey("documents.id", ondelete="CASCADE")),
        sa.Column("source_id", UUID, sa.ForeignKey("sources.id", ondelete="SET NULL")),
        sa.Column("check_name", sa.String(120), nullable=False),
        sa.Column("expected", sa.Text(), nullable=False),
        sa.Column("actual", sa.Text(), nullable=False),
        sa.Column("severity", sa.String(32), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("details", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_quality_job", "data_quality_results", ["job_id"])
    op.create_index("ix_quality_document", "data_quality_results", ["document_id"])
    op.create_index("ix_quality_source", "data_quality_results", ["source_id"])
    op.create_table(
        "schema_contracts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("source_id", UUID, sa.ForeignKey("sources.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("version", sa.String(32), nullable=False),
        sa.Column("contract", JSONB, nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("name", "version", name="uq_schema_contract_version"),
    )
    op.create_table(
        "schema_drift_events",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("source_id", UUID, sa.ForeignKey("sources.id", ondelete="SET NULL")),
        sa.Column("job_id", UUID, sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL")),
        sa.Column("schema_name", sa.String(200), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("severity", sa.String(32), nullable=False),
        sa.Column("drift_type", sa.String(64), nullable=False),
        sa.Column("field_path", sa.String(500), nullable=False),
        sa.Column("expected", sa.Text(), nullable=False),
        sa.Column("actual", sa.Text(), nullable=False),
        sa.Column("record_key", sa.String(1024)),
        sa.Column("signature", sa.String(64), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_drift_source_created", "schema_drift_events", ["source_id", "created_at"])
    op.create_index("ix_drift_signature", "schema_drift_events", ["signature"])
    op.create_table(
        "indexing_outbox",
        sa.Column("id", UUID, primary_key=True),
        sa.Column(
            "document_id", UUID, sa.ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("chunk_id", UUID, sa.ForeignKey("document_chunks.id", ondelete="CASCADE")),
        sa.Column("document_version", sa.Integer(), nullable=False),
        sa.Column("vector_id", sa.String(64), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("payload", JSONB),
        sa.Column("error_message", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "vector_id", "operation", "document_version", name="uq_outbox_vector_op"
        ),
    )
    op.create_index("ix_outbox_status_created", "indexing_outbox", ["status", "created_at"])
    op.create_index("ix_outbox_document", "indexing_outbox", ["document_id"])


def downgrade() -> None:
    op.drop_table("indexing_outbox")
    op.drop_table("schema_drift_events")
    op.drop_table("schema_contracts")
    op.drop_table("data_quality_results")
    op.drop_table("processing_attempts")
    op.drop_table("job_documents")
    op.drop_table("ingestion_jobs")
    op.drop_index("ix_chunks_text_search", table_name="document_chunks")
    op.drop_table("document_chunks")
    op.drop_table("document_versions")
    op.drop_table("documents")
    op.drop_table("sources")
