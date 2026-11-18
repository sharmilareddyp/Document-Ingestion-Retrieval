# Metadata schema

Revision `0001_initial`. Metadata schema version on documents is `1.0.0`. PostgreSQL is authoritative. Pinecone, when configured, stores only the filter fields listed at the bottom.

## Entities

### sources

Registered filesystem, JSON-file, REST, or upload endpoints. `name` is unique. `config` is JSONB and must not contain secrets. A schema contract is stored in `schema_contracts`, not inside the source config that is logged.

### documents

One current row per source and logical key (`uq` on `source_id, logical_key`).

Stored fields include `id`, `source_id`, `source_type`, `source_uri`, `original_filename`, `document_type`, `title`, `content_hash`, `current_version`, `source_created_at`, `source_modified_at`, `ingested_at`, `last_processed_at`, `processing_status`, `extraction_status`, `indexing_status`, `chunk_count`, `metadata_schema_version`, and `custom_metadata`.

`current_version = 0` means no published version. A tombstone sets `deleted_at` and does not physically remove the row until an operator chooses to.

### document_versions

Unique on `(document_id, version)`. Holds the content hash, processing fingerprint, parser and chunker versions, extraction status, and a source payload when the JSON is at most 64 KiB. Larger payloads keep a hash marker instead of the body.

### document_chunks

Unique on `(document_id, document_version, chunk_index)`. Holds chunk text, section title, page, offsets, token count, chunk hash, embedding model and version, and `indexed_at`. Text lives here, not in the vector store.

An expression GIN index on `to_tsvector('english', chunk_text)` supports keyword search. That index is created by Alembic, not by `metadata.create_all`.

### ingestion_jobs

`job_id`, `source_id`, `trigger_type`, `started_at`, `completed_at`, `status`, document counts, and `error_summary`. `idempotency_key` is unique when present. Postgres allows many null keys. `config_snapshot` records the request hash used for idempotency.

Job status is `pending`, `running`, `completed`, `completed_with_errors`, or `failed`.

### job_documents and processing_attempts

One outcome per logical key in a job: `processed`, `skipped`, `failed`, or `quarantined`. Attempts record the stage, error category, and retryability.

### data_quality_results

`check_name`, document or source id, `expected`, `actual`, `severity` (`info`, `warning`, `error`), `passed`, `created_at`, and `details`.

### schema_contracts and schema_drift_events

Contracts are versioned by `(name, version)`. Drift rows store severity (`informational`, `warning`, `breaking`), drift type, field path, expected and actual shapes, and a signature used to suppress duplicates for one hour.

### indexing_outbox

Unique on `(vector_id, operation, document_version)`. Operations are `upsert` and `delete`. Status is `pending`, `processing`, `completed`, or `failed`. Upsert payloads contain vectors until the call succeeds, then the payload is cleared.

## Processing states

`pending → fetching → extracting → normalizing → validating → chunking → embedding → indexing → completed`

`failed`, `quarantined`, `skipped`, and `tombstoned` are terminal for that attempt. `tombstoned` cannot move to `completed`. The legal map is `ALLOWED_TRANSITIONS` in `app/domain/enums.py`.

## Filters

| Filter | Where it runs |
| --- | --- |
| `source_id`, `source_type`, `document_type` | Vector metadata |
| Source modified time range | Vector metadata as `source_modified_ts` |
| Document ids from a title or custom-metadata prefilter | Vector metadata `$in`, capped at 200 |
| Title, processing status, ingested date range | PostgreSQL document queries |
| Custom metadata | PostgreSQL `@>` prefilter, then vector `$in` |
| Section title and page | Returned from PostgreSQL after hydration, not used as a vector predicate |
| Full text | PostgreSQL `tsvector` in `keyword` and `hybrid` modes only |

Vector metadata is limited to short strings: source, document type, title, version, section, and page. Chunk text is not copied into the vector store.
