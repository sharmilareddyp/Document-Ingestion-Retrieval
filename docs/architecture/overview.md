# Architecture

## Boundaries

| Package | Responsibility |
| --- | --- |
| `app/connectors` | Discover and fetch. No parsing |
| `app/parsers` | Bytes or JSON to blocks. No I/O beyond the bytes they are given |
| `app/processing` | Normalize, hash, chunk, quality, schema drift, state transitions |
| `app/embeddings` | `embed_texts` / `embed_query` |
| `app/vectorstores` | Upsert, query, delete. Pinecone and fakes |
| `app/services` | Jobs, documents, indexing, quality. Own the database transactions |
| `app/retrieval` | Search independent of FastAPI |
| `app/api` | HTTP, validation, status codes |
| `app/workers` | Claim pending jobs |
| `app/orchestration` | Functions Airflow calls. Return ids only |
| `airflow/dags` | Schedules and task dependencies |

## Identity and versions

- Document id: UUID5 of `source_id` plus the logical key. Namespace `b6c1c2a0-7e4d-4f0a-9c1a-2a6d8e5b7c31`.
- Chunk id: UUID5 of document id, version, chunk index, and chunk hash. Namespace `c2e4a8b1-1d55-4a6e-8f0c-91a2b3c4d5e6`.
- Vector id: the chunk id as text.
- Fingerprint: SHA-256 of the content hash, parser version, chunker version, chunk size, overlap, embedding model, embedding version, and metadata schema version.
- Same fingerprint and `indexing_status=indexed` skips the document.
- Same fingerprint but not indexed, or `force`, reindexes that version and re-embeds when forced.
- A changed fingerprint creates the next version. The first published version is 1.

## Consistency

Order of a successful new version:

1. Persist the version and chunks in PostgreSQL and commit.
2. Embed the chunk texts.
3. Insert outbox rows that still hold the vectors, and commit.
4. Upsert the vectors.
5. Mark chunks `indexed_at` and the outbox rows completed.
6. Delete vectors that belonged to the previous version or to replaced chunks.

A crash at any step leaves a durable row: a job status, a document lock, or an outbox record. The worker can resume a pending job. Reconciliation replays outbox upserts and deletes. If an upsert payload lost its vectors, reconciliation embeds the stored chunk text again.

Do not treat the two stores as atomic. A query issued immediately after a successful upsert may miss the vector.

## Locks

- `pg_try_advisory_lock` on the job id prevents two runners from executing the same job. The connection commits after the lock so the lock is session-scoped and the transaction is not held open.
- Each document row stores `lock_job_id`. A second job skips that document instead of writing the same version. A lock older than `job_stale_seconds` can be taken over.

## Deletion

`DELETE /documents/{id}` with `X-Confirm-Delete: true` tombstones the document, sets indexing status to `deleting`, and enqueues vector deletes. Search drops rows with `deleted_at` set and chunks whose version is not the current version.

Filesystem connectors detect deletions only after a complete discovery that was not capped and was not a retry limited to specific keys. API sources do not delete by default, because a page of an API is not a full inventory.

## Security assumptions

- Credentials come from the environment. Source config may name an env var (`auth_token_env`). It must not contain an `Authorization` header.
- HTTP sources allow only `http` and `https`, reject userinfo, require an allowlisted host, re-check every redirect, and block link-local, metadata, multicast, and non-loopback reserved addresses. Loopback is allowed only when `ALLOW_PRIVATE_SOURCE_HOSTS` is true.
- File reads must stay under `ALLOWED_FILE_ROOTS` or `DATA_DIR` after `resolve()`.
- Extracted text is stored and embedded. It is never executed.
- When `API_AUTH_ENABLED=true`, every route except health, readiness, metrics, and OpenAPI requires `X-API-Key`. An empty key list rejects all protected calls.

## Embedding and vector backends

| Setting | Behavior |
| --- | --- |
| `EMBEDDING_BACKEND=fake` | Hashed-bag vectors. Deterministic. Not semantic |
| `EMBEDDING_BACKEND=openai` | `text-embedding-3-small` by default, with the configured dimension |
| `VECTOR_BACKEND=memory` | Process-local. Tests and one-process demos |
| `VECTOR_BACKEND=fake` | JSON file at `VECTOR_STORE_PATH`. API and worker can share it |
| `VECTOR_BACKEND=pinecone` | Official client. Fails if the key or index is missing |

Selecting Pinecone without a key is an error. The index is created only by `python -m app.cli setup-pinecone`.
