# Operations

## A job stays pending

The API only inserts the job. Start a worker:

```bash
.venv/bin/python -m app.workers.worker
```

Or run one id:

```bash
.venv/bin/python -m app.cli run-job JOB_ID
```

`GET /jobs/{job_id}` shows `pending`, `running`, or a terminal status and the document counts.

## A running job was interrupted

The document lock and the advisory lock are released when the process exits. The worker's `requeue_stale_jobs` returns a `running` job to `pending` after `job_stale_seconds` when the heartbeat is old or missing. Starting the worker again is the recovery step. Do not delete the job row.

## Vectors and rows disagree

`GET /documents/{id}` may show `indexing_status=failed` or chunks with a null `indexed_at` while an outbox row is `pending` or `failed`.

```bash
.venv/bin/python -m app.cli reconcile --limit 500
```

The same path is the `pinecone_reconciliation` DAG. Reconciliation is idempotent: vector ids are deterministic, and a second upsert of the same id replaces the vector. The verified failure demo flushed the outbox and reported `failed: 0`.

`POST /jobs/{job_id}/retry` creates a new job for logical keys that failed. Quarantined keys are included only when the body says `include_quarantined: true`.

## A document was quarantined

Read `GET /quality/reports?document_id=...` and the job's document outcomes. Typical reasons:

- empty extracted text
- text outside the configured length bounds
- malformed JSON or a JSON array passed to the single-object parser
- a PDF that does not open
- breaking schema drift

Fix the source or the contract. Quarantine does not unpublish an older indexed version.

## Schema drift

`GET /schema-drift/events?severity=breaking` lists records that were refused. The `schema_drift` DAG fails the task when a probe sees breaking drift, which is intentional. Informational extra fields do not fail the task.

## Search returns nothing

- The worker and the API must share a vector backend. `memory` is not shared. Use `fake` with the same `VECTOR_STORE_PATH`, or Pinecone.
- The document's current chunks need `indexed_at`.
- Tombstoned documents and chunks from old versions are hidden on purpose.
- A title or metadata prefilter that matches more than 200 documents returns 422.
- Fake embeddings do not rank language the way `text-embedding-3-small` does. An empty or odd ranking on `EMBEDDING_BACKEND=fake` is expected.

## Readiness is 503

`/ready` fails when PostgreSQL is down. It also fails when `VECTOR_BACKEND=pinecone` and `describe_index_stats` fails. Fake and memory backends report the vector store as ok without a network call.

## Pinecone index missing

```bash
.venv/bin/python -m app.cli setup-pinecone
```

The command creates a serverless index when the name is absent and refuses to continue when the dimension does not equal `EMBEDDING_DIMENSIONS`. It does not run on API startup.

## Path and URL rejects

- `UnsafePathError`: the path escaped `ALLOWED_FILE_ROOTS` and `DATA_DIR`.
- `UnsafeURLError`: scheme, credentials, host allowlist, DNS failure, or a blocked address. Redirects are checked one hop at a time, up to three.
- `415`: the extension is not pdf, docx, txt, html, htm, or json.
- `413`: the upload exceeds `MAX_UPLOAD_SIZE_MB`.
- `401`: `API_AUTH_ENABLED=true` and the key is missing or not in `API_KEYS`.

## Logs

Each line is JSON with `timestamp`, `level`, `service`, `job_id`, `document_id`, `request_id`, `operation`, and `error_category` when present. Document bodies and secret-like keys are not written. `GET /metrics` exposes Prometheus counters for documents, drift, quality failures, embedding calls, estimated tokens, vector failures, search latency, and pending outbox rows.

## What this runbook does not claim

Live OpenAI, live Pinecone, and the Airflow webserver were not exercised while writing this guide. Follow the command output you get. Do not treat the offline benchmark numbers in `benchmarks/README.md` as production throughput.
