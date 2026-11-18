# Document Ingestion & Retrieval Pipeline

Enterprise ETL and semantic retrieval platform. It ingests unstructured documents and JSON API records, normalizes them into a versioned canonical model, checks data quality and schema contracts, chunks the text, embeds the chunks, and indexes vectors for search. PostgreSQL is the system of record for documents, jobs, lineage, quality, and schema drift. Pinecone stores vectors only.

This is a single-tenant system. Tenant isolation is not implemented.

## Architecture

```mermaid
flowchart LR
  subgraph sources [Sources]
    FS[Filesystem]
    API[REST API]
    UP[Uploads]
  end
  subgraph app [Application]
    CONN[Connectors]
    PARSE[Parsers]
    ETL[ETL pipeline]
    JOBS[Job queue]
    SEARCH[Search service]
  end
  subgraph stores [Stores]
    PG[(PostgreSQL)]
    VEC[Vector store]
  end
  FS --> CONN
  API --> CONN
  UP --> JOBS
  CONN --> PARSE --> ETL
  JOBS --> ETL
  ETL --> PG
  ETL --> VEC
  SEARCH --> VEC
  SEARCH --> PG
  AIR[Airflow DAGs] --> JOBS
  APIAPP[FastAPI] --> JOBS
  APIAPP --> SEARCH
  WORKER[Worker] --> JOBS
```

Connectors discover and fetch bytes or JSON. Parsers turn bytes into structured blocks. The pipeline normalizes, validates, deduplicates, chunks, embeds, and writes an indexing outbox. A worker or an Airflow task runs the job. Search embeds the query with the same provider, queries the vector store, and hydrates titles, pages, and provenance from PostgreSQL.

PostgreSQL and the vector store do not share a transaction. A successful upsert is recorded in the outbox only after the vector call returns. Queries can still miss a vector until the index refreshes. Reconciliation replays pending and failed outbox rows.

## Technology stack

- Python 3.11+ (verified on 3.12.16)
- FastAPI and Pydantic v2
- SQLAlchemy 2.x and Alembic
- PostgreSQL 16
- Apache Airflow 2.10 DAGs
- Pinecone Python SDK (`pinecone`) behind a `VectorStore` adapter
- OpenAI embeddings behind an embedding-provider interface
- PyMuPDF, python-docx, BeautifulSoup, httpx
- pytest, Ruff, Docker Compose, GitHub Actions
- Structured JSON logs and Prometheus metrics

Offline development uses a hashed-bag embedding provider and either an in-memory or file-backed vector store. Those adapters exercise the pipeline. They are not a semantic model and they are not Pinecone.

## Prerequisites

- Python 3.11 or newer (`python3.12` is what the Makefile uses)
- PostgreSQL 16 reachable through `DATABASE_URL`
- Optional: Docker, for the Compose stack
- Optional: `OPENAI_API_KEY` and `PINECONE_API_KEY` for live embeddings and vectors

Docker was not available on the machine used to verify this repository. The Compose files are present and untested here.

## Local setup

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

Create the database. With Docker:

```bash
docker compose up -d postgres
```

Without Docker, any PostgreSQL 16 instance is fine. The verified local cluster used trust authentication:

```bash
export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
initdb -D ./data/postgres --auth=trust --username=ingestion
pg_ctl -D ./data/postgres -l /tmp/ingestion-postgres.log -o "-p 5432 -k /tmp" start
createdb -h localhost -p 5432 -U ingestion ingestion
export DATABASE_URL='postgresql+psycopg://ingestion@localhost:5432/ingestion'
```

`./data` is gitignored. Stop that cluster with `pg_ctl -D ./data/postgres stop`.

Generate the sample corpus:

```bash
.venv/bin/python scripts/generate_sample_data.py
```

## Environment variables

Copy `.env.example`. Placeholders only; do not commit `.env`.

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | SQLAlchemy URL, `postgresql+psycopg://...` |
| `PINECONE_API_KEY` | Pinecone credential. Empty keeps the offline vector backend |
| `PINECONE_INDEX_NAME` | Index name. Default `documents` |
| `PINECONE_NAMESPACE` | Namespace. Default `default` |
| `PINECONE_CLOUD`, `PINECONE_REGION` | Used only by `setup-pinecone` |
| `OPENAI_API_KEY` | OpenAI credential. Empty keeps the fake embedding backend |
| `EMBEDDING_MODEL` | Default `text-embedding-3-small` |
| `EMBEDDING_DIMENSIONS` | Default `1536`. Must match the Pinecone index |
| `EMBEDDING_BACKEND` | `fake` or `openai`. Default follows whether a key is set |
| `VECTOR_BACKEND` | `memory`, `fake`, or `pinecone`. `fake` is a JSON file shared by processes |
| `VECTOR_STORE_PATH` | File used when `VECTOR_BACKEND=fake` |
| `LOG_LEVEL` | Default `INFO` |
| `MAX_UPLOAD_SIZE_MB` | Upload limit. Default 25 |
| `ALLOWED_SOURCE_HOSTS` | Exact hosts or parent suffixes for HTTP sources |
| `ALLOWED_FILE_ROOTS` | Directories connectors and uploads may read |
| `ALLOW_PRIVATE_SOURCE_HOSTS` | Allow loopback and private IPs. Default true for local use |
| `API_AUTH_ENABLED` | When true, require `X-API-Key` |
| `API_KEYS` | Comma-separated keys. Empty with auth enabled fails closed |
| `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY` | Airflow local secrets. The example Fernet key is public and local-only |
| `INGESTION_CRON`, `API_SYNC_CRON`, `QUALITY_CRON`, `DRIFT_CRON`, `RECONCILIATION_CRON` | DAG schedules |
| `SAMPLE_API_TOKEN` | Optional bearer token for the sample API, read from the environment at fetch time |

`DATA_DIR` defaults to `./data` and is always an allowed file root. The API process creates it.

## Docker Compose

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f api
```

Services:

- `postgres` on port 5432, user and password `ingestion`, database `ingestion`. The init script also creates the `airflow` database.
- `api` on port 8000. The entrypoint waits for Postgres, runs `alembic upgrade head`, then uvicorn.
- `worker` claims pending jobs.
- `sample-api` on port 8090.
- `airflow-init`, `airflow-webserver` on port 8080, and `airflow-scheduler`.

Compose defaults `EMBEDDING_BACKEND` and `VECTOR_BACKEND` to `fake` so the stack starts without OpenAI or Pinecone. The Airflow image build and UI were not executed in this workspace.

```bash
docker compose down
```

## Database migrations

```bash
export DATABASE_URL='postgresql+psycopg://ingestion:ingestion@localhost:5432/ingestion'
.venv/bin/alembic upgrade head
```

The only revision is `0001_initial`. It creates sources, documents, versions, chunks, jobs, attempts, quality results, schema contracts, drift events, and the indexing outbox. A GIN index supports English full-text search on chunk text, and another GIN index supports JSONB containment on custom metadata.

## Airflow

DAGs live in `airflow/dags`. They import application services inside the task callable, so parsing a DAG does not open the database. Tasks return job ids, not document text.

| DAG | Schedule | What it runs |
| --- | --- | --- |
| `document_ingestion` | `INGESTION_CRON` (`0 * * * *`) | Filesystem sources, then a quality audit |
| `api_sync` | `API_SYNC_CRON` (`15 * * * *`) | REST sources |
| `incremental_processing` | manual | Filesystem, then REST, without force |
| `full_backfill` | manual | Force reprocess of filesystem and REST sources |
| `data_quality` | `QUALITY_CRON` (`30 * * * *`) | Corpus audit |
| `schema_drift` | `DRIFT_CRON` (`45 * * * *`) | Probe one page of each contracted API. The task fails when breaking drift is found |
| `failed_document_reprocessing` | manual | Retry failed documents |
| `pinecone_reconciliation` | `RECONCILIATION_CRON` (`*/15 * * * *`) | Replay the indexing outbox |

Scheduled tasks use retries, exponential backoff, an execution timeout, and `max_active_runs=1`. Catchup is off.

After Compose is up, open `http://localhost:8080` and sign in as `admin` / `admin`. That password is for local development. Trigger a DAG from the UI or:

```bash
docker compose exec airflow-webserver airflow dags trigger document_ingestion
```

Airflow tasks call `IngestionService.run_job` in-process and mark the job running immediately so the API worker does not claim the same row. Advisory locks use a session-level Postgres connection. Transaction-mode pooling such as PgBouncer in transaction mode will break those locks.

## FastAPI

```bash
export DATABASE_URL EMBEDDING_BACKEND=fake VECTOR_BACKEND=fake
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

- Health: `GET /health`
- Readiness: `GET /ready` checks Postgres and, when the backend is Pinecone, index stats
- OpenAPI: `http://127.0.0.1:8000/docs`

Use `VECTOR_BACKEND=fake` when the API and the worker are separate processes. `memory` is private to one process, which is what the tests use.

## Ingestion worker

```bash
.venv/bin/python -m app.workers.worker
```

The worker requeues stale running jobs, claims one pending job with `FOR UPDATE SKIP LOCKED`, and runs it. API uploads return `202` and stay pending until a worker or `python -m app.cli run-job JOB_ID` executes them. FastAPI `BackgroundTasks` are not used for ingestion.

Other commands:

```bash
.venv/bin/python -m app.cli wait-db
.venv/bin/python -m app.cli setup-pinecone
.venv/bin/python -m app.cli reconcile --limit 200
```

`setup-pinecone` is the only path that creates an index. Startup does not create one.

## Tests

```bash
export DATABASE_URL='postgresql+psycopg://ingestion@localhost:5432/ingestion'
export EMBEDDING_BACKEND=fake VECTOR_BACKEND=memory API_AUTH_ENABLED=false
.venv/bin/pytest
.venv/bin/ruff check app tests scripts airflow migrations
.venv/bin/ruff format --check app tests scripts airflow migrations
```

Without `DATABASE_URL`, integration tests skip. They are not a pass. Live OpenAI and Pinecone tests are not part of the default suite.

## Sample ingestion

Register the mixed directory and run the job in-process:

```bash
.venv/bin/python scripts/seed.py
.venv/bin/python scripts/demo.py
```

Or through the API, then the worker:

```bash
curl -sS -F 'upload=@sample_data/mixed/onboarding.txt;type=text/plain' \
  -H 'Idempotency-Key: onboarding-1' \
  http://127.0.0.1:8000/documents/upload
```

The same idempotency key and the same bytes return the original job. The same key with different bytes returns `409`, and the stored file is not overwritten. `DELETE /documents/{id}` requires `X-Confirm-Delete: true`.

Filesystem source:

```bash
curl -sS -X POST http://127.0.0.1:8000/sources \
  -H 'Content-Type: application/json' \
  -d '{"name":"policies","source_type":"filesystem","config":{"root_path":"./sample_data/mixed","detect_deletions":false}}'
```

Use the returned `source_id`:

```bash
curl -sS -X POST http://127.0.0.1:8000/sources/SOURCE_ID/ingest
```

## Sample search

Semantic search embeds the query and reads vectors, then loads chunk text and provenance from PostgreSQL.

```bash
curl -sS -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"security training before production access","top_k":5,"mode":"semantic"}'
```

`mode` may be `semantic`, `keyword`, or `hybrid`. Keyword search is PostgreSQL `plainto_tsquery` against indexed current-version chunks. Hybrid combines the two lists with reciprocal rank fusion (`k=60`). The `score_type` field is `cosine`, `ts_rank`, or `rrf`. Those numbers are not on the same scale, and none of them is exact-match search.

Pagination over-fetches `offset + top_k` because Pinecone has no offset. The window is capped at 100.

## Metadata filtering

Applied in the vector query when the backend is Pinecone or the fake store:

- `source_id`
- `source_type`
- `document_type`
- document id list produced by a PostgreSQL prefilter
- `source_modified_at` as a numeric range

Applied in PostgreSQL before the vector query:

- title (`ILIKE`, wildcards escaped)
- custom metadata containment (`custom_metadata @> filters`)
- processing status and date bounds on the document list endpoints

Custom metadata keys must match `^[A-Za-z][A-Za-z0-9_]{0,63}$`. Values are scalar equality only. More than 200 prefilter ids returns `422` rather than building an unbounded `$in` filter.

```bash
curl -sS -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"retention","top_k":5,"document_type":"pdf","metadata_filters":{"department":"legal"}}'
```

```bash
curl -sS 'http://127.0.0.1:8000/documents?document_type=pdf&processing_status=completed&limit=20'
```

## Schema drift

The sample contract expects `id`, `title`, `body`, and `updated_at`, plus an integer `priority`. Start the sample API and point a source at the drifted route:

```bash
.venv/bin/python scripts/sample_api.py
```

```bash
curl -sS -X POST http://127.0.0.1:8000/sources \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"drift-demo",
    "source_type":"rest_api",
    "config":{
      "url":"http://127.0.0.1:8090/drift/records",
      "records_path":"items",
      "id_field":"id",
      "pagination":{"mode":"page","page_param":"page","page_size_param":"page_size","page_size":2},
      "schema_contract":{
        "name":"sample_records",
        "version":"1.0.0",
        "schema":{
          "type":"object",
          "required":["id","title","body","updated_at"],
          "properties":{
            "id":{"type":"string"},
            "title":{"type":"string"},
            "body":{"type":"string"},
            "updated_at":{"type":"timestamp"},
            "priority":{"type":"integer","nullable":true}
          },
          "additional_properties":"informational"
        }
      }
    }
  }'
```

Breaking changes (a required field removed, an integer that becomes a string, an object that becomes an array, or an unparseable timestamp) quarantine that record and keep it out of the index. Extra optional fields are informational and do not stop the record. A previously indexed version stays searchable if a later revision is quarantined.

```bash
curl -sS http://127.0.0.1:8000/schema-drift/events
```

`GET /records` on the sample API is the compatible payload. `sample_data/api/drift_record.json` is the same shape on disk.

## Failure recovery

One bad file does not fail the batch. Empty text, malformed JSON, and a corrupted PDF are quarantined. Unsupported extensions are skipped. The job finishes as `completed_with_errors` when any document is quarantined or failed and at least one outcome was recorded.

Vector writes go through `indexing_outbox`. If the upsert fails, the chunk stays unindexed and the outbox row stays pending or failed. Replay with:

```bash
.venv/bin/python -m app.cli reconcile --limit 200
curl -sS -X POST http://127.0.0.1:8000/jobs/JOB_ID/retry \
  -H 'Content-Type: application/json' \
  -d '{"include_quarantined": false}'
```

`scripts/demo.py` forces one upsert to fail and then flushes the outbox. On the verification run that flush reported `upserted: 2`, `deleted: 0`, `failed: 0`.

Quarantine is a permanent validation outcome. Retry it only with `include_quarantined: true`. Timeouts, rate limits, and database or vector outages are retryable.

## Known limitations

- Single tenant. Auth is an API key gate, not per-user authorization. `/health`, `/ready`, `/metrics`, and the OpenAPI routes stay open when auth is enabled. Restrict `/metrics` on the network.
- Fake embeddings are hashed token bags. Similarity on that backend is not semantic quality, and precision or recall against a labeled set was not measured.
- Live OpenAI and Pinecone calls were not executed. No credentials were configured.
- Docker and the Airflow UI were not started here. DAG modules are source-checked by unit tests; Airflow itself is not installed in the test environment.
- The file-backed vector store uses a process lock, not a cross-process file lock. Two writers can race. Use one writer, or Pinecone, for anything beyond a local demo.
- Pinecone serverless reads are eventually consistent. The application does not wait for a query to observe an upsert.
- Chunk size and overlap are characters. Token counts use `cl100k_base` when tiktoken can load it.
- PyMuPDF is AGPL. The PDF parser is isolated so it can be replaced.
- Filesystem deletion detection tombstones documents that disappear from a completed listing. It is on by default for filesystem sources and off for APIs. A truncated run or a `max_documents_per_run` cap does not delete.
- Semantic pagination cannot walk an arbitrary result set. The window stops at 100.
- Advisory locks require a session-mode database connection.

## Architectural trade-offs

- One deployable Python package instead of separate microservices. Airflow, the API, and the worker import the same services.
- Synchronous SQLAlchemy so Airflow tasks and the worker share one session style.
- An outbox instead of a two-phase commit. The index can lag the database, and reconciliation is an explicit operation.
- Chunk text stays in PostgreSQL. Pinecone metadata is a small filter payload, so a vector hit is useless until hydration succeeds. Missing or stale vectors are dropped.
- Unchanged content is skipped by a fingerprint of the content hash, parser and chunker versions, chunk size, overlap, embedding model, and metadata schema version. `force` re-embeds the current version.
- A new version is indexed before the previous version's vectors are deleted, so search keeps some coverage during the swap.
- API jobs are durable rows. The request returns when the job is queued, not when vectors exist.
