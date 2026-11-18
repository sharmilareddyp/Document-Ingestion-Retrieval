# Benchmarks

Run the measurement from the repository root after migrations:

```bash
export DATABASE_URL='postgresql+psycopg://ingestion@localhost:5432/ingestion'
export EMBEDDING_BACKEND=fake VECTOR_BACKEND=memory
.venv/bin/python scripts/benchmark.py
```

`BENCH_DOCS` defaults to 20. The script writes temporary text files, ingests them once, and times ten semantic queries. It prints only values it measured. It does not call OpenAI or Pinecone.

## Measurement from 2026-11-17

Host: local macOS, Python 3.12.16, PostgreSQL 16.16 on localhost, fake hashed-bag embeddings, in-memory vectors, 20 synthetic documents.

```text
documents: 20
processed: 20
elapsed_seconds: 0.2545
documents_per_minute: 4714.72
chunks_total: 20
chunks_per_document: 1.0
search_p50_seconds: 0.00263
search_p95_seconds: 0.003802
```

This is one process ingesting tiny text files. It is not evidence of million-document scale, embedding-provider throughput, or Pinecone latency.

Precision@k and recall@k were not computed. The offline embedding provider is not a semantic model, and no labeled evaluation set was scored against OpenAI embeddings.
