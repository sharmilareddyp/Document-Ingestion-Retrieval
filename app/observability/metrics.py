"""Prometheus metrics. Label values are low-cardinality statuses, never document text."""

from prometheus_client import Counter, Gauge, Histogram

DOCUMENTS = Counter("ingestion_documents_total", "Documents by terminal outcome", ["status"])
EXTRACTION_FAILURES = Counter("ingestion_extraction_failures_total", "Extraction failures")
DRIFT_EVENTS = Counter("schema_drift_events_total", "Schema drift events", ["severity"])
QUALITY_FAILURES = Counter("data_quality_failures_total", "Failed data quality checks")
EMBEDDING_REQUESTS = Counter("embedding_requests_total", "Embedding API batches")
EMBEDDING_ERRORS = Counter("embedding_errors_total", "Embedding errors")
EMBEDDING_TOKENS = Counter("embedding_tokens_estimated_total", "Estimated embedding tokens")
VECTOR_FAILURES = Counter("vector_index_failures_total", "Vector index failures")
RECONCILIATION_FAILURES = Counter(
    "indexing_reconciliation_failures_total", "Reconciliation failures"
)
SEARCH_REQUESTS = Counter("search_requests_total", "Semantic and keyword searches", ["mode"])
SEARCH_LATENCY = Histogram(
    "search_latency_seconds",
    "Search latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
INGESTION_DURATION = Histogram(
    "ingestion_duration_seconds",
    "End-to-end ingestion job duration",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 15, 60, 180, 600),
)
OUTBOX_PENDING = Gauge("indexing_outbox_pending", "Pending or failed vector operations")
DISCOVERED = Counter("ingestion_documents_discovered_total", "Documents and rejections discovered")
