"""Airflow-facing task functions.

DAGs call these and return identifiers only. Document bodies never enter XCom.
"""

from __future__ import annotations

from app.core.logging import configure_logging
from app.services.context import build_context
from app.services.indexing_service import IndexingService
from app.services.ingestion_service import IngestionService
from app.services.quality_service import QualityService


def _service() -> IngestionService:
    configure_logging()
    return IngestionService(build_context())


def run_scheduled_ingestion() -> dict:
    return _ingest(source_type="filesystem", trigger="schedule", force=False)


def run_api_sync() -> dict:
    return _ingest(source_type="rest_api", trigger="schedule", force=False)


def run_incremental() -> dict:
    filesystem = _ingest(source_type="filesystem", trigger="schedule", force=False)
    api = _ingest(source_type="rest_api", trigger="schedule", force=False)
    return {"filesystem": filesystem, "api": api}


def run_backfill() -> dict:
    filesystem = _ingest(source_type="filesystem", trigger="backfill", force=True)
    api = _ingest(source_type="rest_api", trigger="backfill", force=True)
    return {"filesystem": filesystem, "api": api}


def run_quality_audit() -> dict:
    return QualityService(build_context()).audit()


def run_schema_probe() -> dict:
    summary = QualityService(build_context()).probe_contracts()
    if summary["breaking"]:
        raise RuntimeError(f"Breaking schema drift detected for {summary['breaking']} source(s)")
    return summary


def run_reprocessing() -> dict:
    service = _service()
    jobs, _ = service.list_jobs(status="completed_with_errors", source_id=None, limit=20, offset=0)
    failed_jobs, _ = service.list_jobs(status="failed", source_id=None, limit=20, offset=0)
    created = []
    for job in jobs + failed_jobs:
        try:
            created.append(service.retry_job(job["job_id"], include_quarantined=False)["job_id"])
        except Exception:
            continue
    for job_id in created:
        service.run_job(job_id)
    return {"retry_job_ids": [str(job_id) for job_id in created]}


def run_reconciliation() -> dict:
    context = build_context()
    stats = IndexingService(context).flush(limit=500, raise_on_error=False)
    if stats["failed"]:
        from app.observability.metrics import RECONCILIATION_FAILURES

        RECONCILIATION_FAILURES.inc(stats["failed"])
    return stats


def _ingest(*, source_type: str, trigger: str, force: bool) -> dict:
    service = _service()
    from app.database.repositories import SourceRepository
    from app.database.session import session_scope

    with session_scope(service.context.settings) as session:
        sources = SourceRepository(session).list_active(source_type=source_type)
        source_ids = [source.id for source in sources]
    job_ids = []
    for source_id in source_ids:
        job = service.enqueue_source(
            source_id, trigger=trigger, force=force, start_immediately=True
        )
        finished = service.run_job(job["job_id"])
        job_ids.append(str(finished["job_id"]))
    return {"job_ids": job_ids, "source_type": source_type}
