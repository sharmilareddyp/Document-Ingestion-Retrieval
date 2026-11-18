"""Background worker for API-created ingestion jobs.

Airflow executes scheduled jobs inside its tasks. This process claims pending rows
with SKIP LOCKED so an API upload is durable across restarts and is not tied to the
FastAPI process.
"""

from __future__ import annotations

import signal
import time

from app.core.logging import configure_logging, get_logger, log_extra
from app.database.repositories import JobRepository
from app.database.session import session_scope, wait_for_database
from app.services.context import build_context
from app.services.ingestion_service import IngestionService

logger = get_logger(__name__)


def worker_once(context=None) -> bool:
    context = context or build_context()
    service = IngestionService(context)
    service.requeue_stale_jobs()
    with session_scope(context.settings) as session:
        job = JobRepository(session).claim_next()
        job_id = None if job is None else job.id
    if job_id is None:
        return False
    logger.info(
        "claimed ingestion job", extra=log_extra(operation="worker_claim", job_id=str(job_id))
    )
    service.run_job(job_id)
    return True


def serve() -> None:
    configure_logging()
    context = build_context()
    wait_for_database(context.settings)
    stopped = False

    def _stop(signum, _frame) -> None:
        nonlocal stopped
        stopped = True
        logger.info(
            "worker shutdown requested", extra=log_extra(operation="worker_stop", signal=signum)
        )

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    while not stopped:
        worked = worker_once(context)
        if not worked:
            time.sleep(context.settings.worker_poll_interval_seconds)


if __name__ == "__main__":
    serve()
