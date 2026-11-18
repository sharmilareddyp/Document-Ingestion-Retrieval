"""Ingestion job persistence and SKIP LOCKED claiming."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.database.models import IngestionJob, JobDocument, ProcessingAttempt


class JobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, job: IngestionJob) -> IngestionJob:
        self.session.add(job)
        self.session.flush()
        return job

    def get(self, job_id: UUID) -> IngestionJob | None:
        return self.session.get(IngestionJob, job_id)

    def find_by_idempotency(self, key: str) -> IngestionJob | None:
        return self.session.scalar(select(IngestionJob).where(IngestionJob.idempotency_key == key))

    def list(
        self,
        *,
        status: str | None = None,
        source_id: UUID | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[IngestionJob], int]:
        stmt = select(IngestionJob)
        if status:
            stmt = stmt.where(IngestionJob.status == status)
        if source_id:
            stmt = stmt.where(IngestionJob.source_id == source_id)
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = list(
            self.session.scalars(
                stmt.order_by(IngestionJob.created_at.desc()).limit(limit).offset(offset)
            ).all()
        )
        return rows, int(total)

    def claim_next(self) -> IngestionJob | None:
        job = self.session.scalar(
            select(IngestionJob)
            .where(IngestionJob.status == "pending")
            .order_by(IngestionJob.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if job is None:
            return None
        now = datetime.now(UTC)
        job.status = "running"
        job.stage = "fetching"
        job.started_at = job.started_at or now
        job.heartbeat_at = now
        self.session.flush()
        return job

    def touch(self, job: IngestionJob, *, stage: str | None = None) -> None:
        job.heartbeat_at = datetime.now(UTC)
        if stage:
            job.stage = stage
        self.session.flush()

    def requeue_stale(self, stale_before: datetime, max_retries: int) -> tuple[int, int]:
        rows = list(
            self.session.scalars(
                select(IngestionJob).where(
                    IngestionJob.status == "running",
                    or_(
                        IngestionJob.heartbeat_at < stale_before,
                        (
                            IngestionJob.heartbeat_at.is_(None)
                            & (IngestionJob.created_at < stale_before)
                        ),
                    ),
                )
            ).all()
        )
        requeued = 0
        failed = 0
        for job in rows:
            if job.retry_count >= max_retries:
                job.status = "failed"
                job.completed_at = datetime.now(UTC)
                job.error_summary = "Job exceeded retry limit after worker interruption"
                failed += 1
            else:
                job.retry_count += 1
                job.status = "pending"
                job.stage = "pending"
                requeued += 1
        self.session.flush()
        return requeued, failed

    def add_document_result(self, row: JobDocument) -> None:
        existing = self.session.scalar(
            select(JobDocument).where(
                JobDocument.job_id == row.job_id, JobDocument.logical_key == row.logical_key
            )
        )
        if existing:
            existing.document_id = row.document_id
            existing.outcome = row.outcome
            existing.stage = row.stage
            existing.error_category = row.error_category
            existing.error_message = row.error_message
            self.session.flush()
            return
        self.session.add(row)
        self.session.flush()

    def list_document_results(
        self, job_id: UUID, *, outcomes: set[str] | None = None
    ) -> list[JobDocument]:
        stmt = select(JobDocument).where(JobDocument.job_id == job_id)
        if outcomes:
            stmt = stmt.where(JobDocument.outcome.in_(outcomes))
        return list(self.session.scalars(stmt).all())

    def add_attempt(self, attempt: ProcessingAttempt) -> ProcessingAttempt:
        self.session.add(attempt)
        self.session.flush()
        return attempt

    def list_attempts(self, document_id: UUID) -> list[ProcessingAttempt]:
        return list(
            self.session.scalars(
                select(ProcessingAttempt)
                .where(ProcessingAttempt.document_id == document_id)
                .order_by(ProcessingAttempt.created_at.asc())
            ).all()
        )

    def failed_document_ids(self, job_id: UUID, *, include_quarantined: bool) -> list[UUID]:
        outcomes = {"failed"}
        if include_quarantined:
            outcomes.add("quarantined")
        rows = self.list_document_results(job_id, outcomes=outcomes)
        return [row.document_id for row in rows if row.document_id is not None]


def stale_cutoff(seconds: int) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)
