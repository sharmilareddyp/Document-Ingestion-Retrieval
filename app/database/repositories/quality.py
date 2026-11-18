"""Quality results and schema drift events."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.database.models import DataQualityResult, SchemaDriftEvent


class QualityRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_many(self, rows: list[DataQualityResult]) -> None:
        if rows:
            self.session.add_all(rows)
            self.session.flush()

    def list(
        self,
        *,
        job_id: UUID | None = None,
        document_id: UUID | None = None,
        source_id: UUID | None = None,
        severity: str | None = None,
        passed: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DataQualityResult], int]:
        stmt = select(DataQualityResult)
        if job_id:
            stmt = stmt.where(DataQualityResult.job_id == job_id)
        if document_id:
            stmt = stmt.where(DataQualityResult.document_id == document_id)
        if source_id:
            stmt = stmt.where(DataQualityResult.source_id == source_id)
        if severity:
            stmt = stmt.where(DataQualityResult.severity == severity)
        if passed is not None:
            stmt = stmt.where(DataQualityResult.passed.is_(passed))
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = list(
            self.session.scalars(
                stmt.order_by(DataQualityResult.created_at.desc()).limit(limit).offset(offset)
            ).all()
        )
        return rows, int(total)

    def summary(
        self, *, job_id: UUID | None = None, source_id: UUID | None = None
    ) -> dict[str, int]:
        stmt = select(DataQualityResult.passed, func.count()).group_by(DataQualityResult.passed)
        if job_id:
            stmt = stmt.where(DataQualityResult.job_id == job_id)
        if source_id:
            stmt = stmt.where(DataQualityResult.source_id == source_id)
        counts = {"passed": 0, "failed": 0}
        for passed, count in self.session.execute(stmt).all():
            counts["passed" if passed else "failed"] = int(count)
        return counts


class DriftRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_if_new(
        self, event: SchemaDriftEvent, *, dedupe_seconds: int = 3600
    ) -> SchemaDriftEvent | None:
        cutoff = datetime.now(UTC) - timedelta(seconds=dedupe_seconds)
        existing = self.session.scalar(
            select(SchemaDriftEvent).where(
                SchemaDriftEvent.signature == event.signature,
                SchemaDriftEvent.created_at >= cutoff,
            )
        )
        if existing:
            return None
        self.session.add(event)
        self.session.flush()
        return event

    def list(
        self,
        *,
        source_id: UUID | None = None,
        severity: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[SchemaDriftEvent], int]:
        stmt = select(SchemaDriftEvent)
        if source_id:
            stmt = stmt.where(SchemaDriftEvent.source_id == source_id)
        if severity:
            stmt = stmt.where(SchemaDriftEvent.severity == severity)
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = list(
            self.session.scalars(
                stmt.order_by(SchemaDriftEvent.created_at.desc()).limit(limit).offset(offset)
            ).all()
        )
        return rows, int(total)
