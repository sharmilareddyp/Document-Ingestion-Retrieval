"""Source and schema-contract persistence."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.database.models import SchemaContract, Source


class SourceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, source: Source) -> Source:
        self.session.add(source)
        self.session.flush()
        return source

    def get(self, source_id: UUID) -> Source | None:
        return self.session.get(Source, source_id)

    def get_by_name(self, name: str) -> Source | None:
        return self.session.scalar(select(Source).where(Source.name == name))

    def list(
        self, *, source_type: str | None = None, limit: int = 50, offset: int = 0
    ) -> tuple[list[Source], int]:
        stmt = select(Source)
        if source_type:
            stmt = stmt.where(Source.source_type == source_type)
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = list(
            self.session.scalars(
                stmt.order_by(Source.created_at.desc()).limit(limit).offset(offset)
            ).all()
        )
        return rows, int(total)

    def list_active(self, source_type: str | None = None) -> list[Source]:
        stmt = select(Source).where(Source.status == "active")
        if source_type:
            stmt = stmt.where(Source.source_type == source_type)
        return list(self.session.scalars(stmt.order_by(Source.name)).all())


class SchemaContractRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, contract: SchemaContract) -> SchemaContract:
        existing = self.session.scalar(
            select(SchemaContract).where(
                SchemaContract.name == contract.name,
                SchemaContract.version == contract.version,
            )
        )
        if existing:
            existing.contract = contract.contract
            existing.active = contract.active
            existing.source_id = contract.source_id
            self.session.flush()
            return existing
        self.session.add(contract)
        self.session.flush()
        return contract

    def get_active_for_source(self, source_id: UUID) -> SchemaContract | None:
        return self.session.scalar(
            select(SchemaContract)
            .where(SchemaContract.source_id == source_id, SchemaContract.active.is_(True))
            .order_by(SchemaContract.created_at.desc())
        )
