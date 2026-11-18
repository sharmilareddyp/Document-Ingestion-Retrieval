"""Source registration and ingestion triggers."""

from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query

from app.api.dependencies import get_ingestion
from app.api.schemas import JobAccepted, Page, SourceCreate, SourceResponse
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/sources", tags=["sources"])


@router.post("", status_code=201, response_model=SourceResponse)
def create_source(
    body: SourceCreate, service: IngestionService = Depends(get_ingestion)
) -> SourceResponse:
    created = service.register_source(
        name=body.name,
        source_type=body.source_type,
        config=body.config,
        contract=body.schema_contract,
        contract_name=body.contract_name,
    )
    return SourceResponse.model_validate(created)


@router.get("", response_model=Page)
def list_sources(
    source_type: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: IngestionService = Depends(get_ingestion),
) -> Page:
    items, total = service.list_sources(source_type=source_type, limit=limit, offset=offset)
    return Page(
        items=[SourceResponse.model_validate(item).model_dump() for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post("/{source_id}/ingest", status_code=202, response_model=JobAccepted)
def ingest_source(
    source_id: UUID,
    force: bool = False,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    service: IngestionService = Depends(get_ingestion),
) -> JobAccepted:
    job = service.enqueue_source(
        source_id, trigger="api", force=force, idempotency_key=idempotency_key
    )
    return JobAccepted(
        job_id=job["job_id"], status=job["status"], document_id=job.get("document_id")
    )
