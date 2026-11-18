"""Ingestion job status."""

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_ingestion
from app.api.schemas import JobAccepted, JobResponse, Page, RetryRequest
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=Page)
def list_jobs(
    status: str | None = None,
    source_id: UUID | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: IngestionService = Depends(get_ingestion),
) -> Page:
    items, total = service.list_jobs(status=status, source_id=source_id, limit=limit, offset=offset)
    return Page(
        items=[JobResponse.model_validate(item).model_dump() for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: UUID, service: IngestionService = Depends(get_ingestion)) -> JobResponse:
    return JobResponse.model_validate(service.get_job(job_id))


@router.post("/{job_id}/retry", status_code=202, response_model=JobAccepted)
def retry_job(
    job_id: UUID,
    body: RetryRequest | None = None,
    service: IngestionService = Depends(get_ingestion),
) -> JobAccepted:
    payload = body or RetryRequest()
    job = service.retry_job(job_id, include_quarantined=payload.include_quarantined)
    return JobAccepted(
        job_id=job["job_id"], status=job["status"], document_id=job.get("document_id")
    )
