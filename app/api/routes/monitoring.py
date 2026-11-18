"""Quality and schema drift reports."""

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_quality
from app.api.schemas import DriftResponse, QualityResponse
from app.services.quality_service import QualityService

router = APIRouter(tags=["monitoring"])


@router.get("/quality/reports", response_model=QualityResponse)
def quality_reports(
    job_id: UUID | None = None,
    document_id: UUID | None = None,
    source_id: UUID | None = None,
    severity: str | None = None,
    passed: bool | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: QualityService = Depends(get_quality),
) -> QualityResponse:
    return QualityResponse.model_validate(
        service.list_results(
            job_id=job_id,
            document_id=document_id,
            source_id=source_id,
            severity=severity,
            passed=passed,
            limit=limit,
            offset=offset,
        )
    )


@router.get("/schema-drift/events", response_model=DriftResponse)
def drift_events(
    source_id: UUID | None = None,
    severity: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: QualityService = Depends(get_quality),
) -> DriftResponse:
    return DriftResponse.model_validate(
        service.list_drift(source_id=source_id, severity=severity, limit=limit, offset=offset)
    )
