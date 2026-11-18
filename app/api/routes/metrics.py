"""Prometheus metrics."""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.core.config import get_settings
from app.database.repositories import OutboxRepository
from app.database.session import session_scope
from app.observability.metrics import OUTBOX_PENDING

router = APIRouter(tags=["monitoring"])


@router.get("/metrics")
def metrics() -> Response:
    settings = get_settings()
    try:
        with session_scope(settings) as session:
            OUTBOX_PENDING.set(OutboxRepository(session).pending_count())
    except Exception:
        OUTBOX_PENDING.set(0)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
