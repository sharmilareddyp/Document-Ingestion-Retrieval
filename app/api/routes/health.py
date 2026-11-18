"""Health and readiness."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app import __version__
from app.api.schemas import HealthResponse
from app.core.config import get_settings
from app.core.exceptions import ServiceUnavailableError
from app.database.session import check_database
from app.services.context import build_context

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", service=settings.service_name, version=__version__)


@router.get("/ready")
def ready():
    settings = get_settings()
    database = "ok"
    try:
        check_database(settings)
    except ServiceUnavailableError:
        database = "unavailable"
    vector = "ok"
    if settings.resolved_vector_backend == "pinecone":
        try:
            build_context(settings).vector_store.stats()
        except Exception:
            vector = "unavailable"
    body = {
        "status": "ready" if database == "ok" and vector == "ok" else "not_ready",
        "database": database,
        "vector_store": vector,
        "embedding_backend": settings.resolved_embedding_backend,
        "vector_backend": settings.resolved_vector_backend,
    }
    return JSONResponse(status_code=200 if body["status"] == "ready" else 503, content=body)
