"""Application factory."""

from __future__ import annotations

import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app import __version__
from app.api.routes import documents, health, jobs, metrics, monitoring, search, sources
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.core.logging import configure_logging, get_logger, log_extra, request_id_var
from app.observability.tracing import configure_tracing

logger = get_logger(__name__)
OPEN_PATHS = {"/health", "/ready", "/metrics", "/docs", "/redoc", "/openapi.json"}


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        token = request_id_var.set(request_id)
        settings = get_settings()
        try:
            if settings.api_auth_enabled and request.url.path not in OPEN_PATHS:
                provided = request.headers.get("x-api-key", "")
                if not provided or provided not in settings.api_key_set:
                    return JSONResponse(
                        status_code=401,
                        content={
                            "error": "unauthorized",
                            "message": "A valid X-API-Key header is required",
                            "request_id": request_id,
                        },
                    )
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            request_id_var.reset(token)


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_tracing(settings.otel_exporter_otlp_endpoint)
    app = FastAPI(
        title="Document Ingestion & Retrieval",
        version=__version__,
        summary="ETL, provenance, and semantic retrieval for unstructured documents.",
    )
    app.add_middleware(RequestContextMiddleware)
    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(sources.router)
    app.include_router(jobs.router)
    app.include_router(search.router)
    app.include_router(monitoring.router)
    app.include_router(metrics.router)

    @app.exception_handler(AppError)
    async def handle_app_error(_request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.code,
                "message": exc.message,
                "request_id": request_id_var.get(),
                "details": exc.details,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": "validation_error",
                "message": "Request validation failed",
                "request_id": request_id_var.get(),
                "details": {"errors": exc.errors()},
            },
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(_request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "unhandled request error",
            extra=log_extra(operation="http", error_category=type(exc).__name__),
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "message": "The request could not be completed",
                "request_id": request_id_var.get(),
            },
        )

    return app


app = create_app()
