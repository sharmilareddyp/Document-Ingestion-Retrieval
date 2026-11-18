"""Structured JSON logging.

Log records include correlation identifiers and never include document bodies
or credential fields. Callers must not pass raw content as `extra` values.
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from app.core.config import get_settings

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
job_id_var: ContextVar[str | None] = ContextVar("job_id", default=None)
document_id_var: ContextVar[str | None] = ContextVar("document_id", default=None)
operation_var: ContextVar[str | None] = ContextVar("operation", default=None)

_CONFIGURED = False
_SECRET_KEYS = {
    "api_key",
    "authorization",
    "password",
    "secret",
    "token",
    "pinecone_api_key",
    "openai_api_key",
}


class JsonLogFormatter(logging.Formatter):
    """Emit one JSON object per log line using the standard library only."""

    def format(self, record: logging.LogRecord) -> str:
        import json

        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": get_settings().service_name,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": request_id_var.get(),
            "job_id": job_id_var.get(),
            "document_id": document_id_var.get(),
            "operation": operation_var.get(),
        }
        if record.exc_info:
            payload["error_category"] = (
                record.exc_info[0].__name__ if record.exc_info[0] else "exception"
            )
            payload["exception"] = self.formatException(record.exc_info)
        extras = getattr(record, "structured", None)
        if isinstance(extras, dict):
            for key, value in extras.items():
                if key.lower() in _SECRET_KEYS:
                    continue
                if payload.get(key) not in (None, ""):
                    continue
                payload[key] = value
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str | None = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    settings_level = level or get_settings().log_level
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root.addHandler(handler)
    root.setLevel(settings_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("pinecone").setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


def log_extra(**fields: Any) -> dict[str, Any]:
    """Return a logging `extra` payload under the structured key."""
    return {"structured": fields}
