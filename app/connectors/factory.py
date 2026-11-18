"""Build a connector for a persisted source configuration."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from app.connectors.filesystem import FileSystemConnector
from app.connectors.json_file import JSONConnector
from app.connectors.rest_api import RESTAPIConnector
from app.core.config import Settings
from app.core.exceptions import UnprocessableError
from app.core.security import assert_url_allowed, resolve_allowed_path
from app.domain.enums import SourceType


def validate_source_config(
    source_type: str, config: dict[str, Any], settings: Settings
) -> dict[str, Any]:
    if source_type in {SourceType.FILESYSTEM.value, SourceType.JSON_FILE.value}:
        root = config.get("root_path") or config.get("root")
        if not root:
            raise UnprocessableError("Filesystem sources require root_path")
        resolved = resolve_allowed_path(str(root), settings)
        normalized = dict(config)
        normalized["root_path"] = str(resolved)
        normalized.setdefault("detect_deletions", source_type == SourceType.FILESYSTEM.value)
        return normalized
    if source_type == SourceType.REST_API.value:
        url = config.get("url")
        if not url:
            raise UnprocessableError("REST API sources require url")
        assert_url_allowed(str(url), settings)
        headers = {str(key).lower() for key in (config.get("headers") or {})}
        if "authorization" in headers or "proxy-authorization" in headers:
            raise UnprocessableError(
                "Store API credentials in auth_token_env instead of source headers"
            )
        normalized = dict(config)
        normalized.setdefault("detect_deletions", False)
        normalized.setdefault("records_path", "items")
        normalized.setdefault("id_field", "id")
        return normalized
    if source_type == SourceType.UPLOAD.value:
        return dict(config)
    raise UnprocessableError(f"Unsupported source type {source_type}")


def build_connector(
    *, source_id: UUID, source_type: str, config: dict[str, Any], settings: Settings
):
    if source_type == SourceType.FILESYSTEM.value:
        return FileSystemConnector(source_id=source_id, root=config["root_path"], settings=settings)
    if source_type == SourceType.JSON_FILE.value:
        return JSONConnector(
            source_id=source_id,
            root=config["root_path"],
            settings=settings,
            id_field=config.get("id_field", "id"),
        )
    if source_type == SourceType.REST_API.value:
        return RESTAPIConnector(source_id=source_id, config=config, settings=settings)
    raise UnprocessableError(f"Source type {source_type} cannot be polled")
