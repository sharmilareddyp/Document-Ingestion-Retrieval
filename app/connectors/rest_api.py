"""Paginated REST API connector.

Authentication tokens are read from the environment variable named by
`auth_token_env`. They are not stored in source configuration or logs.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import httpx

from app.connectors.base import Connector
from app.connectors.http import request_json
from app.core.config import Settings
from app.core.exceptions import PermanentProcessingError
from app.core.security import assert_url_allowed
from app.domain.enums import DocumentType, SourceType
from app.domain.models import DiscoveredItem, SourceRecord
from app.processing.hashing import sha256_text
from app.processing.normalization import parse_timestamp


def dig(payload: Any, path: str | None) -> Any:
    if not path:
        return payload
    current = payload
    for part in path.split("."):
        if part == "":
            continue
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


class RESTAPIConnector(Connector):
    def __init__(
        self,
        *,
        source_id: UUID,
        config: dict[str, Any],
        settings: Settings,
        client: httpx.Client | None = None,
    ) -> None:
        self.source_id = source_id
        self.source_type = SourceType.REST_API.value
        self.config = config
        self.settings = settings
        self._client = client
        self._owns_client = client is None
        assert_url_allowed(str(config["url"]), settings)

    def discover(self) -> Iterator[DiscoveredItem]:
        for record in self._records():
            yield DiscoveredItem(
                logical_key=record.logical_key,
                source_uri=record.source_uri,
                filename=record.filename,
                content_type=record.content_type,
                locator=record.logical_key,
            )
            self._cache[record.logical_key] = record

    def fetch(self, item: DiscoveredItem) -> SourceRecord:
        cached = getattr(self, "_cache", {}).get(item.logical_key)
        if cached is None:
            raise PermanentProcessingError("API record was not discovered", category="not_found")
        return cached

    def iter_records(self) -> Iterator[SourceRecord]:
        yield from self._records()

    def _records(self) -> Iterator[SourceRecord]:
        self._cache: dict[str, SourceRecord] = {}
        client = self._client or httpx.Client()
        try:
            for index, item in enumerate(self._pages(client)):
                yield self._to_record(item, index)
        finally:
            if self._owns_client:
                client.close()

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        extra = self.config.get("headers") or {}
        for key, value in extra.items():
            if key.lower() == "authorization":
                raise PermanentProcessingError(
                    "Authorization headers must be supplied through auth_token_env",
                    category="configuration",
                )
            headers[str(key)] = str(value)
        token_env = self.config.get("auth_token_env")
        if token_env:
            token = os.environ.get(str(token_env), "")
            if token:
                headers["Authorization"] = f"Bearer {token}"
        return headers

    def _pages(self, client: httpx.Client) -> Iterator[Any]:
        pagination = self.config.get("pagination") or {"strategy": "none"}
        strategy = pagination.get("strategy", "none")
        page = int(pagination.get("start_page", 1))
        page_size = int(pagination.get("page_size", 50))
        max_pages = int(pagination.get("max_pages", 100))
        cursor = None
        for _ in range(max_pages):
            params = dict(self.config.get("query") or {})
            if strategy == "page":
                params[pagination.get("page_param", "page")] = page
                params[pagination.get("page_size_param", "page_size")] = page_size
            elif strategy == "cursor" and cursor:
                params[pagination.get("cursor_param", "cursor")] = cursor
            body = request_json(
                client,
                self.config.get("method", "GET"),
                self.config["url"],
                settings=self.settings,
                params=params,
                headers=self._headers(),
            )
            records = dig(body, self.config.get("records_path"))
            if records is None and isinstance(body, list):
                records = body
            if not isinstance(records, list):
                raise PermanentProcessingError(
                    "API records_path did not return a list", category="schema"
                )
            if not records:
                break
            yield from records
            if strategy == "none":
                break
            if strategy == "page":
                if len(records) < page_size:
                    break
                page += 1
                continue
            if strategy == "cursor":
                cursor = dig(body, pagination.get("next_cursor_path", "next_cursor"))
                if not cursor:
                    break
                continue
            break

    def _to_record(self, item: Any, index: int) -> SourceRecord:
        id_field = self.config.get("id_field", "id")
        if not isinstance(item, dict):
            return SourceRecord(
                logical_key=f"malformed-{index}",
                source_uri=f"{self.config['url']}#malformed-{index}",
                filename=None,
                content_type=DocumentType.API_RECORD.value,
                content=b"",
                document_type=DocumentType.API_RECORD,
                prefetched_error="API record was not an object",
            )
        identity = item.get(id_field)
        logical_key = (
            str(identity)
            if identity is not None
            else sha256_text(json.dumps(item, sort_keys=True, default=str))
        )
        updated = _optional_time(item.get(self.config.get("updated_field", "updated_at")))
        created = _optional_time(item.get(self.config.get("created_field", "created_at")))
        return SourceRecord(
            logical_key=logical_key,
            source_uri=f"{self.config['url']}#{logical_key}",
            filename=None,
            content_type=DocumentType.API_RECORD.value,
            content=json.dumps(item, sort_keys=True, default=str).encode("utf-8"),
            metadata={"api_id": logical_key},
            source_created_at=created,
            source_modified_at=updated,
            payload=item,
            document_type=DocumentType.API_RECORD,
        )


def _optional_time(value):
    if value is None:
        return None
    try:
        return parse_timestamp(value)
    except (ValueError, OSError, OverflowError):
        return None
