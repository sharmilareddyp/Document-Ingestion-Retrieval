"""JSON document and API-record normalization.

A top-level array is rejected here. Use JSONConnector when a file contains many
records so each record keeps its own identity and quarantine status.
"""

from __future__ import annotations

import json
from typing import Any

from app.core.exceptions import ParseError
from app.domain.enums import DocumentType
from app.domain.models import ContentBlock, ParsedDocument
from app.parsers.common import decode_bytes

_DEFAULT_TEXT_FIELDS = ("body", "text", "content", "description", "summary")
_TITLE_FIELDS = ("title", "name", "subject", "headline")


class JSONParser:
    def __init__(
        self,
        text_fields: list[str] | None = None,
        title_field: str | None = None,
        *,
        as_record: bool = False,
    ) -> None:
        self.text_fields = text_fields
        self.title_field = title_field
        self.as_record = as_record

    def parse(
        self, content: bytes, *, filename: str | None = None, payload: dict | None = None
    ) -> ParsedDocument:
        if payload is None:
            try:
                decoded = decode_bytes(content)
                loaded = json.loads(decoded) if decoded.strip() else None
            except ParseError:
                raise
            except json.JSONDecodeError as exc:
                raise ParseError("JSON document is malformed", category="malformed_json") from exc
        else:
            loaded = payload
        if isinstance(loaded, list):
            raise ParseError(
                "JSON arrays are ingested as separate records through JSONConnector",
                category="json_array",
            )
        if not isinstance(loaded, dict):
            raise ParseError("JSON document must be an object", category="malformed_json")
        title = self._title(loaded)
        blocks = self._blocks(loaded)
        document_type = DocumentType.API_RECORD if self.as_record else DocumentType.JSON
        return ParsedDocument(
            document_type=document_type,
            title=title,
            blocks=blocks,
            payload=loaded,
            metadata={"json_keys": sorted(loaded.keys())[:50]},
        )

    def _title(self, payload: dict[str, Any]) -> str | None:
        if self.title_field and isinstance(payload.get(self.title_field), str):
            return payload[self.title_field].strip() or None
        for field in _TITLE_FIELDS:
            value = payload.get(field)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _blocks(self, payload: dict[str, Any]) -> list[ContentBlock]:
        fields = self.text_fields or list(_DEFAULT_TEXT_FIELDS)
        blocks: list[ContentBlock] = []
        title = self._title(payload)
        if title:
            blocks.append(
                ContentBlock(text=title, block_type="heading", section_title=title, level=1)
            )
        for field in fields:
            value = payload.get(field)
            if isinstance(value, str) and value.strip() and value.strip() != (title or ""):
                blocks.append(
                    ContentBlock(text=value.strip(), block_type="paragraph", section_title=title)
                )
        if len(blocks) <= (1 if title else 0):
            flattened = _flatten(payload)
            if flattened and flattened != (title or ""):
                blocks.append(
                    ContentBlock(text=flattened, block_type="paragraph", section_title=title)
                )
        return blocks


def _flatten(value: Any, *, depth: int = 0) -> str:
    if depth > 4:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            if key.lower() in {"id", "uuid"}:
                continue
            text = _flatten(item, depth=depth + 1)
            if text:
                parts.append(text)
        return "\n".join(parts)
    if isinstance(value, list):
        parts = [_flatten(item, depth=depth + 1) for item in value[:50]]
        return "\n".join(part for part in parts if part)
    return ""
