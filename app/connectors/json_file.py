"""Split a JSON file or directory of JSON files into one record per object."""

from __future__ import annotations

import json
from collections.abc import Iterator
from uuid import UUID

from app.connectors.filesystem import FileSystemConnector
from app.core.config import Settings
from app.core.exceptions import PermanentProcessingError
from app.domain.enums import DocumentType, SourceType
from app.domain.models import DiscoveredItem, RejectedItem, SourceRecord
from app.parsers.common import decode_bytes
from app.processing.hashing import sha256_text
from app.processing.normalization import parse_timestamp


class JSONConnector(FileSystemConnector):
    """Filesystem connector that expands JSON arrays into individual records."""

    def __init__(
        self, *, source_id: UUID, root: str, settings: Settings, id_field: str = "id"
    ) -> None:
        super().__init__(source_id=source_id, root=root, settings=settings, extensions={".json"})
        self.source_type = SourceType.JSON_FILE.value
        self.id_field = id_field

    def discover(self) -> Iterator[DiscoveredItem | RejectedItem]:
        for item in super().discover():
            if isinstance(item, RejectedItem):
                yield item
                continue
            path_records = self._load(item)
            if isinstance(path_records, RejectedItem):
                yield path_records
                continue
            for record in path_records:
                yield DiscoveredItem(
                    logical_key=record.logical_key,
                    source_uri=record.source_uri,
                    filename=item.filename,
                    content_type=DocumentType.JSON.value,
                    size_bytes=len(record.content),
                    modified_at=item.modified_at,
                    locator=record.logical_key,
                )

    def fetch(self, item: DiscoveredItem) -> SourceRecord:
        # Discovery already bounded the file. Re-read and select the logical key so
        # fetch stays independent of discover's in-memory cache.
        parent_key = item.logical_key.split("#", 1)[0]
        parent = DiscoveredItem(
            logical_key=parent_key,
            source_uri=item.source_uri.split("#", 1)[0],
            filename=item.filename,
            content_type=DocumentType.JSON.value,
            locator=item.source_uri.split("#", 1)[0],
        )
        loaded = self._load(parent)
        if isinstance(loaded, RejectedItem):
            raise PermanentProcessingError(loaded.reason, category=loaded.category)
        for record in loaded:
            if record.logical_key == item.logical_key:
                return record
        raise PermanentProcessingError(
            "JSON record disappeared between discover and fetch", category="not_found"
        )

    def _load(self, item: DiscoveredItem) -> list[SourceRecord] | RejectedItem:
        try:
            parent = super().fetch(item)
        except Exception as exc:
            return RejectedItem(
                source_uri=item.source_uri,
                filename=item.filename,
                logical_key=item.logical_key,
                reason=str(exc)[:500],
                category="fetch_error",
            )
        try:
            loaded = json.loads(decode_bytes(parent.content) or "null")
        except Exception:
            return RejectedItem(
                source_uri=item.source_uri,
                filename=item.filename,
                logical_key=item.logical_key,
                reason="JSON file is malformed",
                category="malformed_json",
            )
        records = loaded if isinstance(loaded, list) else [loaded]
        built: list[SourceRecord] = []
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                built.append(
                    SourceRecord(
                        logical_key=f"{item.logical_key}#{index}",
                        source_uri=f"{item.source_uri}#{index}",
                        filename=item.filename,
                        content_type=DocumentType.JSON.value,
                        content=b"",
                        document_type=DocumentType.JSON,
                        prefetched_error="JSON element was not an object",
                    )
                )
                continue
            identity = record.get(self.id_field)
            suffix = (
                str(identity)
                if identity is not None
                else sha256_text(json.dumps(record, sort_keys=True, default=str))[:16]
            )
            logical_key = (
                item.logical_key if not isinstance(loaded, list) else f"{item.logical_key}#{suffix}"
            )
            updated = _optional_time(record.get("updated_at") or record.get("modified_at"))
            created = _optional_time(record.get("created_at"))
            built.append(
                SourceRecord(
                    logical_key=logical_key,
                    source_uri=f"{item.source_uri}#{suffix}"
                    if isinstance(loaded, list)
                    else item.source_uri,
                    filename=item.filename,
                    content_type=DocumentType.JSON.value,
                    content=json.dumps(record, sort_keys=True).encode("utf-8"),
                    metadata={"record_index": index},
                    source_created_at=created,
                    source_modified_at=updated,
                    payload=record,
                    document_type=DocumentType.JSON,
                )
            )
        return built


def _optional_time(value):
    if value is None:
        return None
    try:
        return parse_timestamp(value)
    except ValueError:
        return None
