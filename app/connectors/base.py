"""Connector primitives."""

from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID

from app.domain.models import DiscoveredItem, RejectedItem, SourceRecord


class Connector:
    """Shared discover/fetch lifecycle. Subclasses supply discovery and fetch."""

    source_id: UUID
    source_type: str

    def discover(self) -> Iterator[DiscoveredItem | RejectedItem]:
        raise NotImplementedError

    def fetch(self, item: DiscoveredItem) -> SourceRecord:
        raise NotImplementedError

    def identify(self, item: DiscoveredItem) -> str:
        return item.logical_key

    def iter_records(self) -> Iterator[SourceRecord | RejectedItem]:
        for item in self.discover():
            if isinstance(item, RejectedItem):
                yield item
                continue
            try:
                yield self.fetch(item)
            except Exception as exc:
                yield RejectedItem(
                    source_uri=item.source_uri,
                    filename=item.filename,
                    logical_key=item.logical_key,
                    reason=str(exc)[:500],
                    category=getattr(exc, "category", None) or "fetch_error",
                )
