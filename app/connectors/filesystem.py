"""Local directory ingestion restricted to configured file roots."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from app.connectors.base import Connector
from app.core.config import Settings
from app.core.security import resolve_allowed_path
from app.domain.enums import DocumentType, SourceType
from app.domain.models import DiscoveredItem, RejectedItem, SourceRecord
from app.parsers.common import EXTENSIONS


class FileSystemConnector(Connector):
    def __init__(
        self,
        *,
        source_id: UUID,
        root: str,
        settings: Settings,
        recursive: bool = True,
        extensions: set[str] | None = None,
    ) -> None:
        self.source_id = source_id
        self.source_type = SourceType.FILESYSTEM.value
        self.settings = settings
        self.root = resolve_allowed_path(root, settings)
        self.recursive = recursive
        self.extensions = extensions or set(EXTENSIONS)

    def discover(self) -> Iterator[DiscoveredItem | RejectedItem]:
        if not self.root.exists() or not self.root.is_dir():
            raise FileNotFoundError(f"Source directory does not exist: {self.root}")
        iterator = self.root.rglob("*") if self.recursive else self.root.glob("*")
        for path in sorted(iterator):
            if not path.is_file() or path.name.startswith("."):
                continue
            logical_key = path.relative_to(self.root).as_posix()
            resolved = resolve_allowed_path(path, self.settings)
            if self.root != resolved and self.root not in resolved.parents:
                yield RejectedItem(
                    source_uri=str(path),
                    filename=path.name,
                    logical_key=logical_key,
                    reason="Path escaped the source root",
                    category="path_violation",
                )
                continue
            extension = path.suffix.lower()
            if extension not in self.extensions:
                yield RejectedItem(
                    source_uri=str(resolved),
                    filename=path.name,
                    logical_key=logical_key,
                    reason=f"Unsupported file type {extension or 'unknown'}",
                    category="unsupported",
                )
                continue
            size = path.stat().st_size
            if size > self.settings.max_upload_bytes:
                yield RejectedItem(
                    source_uri=str(resolved),
                    filename=path.name,
                    logical_key=logical_key,
                    reason="File exceeds MAX_UPLOAD_SIZE_MB",
                    category="too_large",
                )
                continue
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            document_type = EXTENSIONS.get(extension)
            yield DiscoveredItem(
                logical_key=logical_key,
                source_uri=str(resolved),
                filename=path.name,
                content_type=document_type.value if document_type else None,
                size_bytes=size,
                modified_at=modified,
                locator=str(resolved),
            )

    def fetch(self, item: DiscoveredItem) -> SourceRecord:
        path = resolve_allowed_path(item.locator, self.settings)
        content = _read_bounded(path, self.settings.max_upload_bytes)
        document_type = DocumentType(item.content_type) if item.content_type else None
        return SourceRecord(
            logical_key=item.logical_key,
            source_uri=item.source_uri,
            filename=item.filename,
            content_type=item.content_type or "application/octet-stream",
            content=content,
            metadata={"size_bytes": len(content)},
            source_modified_at=item.modified_at,
            document_type=document_type,
        )


def _read_bounded(path: Path, max_bytes: int) -> bytes:
    size = 0
    chunks: list[bytes] = []
    with path.open("rb") as handle:
        while True:
            piece = handle.read(1024 * 1024)
            if not piece:
                break
            size += len(piece)
            if size > max_bytes:
                raise ValueError("File exceeds MAX_UPLOAD_SIZE_MB")
            chunks.append(piece)
    return b"".join(chunks)
