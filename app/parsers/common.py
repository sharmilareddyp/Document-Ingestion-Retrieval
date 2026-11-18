"""Parser helpers shared by format-specific extractors."""

from __future__ import annotations

from pathlib import Path

from charset_normalizer import from_bytes

from app.core.exceptions import ParseError
from app.domain.enums import DocumentType

EXTENSIONS = {
    ".pdf": DocumentType.PDF,
    ".docx": DocumentType.DOCX,
    ".txt": DocumentType.TXT,
    ".html": DocumentType.HTML,
    ".htm": DocumentType.HTML,
    ".json": DocumentType.JSON,
}


def decode_bytes(content: bytes) -> str:
    """Detect the encoding and decode text. Never executes the content."""
    if not content:
        return ""
    if content.startswith(b"\xef\xbb\xbf"):
        return content.decode("utf-8-sig")
    best = from_bytes(content).best()
    if best is None:
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ParseError("Text encoding could not be detected") from exc
    return str(best)


def document_type_for_filename(filename: str | None) -> DocumentType | None:
    if not filename:
        return None
    return EXTENSIONS.get(Path(filename).suffix.lower())


def section_stack_title(stack: dict[int, str]) -> str | None:
    if not stack:
        return None
    return " > ".join(stack[level] for level in sorted(stack))
