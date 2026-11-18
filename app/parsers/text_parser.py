"""Plain-text extraction."""

from __future__ import annotations

from app.core.exceptions import ParseError
from app.domain.enums import DocumentType
from app.domain.models import ContentBlock, ParsedDocument
from app.parsers.common import decode_bytes
from app.processing.normalization import normalize_whitespace


class TextParser:
    def parse(
        self, content: bytes, *, filename: str | None = None, payload: dict | None = None
    ) -> ParsedDocument:
        try:
            decoded = decode_bytes(content)
        except ParseError:
            raise
        except Exception as exc:
            raise ParseError("Text file could not be decoded", category="malformed_text") from exc
        normalized = normalize_whitespace(decoded)
        paragraphs = [part.strip() for part in normalized.split("\n\n") if part.strip()]
        blocks = [ContentBlock(text=paragraph, block_type="paragraph") for paragraph in paragraphs]
        title = paragraphs[0][:200] if paragraphs and len(paragraphs[0]) <= 120 else None
        return ParsedDocument(document_type=DocumentType.TXT, title=title, blocks=blocks)
