"""PDF text extraction using PyMuPDF.

PyMuPDF is AGPL-licensed. Swap this parser if that license is not acceptable.
Extracted text is data. It is never evaluated or executed.
"""

from __future__ import annotations

import pymupdf as fitz

from app.core.exceptions import ParseError
from app.domain.enums import DocumentType
from app.domain.models import ContentBlock, ParsedDocument
from app.parsers.common import section_stack_title


class PDFParser:
    def parse(
        self, content: bytes, *, filename: str | None = None, payload: dict | None = None
    ) -> ParsedDocument:
        if not content.startswith(b"%PDF"):
            raise ParseError("File content is not a PDF", category="corrupted_pdf")
        try:
            document = fitz.open(stream=content, filetype="pdf")
        except Exception as exc:
            raise ParseError("PDF could not be opened", category="corrupted_pdf") from exc
        if document.needs_pass:
            document.close()
            raise ParseError("Encrypted PDFs are not ingested", category="encrypted_pdf")
        warnings: list[str] = []
        blocks: list[ContentBlock] = []
        stack: dict[int, str] = {}
        metadata_title = ""
        try:
            metadata_title = (document.metadata or {}).get("title") or ""
        except Exception:
            warnings.append("PDF metadata could not be read")
        for page_index in range(document.page_count):
            try:
                page = document.load_page(page_index)
                page_dict = page.get_text("dict")
            except Exception:
                warnings.append(f"Page {page_index + 1} could not be extracted")
                continue
            for block in page_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    spans = line.get("spans") or []
                    text = "".join(span.get("text", "") for span in spans).strip()
                    if not text:
                        continue
                    size = max((float(span.get("size") or 0) for span in spans), default=0)
                    if size >= 16:
                        stack[1] = text
                        for level in [key for key in stack if key > 1]:
                            stack.pop(level, None)
                        blocks.append(
                            ContentBlock(
                                text=text,
                                block_type="heading",
                                page_number=page_index + 1,
                                section_title=section_stack_title(stack),
                                level=1,
                            )
                        )
                    else:
                        blocks.append(
                            ContentBlock(
                                text=text,
                                block_type="paragraph",
                                page_number=page_index + 1,
                                section_title=section_stack_title(stack),
                            )
                        )
        page_count = document.page_count
        document.close()
        title = metadata_title.strip() or next(
            (block.text for block in blocks if block.block_type == "heading"), None
        )
        return ParsedDocument(
            document_type=DocumentType.PDF,
            title=title,
            blocks=blocks,
            warnings=warnings,
            metadata={"page_count": page_count},
        )
