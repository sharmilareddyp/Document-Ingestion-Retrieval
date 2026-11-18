"""DOCX extraction. Document content is read as text and is never executed."""

from __future__ import annotations

from io import BytesIO
from zipfile import BadZipFile, ZipFile

from docx import Document
from docx.opc.exceptions import PackageNotFoundError

from app.core.exceptions import ParseError
from app.domain.enums import DocumentType
from app.domain.models import ContentBlock, ParsedDocument
from app.parsers.common import section_stack_title


class DOCXParser:
    def parse(
        self, content: bytes, *, filename: str | None = None, payload: dict | None = None
    ) -> ParsedDocument:
        if not content.startswith(b"PK"):
            raise ParseError("File content is not a DOCX package", category="corrupted_docx")
        try:
            with ZipFile(BytesIO(content)) as archive:
                names = set(archive.namelist())
        except BadZipFile as exc:
            raise ParseError("DOCX archive is malformed", category="corrupted_docx") from exc
        if "word/document.xml" not in names:
            raise ParseError("DOCX is missing word/document.xml", category="corrupted_docx")
        try:
            document = Document(BytesIO(content))
        except (PackageNotFoundError, ValueError) as exc:
            raise ParseError("DOCX could not be opened", category="corrupted_docx") from exc
        blocks: list[ContentBlock] = []
        stack: dict[int, str] = {}
        for paragraph in document.paragraphs:
            text = paragraph.text.strip()
            if not text:
                continue
            style = ""
            if paragraph.style is not None and paragraph.style.name:
                style = paragraph.style.name
            if style.startswith("Heading"):
                digits = "".join(char for char in style if char.isdigit())
                level = int(digits) if digits else 1
                stack[level] = text
                for key in [key for key in stack if key > level]:
                    stack.pop(key, None)
                blocks.append(
                    ContentBlock(
                        text=text,
                        block_type="heading",
                        section_title=section_stack_title(stack),
                        level=level,
                    )
                )
            else:
                blocks.append(
                    ContentBlock(
                        text=text,
                        block_type="paragraph",
                        section_title=section_stack_title(stack),
                    )
                )
        for table in document.tables:
            for row in table.rows:
                cells = []
                seen: set[str] = set()
                for cell in row.cells:
                    value = cell.text.strip()
                    if value and value not in seen:
                        seen.add(value)
                        cells.append(value)
                if cells:
                    blocks.append(
                        ContentBlock(
                            text=" | ".join(cells),
                            block_type="paragraph",
                            section_title=section_stack_title(stack),
                        )
                    )
        title = (document.core_properties.title or "").strip() or next(
            (block.text for block in blocks if block.block_type == "heading"),
            None,
        )
        return ParsedDocument(document_type=DocumentType.DOCX, title=title, blocks=blocks)
