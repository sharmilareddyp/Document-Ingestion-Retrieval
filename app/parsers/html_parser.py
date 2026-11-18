"""HTML extraction.

Script, style, and noscript nodes are removed before text is collected.
The remaining text is data and is never executed.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from app.core.exceptions import ParseError
from app.domain.enums import DocumentType
from app.domain.models import ContentBlock, ParsedDocument
from app.parsers.common import decode_bytes, section_stack_title

_CONTENT_TAGS = {"p", "li", "pre", "blockquote", "td", "figcaption"}
_HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
_SKIP_TAGS = {"script", "style", "noscript", "template"}


class HTMLParser:
    def parse(
        self, content: bytes, *, filename: str | None = None, payload: dict | None = None
    ) -> ParsedDocument:
        try:
            decoded = decode_bytes(content)
        except ParseError:
            raise
        except Exception as exc:
            raise ParseError("HTML could not be decoded", category="malformed_html") from exc
        try:
            soup = BeautifulSoup(decoded, "lxml")
        except Exception as exc:
            raise ParseError("HTML could not be parsed", category="malformed_html") from exc
        for tag in soup.find_all(list(_SKIP_TAGS)):
            tag.decompose()
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        stack: dict[int, str] = {}
        blocks: list[ContentBlock] = []
        root = soup.body or soup
        selected = _HEADING_TAGS | _CONTENT_TAGS
        for element in root.find_all(list(selected)):
            if element.find(list(selected)):
                continue
            text = element.get_text(" ", strip=True)
            if not text:
                continue
            name = element.name.lower()
            if name in _HEADING_TAGS:
                level = int(name[1])
                stack[level] = text
                for key in [key for key in stack if key > level]:
                    stack.pop(key, None)
                if title is None and level == 1:
                    title = text
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
        if not blocks:
            fallback = soup.get_text("\n", strip=True)
            if fallback:
                blocks.append(ContentBlock(text=fallback, block_type="paragraph"))
        return ParsedDocument(document_type=DocumentType.HTML, title=title, blocks=blocks)
