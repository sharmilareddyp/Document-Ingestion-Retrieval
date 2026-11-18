"""Select a parser for a document type."""

from __future__ import annotations

from app.domain.enums import DocumentType
from app.parsers.docx_parser import DOCXParser
from app.parsers.html_parser import HTMLParser
from app.parsers.json_parser import JSONParser
from app.parsers.pdf_parser import PDFParser
from app.parsers.text_parser import TextParser

SUPPORTED_TYPES = {
    DocumentType.PDF.value,
    DocumentType.DOCX.value,
    DocumentType.TXT.value,
    DocumentType.HTML.value,
    DocumentType.JSON.value,
    DocumentType.API_RECORD.value,
}


def parser_for(
    document_type: str, *, text_fields: list[str] | None = None, title_field: str | None = None
):
    if document_type == DocumentType.PDF.value:
        return PDFParser()
    if document_type == DocumentType.DOCX.value:
        return DOCXParser()
    if document_type == DocumentType.HTML.value:
        return HTMLParser()
    if document_type == DocumentType.TXT.value:
        return TextParser()
    if document_type in {DocumentType.JSON.value, DocumentType.API_RECORD.value}:
        return JSONParser(
            text_fields=text_fields,
            title_field=title_field,
            as_record=document_type == DocumentType.API_RECORD.value,
        )
    raise KeyError(document_type)
