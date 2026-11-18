from __future__ import annotations

from io import BytesIO

import pymupdf as fitz
import pytest
from app.core.config import Settings
from app.core.exceptions import ParseError
from app.parsers.docx_parser import DOCXParser
from app.parsers.html_parser import HTMLParser
from app.parsers.json_parser import JSONParser
from app.parsers.pdf_parser import PDFParser
from app.parsers.text_parser import TextParser
from app.processing.quality import check_embedding_dimensions, check_text, failing
from app.processing.schema_drift import SAMPLE_RECORDS_CONTRACT, detect_drift, has_breaking
from docx import Document


def _pdf_bytes(title: str, body: str) -> bytes:
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), title, fontsize=18)
    page.insert_text((72, 110), body, fontsize=12)
    buffer = BytesIO()
    document.save(buffer)
    document.close()
    return buffer.getvalue()


def _docx_bytes() -> bytes:
    document = Document()
    document.add_heading("Incident Response", level=1)
    document.add_heading("Detection", level=2)
    document.add_paragraph("Confirm the alert before paging the full team.")
    buffer = BytesIO()
    document.save(buffer)
    return buffer.getvalue()


def test_pdf_extraction_keeps_page_numbers():
    parsed = PDFParser().parse(
        _pdf_bytes("Data Retention Policy", "Contracts are kept for seven years.")
    )
    assert parsed.title
    assert any(block.page_number == 1 for block in parsed.blocks)
    assert any("seven years" in block.text for block in parsed.blocks)


def test_corrupted_pdf_is_rejected():
    with pytest.raises(ParseError):
        PDFParser().parse(b"not a pdf")


def test_docx_extraction_preserves_headings():
    parsed = DOCXParser().parse(_docx_bytes())
    headings = [block.text for block in parsed.blocks if block.block_type == "heading"]
    assert "Incident Response" in headings
    assert any("Detection" in (block.section_title or "") for block in parsed.blocks)


def test_html_removes_script_and_keeps_sections():
    html = b"""<html><head><title>Security Architecture</title></head>
    <body><h1>Security Architecture</h1><p>Metadata lives in PostgreSQL.</p>
    <script>alert(1)</script><h2>Access</h2><p>Keys stay in the environment.</p></body></html>"""
    parsed = HTMLParser().parse(html)
    text = "\n".join(block.text for block in parsed.blocks)
    assert "alert" not in text
    assert "PostgreSQL" in text
    assert any(block.section_title and "Access" in block.section_title for block in parsed.blocks)


def test_json_and_text_parsers():
    parsed = JSONParser().parse(b'{"title":"Catalog","description":"A sealed widget."}')
    assert parsed.title == "Catalog"
    assert any("widget" in block.text for block in parsed.blocks)
    with pytest.raises(ParseError):
        JSONParser().parse(b"{")
    with pytest.raises(ParseError):
        JSONParser().parse(b"[1, 2]")
    text = TextParser().parse(b"Employee onboarding guide\n\nComplete security training.\n")
    assert len(text.blocks) == 2


def test_schema_drift_classifications():
    valid = {
        "id": "1",
        "title": "Retention",
        "body": "Seven years.",
        "updated_at": "2024-05-01T00:00:00Z",
        "priority": 1,
        "tags": ["legal"],
        "owner": {"name": "Records"},
        "extra": "optional",
    }
    informational = detect_drift(valid, SAMPLE_RECORDS_CONTRACT)
    assert informational
    assert not has_breaking(informational)
    assert informational[0].severity == "informational"
    missing = dict(valid)
    del missing["title"]
    missing_findings = detect_drift(missing, SAMPLE_RECORDS_CONTRACT)
    assert has_breaking(missing_findings)
    assert any(item.drift_type == "missing_required_field" for item in missing_findings)
    typed = dict(valid)
    typed["priority"] = "high"
    typed_findings = detect_drift(typed, SAMPLE_RECORDS_CONTRACT)
    assert any(item.drift_type == "type_change" for item in typed_findings)
    structured = dict(valid)
    structured["owner"] = ["Records"]
    assert any(
        item.drift_type == "structure_change"
        for item in detect_drift(structured, SAMPLE_RECORDS_CONTRACT)
    )
    stamped = dict(valid)
    stamped["updated_at"] = "yesterday"
    assert any(
        item.drift_type == "timestamp_format"
        for item in detect_drift(stamped, SAMPLE_RECORDS_CONTRACT)
    )
    assert has_breaking(detect_drift(["nope"], SAMPLE_RECORDS_CONTRACT))


def test_quality_checks():
    settings = Settings(quality_min_chars=5, quality_max_chars=20)
    assert failing(check_text("", settings))
    assert not failing(check_text("hello world", settings))
    assert failing(check_text("x" * 30, settings))
    assert check_embedding_dimensions([[0.1, 0.2]], 2).passed
    assert not check_embedding_dimensions([[0.1]], 2).passed
