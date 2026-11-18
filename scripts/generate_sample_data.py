"""Create the checked-in sample corpus. Safe to re-run."""

from __future__ import annotations

import json
from pathlib import Path

import pymupdf as fitz
from docx import Document

ROOT = Path(__file__).resolve().parents[1] / "sample_data"


def write_pdf(path: Path, title: str, paragraphs: list[str]) -> None:
    document = fitz.open()
    page = document.new_page()
    cursor = 72
    page.insert_text((72, cursor), title, fontsize=18)
    cursor += 36
    for paragraph in paragraphs:
        for line in _wrap(paragraph, 90):
            page.insert_text((72, cursor), line, fontsize=11)
            cursor += 16
        cursor += 10
    path.parent.mkdir(parents=True, exist_ok=True)
    document.save(path)
    document.close()


def write_docx(path: Path, title: str, sections: list[tuple[str, str]]) -> None:
    document = Document()
    document.add_heading(title, level=1)
    document.core_properties.title = title
    for heading, body in sections:
        document.add_heading(heading, level=2)
        document.add_paragraph(body)
    path.parent.mkdir(parents=True, exist_ok=True)
    document.save(path)


def _wrap(text: str, width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    for word in words:
        current.append(word)
        if len(" ".join(current)) > width:
            lines.append(" ".join(current))
            current = []
    if current:
        lines.append(" ".join(current))
    return lines


def main() -> None:
    mixed = ROOT / "mixed"
    bad = ROOT / "bad"
    api = ROOT / "api"
    for directory in (mixed, bad, api):
        directory.mkdir(parents=True, exist_ok=True)
    write_pdf(
        mixed / "retention_policy.pdf",
        "Data Retention Policy",
        [
            "Customer contracts and billing records are retained for seven years.",
            "Security logs are retained for one year unless a legal hold applies.",
        ],
    )
    write_docx(
        mixed / "incident_response.docx",
        "Incident Response Procedure",
        [
            ("Detection", "The on-call engineer confirms the alert and opens an incident channel."),
            ("Containment", "Affected credentials are rotated and network access is restricted."),
        ],
    )
    (mixed / "architecture.html").write_text(
        """<!DOCTYPE html><html><head><title>Security Architecture</title></head>
<body><h1>Security Architecture</h1>
<p>The platform separates metadata in PostgreSQL from vectors in Pinecone.</p>
<script>alert('this must not be executed')</script>
<h2>Access</h2><p>API keys are required outside local development.</p>
</body></html>
""",
        encoding="utf-8",
    )
    (mixed / "onboarding.txt").write_text(
        "Employee onboarding guide\n\nNew hires complete security training before production access.\n",
        encoding="utf-8",
    )
    (mixed / "catalog.json").write_text(
        json.dumps(
            {
                "title": "Widget catalog",
                "description": "The northwind widget includes a sealed battery and a two-year warranty.",
                "sku": "W-100",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (bad / "empty.txt").write_text("", encoding="utf-8")
    (bad / "malformed.json").write_text("{not-json", encoding="utf-8")
    (bad / "notes.md").write_text("markdown is not a supported ingestion type\n", encoding="utf-8")
    (bad / "corrupted.pdf").write_bytes(b"this is not a pdf")
    (api / "records.json").write_text(
        json.dumps(
            [
                {
                    "id": "file-1",
                    "title": "Archive notice",
                    "body": "Paper archives move to the climate-controlled vault.",
                    "updated_at": "2024-01-02T00:00:00Z",
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    (api / "drift_record.json").write_text(
        json.dumps(
            {
                "id": "bad-1",
                "title": "Broken priority",
                "body": "This record changes priority from an integer to a string.",
                "updated_at": "yesterday",
                "priority": "high",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote sample data under {ROOT}")


if __name__ == "__main__":
    main()
