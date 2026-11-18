"""Section-aware chunking.

Short sections stay intact. Longer sections use a whitespace-aligned sliding
window. Chunk ids are assigned by the caller once the document version is known;
this module returns hashes and positions only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.domain.models import ChunkDraft, ContentBlock
from app.processing.hashing import sha256_text

_TOKEN_ENCODER = None


def count_tokens(text: str) -> int:
    """Approximate tokens with cl100k_base, falling back to whitespace tokens."""
    global _TOKEN_ENCODER
    try:
        if _TOKEN_ENCODER is None:
            import tiktoken

            _TOKEN_ENCODER = tiktoken.get_encoding("cl100k_base")
        return len(_TOKEN_ENCODER.encode(text))
    except Exception:
        words = text.split()
        return len(words) if words else 0


@dataclass
class _Section:
    title: str | None
    page_number: int | None
    text: str
    start_offset: int


def _sections(blocks: list[ContentBlock]) -> list[_Section]:
    grouped: list[list[ContentBlock]] = []
    current: list[ContentBlock] = []
    for block in blocks:
        if block.block_type == "heading" and current:
            grouped.append(current)
            current = [block]
        else:
            current.append(block)
    if current:
        grouped.append(current)
    sections: list[_Section] = []
    cursor = 0
    for group in grouped:
        parts: list[str] = []
        title = None
        page = None
        for block in group:
            if block.block_type == "heading" and title is None:
                title = block.section_title or block.text
            if page is None and block.page_number is not None:
                page = block.page_number
            if block.text.strip():
                parts.append(block.text.strip())
        text = "\n\n".join(parts).strip()
        if not text:
            continue
        if sections:
            cursor += 2
        start = cursor
        sections.append(_Section(title=title, page_number=page, text=text, start_offset=start))
        cursor += len(text)
    return sections


def _windows(text: str, size: int, overlap: int) -> list[tuple[int, str]]:
    if len(text) <= size:
        return [(0, text)]
    windows: list[tuple[int, str]] = []
    start = 0
    overlap = max(0, min(overlap, size // 2))
    while start < len(text):
        end = min(len(text), start + size)
        if end < len(text):
            breakpoint = text.rfind(" ", start + size // 2, end)
            if breakpoint > start:
                end = breakpoint
        piece = text[start:end].strip()
        piece_start = start + (text[start:end].find(piece) if piece else 0)
        if piece:
            windows.append((piece_start, piece))
        if end >= len(text):
            break
        next_start = end - overlap
        if next_start <= start:
            next_start = end
        start = next_start
    return windows


def chunk_blocks(
    blocks: list[ContentBlock],
    *,
    chunk_size: int,
    chunk_overlap: int,
) -> list[ChunkDraft]:
    """Split blocks into ordered chunks. Identical input yields identical output."""
    if chunk_size < 100:
        raise ValueError("chunk_size must be at least 100 characters")
    drafts: list[ChunkDraft] = []
    seen_consecutive: str | None = None
    for section in _sections(blocks):
        for local_start, text in _windows(section.text, chunk_size, chunk_overlap):
            digest = sha256_text(text)
            if digest == seen_consecutive:
                continue
            seen_consecutive = digest
            drafts.append(
                ChunkDraft(
                    chunk_index=len(drafts),
                    text=text,
                    section_title=section.title,
                    page_number=section.page_number,
                    start_offset=section.start_offset + local_start,
                    end_offset=section.start_offset + local_start + len(text),
                    token_count=count_tokens(text),
                    chunk_hash=digest,
                )
            )
    return drafts


def normalized_document_text(blocks: list[ContentBlock]) -> str:
    sections = _sections(blocks)
    return "\n\n".join(section.text for section in sections)


def assign_chunk_ids(chunks: list[ChunkDraft], *, document_id, version: int) -> list[ChunkDraft]:
    from app.processing.hashing import chunk_uuid

    assigned: list[ChunkDraft] = []
    for chunk in chunks:
        chunk.chunk_id = chunk_uuid(document_id, version, chunk.chunk_index, chunk.chunk_hash)
        assigned.append(chunk)
    return assigned


def looks_like_heading(text: str) -> bool:
    stripped = text.strip()
    if not stripped or len(stripped) > 120:
        return False
    if stripped.endswith((".", "?", "!")):
        return False
    return bool(re.search(r"[A-Za-z]", stripped))
