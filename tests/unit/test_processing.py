from __future__ import annotations

import uuid
from datetime import UTC, datetime

from app.core.config import Settings
from app.core.exceptions import PermanentProcessingError, TransientProcessingError
from app.domain.enums import DocumentType, ProcessingStatus
from app.domain.models import ContentBlock, SourceRecord
from app.processing.chunking import assign_chunk_ids, chunk_blocks
from app.processing.hashing import chunk_uuid, document_uuid, processing_fingerprint, sha256_text
from app.processing.planning import ExistingDocument, build_plan, decide_action
from app.processing.state_machine import advance_status, is_retryable_exception


def test_document_and_chunk_ids_are_deterministic():
    source_id = uuid.uuid4()
    first = document_uuid(source_id, "policies/retention.pdf")
    second = document_uuid(source_id, "policies/retention.pdf")
    assert first == second
    digest = sha256_text("same text")
    assert chunk_uuid(first, 1, 0, digest) == chunk_uuid(first, 1, 0, digest)
    assert chunk_uuid(first, 1, 0, digest) != chunk_uuid(first, 2, 0, digest)


def test_fingerprint_changes_when_chunk_config_changes():
    content = sha256_text("body")
    base = processing_fingerprint(
        content,
        parser_version="1",
        chunker_version="section-window-v1",
        chunk_size=1500,
        chunk_overlap=150,
        embedding_model="fake-hash",
        embedding_version="1",
        schema_version="1.0.0",
    )
    changed = processing_fingerprint(
        content,
        parser_version="1",
        chunker_version="section-window-v1",
        chunk_size=800,
        chunk_overlap=150,
        embedding_model="fake-hash",
        embedding_version="1",
        schema_version="1.0.0",
    )
    assert base != changed


def test_short_section_is_not_split_and_ids_are_stable():
    blocks = [
        ContentBlock(text="Retention", block_type="heading", page_number=1, level=1),
        ContentBlock(text="Keep contracts for seven years.", block_type="paragraph", page_number=1),
    ]
    first = chunk_blocks(blocks, chunk_size=1500, chunk_overlap=100)
    second = chunk_blocks(blocks, chunk_size=1500, chunk_overlap=100)
    assert len(first) == 1
    assert first[0].chunk_hash == second[0].chunk_hash
    assert first[0].page_number == 1
    assert first[0].section_title == "Retention"
    document_id = uuid.uuid4()
    assigned = assign_chunk_ids(first, document_id=document_id, version=1)
    again = assign_chunk_ids(
        chunk_blocks(blocks, chunk_size=1500, chunk_overlap=100), document_id=document_id, version=1
    )
    assert assigned[0].chunk_id == again[0].chunk_id


def test_long_section_uses_overlap_windows():
    blocks = [ContentBlock(text="word " * 400, block_type="paragraph", page_number=2)]
    chunks = chunk_blocks(blocks, chunk_size=200, chunk_overlap=40)
    assert len(chunks) > 1
    assert chunks[0].page_number == 2
    assert chunks[0].chunk_hash != chunks[1].chunk_hash


def test_version_decision():
    existing = ExistingDocument(
        document_id=uuid.uuid4(),
        current_version=3,
        content_hash="abc",
        fingerprint="same",
        indexing_status="indexed",
        processing_status="completed",
        chunks_persisted=True,
    )
    assert decide_action(existing, fingerprint="same", force=False) == ("skip", 3)
    assert decide_action(existing, fingerprint="same", force=True) == ("reindex", 3)
    assert decide_action(existing, fingerprint="new", force=False) == ("new_version", 4)
    assert decide_action(None, fingerprint="new", force=False) == ("new_version", 1)


def test_plan_quarantines_empty_text_and_versions_changes():
    settings = Settings(embedding_dimensions=32, chunk_size=200, chunk_overlap=20)
    source_id = uuid.uuid4()
    empty = SourceRecord(
        logical_key="empty.txt",
        source_uri="/tmp/empty.txt",
        filename="empty.txt",
        content_type="txt",
        content=b"   \n",
        document_type=DocumentType.TXT,
    )
    plan = build_plan(
        empty,
        source_id=source_id,
        source_type="filesystem",
        settings=settings,
        existing=None,
        contract=None,
        schema_name=None,
        schema_version=None,
        force=False,
        embedding_model="fake-hash",
    )
    assert plan.action == "quarantine"
    record = SourceRecord(
        logical_key="policy.txt",
        source_uri="/tmp/policy.txt",
        filename="policy.txt",
        content_type="txt",
        content=b"Customer contracts are retained for seven years.",
        document_type=DocumentType.TXT,
    )
    created = build_plan(
        record,
        source_id=source_id,
        source_type="filesystem",
        settings=settings,
        existing=None,
        contract=None,
        schema_name=None,
        schema_version=None,
        force=False,
        embedding_model="fake-hash",
    )
    assert created.action == "new_version"
    assert created.version == 1
    assert created.chunks
    existing = ExistingDocument(
        document_id=created.document_id,
        current_version=1,
        content_hash=created.content_hash,
        fingerprint=created.fingerprint,
        indexing_status="indexed",
        processing_status="completed",
        chunks_persisted=True,
    )
    skipped = build_plan(
        record,
        source_id=source_id,
        source_type="filesystem",
        settings=settings,
        existing=existing,
        contract=None,
        schema_name=None,
        schema_version=None,
        force=False,
        embedding_model="fake-hash",
    )
    assert skipped.action == "skip"
    assert skipped.duplicate_state == "unchanged"


def test_state_transitions_and_retry_classification():
    assert (
        advance_status(ProcessingStatus.PENDING, ProcessingStatus.QUARANTINED)
        == ProcessingStatus.QUARANTINED
    )
    assert (
        advance_status(ProcessingStatus.PENDING, ProcessingStatus.COMPLETED)
        == ProcessingStatus.COMPLETED
    )
    assert advance_status("failed", "indexing") == ProcessingStatus.INDEXING
    try:
        advance_status(ProcessingStatus.TOMBSTONED, ProcessingStatus.COMPLETED)
        raise AssertionError("tombstoned documents cannot be completed")
    except ValueError:
        pass
    assert is_retryable_exception(TransientProcessingError("timeout"))
    assert not is_retryable_exception(PermanentProcessingError("bad file"))
    assert is_retryable_exception(TimeoutError("timed out"))


def test_timestamps_are_normalized_to_utc():
    from app.processing.normalization import parse_timestamp

    parsed = parse_timestamp("2024-05-01T00:00:00Z")
    assert parsed == datetime(2024, 5, 1, tzinfo=UTC)
    assert parse_timestamp(1_700_000_000).tzinfo is UTC
