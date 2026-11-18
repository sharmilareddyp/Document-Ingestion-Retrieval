"""Deterministic identifiers, content hashes, and processing fingerprints."""

from __future__ import annotations

import hashlib
import uuid

DOCUMENT_NAMESPACE = uuid.UUID("b6c1c2a0-7e4d-4f0a-9c1a-2a6d8e5b7c31")
CHUNK_NAMESPACE = uuid.UUID("c2e4a8b1-1d55-4a6e-8f0c-91a2b3c4d5e6")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def document_uuid(source_id: uuid.UUID, logical_key: str) -> uuid.UUID:
    """Stable document id for one source object, independent of content revisions."""
    return uuid.uuid5(DOCUMENT_NAMESPACE, f"{source_id}:{logical_key}")


def chunk_uuid(document_id: uuid.UUID, version: int, index: int, chunk_hash: str) -> uuid.UUID:
    """Stable chunk id for a specific document version, position, and text."""
    material = f"{document_id}:{version}:{index}:{chunk_hash}"
    return uuid.uuid5(CHUNK_NAMESPACE, material)


def processing_fingerprint(
    content_hash: str,
    *,
    parser_version: str,
    chunker_version: str,
    chunk_size: int,
    chunk_overlap: int,
    embedding_model: str,
    embedding_version: str,
    schema_version: str,
) -> str:
    """Change this value when re-embedding or re-chunking is required."""
    material = "|".join(
        [
            content_hash,
            parser_version,
            chunker_version,
            str(chunk_size),
            str(chunk_overlap),
            embedding_model,
            embedding_version,
            schema_version,
        ]
    )
    return sha256_text(material)


def drift_signature(*parts: str) -> str:
    return sha256_text("|".join(parts))
