"""Document upload and retrieval routes."""

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, File, Header, Query, UploadFile

from app.api.dependencies import get_documents, get_ingestion
from app.api.schemas import ChunkResponse, DocumentResponse, JobAccepted, Page
from app.core.config import get_settings
from app.core.exceptions import PayloadTooLargeError, UnprocessableError
from app.services.document_service import DocumentService
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("/upload", status_code=202, response_model=JobAccepted)
def upload_document(
    upload: UploadFile = File(...),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    service: IngestionService = Depends(get_ingestion),
) -> JobAccepted:
    settings = get_settings()
    content = _read_limited(upload, settings.max_upload_bytes)
    job = service.enqueue_upload(
        filename=upload.filename or "document", content=content, idempotency_key=idempotency_key
    )
    return JobAccepted(
        job_id=job["job_id"], status=job["status"], document_id=job.get("document_id")
    )


@router.get("", response_model=Page)
def list_documents(
    source_id: UUID | None = None,
    document_type: str | None = None,
    processing_status: str | None = None,
    title: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    include_deleted: bool = False,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: DocumentService = Depends(get_documents),
) -> Page:
    items, total = service.list_documents(
        source_id=source_id,
        document_type=document_type,
        processing_status=processing_status,
        title=title,
        date_from=date_from,
        date_to=date_to,
        include_deleted=include_deleted,
        limit=limit,
        offset=offset,
    )
    return Page(
        items=[DocumentResponse.model_validate(item).model_dump() for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{document_id}", response_model=DocumentResponse)
def get_document(
    document_id: UUID, service: DocumentService = Depends(get_documents)
) -> DocumentResponse:
    return DocumentResponse.model_validate(service.get_document(document_id))


@router.get("/{document_id}/chunks", response_model=list[ChunkResponse])
def get_chunks(
    document_id: UUID,
    version: int | None = None,
    service: DocumentService = Depends(get_documents),
) -> list[ChunkResponse]:
    return [
        ChunkResponse.model_validate(item) for item in service.list_chunks(document_id, version)
    ]


@router.get("/{document_id}/versions")
def get_versions(
    document_id: UUID,
    include_text: bool = False,
    service: DocumentService = Depends(get_documents),
):
    return service.list_versions(document_id, include_text=include_text)


@router.get("/{document_id}/lineage")
def get_lineage(document_id: UUID, service: DocumentService = Depends(get_documents)):
    return service.lineage(document_id)


@router.delete("/{document_id}", response_model=DocumentResponse)
def delete_document(
    document_id: UUID,
    confirm: str | None = Header(default=None, alias="X-Confirm-Delete"),
    service: DocumentService = Depends(get_documents),
) -> DocumentResponse:
    if confirm != "true":
        raise UnprocessableError("Set X-Confirm-Delete: true to tombstone a document")
    return DocumentResponse.model_validate(service.delete_document(document_id))


def _read_limited(upload: UploadFile, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        piece = upload.file.read(1024 * 1024)
        if not piece:
            break
        size += len(piece)
        if size > max_bytes:
            raise PayloadTooLargeError("Upload exceeds MAX_UPLOAD_SIZE_MB")
        chunks.append(piece)
    return b"".join(chunks)
