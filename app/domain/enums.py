"""String enumerations stored in PostgreSQL and returned by the API."""

from enum import Enum


class SourceType(str, Enum):
    FILESYSTEM = "filesystem"
    REST_API = "rest_api"
    JSON_FILE = "json_file"
    UPLOAD = "upload"


class DocumentType(str, Enum):
    PDF = "pdf"
    DOCX = "docx"
    TXT = "txt"
    HTML = "html"
    JSON = "json"
    API_RECORD = "api_record"


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    FETCHING = "fetching"
    EXTRACTING = "extracting"
    NORMALIZING = "normalizing"
    VALIDATING = "validating"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    INDEXING = "indexing"
    COMPLETED = "completed"
    FAILED = "failed"
    QUARANTINED = "quarantined"
    SKIPPED = "skipped"
    TOMBSTONED = "tombstoned"


class ExtractionStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class IndexingStatus(str, Enum):
    NOT_INDEXED = "not_indexed"
    PENDING = "pending"
    INDEXED = "indexed"
    FAILED = "failed"
    DELETING = "deleting"
    DELETED = "deleted"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


class TriggerType(str, Enum):
    SCHEDULE = "schedule"
    MANUAL = "manual"
    API = "api"
    BACKFILL = "backfill"
    RETRY = "retry"
    RECONCILE = "reconcile"


class DriftSeverity(str, Enum):
    INFORMATIONAL = "informational"
    WARNING = "warning"
    BREAKING = "breaking"


class QualitySeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class OutboxStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class DocumentOutcome(str, Enum):
    PROCESSED = "processed"
    SKIPPED = "skipped"
    FAILED = "failed"
    QUARANTINED = "quarantined"


ACTIVE_DOCUMENT_STATES = {
    ProcessingStatus.PENDING,
    ProcessingStatus.FETCHING,
    ProcessingStatus.EXTRACTING,
    ProcessingStatus.NORMALIZING,
    ProcessingStatus.VALIDATING,
    ProcessingStatus.CHUNKING,
    ProcessingStatus.EMBEDDING,
    ProcessingStatus.INDEXING,
}

ALLOWED_TRANSITIONS: dict[ProcessingStatus, set[ProcessingStatus]] = {
    ProcessingStatus.PENDING: {
        ProcessingStatus.FETCHING,
        ProcessingStatus.FAILED,
        ProcessingStatus.SKIPPED,
        ProcessingStatus.QUARANTINED,
        ProcessingStatus.TOMBSTONED,
    },
    ProcessingStatus.FETCHING: {
        ProcessingStatus.EXTRACTING,
        ProcessingStatus.FAILED,
        ProcessingStatus.QUARANTINED,
        ProcessingStatus.SKIPPED,
    },
    ProcessingStatus.EXTRACTING: {
        ProcessingStatus.NORMALIZING,
        ProcessingStatus.FAILED,
        ProcessingStatus.QUARANTINED,
    },
    ProcessingStatus.NORMALIZING: {
        ProcessingStatus.VALIDATING,
        ProcessingStatus.FAILED,
        ProcessingStatus.QUARANTINED,
    },
    ProcessingStatus.VALIDATING: {
        ProcessingStatus.CHUNKING,
        ProcessingStatus.FAILED,
        ProcessingStatus.QUARANTINED,
        ProcessingStatus.SKIPPED,
    },
    ProcessingStatus.CHUNKING: {
        ProcessingStatus.EMBEDDING,
        ProcessingStatus.FAILED,
        ProcessingStatus.QUARANTINED,
        ProcessingStatus.SKIPPED,
    },
    ProcessingStatus.EMBEDDING: {
        ProcessingStatus.INDEXING,
        ProcessingStatus.FAILED,
        ProcessingStatus.QUARANTINED,
    },
    ProcessingStatus.INDEXING: {
        ProcessingStatus.COMPLETED,
        ProcessingStatus.FAILED,
    },
    ProcessingStatus.COMPLETED: {
        ProcessingStatus.FETCHING,
        ProcessingStatus.TOMBSTONED,
        ProcessingStatus.CHUNKING,
        ProcessingStatus.EMBEDDING,
        ProcessingStatus.INDEXING,
    },
    ProcessingStatus.FAILED: {
        ProcessingStatus.FETCHING,
        ProcessingStatus.EMBEDDING,
        ProcessingStatus.INDEXING,
        ProcessingStatus.TOMBSTONED,
    },
    ProcessingStatus.QUARANTINED: {
        ProcessingStatus.FETCHING,
        ProcessingStatus.TOMBSTONED,
    },
    ProcessingStatus.SKIPPED: {
        ProcessingStatus.FETCHING,
        ProcessingStatus.TOMBSTONED,
    },
    ProcessingStatus.TOMBSTONED: set(),
}


def ensure_transition(
    current: ProcessingStatus | str, new: ProcessingStatus | str
) -> ProcessingStatus:
    """Return the new status when the transition is legal."""
    current_status = ProcessingStatus(current)
    new_status = ProcessingStatus(new)
    if new_status == current_status:
        return new_status
    if new_status not in ALLOWED_TRANSITIONS[current_status]:
        raise ValueError(
            f"Illegal processing transition {current_status.value} -> {new_status.value}"
        )
    return new_status
