"""Legal document processing transitions and retry classification."""

from __future__ import annotations

from app.domain.enums import ALLOWED_TRANSITIONS, ProcessingStatus, ensure_transition

_HAPPY_PATH = [
    ProcessingStatus.FETCHING,
    ProcessingStatus.EXTRACTING,
    ProcessingStatus.NORMALIZING,
    ProcessingStatus.VALIDATING,
    ProcessingStatus.CHUNKING,
    ProcessingStatus.EMBEDDING,
    ProcessingStatus.INDEXING,
    ProcessingStatus.COMPLETED,
]


def advance_status(
    current: ProcessingStatus | str, target: ProcessingStatus | str
) -> ProcessingStatus:
    """Walk the legal transition graph from `current` to `target`."""
    status = ProcessingStatus(current)
    goal = ProcessingStatus(target)
    for _ in range(len(ProcessingStatus) + 1):
        if status == goal:
            return status
        if goal in ALLOWED_TRANSITIONS[status]:
            status = ensure_transition(status, goal)
            continue
        moved = False
        for candidate in _HAPPY_PATH:
            if candidate in ALLOWED_TRANSITIONS[status]:
                status = ensure_transition(status, candidate)
                moved = True
                break
        if not moved:
            raise ValueError(f"No legal path from {current} to {target}")
    raise ValueError(f"Transition from {current} to {target} did not converge")


def is_retryable_exception(exc: BaseException) -> bool:
    """Return true for timeouts, rate limits, and dropped connections."""
    from app.core.exceptions import (
        PermanentProcessingError,
        QuarantineError,
        TransientProcessingError,
    )

    if isinstance(exc, TransientProcessingError):
        return True
    if isinstance(
        exc, (PermanentProcessingError, QuarantineError, ValueError, KeyError, TypeError)
    ):
        return False
    try:
        from sqlalchemy.exc import OperationalError

        if isinstance(exc, OperationalError):
            return True
    except Exception:
        pass
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    retry_names = (
        "timeout",
        "ratelimit",
        "rate_limit",
        "connection",
        "temporar",
        "serviceunavailable",
    )
    if any(token in name for token in retry_names):
        return True
    if any(
        token in message
        for token in ("timeout", "rate limit", "429", "temporarily", "connection reset")
    ):
        return True
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status in {408, 409, 425, 429} or (isinstance(status, int) and status >= 500):
        return True
    return False
