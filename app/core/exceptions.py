"""Domain and HTTP exceptions.

Domain exceptions stay independent of FastAPI so Airflow tasks and the worker
can classify failures without importing the web framework.
"""


class AppError(Exception):
    """Base error that maps to a stable client-facing code."""

    code = "app_error"
    status_code = 400

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(AppError):
    code = "not_found"
    status_code = 404


class UnauthorizedError(AppError):
    code = "unauthorized"
    status_code = 401


class ForbiddenError(AppError):
    code = "forbidden"
    status_code = 403


class ConflictError(AppError):
    code = "conflict"
    status_code = 409


class PayloadTooLargeError(AppError):
    code = "payload_too_large"
    status_code = 413


class UnsupportedMediaError(AppError):
    code = "unsupported_media"
    status_code = 415


class UnprocessableError(AppError):
    code = "unprocessable"
    status_code = 422


class ServiceUnavailableError(AppError):
    code = "service_unavailable"
    status_code = 503


class ConfigurationError(AppError):
    code = "configuration_error"
    status_code = 500


class UnsafePathError(AppError):
    code = "unsafe_path"
    status_code = 400


class UnsafeURLError(AppError):
    code = "unsafe_url"
    status_code = 400


class ProcessingError(Exception):
    """Pipeline failure that is recorded against a document or job."""

    def __init__(self, message: str, *, category: str, retryable: bool) -> None:
        super().__init__(message)
        self.message = message
        self.category = category
        self.retryable = retryable


class TransientProcessingError(ProcessingError):
    def __init__(self, message: str, *, category: str = "transient") -> None:
        super().__init__(message, category=category, retryable=True)


class PermanentProcessingError(ProcessingError):
    def __init__(self, message: str, *, category: str = "permanent") -> None:
        super().__init__(message, category=category, retryable=False)


class QuarantineError(ProcessingError):
    def __init__(
        self, message: str, *, category: str = "quarantine", details: dict | None = None
    ) -> None:
        super().__init__(message, category=category, retryable=False)
        self.details = details or {}


class ParseError(QuarantineError):
    def __init__(self, message: str, *, category: str = "parse_error") -> None:
        super().__init__(message, category=category)


class DatabaseFailure(TransientProcessingError):
    def __init__(self, message: str) -> None:
        super().__init__(message, category="database")
