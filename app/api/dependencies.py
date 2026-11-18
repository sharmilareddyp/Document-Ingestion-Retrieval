"""FastAPI dependencies."""

from app.retrieval.search_service import SearchService
from app.services.context import AppContext, build_context
from app.services.document_service import DocumentService
from app.services.ingestion_service import IngestionService
from app.services.quality_service import QualityService


def get_context() -> AppContext:
    return build_context()


def get_ingestion() -> IngestionService:
    return IngestionService(get_context())


def get_documents() -> DocumentService:
    return DocumentService(get_context())


def get_search() -> SearchService:
    return SearchService(get_context())


def get_quality() -> QualityService:
    return QualityService(get_context())
