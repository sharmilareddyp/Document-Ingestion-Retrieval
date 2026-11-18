"""Search route. Retrieval logic lives in SearchService, not in this module."""

from fastapi import APIRouter, Depends

from app.api.dependencies import get_search
from app.api.schemas import SearchHitResponse, SearchRequest, SearchResponse
from app.retrieval.search_service import SearchRequest as ServiceSearchRequest
from app.retrieval.search_service import SearchService

router = APIRouter(tags=["search"])


@router.post("/search", response_model=SearchResponse)
def search(body: SearchRequest, service: SearchService = Depends(get_search)) -> SearchResponse:
    result = service.search(ServiceSearchRequest(**body.model_dump()))
    return SearchResponse(
        items=[SearchHitResponse.model_validate(item.model_dump()) for item in result["items"]],
        limit=result["limit"],
        offset=result["offset"],
        mode=result["mode"],
        score_type=result["score_type"],
    )
