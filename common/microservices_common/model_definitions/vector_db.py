from typing import List, Literal, Optional

from pydantic import BaseModel
from qdrant_client.models import Filter, SearchParams


class InsertData(BaseModel):
    id: str
    payload: dict
    vector: list[float] | str


class VectorDBInsertRequest(BaseModel):
    collection_name: str
    data: List[InsertData]
    model: str = "text-embedding-ada-002"


class VectorDBInsertResponse(BaseModel):
    status: Literal["success", "error"]


class SearchResults(BaseModel):
    id: str
    version: int
    score: float
    payload: dict
    vector: list[float] | None


class VectorDBSearchRequest(BaseModel):
    collection_name: str
    query: list[str] | list[list[float]]
    filter: Optional[Filter | dict] = None
    params: Optional[SearchParams | dict] = None
    limit: int = 5
    with_payload: Optional[List[str]] = None
    with_vector: Optional[bool] = False
    score_threshold: Optional[float] = None
    model: str = "text-embedding-ada-002"


class VectorDBSearchResponse(BaseModel):
    results: List[SearchResults]


__all__ = [
    "VectorDBInsertRequest",
    "VectorDBInsertResponse",
    "VectorDBSearchRequest",
    "VectorDBSearchResponse",
]
