from typing import List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, field_validator
from qdrant_client.models import Filter, SearchParams


class InsertData(BaseModel):
    id: int | str
    payload: dict
    vector: Optional[list[float]] = None
    field_to_embed: Optional[str] = None

    @field_validator("id", mode="before")
    def validate_id(cls, value):
        if isinstance(value, int):
            return value
        try:
            return str(UUID(value))
        except ValueError:
            raise ValueError("id must be a float or a valid UUID string")

    # @field_serializer("id")
    # def serialize_id(cls, value):
    #     if isinstance(value, UUID):
    #         return str(value)
    #     return value


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
    results: list[list[SearchResults]]


__all__ = [
    "VectorDBInsertRequest",
    "VectorDBInsertResponse",
    "VectorDBSearchRequest",
    "VectorDBSearchResponse",
]
