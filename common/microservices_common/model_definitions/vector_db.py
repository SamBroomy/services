from typing import Any, List, Literal, Optional, Self
from uuid import UUID
from uuid import uuid4 as uuid

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)


class InsertData(BaseModel):
    id: int | UUID = Field(default_factory=uuid, alias="id")
    payload: dict[str, Any]
    embeddings: Optional[list[float]] = Field(
        None, alias="vector", description="Vector to insert"
    )
    field_to_embed: Optional[str] = Field(
        None, description="From the payload, which field to embed"
    )

    def __init__(self, id: int | UUID | str, **data):
        super().__init__(id=id, **data)
        if self.embeddings is None and self.field_to_embed is None:
            raise ValidationError(
                "Either embeddings or field_to_embed must be provided"
            )
        if self.field_to_embed is not None and self.field_to_embed not in self.payload:
            raise ValidationError(
                f"field_to_embed '{self.field_to_embed}' not found in payload"
            )

    @field_validator("id", mode="before")
    def validate_id(cls, value) -> int | UUID:
        if isinstance(value, (int | UUID)):
            return value
        try:
            return UUID(value)
        except ValueError:
            try:
                return int(value)
            except ValueError:
                raise ValidationError("id must be a float or a valid UUID string")

    @field_serializer("id")
    def serialize_id(self, value: int | UUID) -> str | int:
        if isinstance(value, UUID):
            return str(value)
        return value


class VectorDBInsertRequest(BaseModel):
    collection_name: str
    data: List[InsertData]
    model: str = "text-embedding-ada-002"

    def needs_embeddings(self) -> bool:
        return any(
            [d.embeddings is None and d.field_to_embed is not None for d in self.data]
        )

    def get_text_to_embed(self) -> list[str]:
        return [
            d.payload[d.field_to_embed]
            for d in self.data
            if d.field_to_embed is not None
        ]

    def update_embeddings(self, embeddings: list[list[float]]):
        for i, d in enumerate(self.data):
            d.embeddings = embeddings[i]


class VectorDBInsertResponse(BaseModel):
    status: Literal["success", "error"]


class SearchData(BaseModel):
    query: Optional[str] = Field(None, description="String to search")
    query_embeddings: Optional[list[float]] = Field(
        None, alias="vector", description="Vector to search"
    )

    @model_validator(mode="before")
    def validate_model(self) -> Self:
        if self.query is None and self.query_embeddings is None:
            raise ValueError("Either query or query_embeddings must be provided")
        return self


class VectorDBSearchRequest(BaseModel):
    collection_name: str
    data: List[SearchData]
    filter: Optional[dict] = None
    params: Optional[dict] = None
    limit: int = 5
    with_payload: Optional[List[str]] = None
    with_vector: Optional[bool] = False
    score_threshold: Optional[float] = None
    model: str = "text-embedding-ada-002"

    def needs_embeddings(self) -> bool:
        return any([d.query_embeddings is None for d in self.data])

    def get_text_to_embed(self) -> list[str]:
        return [d.query for d in self.data if d.query is not None]

    def update_embeddings(self, embeddings: list[list[float]]):
        for i, d in enumerate(self.data):
            d.query_embeddings = embeddings[i]


class SearchResults(BaseModel):
    id: str
    version: int
    score: float
    payload: dict[str, Any]
    vector: list[float] | None


class VectorDBSearchResponse(BaseModel):
    results: list[list[SearchResults]]


__all__ = [
    "VectorDBInsertRequest",
    "VectorDBInsertResponse",
    "VectorDBSearchRequest",
    "VectorDBSearchResponse",
]
