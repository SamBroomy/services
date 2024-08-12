from typing import List, TypedDict

from pydantic import BaseModel


class EmbeddingRequest(BaseModel):
    text: List[str]
    model: str = "text-embedding-ada-002"


class Usage(TypedDict):
    prompt_tokens: int
    """The number of tokens used by the prompt."""

    total_tokens: int
    """The total number of tokens used by the request."""


class EmbeddingResponse(BaseModel):
    embeddings: List[List[float]]
    model: str
    dimensions: int
    usage: Usage


class ModelInfoRequest(BaseModel):
    model: str


class ModelInfoResponse(BaseModel):
    model: str
    dimensions: int
    max_tokens: int


__all__ = [
    "EmbeddingRequest",
    "EmbeddingResponse",
    "ModelInfoRequest",
    "ModelInfoResponse",
]
