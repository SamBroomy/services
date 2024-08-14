from typing import List, Protocol

from microservices_common.model_definitions.embeddings import (
    EmbeddingResponse,
    ModelInfoResponse,
)

from embedding_models.dummy import DummyEmbedding
from embedding_models.oai import OpenAIEmbedding


class EmbeddingModel(Protocol):
    model: str
    dimensions: int
    max_tokens: int

    async def generate_embedding(self, text: List[str]) -> EmbeddingResponse: ...

    def model_info(self) -> ModelInfoResponse:
        return ModelInfoResponse(
            model=self.model, dimensions=self.dimensions, max_tokens=self.max_tokens
        )


EMBEDDING_MODELS: dict[str, EmbeddingModel] = {
    "text-embedding-ada-002": OpenAIEmbedding("text-embedding-ada-002", 1_536),
    #'text-embedding-3-small': OpenAIEmbedding('text-embedding-3-small', 1_536),
    "text-embedding-3-large": OpenAIEmbedding("text-embedding-3-large", 3_072),
    "dummy": DummyEmbedding(),
    # Add more models here as they are implemented
}


__all__ = [
    "EmbeddingModel",
    "OpenAIEmbedding",
    "DummyEmbedding",
    "EMBEDDING_MODELS",
]
