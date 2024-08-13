from typing import List

from microservices_common.model_definitions.embeddings import (
    EmbeddingResponse,
    ModelInfoResponse,
    Usage,
)


class DummyEmbedding:
    def __init__(self):
        self.model = "dummy"
        self.dimensions = 5
        self.max_tokens = 69

    async def generate_embedding(self, text: List[str]) -> EmbeddingResponse:
        # This is a dummy model that returns random embeddings
        import random

        return EmbeddingResponse(
            embeddings=[
                [random.random() for _ in range(self.dimensions)] for _ in text
            ],
            model="dummy",
            dimensions=self.dimensions,
            usage=Usage(prompt_tokens=0, total_tokens=0),
        )

    def model_info(self) -> ModelInfoResponse:
        return ModelInfoResponse(
            model=self.model, dimensions=self.dimensions, max_tokens=self.max_tokens
        )
