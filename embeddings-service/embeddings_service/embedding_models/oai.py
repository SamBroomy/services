from typing import List, Literal

import tiktoken
from microservices_common import setup_logger
from microservices_common.model_definitions.embeddings import (
    EmbeddingResponse,
    ModelInfoResponse,
    Usage,
)
from openai import AzureOpenAI


class OpenAIEmbedding:
    """The following env variables must be set:

    AZURE_OPENAI_API_KEY: The API key for the OpenAI service.
    AZURE_OPENAI_ENDPOINT: The endpoint for the OpenAI service (e.g. https://***.openai.azure.com/).
    OPENAI_API_VERSION: The version of the OpenAI API to use (e.g. 2023-05-15).


    """

    logger = setup_logger("openai-embedding")

    def __init__(
        self,
        model: Literal[
            "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"
        ],
        dimensions: int,
    ):
        # Deployment and model name are not normally the same, but at the moment the deployment names are the same as the model names. This may change in the future.
        self.client = AzureOpenAI(azure_deployment=model)
        self.model = model
        self.max_tokens = 8192
        self.dimensions = dimensions
        self.tokenizer = tiktoken.get_encoding("cl100k_base")

    async def generate_embedding(self, text: List[str]) -> EmbeddingResponse:
        for i, t in enumerate(text):
            if len(self.tokenizer.encode(t)) > self.max_tokens:
                raise ValueError(
                    f"Text in index {i} is too long for model {self.model}. Received {len(self.tokenizer.encode(t))} tokens, max tokens: {self.max_tokens}"
                )

        response = self.client.embeddings.create(input=text, model=self.model)
        self.logger.debug(f"Model: '{self.model}', Usage: {response.usage}")

        embeddings = [d.embedding for d in response.data]

        usage = Usage(
            prompt_tokens=response.usage.prompt_tokens,
            total_tokens=response.usage.total_tokens,
        )

        return EmbeddingResponse(
            embeddings=embeddings,
            model=response.model,
            dimensions=self.dimensions,
            usage=usage,
        )

    def model_info(self) -> ModelInfoResponse:
        return ModelInfoResponse(
            model=self.model, dimensions=self.dimensions, max_tokens=self.max_tokens
        )
