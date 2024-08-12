import asyncio
from typing import Dict, List, Literal, Protocol

import openai
import tiktoken
from microservices_common import setup_logger
from microservices_common.kafka import (
    KafkaConsumer,
    KafkaFactory,
    KafkaMessage,
    KafkaProducer,
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.model_definitions import Error
from microservices_common.model_definitions.embeddings import (
    EmbeddingRequest,
    EmbeddingResponse,
    ModelInfoRequest,
    ModelInfoResponse,
    Usage,
)
from pydantic import BaseModel

KAFKA_TOPICS = KafkaTopicCategory.EMBEDDING
GROUP_ID = "embeddings-service"


logger = setup_logger("embeddings-service")


class EmbeddingModel(Protocol):
    model: str
    dimensions: int
    max_tokens: int

    async def generate_embedding(self, text: List[str]) -> EmbeddingResponse: ...

    def model_info(self) -> ModelInfoResponse:
        return ModelInfoResponse(
            model=self.model, dimensions=self.dimensions, max_tokens=self.max_tokens
        )


class OpenAIEmbedding:
    def __init__(
        self,
        model: Literal[
            "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"
        ],
        dimensions: int,
    ):
        # Deployment and model name are not normally the same, but at the moment the deployment names are the same as the model names. This may change in the future.
        self.client = openai.AzureOpenAI(azure_deployment=model)
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
        logger.debug("Model: '%s', Usage: %s", self.model, response.usage)

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


class DummyEmbedding:
    def __init__(self):
        self.model = "dummy"
        self.dimensions = 100
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


EMBEDDING_MODELS: Dict[str, EmbeddingModel] = {
    "text-embedding-ada-002": OpenAIEmbedding("text-embedding-ada-002", 1_536),
    #'text-embedding-3-small': OpenAIEmbedding('text-embedding-3-small', 1_536),
    "text-embedding-3-large": OpenAIEmbedding("text-embedding-3-large", 3_072),
    "dummy": DummyEmbedding(),
    # Add more models here as they are implemented
}


async def process_embedding_request(
    request: EmbeddingRequest,
) -> EmbeddingResponse:
    if request.model not in EMBEDDING_MODELS:
        raise ValueError(
            f"Model '{request.model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}"
        )
    model = EMBEDDING_MODELS[request.model]
    return await model.generate_embedding(request.text)


def process_model_info_request(
    request: ModelInfoRequest,
) -> ModelInfoResponse:
    if request.model not in EMBEDDING_MODELS:
        raise ValueError(
            f"Model '{request.model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}"
        )
    return EMBEDDING_MODELS[request.model].model_info()


async def process_message(
    msg: KafkaMessage,
) -> EmbeddingResponse | ModelInfoResponse:
    if (message := msg.value) is None:
        raise ValueError("Message does not contain a value, cannot process")
    if isinstance(message, BaseModel):
        message = message.model_dump()

    match msg.topic:
        case KafkaTopic.EMBEDDING_GENERATE:
            logger.info(f"Processing embedding request: {message}")
            request = EmbeddingRequest(**message)
            return await process_embedding_request(request)
        case KafkaTopic.EMBEDDING_MODEL_INFO:
            logger.info(f"Processing model info request: {message}")
            request = ModelInfoRequest(**message)
            return process_model_info_request(request)
        case _:
            raise ValueError(f"Unknown topic: {msg.topic}")


async def consume_messages(consumer: KafkaConsumer, producer: KafkaProducer):
    async for msg in consumer:
        logger.info(f"Received message: {msg}")
        try:
            response = await process_message(msg)
            logger.info(f"Processed message: {response}")
            message = KafkaMessage(
                topic=msg.topic, value=response, key=msg.key, headers=msg.headers
            )
            logger.info(f"Sending response: {message}")
            await producer.send_response(message)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            response = Error(error=str(e), original_request=msg)
            err_message = KafkaMessage(
                topic=msg.topic, value=response, key=msg.key, headers=msg.headers
            )
            await producer.send_response(err_message)
        logger.info("Response sent")


async def main():
    logger.warning("Starting embeddings-service")
    producer, consumer = KafkaFactory.create_producer_consumer(
        KAFKA_TOPICS,
        group_id=GROUP_ID,
    )

    try:
        async with consumer as consumer, producer as producer:
            logger.info("Started Kafka consumer and producer")
            await consume_messages(consumer, producer)
    finally:
        logger.warning("Stopping consumer and producer")


if __name__ == "__main__":
    asyncio.run(main())
