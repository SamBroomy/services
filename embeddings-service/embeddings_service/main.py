import asyncio
import json
import logging
import os
from typing import Dict, List, Literal, Protocol, TypedDict

import openai
import tiktoken
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel
from result import Err, Ok, Result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_INPUT = os.getenv("KAFKA_TOPIC_INPUT", "embedding_requests")
KAFKA_TOPIC_OUTPUT = os.getenv("KAFKA_TOPIC_OUTPUT", "embedding_results")


Text = str | List[str]


class EmbeddingRequest(BaseModel):
    text: Text
    model: str = "text-embedding-ada-002"


class Usage(TypedDict):
    prompt_tokens: int
    """The number of tokens used by the prompt."""

    total_tokens: int
    """The total number of tokens used by the request."""


class EmbeddingResponse(BaseModel):
    embeddings: List[float] | List[List[float]]
    model: str
    dimensions: int
    usage: Usage


class EmbeddingModel(Protocol):
    async def generate_embedding(
        self, text: Text
    ) -> Result[EmbeddingResponse, str]: ...


class OpenAIEmbedding:
    def __init__(
        self,
        model: Literal[
            "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"
        ],
    ):
        # if not (azure_endpoint := os.getenv("AZURE_OPENAI_ENDPOINT")):
        #     raise ValueError("AZURE_OPENAI_ENDPOINT environment variable is required")
        # if not (azure_key := os.getenv("AZURE_OPENAI_API_KEY")):
        #     raise ValueError("AZURE_OPENAI_API_KEY environment variable is required")
        # if not (api_version := os.getenv("AZURE_OPENAI_API_VERSION")):
        #     raise ValueError(
        #         "AZURE_OPENAI_API_VERSION environment variable is required"
        #     )
        # self.client = openai.AzureOpenAI(
        #     azure_endpoint=azure_endpoint, api_key=azure_key, api_version=api_version
        # )
        # Deployment and model name are not normally the same, but at the moment the deployment names are the same as the model names. This may change in the future.
        self.client = openai.AzureOpenAI(azure_deployment=model)
        self.model = model
        self.max_tokens = 8192
        self.tokenizer = tiktoken.get_encoding("cl100k_base")

    async def generate_embedding(
        self, text: str | List[str]
    ) -> Result[EmbeddingResponse, str]:
        if isinstance(text, str):
            text = [text]

        for i, t in enumerate(text):
            if len(self.tokenizer.encode(t)) > self.max_tokens:
                return Err(
                    f"Text in index {i} is too long for model {self.model}. Received {len(self.tokenizer.encode(t))} tokens, max tokens: {self.max_tokens}"
                )

        response = self.client.embeddings.create(input=text, model=self.model)
        logger.debug("Model: '%s', Usage: %s", self.model, response.usage)

        dimensions = len(response.data[0].embedding)

        if len(response.data) == 1:
            embeddings = response.data[0].embedding
        else:
            embeddings = [d.embedding for d in response.data]

        usage = Usage(
            prompt_tokens=response.usage.prompt_tokens,
            total_tokens=response.usage.total_tokens,
        )

        return Ok(
            EmbeddingResponse(
                embeddings=embeddings,
                model=response.model,
                dimensions=dimensions,
                usage=usage,
            )
        )


class DummyEmbedding:
    async def generate_embedding(self, text: Text) -> Result[EmbeddingResponse, str]:
        # This is a dummy model that returns the length of the text as a single-element list
        return Ok(
            EmbeddingResponse(
                embeddings=[len(text)],
                model="dummy",
                dimensions=1,
                usage=Usage(prompt_tokens=0, total_tokens=0),
            )
        )


EMBEDDING_MODELS: Dict[str, EmbeddingModel] = {
    "text-embedding-ada-002": OpenAIEmbedding("text-embedding-ada-002"),
    #'text-embedding-3-small': OpenAIEmbedding('text-embedding-3-small'),
    "text-embedding-3-large": OpenAIEmbedding("text-embedding-3-large"),
    "dummy": DummyEmbedding(),
    # Add more models here as they are implemented
}


async def process_embedding_request(
    request: EmbeddingRequest,
) -> Result[EmbeddingResponse, str]:
    if request.model not in EMBEDDING_MODELS:
        return Err(
            f"Model '{request.model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}"
        )
    model = EMBEDDING_MODELS[request.model]
    return await model.generate_embedding(request.text)


async def consume_messages():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="embedding_service",
        key_deserializer=lambda m: m.decode("ascii") if m is not None else None,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda v: v.encode("ascii") if v is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()

    logger.info("Started Kafka consumer and producer")

    try:
        async for msg in consumer:
            logger.info(
                f"Received message: {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}"
            )
            try:
                if (message := msg.value) is None:
                    raise ValueError("Message does not contain a value")
                if (text := message.get("text")) is None:
                    raise ValueError("Message does not contain a 'text' field")
                if (model := message.get("model")) is None:
                    raise ValueError("Message does not contain a 'model' field")

                request = EmbeddingRequest(text=text, model=model)
                result = await process_embedding_request(request)
                match result:
                    case Ok(response):
                        await producer.send(KAFKA_TOPIC_OUTPUT, response.model_dump())
                        logger.info(f"Sent response for message: {msg.value}")
                    case Err(error):
                        error_response = {"error": error, "original_request": msg.value}
                        await producer.send(KAFKA_TOPIC_OUTPUT, error_response)
                        logger.error(f"Error processing message: {error}")
            except Exception as e:
                error_response = {"error": str(e), "original_request": msg.value}
                await producer.send(KAFKA_TOPIC_OUTPUT, error_response)
                logger.error(f"Error processing message: {e}")
    finally:
        logger.warning("Stopping consumer and producer")
        await consumer.stop()
        await producer.stop()


async def main():
    await consume_messages()


if __name__ == "__main__":
    asyncio.run(main())
