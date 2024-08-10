import asyncio
import logging
import os
from typing import Dict, List, Literal, Protocol

import openai
import tiktoken
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from microservices_common.kafka import KafkaProducerConsumerFactory, send_response
from microservices_common.model_definitions import Error
from microservices_common.model_definitions.embeddings import (
    EmbeddingRequest,
    EmbeddingResponse,
    ModelInfoRequest,
    ModelInfoResponse,
    Usage,
)
from result import Err, Ok, Result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = "embeddings-service"
KAFKA_TOPICS = {
    "embedding_requests": "embedding_responses",
    "model_info": "model_info_response",
}

Text = str | List[str]


class EmbeddingModel(Protocol):
    model: str
    dimensions: int
    max_tokens: int

    async def generate_embedding(
        self, text: Text
    ) -> Result[EmbeddingResponse, str]: ...

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
                dimensions=self.dimensions,
                usage=usage,
            )
        )

    def model_info(self) -> ModelInfoResponse:
        return ModelInfoResponse(
            model=self.model, dimensions=self.dimensions, max_tokens=self.max_tokens
        )


class DummyEmbedding:
    def __init__(self):
        self.model = "dummy"
        self.dimensions = 1
        self.max_tokens = 69

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
) -> Result[EmbeddingResponse, str]:
    if request.model not in EMBEDDING_MODELS:
        return Err(
            f"Model '{request.model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}"
        )
    model = EMBEDDING_MODELS[request.model]
    return await model.generate_embedding(request.text)


def process_model_info_request(
    request: ModelInfoRequest,
) -> Result[ModelInfoResponse, str]:
    if request.model not in EMBEDDING_MODELS:
        return Err(
            f"Model '{request.model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}"
        )
    model = EMBEDDING_MODELS[request.model]
    return Ok(model.model_info())


async def process_message(
    msg: ConsumerRecord,
) -> Result[EmbeddingResponse | ModelInfoResponse, str]:
    topic: str = msg.topic

    if (message := msg.value) is None:
        return Err("Message does not contain a value, cannot process")

    match topic:
        case "embedding_requests":
            request = EmbeddingRequest(**message)
            return await process_embedding_request(request)
        case "model_info":
            request = ModelInfoRequest(**message)
            return process_model_info_request(request)
        case _:
            return Err(f"Unknown topic: {topic}")


async def consume_messages(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer):
    async for msg in consumer:
        logger.info(
            f"Received message: {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}"
        )

        try:
            match await process_message(msg):
                case Ok(response):
                    ...
                case Err(error):
                    response = Error(error=error, topic=msg.topic, original_request=msg)
                    logger.error(f"Error processing message: {error}")
        except Exception as e:
            response = Error(error=str(e), topic=msg.topic, original_request=msg)
            logger.critical(f"Unexpected Error processing message: {e}")
        await send_response(producer, KAFKA_TOPICS[msg.topic], response, msg.key)


async def main():
    logger.warning("Starting embeddings-service")
    producer, consumer = KafkaProducerConsumerFactory.create_producer_consumer(
        *KAFKA_TOPICS.keys(),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
    )

    await consumer.start()
    await producer.start()

    logger.info("Started Kafka consumer and producer")

    try:
        await consume_messages(consumer, producer)
    finally:
        logger.warning("Stopping consumer and producer")
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
