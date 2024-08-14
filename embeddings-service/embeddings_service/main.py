import asyncio

from embedding_models import EMBEDDING_MODELS
from microservices_common import setup_logger
from microservices_common.kafka import (
    KafkaFactory,
    KafkaMessage,
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.model_definitions.embeddings import (
    EmbeddingRequest,
    EmbeddingResponse,
    ModelInfoRequest,
    ModelInfoResponse,
)
from pydantic import BaseModel

KAFKA_TOPICS = KafkaTopicCategory.EMBEDDING
GROUP_ID = "embeddings-service"


logger = setup_logger("embeddings-service")


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


async def handle_message(
    msg: KafkaMessage,
) -> KafkaMessage:
    if (message := msg.value) is None:
        raise ValueError("Message does not contain a value, cannot process")
    if isinstance(message, BaseModel):
        message = message.model_dump()

    logger.debug(f"Message: {message}")

    match msg.topic:
        case KafkaTopic.EMBEDDING_GENERATE:
            logger.info("Processing embedding request")
            request = EmbeddingRequest(**message)
            response = await process_embedding_request(request)
        case KafkaTopic.EMBEDDING_MODEL_INFO:
            logger.info("Processing model info request")
            request = ModelInfoRequest(**message)
            response = process_model_info_request(request)
        case _:
            raise ValueError(f"Unknown topic: {msg.topic}")
    logger.info(f"Processed message: {response}")
    return KafkaMessage.model_construct(
        topic=msg.topic, value=response, key=msg.key, headers=msg.headers
    )


async def main():
    logger.warning("Starting embeddings-service")
    kafka = KafkaFactory.create_kafka_pc(
        KAFKA_TOPICS,
        group_id=GROUP_ID,
    )

    await kafka.run(handle_message)


if __name__ == "__main__":
    asyncio.run(main())
