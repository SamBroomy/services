import asyncio

from embeddings_service.embedding_models import EMBEDDING_MODELS
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
) -> EmbeddingResponse | ModelInfoResponse:
    if (message := msg.value) is None:
        raise ValueError("Message does not contain a value, cannot process")
    if isinstance(message, BaseModel):
        message = message.model_dump()

    logger.debug(f"Message: {message}")

    match msg.topic:
        case KafkaTopic.EMBEDDING_GENERATE:
            logger.info("Processing embedding request")
            request = EmbeddingRequest(**message)
            return await process_embedding_request(request)
        case KafkaTopic.EMBEDDING_MODEL_INFO:
            logger.info("Processing model info request")
            request = ModelInfoRequest(**message)
            return process_model_info_request(request)
        case _:
            raise ValueError(f"Unknown topic: {msg.topic}")


async def process_message(msg: KafkaMessage, producer: KafkaProducer):
    try:
        response = await handle_message(msg)
        logger.info(f"Processed message: {response}")
        message = KafkaMessage.model_construct(
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
        await producer.send_error(err_message)
    logger.info("Response sent")


async def consume_messages(consumer: KafkaConsumer, producer: KafkaProducer):
    async for msg in consumer:
        logger.info(f"Received message: {msg}")
        asyncio.create_task(process_message(msg, producer), name="process_message")


async def main():
    logger.warning("Starting embeddings-service")
    producer, consumer = KafkaFactory.create_producer_consumer(
        KAFKA_TOPICS,
        group_id=GROUP_ID,
    )

    try:
        async with consumer, producer:
            logger.info("Started Kafka consumer and producer")
            await consume_messages(consumer, producer)
    finally:
        logger.warning("Stopping consumer and producer")


if __name__ == "__main__":
    asyncio.run(main())
