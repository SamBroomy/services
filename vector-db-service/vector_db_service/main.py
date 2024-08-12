import asyncio
import os
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from microservices_common.kafka import KafkaProducerConsumerFactory, send_message
from microservices_common.model_definitions.embeddings import (
    EmbeddingRequest,
    EmbeddingResponse,
    ModelInfoRequest,
    ModelInfoResponse,
)
from microservices_common.model_definitions.error import Error
from microservices_common.model_definitions.vector_db import (
    SearchResults,
    VectorDBInsertRequest,
    VectorDBInsertResponse,
    VectorDBSearchRequest,
    VectorDBSearchResponse,
)
from microservices_common.utils import setup_logger
from qdrant_client import AsyncQdrantClient as QdrantClient
from qdrant_client import models
from qdrant_client.http.models import PointStruct
from result import Err, Ok, Result

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = "vector-db-service"

KAFKA_LISTEN_TOPICS = ["vector_db_insert", "vector_db_search"]


KAFKA_TOPICS = {
    "vector_db_search": "vector_db_search_responses",
    "vector_db_insert": "vector_db_insert_responses",
    "embedding": "embedding_responses",
    "model_info": "model_info_response",
}

# Qdrant configuration
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant:6333")


# Logging configuration
logger = setup_logger("vector-db-service")


async def get_embedding(text: list[str]) -> Result[EmbeddingResponse, str]:
    logger.info(f"Getting embeddings for {len(text)} items")
    topic = "embedding"
    request = EmbeddingRequest(text=text, model="text-embedding-ada-002")
    id = str(uuid.uuid4())

    (producer, consumer) = KafkaProducerConsumerFactory.create_producer_consumer(
        KAFKA_TOPICS[topic],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
    )
    await consumer.start()
    await producer.start()
    logger.info("Started Kafka consumer and producer")

    try:
        logger.info(f"Sending request: {request}")
        await send_message(producer, topic, request, id)
        logger.info("Waiting for response")
        # Need timeout here to avoid infinite loop?
        async with asyncio.timeout(5):
            async for msg in consumer:  # Getone? instead.
                logger.info(f"Received message: {msg}")
                if msg.key != id:
                    logger.info(f"Message key does not match: {msg.key} != {id}")
                    continue
                if (message := msg.value) is None:
                    return Err("Message does not contain a value")
                logger.info(f"Received response: {message}")
                return Ok(EmbeddingResponse(**message))
    except asyncio.TimeoutError:
        logger.critical("Timed out waiting for embedding")
        err = Err("TIMEOUT - Embedding not found")
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Stopped Kafka consumer and producer")
        err = Err("Embedding not found")
    return err


async def get_model_info(model: str) -> Result[ModelInfoResponse, str]:
    logger.info(f"Getting model info for: {model}")
    topic = "model_info"
    request = ModelInfoRequest(model=model)
    id = str(uuid.uuid4())

    (producer, consumer) = KafkaProducerConsumerFactory.create_producer_consumer(
        KAFKA_TOPICS[topic],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
    )
    await consumer.start()
    await producer.start()
    logger.info("Started Kafka consumer and producer")
    err: str = ""

    try:
        logger.info(f"Sending request: {request}")
        await send_message(producer, topic, request, id)
        logger.info("Waiting for response")
        # Need timeout here to avoid infinite loop?
        async with asyncio.timeout(5):
            async for msg in consumer:
                logger.info(f"Received message: {msg}")
                if msg.key != id:
                    logger.info(f"Message key does not match: {msg.key} != {id}")
                    continue
                if (message := msg.value) is None:
                    return Err("Message does not contain a value")
                logger.info(f"Received response: {message}")
                return Ok(ModelInfoResponse(**message))
    except asyncio.TimeoutError:
        logger.critical("Timed out waiting for model info")
        err = f"Timeout - {err}"
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Stopped Kafka consumer and producer")
        err = f"Model info not found - {err}"
    return Err(err)


async def process_insert_request(
    request: VectorDBInsertRequest,
    client: QdrantClient,
) -> Result[VectorDBInsertResponse, str]:
    if not (await client.collection_exists(request.collection_name)):
        logger.info(f"Creating collection: {request.collection_name}")
        # client.delete_collection("map-maker")
        match await get_model_info(request.model):
            case Ok(model_info):
                await client.create_collection(
                    collection_name=request.collection_name,
                    vectors_config=models.VectorParams(
                        size=model_info.dimensions,
                        distance=models.Distance.COSINE,
                    ),
                )
            case Err(error):
                return Err("Unable to create collection: " + error)
    data = request.data
    if any([d.vector is None for d in data]):
        text: list[str] = [d.payload[d.field_to_embed] for d in data]
        match await get_embedding(text):
            case Ok(embeddings):
                for d, e in zip(data, embeddings.embeddings):
                    d.vector = e
            case Err(error):
                return Err("Unable to get embeddings: " + error)
        logger.info(f"Got embeddings for {len(data)} items")
    points = [PointStruct(id=d.id, vector=d.vector, payload=d.payload) for d in data]  # type: ignore
    logger.info(
        f"Uploading {len(points)} points to collection: {request.collection_name}"
    )
    await client.upload_points(
        collection_name=request.collection_name,
        points=points,
        wait=True,
    )
    logger.info("Uploaded points!")
    return Ok(VectorDBInsertResponse(status="success"))


async def process_search_request(
    request: VectorDBSearchRequest,
    client: QdrantClient,
) -> Result[VectorDBSearchResponse, str]:
    if not (await client.collection_exists(request.collection_name)):
        logger.error(f"Collection does not exist: {request.collection_name}")
        return Err(f"Collection does not exist: {request.collection_name}")
    if any([isinstance(q, str) for q in request.query]):
        logger.info(f"Getting embeddings for {len(request.query)} items")
        match await get_embedding(request.query):  # type: ignore
            case Ok(embeddings):
                request.query = embeddings.embeddings
            case Err(error):
                return Err("Unable to get embeddings: " + error)
    logger.info(
        f"Searching collection: {request.collection_name} - number of queries: {len(request.query)}"
    )
    search_requests = [
        models.SearchRequest(
            vector=q,  # type: ignore
            filter=request.filter,  # type: ignore
            params=request.params,  # type: ignore
            limit=request.limit,
            with_payload=request.with_payload,
            with_vector=request.with_vector,
            score_threshold=request.score_threshold,
        )
        for q in request.query
    ]
    results = await client.search_batch(
        collection_name=request.collection_name,
        requests=search_requests,
    )
    results = [[SearchResults(**r.model_dump()) for r in res] for res in results]
    logger.info(f"Found {len(results)} results")
    return Ok(VectorDBSearchResponse(results=results))


async def process_message(
    msg: ConsumerRecord,
    qdrant_client: QdrantClient,
) -> Result[VectorDBInsertResponse | VectorDBSearchResponse, str]:
    topic: str = msg.topic

    if (message := msg.value) is None:
        return Err("Message does not contain a value, cannot process")

    match topic:
        case "vector_db_insert":
            logger.info(f"Processing insert request: {message}")
            request = VectorDBInsertRequest(**message)
            return await process_insert_request(request, qdrant_client)
        case "vector_db_search":
            logger.info(f"Processing search request: {message}")
            request = VectorDBSearchRequest(**message)
            return await process_search_request(request, qdrant_client)
        case _:
            return Err(f"Unknown topic: {topic}")


async def consume_messages(
    consumer: AIOKafkaConsumer, producer: AIOKafkaProducer, qdrant_client: QdrantClient
):
    async for msg in consumer:
        logger.info(
            f"Received message: {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}"
        )
        try:
            match await process_message(msg, qdrant_client):
                case Ok(response):
                    logger.info(f"Processed message: {response}")
                case Err(error):
                    response = Error(error=error, topic=msg.topic, original_request=msg)
                    logger.error(f"Error processing message: {error}")
        except Exception as e:
            response = Error(error=str(e), topic=msg.topic, original_request=msg)
            logger.critical(
                f"Unexpected Error processing message: {e}", exc_info=e, stack_info=True
            )
        if (topic := KAFKA_TOPICS.get(msg.topic)) is not None:
            logger.info(f"Sending response: {response}")
            await send_message(producer, topic, response, msg.key)


async def main():
    logger.warning("Starting vector-db service")
    (host, port) = QDRANT_HOST.split(":")
    qdrant_client = QdrantClient(host=host, port=int(port))

    producer, consumer = KafkaProducerConsumerFactory.create_producer_consumer(
        KAFKA_LISTEN_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
    )

    await consumer.start()
    await producer.start()

    logger.info("Started Kafka consumer and producer")

    try:
        await consume_messages(consumer, producer, qdrant_client)
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
