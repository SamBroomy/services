import asyncio
import logging
import os
import uuid
from typing import Optional

import openai
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from microservices_common.kafka import KafkaProducerConsumerFactory, send_response
from microservices_common.model_definitions.embeddings import (
    EmbeddingRequest,
    EmbeddingResponse,
)
from microservices_common.model_definitions.error import Error
from microservices_common.model_definitions.vector_db import (
    VectorDBInsertRequest,
    VectorDBInsertResponse,
    VectorDBSearchRequest,
    VectorDBSearchResponse,
)
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import PointStruct
from result import Err, Ok, Result

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = "vector-db-service"
KAFKA_TOPICS = {
    "vector_db_search_requests": "vector_db_search_responses",
    "vector_db_insert_requests": "vector_db_insert_responses",
    "embedding_requests": "embedding_responses",
    "model_info": "model_info_response",
}

# Qdrant configuration
QDRANT_HOST = "qdrant"
QDRANT_PORT = 6333

client = openai.AzureOpenAI(azure_deployment="text-embedding-ada-002")


client = QdrantClient(url="http://localhost:6333")
if not client.collection_exists("map-maker"):
    client.delete_collection("map-maker")
    client.create_collection(
        collection_name="map-maker",
        vectors_config=models.VectorParams(
            size=encoder.get_sentence_embedding_dimension(),  # type: ignore
            distance=models.Distance.COSINE,
        ),
    )


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def get_embedding(
    text: str, producer: AIOKafkaProducer, consumer: AIOKafkaConsumer
) -> Optional[EmbeddingResponse]:
    request = EmbeddingRequest(text=text, model="text-embedding-ada-002")

    id = str(uuid.uuid4())
    await producer.send(KAFKA_TOPIC_EMBEDDING, value=request, key=id)
    async for msg in consumer:
        if msg.topic == KAFKA_TOPIC_EMBEDDING:
            if msg.key != id:
                continue
            if (message := msg.value) is None:
                raise ValueError("Message does not contain a value")
            return EmbeddingResponse(**message)


async def process_request(
    request: VectorDBRequest, qdrant_client: QdrantClient, consumer: AIOKafkaConsumer
) -> Result[VectorDBResponse, str]:
    if request.operation == "insert":
        text = request.data.text
        metadata = request.data.get("metadata", {})
        embedding = await get_embedding(text, producer, consumer)
        qdrant_client.upsert(
            collection_name=request.collection_name,
            points=[
                PointStruct(id=request.data["id"], vector=embedding, payload=metadata)
            ],
        )
        return VectorDBResponse(status="success")
    elif request.operation == "search":
        query = request.data["query"]
        limit = request.data.get("limit", 10)
        embedding = await get_embedding(query, producer, consumer)
        search_result = qdrant_client.search(
            collection_name=request.collection_name, query_vector=embedding, limit=limit
        )
        return VectorDBResponse(status="success", data={"results": search_result})
    else:
        return VectorDBResponse(status="error", data={"message": "Invalid operation"})


async def process_message(
    msg: ConsumerRecord,
    qdrant_client: QdrantClient,
    consumer: AIOKafkaConsumer,
) -> Result[VectorDBInsertResponse | VectorDBSearchResponse, str]:
    topic: str = msg.topic

    if (message := msg.value) is None:
        return Err("Message does not contain a value, cannot process")

    match topic:
        case "vector_db_insert_requests":
            request = VectorDBInsertRequest(**message)
            return await process_request(request, qdrant_client, consumer)
        case "vector_db_search_requests":
            request = VectorDBSearchRequest(**message)
            return await process_request(request, qdrant_client, consumer)
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
            match await process_message(msg, qdrant_client, consumer):
                case Ok(response):
                    ...
                case Err(error):
                    response = Error(error=error, topic=msg.topic, msg=msg)
                    logger.error(f"Error processing message: {error}")
        except Exception as e:
            response = Error(error=str(e), topic=msg.topic, msg=msg)
            logger.critical(f"Unexpected Error processing message: {e}")
        await send_response(producer, KAFKA_TOPICS[msg.topic], response, msg.key)


async def main():
    logger.warning("Starting vector-db service")
    qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    producer, consumer = KafkaProducerConsumerFactory.create_producer_consumer(
        *KAFKA_TOPICS.keys(),
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
