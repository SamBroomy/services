import asyncio
import os
from functools import partial

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
from microservices_common.model_definitions.vector_db import (
    SearchResult,
    VectorDBInsertRequest,
    VectorDBInsertResponse,
    VectorDBSearchRequest,
    VectorDBSearchResponse,
)
from pydantic import BaseModel
from qdrant_client import AsyncQdrantClient as QdrantClient
from qdrant_client import models
from qdrant_client.http.models import PointStruct

# TODO: Interface with Qdrant thats general enough to be used in other services
# That way we can swap out the QdrantClient for a different client if needed

# Kafka configuration
KAFKA_TOPICS = KafkaTopicCategory.VECTOR_DB
GROUP_ID = "vector-db-service"

# Qdrant configuration
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant:6333")

# Logging configuration
logger = setup_logger("vector-db-service")


async def get_embedding(request: EmbeddingRequest) -> EmbeddingResponse:
    # request = EmbeddingRequest(text=text, model="text-embedding-ada-002")
    logger.info(f"Getting embeddings for {len(request.text)} items")
    kafak_one_shot = KafkaFactory.create_one_shot(
        KafkaTopic.EMBEDDING_GENERATE,
        GROUP_ID,
    )

    async with kafak_one_shot:
        response = (
            await kafak_one_shot.call(
                request,
                timeout=None,
            )
        ).value
        logger.info(f"Received response: {response}")
        return EmbeddingResponse.model_validate(response)


async def get_model_info(model: ModelInfoRequest) -> ModelInfoResponse:
    # request = ModelInfoRequest(model=model)
    logger.info(f"Getting model info for: {model.model}")
    kafka_one_shot = KafkaFactory.create_one_shot(
        KafkaTopic.EMBEDDING_MODEL_INFO,
        GROUP_ID,
    )

    async with kafka_one_shot:
        response = (
            await kafka_one_shot.call(
                model,
                timeout=None,
            )
        ).value
        logger.info(f"Received response: {response}")
        return ModelInfoResponse.model_validate(response)


async def process_insert_request(
    request: VectorDBInsertRequest,
    client: QdrantClient,
) -> VectorDBInsertResponse:
    if not (await client.collection_exists(request.collection_name)):
        logger.info(f"Creating collection: {request.collection_name}")
        model_info = await get_model_info(ModelInfoRequest(model=request.model))

        if not (
            await client.create_collection(
                collection_name=request.collection_name,
                vectors_config=models.VectorParams(
                    size=model_info.dimensions,
                    distance=models.Distance.COSINE,
                ),
            )
        ):
            raise ValueError(f"Failed to create collection: {request.collection_name}")

    if request.needs_embeddings():
        text = request.get_text_to_embed()

        embeddings = await get_embedding(
            EmbeddingRequest(text=text, model=request.model)
        )
        request.update_embeddings(embeddings.embeddings)
        logger.info(f"Updated embeddings: {request.data}")

    points = [
        PointStruct(**d.model_dump(by_alias=True, exclude={"field_to_embed"}))
        for d in request.data
    ]
    logger.info(
        f"Uploading {len(points)} points to collection: {request.collection_name}"
    )
    # No async for some reason?
    client.upload_points(
        collection_name=request.collection_name,
        points=points,
        # Because no async we can't wait for the response as it will block the event loop
        # wait=True,
    )
    logger.info("Uploaded points!")
    return VectorDBInsertResponse(status="success")


async def process_search_request(
    request: VectorDBSearchRequest,
    client: QdrantClient,
) -> VectorDBSearchResponse:
    if not (await client.collection_exists(request.collection_name)):
        raise ValueError(f"Collection does not exist: {request.collection_name}")

    if request.needs_embeddings():
        text = request.get_text_to_embed()

        embeddings = await get_embedding(
            EmbeddingRequest(text=text, model=request.model)
        )
        request.update_embeddings(embeddings.embeddings)

    # TODO: Implement filter parsing
    # filter = parse_filter(request.filter) if request.filter else None
    # TODO: Implement params parsing
    # params = parse_params(request.params) if request.params else None

    logger.info(
        f"Searching collection: {request.collection_name} - number of queries: {len(request.data)}"
    )
    search_requests = [
        models.SearchRequest(
            vector=d.query_embeddings,  # type: ignore
            filter=request.filter,  # type: ignore
            params=request.params,  # type: ignore
            limit=request.limit,
            with_payload=request.with_payload,
            with_vector=request.with_vector,
            score_threshold=request.score_threshold,
        )
        for d in request.data
    ]
    results = await client.search_batch(
        collection_name=request.collection_name,
        requests=search_requests,
    )
    # output = []
    # for res in results:
    #     in_out = []
    #     for r in res:
    #         logger.info(f"Found result: {r}")
    #         o = r.model_dump()
    #         logger.info(f"Output: {o}")
    #         in_out.append(
    #             SearchResult(
    #                 id=r.id,
    #                 version=r.version,
    #                 score=r.score,
    #                 payload=r.payload,
    #                 vector=r.vector,
    #             )
    #         )
    #     output.append(in_out)

    results = [[SearchResult(**r.model_dump()) for r in res] for res in results]
    logger.info(f"Found {len(results)} results")
    return VectorDBSearchResponse(results=results)


async def handle_message(
    msg: KafkaMessage,
    qdrant_client: QdrantClient,
) -> KafkaMessage:
    if (message := msg.value) is None:
        raise ValueError("Message does not contain a value, cannot process")
    if isinstance(message, BaseModel):
        message = message.model_dump()

    match msg.topic:
        case KafkaTopic.VECTOR_DB_INSERT:
            logger.info(f"Processing insert request: {message}")
            request = VectorDBInsertRequest(**message)
            response = await process_insert_request(request, qdrant_client)
        case KafkaTopic.VECTOR_DB_SEARCH:
            logger.info(f"Processing search request: {message}")
            request = VectorDBSearchRequest(**message)
            response = await process_search_request(request, qdrant_client)
        case _:
            raise ValueError(f"Unknown topic: {msg.topic}")
    logger.info(f"Processed_message: {response}")
    return KafkaMessage.model_construct(
        topic=msg.topic, value=response, key=msg.key, headers=msg.headers
    )


async def main():
    logger.warning("Starting vector-db service")
    (host, port) = QDRANT_HOST.split(":")
    qdrant_client = QdrantClient(host=host, port=int(port))

    kafka = KafkaFactory.create_kafka_pc(
        KAFKA_TOPICS,
        group_id=GROUP_ID,
    )

    message_handler = partial(handle_message, qdrant_client=qdrant_client)

    await kafka.run(message_handler)


if __name__ == "__main__":
    asyncio.run(main())
