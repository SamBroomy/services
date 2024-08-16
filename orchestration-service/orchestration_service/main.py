import asyncio

from microservices_common import setup_logger
from microservices_common.kafka import (
    KafkaFactory,
    KafkaMessage,
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.model_definitions.document import (
    Document,
    DocumentParseRequest,
    DocumentParseResponse,
)
from microservices_common.model_definitions.vector_db import (
    InsertData,
    SearchData,
    VectorDBInsertRequest,
    VectorDBInsertResponse,
    VectorDBSearchRequest,
    VectorDBSearchResponse,
)
from pydantic import BaseModel

KAFKA_TOPICS = KafkaTopicCategory.ORCHESTRATION
GROUP_ID = "orchestration-service"


logger = setup_logger("orchestration-service")


async def orchestration_upload_and_search(msg: KafkaMessage) -> KafkaMessage:
    logger.info(f"Message received: {msg.key} - {msg.value}")

    logger.info("Processing document")

    document_kafak_one_shot = KafkaFactory.create_one_shot(
        KafkaTopic.DOCUMENT_PARSE,
        GROUP_ID,
    )

    document_request = DocumentParseRequest(
        documents=[
            Document(
                **{
                    "filename": "/code/document_service/test.pdf",
                    "chunking_parameters": {
                        "chunking_strategy": "by_title",
                        "max_characters": 4000,
                    },
                    "WTF IS GOING ON": "WTF IS GOING ON",
                },
            )
        ]
    )

    async with document_kafak_one_shot:
        response = (
            await document_kafak_one_shot.call(
                document_request,
                timeout=300,
            )
        ).value
        logger.info(f"Received response: {response}")
        output = DocumentParseResponse.model_validate(response)

    vector_db_one_shot = KafkaFactory.create_one_shot(
        KafkaTopic.VECTOR_DB_INSERT,
        GROUP_ID,
    )

    insert_data = []
    for document in output.documents:
        if document.chunks is None:
            continue
        for chunk in document.chunks:
            payload = chunk.model_dump()
            if document.chunking_parameters is not None:
                payload["metadata"]["chunking_parameters"] = (
                    document.chunking_parameters.model_dump()
                )
            insert_data.append(
                InsertData(
                    **{
                        "id": chunk.element_id,
                        "payload": chunk.model_dump(),
                        "field_to_embed": "text",
                    }
                )
            )

    vector_db_request = VectorDBInsertRequest(
        collection_name="test_collection",
        data=insert_data,
    )

    async with vector_db_one_shot:
        response = (
            await vector_db_one_shot.call(
                vector_db_request,
                timeout=300,
            )
        ).value
        logger.info(f"Received response: {response}")
        output = VectorDBInsertResponse.model_validate(response)

    vector_db_search_one_shot = KafkaFactory.create_one_shot(
        KafkaTopic.VECTOR_DB_SEARCH,
        GROUP_ID,
    )

    search_request = VectorDBSearchRequest(
        collection_name="test_collection",
        data=[
            SearchData(
                **{
                    "query": "How should we proceed if there are significant changes?",
                    "vector": None,
                }
            )
        ],
        with_payload=["text", "metadata"],
    )

    async with vector_db_search_one_shot:
        response = (
            await vector_db_search_one_shot.call(
                search_request,
                timeout=300,
            )
        ).value
        logger.info(f"Received response: {response}")
        output = VectorDBSearchResponse.model_validate(response)

    logger.warning("Finished orchestration pipeline")
    logger.info(output)

    return KafkaMessage(
        topic=msg.topic,
        value={"response": "success"},
        key=msg.key,
        headers=msg.headers,
    )


async def handle_message(
    msg: KafkaMessage,
) -> KafkaMessage:
    if (message := msg.value) is None:
        raise ValueError("Message does not contain a value, cannot process")
    if isinstance(message, BaseModel):
        message = message.model_dump()

    logger.debug(f"Message: {message}")

    match msg.topic:
        case KafkaTopic.ORCHESTRATION_UPLOAD_AND_SEARCH:
            logger.info("Processing upload and search request")
            # Ignore proper response handling for now
            # request = EmbeddingRequest(**message)
            response = await orchestration_upload_and_search(msg)
            # response = await process_embedding_request(request)

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
