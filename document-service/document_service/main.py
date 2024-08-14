import json
import os
from pathlib import Path

from microservices_common import setup_logger
from microservices_common.kafka import (
    KafkaFactory,
    KafkaMessage,
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.model_definitions.embeddings import (
    EmbeddingRequest,
    ModelInfoRequest,
)
from pydantic import BaseModel
from unstructured_client import UnstructuredClient
from unstructured_client.models import operations, shared

CURRENT_DIR = Path(__file__).parent
BASE_URL = "http://{server_url}/general/v0/general"


client = UnstructuredClient(
    server_url=BASE_URL.format(
        server_url=os.getenv("UNSTRUCTURED_HOST", "localhost:8000")
    )
)

KAFKA_TOPICS = KafkaTopicCategory.DOCUMENT
GROUP_ID = "document-service"


logger = setup_logger("document-service")

filename = CURRENT_DIR / "test.pdf"

with open(filename, "rb") as f:
    data = f.read()

req = operations.PartitionRequest(
    partition_parameters=shared.PartitionParameters(
        files=shared.Files(
            content=data,
            file_name=str(filename),
        ),
        # --- Other partition parameters ---
        # Note: Defining `strategy`, `chunking_strategy`, and `output_format`
        # parameters as strings is accepted, but will not pass strict type checking. It is
        # advised to use the defined enum classes as shown below.
        strategy=shared.Strategy.AUTO,
        languages=["eng"],
    ),
)

try:
    print("Processing the data...")
    res = client.general.partition(request=req)
    print("Data processed successfully!")
    element_dicts = [element for element in res.elements]
    json_elements = json.dumps(element_dicts, indent=2)

    # Print the processed data.
    print(json_elements)

    # Write the processed data to a local file.
    with open("PATH_TO_OUTPUT_FILE", "w") as file:
        file.write(json_elements)
except Exception as e:
    print(e)


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
    logger.warning("Starting document-service")

    kafka = KafkaFactory.create_kafka_pc(
        KAFKA_TOPICS,
        group_id=GROUP_ID,
    )

    await kafka.run(handle_message)


# if __name__ == "__main__":
#     asyncio.run(main())
