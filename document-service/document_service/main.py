import asyncio
import codecs
import os
import pprint
import re
from functools import partial
from pathlib import Path
from typing import Any, Optional, Type

from microservices_common import setup_logger
from microservices_common.kafka import (
    KafkaFactory,
    KafkaMessage,
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.model_definitions.document import (
    ChunkingParams,
    Document,
    DocumentParseRequest,
    DocumentParseResponse,
    ParsedDocument,
)
from pydantic import BaseModel
from unstructured.chunking.basic import chunk_elements
from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import clean
from unstructured.documents.elements import (
    Element,
    Footer,
    Header,
    ListItem,
    NarrativeText,
    Text,
    Title,
)
from unstructured.staging.base import elements_from_dicts
from unstructured_client import UnstructuredClient
from unstructured_client.models import operations, shared

KAFKA_TOPICS = KafkaTopicCategory.DOCUMENT
GROUP_ID = "document-service"

CURRENT_DIR = Path(__file__).parent
BASE_URL = "http://{server_url}/general/v0/general"

DEFAULT_PARAMETERS = {
    "strategy": "auto",
    "languages": ["eng"],
}

logger = setup_logger("document-service")


def process_elements(elements: list[Element]) -> list[Element]:
    def convert_element(element: Element, element_type: Type[Element]) -> Element:
        logger.debug(f"Converting {element.category} to {element_type.category}")
        params = element.__dict__
        params.pop("category", None)
        element = element_type(params.pop("text"))
        element.__dict__.update(params)
        return element

    def clean_text(text: str) -> str:
        def remove_repeating_sequences(s: str) -> str:
            # Function to find and remove repeating sequences
            def remove_repeats(s, min_repeats=4):
                for length in range(1, len(s) // min_repeats + 1):
                    # Create a regex pattern for sequences repeating at least `min_repeats` times
                    pattern = re.compile(
                        r"((.{" + str(length) + r"}))\1{" + str(min_repeats - 1) + r",}"
                    )
                    s = re.sub(pattern, "", rf"{s}")
                return s

            # Remove simple repeated characters
            s = remove_repeats(s.encode("unicode_escape").decode())
            s = codecs.decode(s, "unicode_escape").strip()
            return s

        text = text.strip()

        text = clean(
            text,
            extra_whitespace=True,
            bullets=True,
        )
        return remove_repeating_sequences(text)

    new_elements: list[Element] = []
    previous_section: int = 0
    current_section: int = 0
    skip_next = False
    next_element = None
    initial_length = len(elements)
    header_regex = re.compile(r"^(\d+)\.?")

    for i, element in enumerate(elements):
        if skip_next:
            skip_next = False
            continue

        if i < len(elements) - 1:
            next_element = elements[i + 1]
        else:
            next_element = None

        # Skip elements with less than 4 characters (drop it essentially)
        if len(element.text) <= 4:
            continue

        # Continue if element is Header or Footer and does not start with a number
        if isinstance(element, (Header, Footer)) and not re.match(
            r"^[0-9]+", element.text
        ):
            continue
        # Skip elements with "Docusign" or pagination text
        if element.text.lower().startswith("docusign") or re.match(
            r"page [0-9]+", element.text.lower()
        ):
            continue
        if "docusign envelope id:" in element.text.lower():
            continue
        # Stop processing at explicit blank pages for signatures
        # if (
        #     element.text
        #     == "[Remainder of this page left intentionally blank. Execution page follows.]"
        # ):
        #     break

        # Clean the text of the element
        if not isinstance(element, Text):
            element.text = clean_text(element.text)
            continue
        element.apply(clean_text)

        # Extract section number from the text
        if match := header_regex.search(element.text):
            previous_section = current_section
            current_section = int(match.group(1))

        # Handles case where a uncategorized text number preceded a title
        if (
            (element.category == "UncategorizedText")  # type: ignore
            and next_element
            and isinstance(next_element, Title)
            and header_regex.match(element.text)
        ) or (
            isinstance(element, Title)
            and next_element
            and next_element.category == "UncategorizedText"  # type: ignore
            and header_regex.match(next_element.text)
        ):
            # Append the current text to the next element's text
            element.text = (
                f"{element.text} {next_element.text}"
                if element.category == "UncategorizedText"  # type: ignore
                else f"{next_element.text} {element.text}"
            ).upper()
            # Determine the type of the convert based on section comparison and element type
            if match := header_regex.match(next_element.text):
                previous_section = current_section
                current_section = int(match.group(1))
            new_type = Title if previous_section != current_section else NarrativeText
            element = convert_element(element, new_type)
            skip_next = True

        # Additional processing based on selection transition or element types
        if previous_section != current_section:
            if isinstance(element, ListItem):
                # Convert the ListItem to a Title on section change.
                element = convert_element(element, Title)
            # Update the previous section number
            previous_section = current_section
        else:
            # Converts Titles to NarrativeText if they are in the same section
            if isinstance(element, Title):
                element = convert_element(element, NarrativeText)

        new_elements.append(element)

    logger.info(
        f"Before processing: {initial_length} - after processing: {len(new_elements)} - difference: {abs(initial_length - len(new_elements))}"
    )
    return new_elements


def chunk(elements: list[Element], params: dict[str, Any]) -> list[Element]:
    logger.info(f"Chunking elements with params: {params}")
    before_chunking = len(elements)
    if params.pop("chunking_strategy") == "by_title":
        elements = chunk_by_title(
            elements,
            **params,
        )
    else:
        elements = chunk_elements(elements, **params)
    logger.info(
        f"Before chunking: {before_chunking} - after chunking: {len(elements)} - difference: {abs(before_chunking - len(elements))}"
    )
    return elements


remove_heading_regex = re.compile(r"^\d+(\.\d+)*\.?\s*")


def parse_text_to_remove_heading_numbers(text: str) -> str:
    return remove_heading_regex.sub("", text.strip()).strip()


def remove_headings_in_paragraph(text: str) -> str:
    texts: list[str] = text.strip().split("\n")

    filtered_lines = list(
        filter(
            lambda x: bool(x) and "docusign envelope id:" not in x.lower(),
            map(parse_text_to_remove_heading_numbers, texts),
        )
    )
    return "\n".join(filtered_lines)


def remove_section_headings(elements: list[Element]) -> list[Element]:
    for element in elements:
        element.text = remove_headings_in_paragraph(element.text)
    return elements


def process_document(
    document: Document,
    parameters: dict[str, Any],
    chunking_parameters: Optional[ChunkingParams],
    client: UnstructuredClient,
) -> ParsedDocument:
    try:
        request = operations.PartitionRequest(
            partition_parameters=shared.PartitionParameters(
                files=shared.Files(
                    content=document.file_content,
                    file_name=str(document.filename),
                ),
                **parameters,
            ),
        )

        result = client.general.partition(request=request)
        if result.status_code != 200 or not result.elements:
            logger.error(
                f"Failed to process document: {document.filename} - Status: {result.status_code}"
            )
            raise ValueError("Failed to process document")

        elements = elements_from_dicts(result.elements)

        elements = process_elements(elements)

        if chunking_parameters:
            params = chunking_parameters.model_dump(by_alias=True, exclude_unset=True)
            if params:
                elements = chunk(
                    elements,
                    params,
                )
            else:
                logger.warning("No chunking parameters provided")

        elements = remove_section_headings(elements)

        return ParsedDocument(
            **{
                "filename": document.filename,
                "file_hash": document.file_hash,
                "chunks": [e.to_dict() for e in elements],
                "parameters": parameters,
                "chunking_parameters": chunking_parameters,
            }
        )

    except Exception:
        logger.exception("Failed to process document", exc_info=True)
        return ParsedDocument(
            filename=document.filename, file_hash=document.file_hash, chunks=None
        )


async def process_parse_request(
    request: DocumentParseRequest,
    client: UnstructuredClient,
) -> DocumentParseResponse:
    parameters = DEFAULT_PARAMETERS.copy()
    global_parameters = request.parameters
    if global_parameters:
        parameters.update(global_parameters)

    chunking_parameters = request.chunking_parameters

    tasks = []

    for document in request.documents:
        if file_parameters := document.parameters:
            parameters.update(file_parameters)

        if local_chunk_parameters := document.chunking_parameters:
            if chunking_parameters:
                chunking_parameters.update(local_chunk_parameters)
            else:
                chunking_parameters = local_chunk_parameters

        task = asyncio.to_thread(
            process_document, document, parameters, chunking_parameters, client
        )
        tasks.append(task)

    documents: list[ParsedDocument] = await asyncio.gather(*tasks)

    return DocumentParseResponse(documents=documents)


async def handle_message(
    msg: KafkaMessage,
    client: UnstructuredClient,
) -> KafkaMessage:
    if (message := msg.value) is None:
        raise ValueError("Message does not contain a value, cannot process")
    if isinstance(message, BaseModel):
        message = message.model_dump()

    logger.debug(f"Message: {message}")

    match msg.topic:
        case KafkaTopic.DOCUMENT_PARSE:
            logger.info("Parsing the document")
            request = DocumentParseRequest(**message)
            response = await process_parse_request(request, client)
        case _:
            raise ValueError(f"Unknown topic: {msg.topic}")
    logger.info(f"Processed message: {response}")
    return KafkaMessage.model_construct(
        topic=msg.topic, value=response, key=msg.key, headers=msg.headers
    )


async def main():
    logger.warning("Starting document-service")
    client = UnstructuredClient(
        server_url=BASE_URL.format(
            server_url=os.getenv("UNSTRUCTURED_HOST", "localhost:8000")
        )
    )

    kafka = KafkaFactory.create_kafka_pc(
        KAFKA_TOPICS,
        group_id=GROUP_ID,
    )

    message_handler = partial(handle_message, client=client)

    await kafka.run(message_handler)


async def example():
    client = UnstructuredClient(
        server_url=BASE_URL.format(
            server_url=os.getenv("UNSTRUCTURED_HOST", "localhost:8000")
        )
    )
    filename = CURRENT_DIR / "test.pdf"
    try:
        req = DocumentParseRequest(
            documents=[
                Document(
                    **{
                        "filename": str(filename),
                        "chunking_parameters": {
                            "chunking_strategy": "by_title",
                            "max_characters": 4000,
                        },
                    },
                )
            ]
        )

        out = await process_parse_request(req, client)

        pprint.pprint(out.model_dump())

    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    asyncio.run(main())
