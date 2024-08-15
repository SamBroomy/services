# Document Service

This service is responsible for parsing & chunking documents.

## Components

- Kafka consumer & producer
- Unstructured api docker container
- Document service python script

## Workflow

First thing is a message is sent to the kafka `DOCUMENT_PARSE` (document.parse) topic. The message contains the document content (maybe this should be done by sending a document id and then fetched from blob or something).

Once the document content is received, the document service python script parses the document to be parsed & optionally chunked.

Parsing happens in two steps:

1. Document parsing:

    Document parsing is done by the unstructured docker container. The document is sent to the unstructured docker container (from the main document service python script) which extracts the text from the document. This is then sent back to the main document service python script.

2. Chunking:

    The extracted text is then chunked into smaller parts. This is done by the main document service python script. This is because it allows for more control and flexibility in the chunking process, as well as additional cleaning and processing of the text.

Once the document is parsed & chunked, the chunks are sent to the kafka `DOCUMENT_CHUNK` response topic (document.parse.response) where they are picked up by the next service in the pipeline.
