import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, List, Literal, Protocol, TypedDict

import openai
import tiktoken
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from result import Err, Ok, Result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_INPUT = os.getenv("KAFKA_TOPIC_INPUT", "embedding_requests")
KAFKA_TOPIC_OUTPUT = os.getenv("KAFKA_TOPIC_OUTPUT", "embedding_results")


Text = str | List[str]


class EmbeddingRequest(BaseModel):
    text: Text
    model: str = "text-embedding-ada-002"


class Usage(TypedDict):
    prompt_tokens: int
    """The number of tokens used by the prompt."""

    total_tokens: int
    """The total number of tokens used by the request."""


class EmbeddingResponse(BaseModel):
    embeddings: List[float] | List[List[float]]
    model: str
    dimensions: int
    usage: Usage


class EmbeddingModel(Protocol):
    async def generate_embedding(
        self, text: Text
    ) -> Result[EmbeddingResponse, str]: ...


class OpenAIEmbedding:
    def __init__(
        self,
        model: Literal[
            "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"
        ],
    ):
        # if not (azure_endpoint := os.getenv("AZURE_OPENAI_ENDPOINT")):
        #     raise ValueError("AZURE_OPENAI_ENDPOINT environment variable is required")
        # if not (azure_key := os.getenv("AZURE_OPENAI_API_KEY")):
        #     raise ValueError("AZURE_OPENAI_API_KEY environment variable is required")
        # if not (api_version := os.getenv("AZURE_OPENAI_API_VERSION")):
        #     raise ValueError(
        #         "AZURE_OPENAI_API_VERSION environment variable is required"
        #     )
        # self.client = openai.AzureOpenAI(
        #     azure_endpoint=azure_endpoint, api_key=azure_key, api_version=api_version
        # )
        # Deployment and model name are not normally the same, but at the moment the deployment names are the same as the model names. This may change in the future.
        self.client = openai.AzureOpenAI(azure_deployment=model)
        self.model = model
        self.max_tokens = 8192
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

        dimensions = len(response.data[0].embedding)

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
                dimensions=dimensions,
                usage=usage,
            )
        )


class DummyEmbedding:
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


EMBEDDING_MODELS: Dict[str, EmbeddingModel] = {
    "text-embedding-ada-002": OpenAIEmbedding("text-embedding-ada-002"),
    #'text-embedding-3-small': OpenAIEmbedding('text-embedding-3-small'),
    "text-embedding-3-large": OpenAIEmbedding("text-embedding-3-large"),
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


async def consume_messages():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="embedding_service",
        value_deserializer=lambda m: json.loads(m.decode("ascii")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("ascii"),
    )

    await consumer.start()
    await producer.start()

    logger.info("WAITING FOR MESSAGES!")

    try:
        async for msg in consumer:
            logger.warning(f"Received message: {msg.value}")
            try:
                if (message := msg.value) is None:
                    raise ValueError("Invalid message received")
                if (text := message.get("text")) is None:
                    raise ValueError("Text not provided")
                if (model := message.get("model")) is None:
                    raise ValueError("Model not provided")
                if model not in EMBEDDING_MODELS:
                    raise ValueError(
                        f"Model '{model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}"
                    )
                request = EmbeddingRequest(text=text, model=model)
                match await process_embedding_request(request):
                    case Ok(response):
                        await producer.send(KAFKA_TOPIC_OUTPUT, response.model_dump())
                        logger.info(f"Sent response for message: {msg.value}")
                    case Err(e):
                        raise ValueError(f"Error processing message: {e}")
            except Exception as e:
                error_response = {"error": str(e), "original_request": msg.value}
                await producer.send(KAFKA_TOPIC_OUTPUT, error_response)
                logger.error(f"Error processing message: {e}")
    finally:
        logger.warning("Stopping consumer and producer")
        await consumer.stop()
        await producer.stop()


@asynccontextmanager
async def lifespan(_: FastAPI):
    import asyncio

    consumer_task = asyncio.create_task(consume_messages())
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        logger.info("Consumer task was cancelled")


app = FastAPI(lifespan=lifespan)


@app.post("/embed", response_model=EmbeddingResponse)
async def create_embedding(request: EmbeddingRequest):
    if request.model not in EMBEDDING_MODELS:
        raise HTTPException(
            status_code=400,
            detail=f"Model '{request.model}' not supported, available models: {list(EMBEDDING_MODELS.keys())}",
        )

    try:
        model = EMBEDDING_MODELS[request.model]
        result = await model.generate_embedding(request.text)

        match result:
            case Ok(embeddings):
                return embeddings
            case Err(error):
                raise HTTPException(status_code=400, detail=error)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/models")
async def list_models():
    return {"available_models": list(EMBEDDING_MODELS.keys())}


@app.get("/")
async def root():
    return {"message": "Welcome to the embeddings service!"}
