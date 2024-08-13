import asyncio
import json
import os
from typing import Any, Dict, List, Optional, Union
from uuid import UUID
from uuid import uuid4 as uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import (
    BaseModel,
    Field,
    SerializationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from microservices_common.kafka.config import KafkaConfig
from microservices_common.kafka.topics import (
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.logging import setup_logger

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

logger = setup_logger("kafka-factory")


class KafkaAIOFactory:
    @staticmethod
    def _custom_key_serializer(k: Optional[str | int | UUID]) -> Optional[bytes]:
        if k is None:
            return None
        if isinstance(k, UUID):
            return k.bytes
        return str(k).encode("utf-8")

    @staticmethod
    def _custom_value_serializer(v: Optional[dict | BaseModel]) -> Optional[bytes]:
        if v is None:
            return None
        if isinstance(v, BaseModel):
            return v.model_dump_json().encode("utf-8")
        return json.dumps(v).encode("utf-8")

    @classmethod
    def create_producer(
        cls,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    ) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=cls._custom_key_serializer,
            value_serializer=cls._custom_value_serializer,
        )

    @staticmethod
    def _custom_key_deserializer(k: Optional[bytes]) -> Optional[str | int | UUID]:
        if k is None:
            return None
        try:
            return UUID(bytes=k)
        except ValueError:
            key = k.decode("utf-8")
            try:
                return int(key)
            except ValueError:
                return key

    @staticmethod
    def _custom_value_deserializer(v: Optional[bytes]) -> Optional[dict]:
        return json.loads(v.decode("utf-8")) if v is not None else None

    @classmethod
    def create_consumer(
        cls,
        topics: Union[str, list[str]],
        group_id: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    ) -> AIOKafkaConsumer:
        if isinstance(topics, str):
            topic_names = [topics]
        else:
            topic_names = topics

        return AIOKafkaConsumer(
            *topic_names,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            key_deserializer=cls._custom_key_deserializer,
            value_deserializer=cls._custom_value_deserializer,
        )

    @classmethod
    def create_producer_consumer(
        cls,
        topics: Union[str, list[str]],
        group_id: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    ) -> tuple[AIOKafkaProducer, AIOKafkaConsumer]:
        producer = cls.create_producer(bootstrap_servers)
        consumer = cls.create_consumer(
            topics, group_id=group_id, bootstrap_servers=bootstrap_servers
        )
        return producer, consumer


class KafkaMessage(BaseModel):
    topic: KafkaTopic = Field(
        alias="kafka_topic",
        title="Kafka topic",
        description="The topic to send the message to. Can be a `KafkaTopic` or a string.",
    )
    value: Union[None, dict[str, Any], BaseModel] = Field(
        None,
        alias="payload",
        title="Message value",
        description="The message value to send. Can be a `BaseModel` or a dictionary. Need at least a value or a key.",
    )
    key: Optional[str | int | UUID] = Field(
        None,
        alias="message_key",
        title="Message key",
        description="The message key to send. Can be a string, int or UUID. Need at least a value or a key.",
    )
    partition: Optional[int] = Field(None)
    timestamp_ms: Optional[int] = Field(None)
    headers: Optional[List[tuple[str, bytes]]] = Field(default_factory=list)
    offset: Optional[int] = Field(
        None,
        description="The offset of the message, used for incoming messages, not for sending.",
    )
    partition: Optional[int] = Field(
        None,
        description="The partition of the message, used for incoming messages, not for sending.",
    )

    # To allow input fo topic as string
    def __init__(self, topic: str | KafkaTopic, **data):
        super().__init__(topic=topic, **data)

    @model_validator(mode="before")
    def check(cls, v):
        if not v.get("value") and not v.get("key"):
            raise ValueError("Either value or key must be present")
        value = v.get("value")
        if value is None or isinstance(value, (BaseModel, dict)):
            return v
        raise ValueError("Value must be a BaseModel, a dictionary or None")

    @field_validator("topic", mode="before")
    def validate_topic(cls, v: Union[str, KafkaTopic]) -> KafkaTopic:
        if isinstance(v, str):
            return KafkaTopic.from_string(v)
        return v

    @field_serializer("topic")
    def serilize_topic(self, topic: KafkaTopic, info: SerializationInfo):
        context = info.context
        if context:
            if context == "response":
                return KafkaConfig.get_response_topic(topic)
            elif context == "error":
                return KafkaConfig.get_error_topic(topic)
        return KafkaConfig.get_topic(topic)

    def to_kafka(self) -> Dict[str, Any]:
        dumped = self.model_dump(
            exclude={"value", "offset", "partition"},
            exclude_unset=True,
            exclude_defaults=True,
        )
        if self.value is not None:
            dumped["value"] = self.value
        return dumped

    # Cant do this for some reason, I think its a bug in pydantic
    # The value field wil be ignored in the model_dump and the value will be {}.
    # def to_kafka_response(self) -> Dict[str, Any]:
    #     return self.model_dump(context="response")
    def to_kafka_response(self) -> Dict[str, Any]:
        dumped = self.model_dump(
            context="response",
            exclude={"value", "offset", "partition"},
            exclude_unset=True,
            exclude_defaults=True,
        )
        if self.value is not None:
            dumped["value"] = self.value
        return dumped

    def to_kafka_error(self) -> Dict[str, Any]:
        dumped = self.model_dump(
            context="error",
            exclude={"value", "offset", "partition"},
            exclude_unset=True,
            exclude_defaults=True,
        )
        if self.value is not None:
            dumped["value"] = self.value
        return dumped


class KafkaProducer:
    logger = setup_logger("kafka-producer")

    def __init__(self):
        self.producer = KafkaAIOFactory.create_producer()

    @staticmethod
    def _custom_key_serializer(k: Optional[str]) -> Optional[bytes]:
        return k.encode("utf-8") if k is not None else None

    @staticmethod
    def _custom_value_serializer(v: Optional[dict | BaseModel]) -> Optional[bytes]:
        if v is None:
            return None
        if isinstance(v, BaseModel):
            v = v.model_dump()
        return json.dumps(v).encode("utf-8")

    async def start(self):
        self.logger.info(f"Starting producer [{self.producer.client._client_id}]")
        await self.producer.start()

    async def stop(self):
        self.logger.info(f"Stopping producer [{self.producer.client._client_id}]")
        await self.producer.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        await self.stop()
        return False

    async def send_message(
        self,
        kafka_message: KafkaMessage,
    ):
        message = kafka_message.to_kafka()
        self.logger.info(f"Sending message to topic: {message['topic']}")
        await self.producer.send(**message)

    async def send_response(
        self,
        kafka_message: KafkaMessage,
    ):
        message = kafka_message.to_kafka_response()
        self.logger.info(f"Sending response to topic: {message["topic"]}")
        await self.producer.send(**message)

    async def send_error(
        self,
        kafka_message: KafkaMessage,
    ):
        message = kafka_message.to_kafka_error()
        self.logger.info(f"Sending error to topic: {message['topic']}")
        await self.producer.send(**message)


class KafkaConsumer:
    logger = setup_logger("kafka-consumer")

    def _topic_to_string(
        self, topic: KafkaTopic | KafkaTopicCategory | str
    ) -> list[str]:
        if isinstance(topic, KafkaTopic):
            return [KafkaConfig.get_topic(topic)]
        if isinstance(topic, KafkaTopicCategory):
            return KafkaConfig.get_category_topics(topic)
        return [topic]

    def __init__(
        self,
        topics: Union[
            KafkaTopic,
            List[KafkaTopic],
            KafkaTopicCategory | List[KafkaTopicCategory] | str | List[str],
        ],
        group_id: str,
    ):
        if not isinstance(topics, list):
            tmp = [topics]
        else:
            tmp = topics
        kafka_topics: list[str] = []
        for topic in tmp:
            kafka_topics.extend(self._topic_to_string(topic))
        self.consumer = KafkaAIOFactory.create_consumer(kafka_topics, group_id=group_id)

    async def start(self):
        self.logger.info(f"Starting consumer [{self.consumer._client._client_id}]")
        await self.consumer.start()

    async def stop(self):
        self.logger.info(f"Stopping consumer [{self.consumer._client._client_id}]")
        await self.consumer.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        await self.consumer.stop()
        return False

    def __aiter__(self):
        return self

    async def __anext__(self) -> KafkaMessage:
        if self.consumer is None:
            raise ValueError("Consumer is not initialized")
        msg = await self.consumer.__anext__()
        return KafkaMessage.model_validate(msg, from_attributes=True)
        #     topic=msg.topic,
        #     value=msg.value,
        #     key=msg.key,
        #     partition=msg.partition,
        #     timestamp_ms=msg.timestamp,
        #     headers=msg.headers,
        #     offset=msg.offset,
        # )


class KafkaOneShot:
    """A class to send a message and wait for a response, used when you need to send a message and get a response from another service"""

    logger = setup_logger("kafka-oneshot")

    def __init__(self, topic: KafkaTopic, group_id: str):
        producer, consumer = KafkaFactory.create_producer_response_consumer(
            topic, group_id
        )
        self.topic = topic
        self.producer = producer
        self.consumer = consumer
        self.pending_requests: Dict[UUID, asyncio.Future] = {}

    async def start(self):
        await self.consumer.start()
        await self.producer.start()
        logger.info("Started Kafka one-shot producer and consumer")
        asyncio.create_task(self._consume_responses())
        logger.info("Started consuming responses")

    async def stop(self):
        await self.consumer.stop()
        await self.producer.stop()
        logger.info("Stopped Kafka one-shot producer and consumer")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        await self.stop()
        return False

    async def _consume_responses(self):
        async for msg in self.consumer:
            logger.info(f"Received response: {msg}")
            if msg.key in self.pending_requests:
                logger.info(f"Found pending request for key: {msg.key}")
                self.pending_requests[msg.key].set_result(msg)
                del self.pending_requests[msg.key]

    async def call(self, payload: BaseModel, timeout: float = 30.0) -> KafkaMessage:
        request_id = uuid()
        kafka_message = KafkaMessage(
            topic=self.topic,
            value=payload,
            key=request_id,
        )

        future = asyncio.Future()
        self.pending_requests[request_id] = future

        await self.producer.send_message(kafka_message)

        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            del self.pending_requests[request_id]
            raise TimeoutError(
                f"Request to {kafka_message.topic} timed out after {timeout} seconds"
            )


class KafkaFactory:
    @staticmethod
    def create_producer() -> KafkaProducer:
        return KafkaProducer()

    @staticmethod
    def create_consumer(
        topics: Union[
            KafkaTopic,
            List[KafkaTopic],
            KafkaTopicCategory | List[KafkaTopicCategory] | str | List[str],
        ],
        group_id: str,
    ) -> KafkaConsumer:
        return KafkaConsumer(topics, group_id=group_id)

    @classmethod
    def create_producer_consumer(
        cls,
        topics: Union[
            KafkaTopic,
            List[KafkaTopic],
            KafkaTopicCategory | List[KafkaTopicCategory] | str | List[str],
        ],
        group_id: str,
    ) -> tuple[KafkaProducer, KafkaConsumer]:
        """Create a producer and a consumer for the given topics and group_id"""
        return cls.create_producer(), cls.create_consumer(topics, group_id=group_id)

    @classmethod
    def create_producer_response_consumer(
        cls,
        topics: KafkaTopic,
        group_id: str,
    ) -> tuple[KafkaProducer, KafkaConsumer]:
        """Create a producer and a consumer that listens to the response topic of the given topic"""
        response_topic = KafkaConfig.get_response_topic(topics)
        return cls.create_producer(), cls.create_consumer(
            response_topic, group_id=group_id
        )

    @classmethod
    def create_one_shot(
        cls,
        topic: KafkaTopic,
        group_id: str,
    ) -> KafkaOneShot:
        return KafkaOneShot(topic, group_id)


__all__ = ["KafkaFactory", "KafkaMessage", "KafkaProducer", "KafkaConsumer"]
