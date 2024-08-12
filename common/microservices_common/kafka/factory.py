import json
import os
from typing import Any, Dict, List, Optional, Union

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
    def _custom_key_serializer(k: Optional[str]) -> Optional[bytes]:
        return k.encode("utf-8") if k is not None else None

    @staticmethod
    def _custom_value_serializer(v: Optional[dict | BaseModel]) -> Optional[bytes]:
        if v is None:
            return None
        if isinstance(v, BaseModel):
            v = v.model_dump()  # type: ignore
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
    def _custom_key_deserializer(k: Optional[bytes]) -> Optional[str]:
        return k.decode("utf-8") if k is not None else None

    @staticmethod
    def _custom_value_deserializer(v: Optional[bytes]) -> Optional[dict]:
        return json.loads(v.decode("utf-8")) if v is not None else None

    @classmethod
    def create_consumer(
        cls,
        topics: Union[KafkaTopic, List[KafkaTopic], KafkaTopicCategory],
        group_id: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    ) -> AIOKafkaConsumer:
        if isinstance(topics, KafkaTopic):
            topics = [topics]
        elif isinstance(topics, KafkaTopicCategory):
            topic_names = KafkaConfig.get_category_topics(topics)
        else:
            topic_names = [KafkaConfig.get_topic(topic) for topic in topics]

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
        topics: Union[KafkaTopic, List[KafkaTopic], KafkaTopicCategory],
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
        title="Kafka topic",
        description="The topic to send the message to. Can be a `KafkaTopic` or a string.",
    )
    value: Union[None, dict[str, Any], BaseModel] = Field(
        None,
        title="Message value",
        description="The message value to send. Can be a `BaseModel` or a dictionary. Need at least a value or a key.",
    )
    key: Optional[str] = Field(
        None,
        title="Message key",
        description="The message key to send. Can be a string or None. Need at least a value or a key.",
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
        raise ValueError("Value must be a BaseModel or a dictionary")

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

    def __init__(
        self,
        topics: Union[
            KafkaTopic,
            List[KafkaTopic],
            KafkaTopicCategory | List[KafkaTopicCategory] | str | List[str],
        ],
        group_id: str,
    ):
        if isinstance(topics, list):
            new_topics: list[KafkaTopic] = []
            for topic in topics:
                if isinstance(topic, str):
                    new_topics.append(KafkaTopic.from_string(topic))
                elif isinstance(topic, KafkaTopicCategory):
                    new_topics.extend(
                        [
                            KafkaTopic.from_string(t)
                            for t in KafkaConfig.get_category_topics(topic)
                        ]
                    )
                else:
                    new_topics.append(topic)
            topics = new_topics
        elif isinstance(topics, KafkaTopic):
            topics = [topics]
        elif isinstance(topics, KafkaTopicCategory):
            topics = [
                KafkaTopic.from_string(t)
                for t in KafkaConfig.get_category_topics(topics)
            ]
        elif isinstance(topics, str):
            topics = [KafkaTopic.from_string(topics)]

        self.consumer = KafkaAIOFactory.create_consumer(topics, group_id=group_id)

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
        return KafkaMessage(
            topic=msg.topic,
            value=msg.value,
            key=msg.key,
            partition=msg.partition,
            timestamp_ms=msg.timestamp,
            headers=msg.headers,
            offset=msg.offset,
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
        return cls.create_producer(), cls.create_consumer(topics, group_id=group_id)


__all__ = ["KafkaFactory", "KafkaMessage", "KafkaProducer", "KafkaConsumer"]
