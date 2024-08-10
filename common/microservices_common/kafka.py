from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from .utils import (
    custom_key_deserializer,
    custom_key_serializer,
    custom_value_deserializer,
    custom_value_serializer,
)


class KafkaProducerConsumerFactory:
    @staticmethod
    def create_producer(bootstrap_servers: str) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=custom_key_serializer,
            value_serializer=custom_value_serializer,
        )

    @staticmethod
    def create_consumer(
        *topics: str, bootstrap_servers: str, group_id: str
    ) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            key_deserializer=custom_key_deserializer,
            value_deserializer=custom_value_deserializer,
        )

    @staticmethod
    def create_producer_consumer(
        *topics: str, bootstrap_servers: str, group_id: str
    ) -> tuple[AIOKafkaProducer, AIOKafkaConsumer]:
        producer = KafkaProducerConsumerFactory.create_producer(bootstrap_servers)
        consumer = KafkaProducerConsumerFactory.create_consumer(
            *topics, bootstrap_servers=bootstrap_servers, group_id=group_id
        )
        return producer, consumer


async def send_response(
    producer: AIOKafkaProducer,
    topic: str,
    value: dict | BaseModel,
    key: Optional[str],
):
    await producer.send(topic, value=value, key=key)


__all__ = [
    "KafkaProducerConsumerFactory",
    "send_response",
]
