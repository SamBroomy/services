import json
import os
from typing import Optional, Union
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import (
    BaseModel,
)

from microservices_common.logging import setup_logger

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

logger = setup_logger("kafka-aio-factory-utils")


def _custom_key_serializer(k: Optional[str | int | UUID]) -> Optional[bytes]:
    if k is None:
        return None
    if isinstance(k, UUID):
        return k.bytes
    return str(k).encode("utf-8")


def _custom_value_serializer(v: Optional[dict | BaseModel | str]) -> Optional[bytes]:
    if v is None:
        return None
    if isinstance(v, BaseModel):
        return v.model_dump_json().encode("utf-8")
    try:
        return json.dumps(v).encode("utf-8")
    except json.JSONDecodeError as e:
        logger.exception(f"Error serializing value: {v} - {e}")
        return str(v).encode("utf-8")


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


def _custom_value_deserializer(v: Optional[bytes]) -> Optional[dict | str]:
    if v is None:
        return None
    try:
        return json.loads(v.decode("utf-8"))
    except json.JSONDecodeError as e:
        logger.exception(f"Error deserializing value: {v} - {e}")
        return v.decode("utf-8")


class KafkaAIOFactory:
    @classmethod
    def create_producer(
        cls,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    ) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=_custom_key_serializer,
            value_serializer=_custom_value_serializer,
        )

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
            key_deserializer=_custom_key_deserializer,
            value_deserializer=_custom_value_deserializer,
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
