import os
from typing import List, Union

from microservices_common.kafka.client import KafkaConsumer, KafkaOneShot, KafkaProducer
from microservices_common.kafka.config import KafkaConfig
from microservices_common.kafka.topics import (
    KafkaTopic,
    KafkaTopicCategory,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


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


__all__ = ["KafkaFactory"]
