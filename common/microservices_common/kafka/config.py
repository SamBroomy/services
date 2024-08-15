import os
from typing import Dict, List

from microservices_common.kafka.topics import KafkaTopic, KafkaTopicCategory


class KafkaConfig:
    TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "SERVICE")

    @classmethod
    def get_topic(cls, topic: KafkaTopic) -> str:
        parts = [cls.TOPIC_PREFIX, topic.category.value]
        if topic.operation:
            parts.append(topic.operation)
        return ".".join(parts)

    @classmethod
    def get_response_topic(cls, topic: KafkaTopic) -> str:
        return f"{cls.get_topic(topic)}.response"

    @classmethod
    def get_error_topic(cls, topic: KafkaTopic) -> str:
        return f"{cls.get_topic(topic)}.error"

    @classmethod
    def get_all_topics(cls) -> Dict[str, str]:
        return {topic.name: cls.get_topic(topic) for topic in KafkaTopic}

    @classmethod
    def get_all_response_topics(cls) -> Dict[str, str]:
        return {
            f"{topic.name}_RESPONSE": cls.get_response_topic(topic)
            for topic in KafkaTopic
        }

    @classmethod
    def get_all_error_topics(cls) -> Dict[str, str]:
        return {
            f"{topic.name}_ERROR": cls.get_error_topic(topic) for topic in KafkaTopic
        }

    @classmethod
    def get_category_topics(cls, category: KafkaTopicCategory) -> List[str]:
        return [
            cls.get_topic(topic) for topic in KafkaTopic if topic.category == category
        ]

    @classmethod
    def topic_from_string(cls, s: str) -> KafkaTopic:
        return KafkaTopic.from_string(s)

    @classmethod
    def category_from_string(cls, s: str) -> KafkaTopicCategory:
        return KafkaTopicCategory.from_string(s)
