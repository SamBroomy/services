import asyncio
from typing import Dict, List, Union
from uuid import UUID
from uuid import uuid4 as uuid

from pydantic import (
    BaseModel,
)

from microservices_common.kafka.aio_factory import KafkaAIOFactory
from microservices_common.kafka.config import KafkaConfig
from microservices_common.kafka.factory import KafkaFactory
from microservices_common.kafka.message import KafkaMessage
from microservices_common.kafka.topics import (
    KafkaTopic,
    KafkaTopicCategory,
)
from microservices_common.logging import setup_logger


class KafkaProducer:
    def __init__(self):
        self.producer = KafkaAIOFactory.create_producer()
        self.logger = setup_logger(f"kafka-producer:{self.producer.client._client_id}")

    async def start(self):
        await self.producer.start()
        self.logger.debug("Producer started")

    async def stop(self):
        await self.producer.stop()
        self.logger.debug("Producer stopped")

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
        await self.producer.send(**message)
        self.logger.debug(f"Message sent to topic: {message['topic']}")

    async def send_response(
        self,
        kafka_message: KafkaMessage,
    ):
        message = kafka_message.to_kafka_response()
        await self.producer.send(**message)
        self.logger.debug(f"Response message sent to topic: {message["topic"]}")

    async def send_error(
        self,
        kafka_message: KafkaMessage,
    ):
        message = kafka_message.to_kafka_error()
        await self.producer.send(**message)
        self.logger.debug(f"Error message sent to topic: {message['topic']}")


class KafkaConsumer:
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
        self.logger = setup_logger(f"kafka-consumer:{self.consumer._client._client_id}")

    async def start(self):
        await self.consumer.start()
        self.logger.debug("Started consumer")

    async def stop(self):
        await self.consumer.stop()
        self.logger.debug("Stopped consumer")

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


class KafkaOneShot:
    """A class to send a message and wait for a response, used when you need to send a message and get a response from another service"""

    def __init__(self, topic: KafkaTopic, group_id: str):
        producer, consumer = KafkaFactory.create_producer_response_consumer(
            topic, group_id
        )
        self.topic = topic
        self.producer = producer
        self.consumer = consumer
        self.pending_requests: Dict[UUID, asyncio.Future] = {}
        self.logger = setup_logger(f"kafka-one-shot:{group_id}:{topic.name}")

    async def start(self):
        await self.consumer.start()
        await self.producer.start()
        self.logger.debug("Started Kafka one-shot")
        asyncio.create_task(self._consume_responses())
        self.logger.debug("Started consuming responses")

    async def stop(self):
        await self.consumer.stop()
        await self.producer.stop()
        self.logger.debug("Stopped Kafka one-shot")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        await self.stop()
        return False

    async def _consume_responses(self):
        async for msg in self.consumer:
            if msg.key in self.pending_requests:
                self.logger.debug(f"Found pending request for key: {msg.key}")
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
            self.logger.error(f"Request to {kafka_message.topic} timed out")
            del self.pending_requests[request_id]
            raise TimeoutError(
                f"Request to {kafka_message.topic} timed out after {timeout} seconds"
            )
