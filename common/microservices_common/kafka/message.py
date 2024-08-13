from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import (
    BaseModel,
    Field,
    SerializationInfo,
    ValidationError,
    field_serializer,
    field_validator,
)

from microservices_common.kafka.config import KafkaConfig
from microservices_common.kafka.topics import (
    KafkaTopic,
)


class KafkaMessage(BaseModel):
    topic: KafkaTopic = Field(
        # alias="kafka_topic",
        title="Kafka topic",
        description="The topic to send the message to. Can be a `KafkaTopic` or a string.",
    )
    value: Union[None, dict[str, Any], BaseModel] = Field(
        None,
        # alias="payload",
        title="Message value",
        description="The message value to send. Can be a `BaseModel` or a dictionary. Need at least a value or a key.",
    )
    key: Optional[str | int | UUID] = Field(
        None,
        # alias="message_key",
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
        if not data.get("value") and not data.get("key"):
            raise ValidationError("Either value or key must be present")
        if data.get("value") is not None and not isinstance(
            data.get("value"), (BaseModel, dict)
        ):
            raise ValidationError("Value must be a BaseModel, a dictionary or None")
        super().__init__(topic=topic, **data)

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

    @field_validator("key", mode="before")
    def validate_key(
        cls, value: Optional[str | int | UUID]
    ) -> Optional[int | UUID | str]:
        if value is None or isinstance(value, (int | UUID)):
            return value
        try:
            return UUID(value)
        except TypeError:
            try:
                return int(value)
            except ValueError:
                return value

    @field_serializer("key")
    def serialize_key(self, value: int | UUID | str) -> str | int:
        if isinstance(value, UUID):
            return str(value)
        return value

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
