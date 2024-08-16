from typing import Any, Optional, Self

from pydantic import BaseModel

from microservices_common.kafka.message import KafkaMessage


class ServiceError(BaseModel):
    error: str | KafkaMessage | Self | dict[str, Any] | BaseModel | None
    traceback: Optional[str] = None
    original_request: dict | KafkaMessage


class OneShotError(Exception):
    error: KafkaMessage


__all__ = ["ServiceError", "OneShotError"]
