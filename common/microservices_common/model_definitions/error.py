from aiokafka import ConsumerRecord
from pydantic import BaseModel


class Error(BaseModel):
    error: str
    topic: str
    original_request: dict | BaseModel | ConsumerRecord


__all__ = ["Error"]
