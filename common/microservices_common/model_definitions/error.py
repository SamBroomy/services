from aiokafka import ConsumerRecord
from pydantic import BaseModel


class Error(BaseModel):
    error: str
    original_request: dict | BaseModel | ConsumerRecord


__all__ = ["Error"]
