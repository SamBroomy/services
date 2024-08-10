import json
from typing import Optional

from pydantic import BaseModel


def custom_key_deserializer(k: Optional[bytes]) -> Optional[str]:
    return k.decode("utf-8") if k is not None else None


def custom_value_deserializer(v: Optional[bytes]) -> Optional[dict]:
    return json.loads(v.decode("utf-8")) if v is not None else None


def custom_key_serializer(k: Optional[str]) -> Optional[bytes]:
    return k.encode("utf-8") if k is not None else None


def custom_value_serializer(v: Optional[dict | BaseModel]) -> Optional[bytes]:
    if v is None:
        return None
    if isinstance(v, BaseModel):
        v = v.model_dump()  # type: ignore
    return json.dumps(v).encode("utf-8")
