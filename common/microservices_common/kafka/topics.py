from enum import Enum
from typing import Optional


class KafkaTopicCategory(Enum):
    ORCHESTRATION = "orchestration"
    DOCUMENT = "document"
    EMBEDDING = "embedding"
    VECTOR_DB = "vector_db"
    LLM = "llm"
    QUESTION = "question"
    RERANK = "rerank"

    @classmethod
    def from_string(cls, s: str) -> "KafkaTopicCategory":
        for category in cls:
            if category.value == s:
                return category
        raise ValueError(f"No KafkaTopicCategory found for '{s}'")

    # def __str__(self) -> str:
    #     return self.value


class TopicResponseType(Enum):
    RESPONSE = "response"
    ERROR = "error"

    @classmethod
    def from_string(cls, s: str) -> "TopicResponseType":
        for topic_type in cls:
            if topic_type.value == s:
                return topic_type
        raise ValueError(f"No TopicType found for '{s}'")


class KafkaTopic(Enum):
    # Orchestration topics
    ORCHESTRATION = ("orchestration", None)
    ORCHESTRATION_UPLOAD_AND_SEARCH = ("orchestration", "upload_and_search")

    # Document topics
    DOCUMENT_PARSE = ("document", "parse")

    # Embedding topics
    EMBEDDING_GENERATE = ("embedding", "generate")
    EMBEDDING_MODEL_INFO = ("embedding", "model_info")

    # Vector DB topics
    VECTOR_DB_INSERT = ("vector_db", "insert")
    VECTOR_DB_SEARCH = ("vector_db", "search")

    # LLM topics
    LLM_GENERATE = ("llm", "generate")

    # Question topics
    QUESTION_REPHRASE = ("question", "rephrase")

    # Rerank topics
    RERANK = ("rerank", None)

    def __init__(
        self,
        category: str,
        operation: Optional[str],
    ):
        self.category = KafkaTopicCategory(category)
        self.operation = operation
        self.response: Optional[TopicResponseType] = None

    @classmethod
    def from_string(cls, s: str) -> "KafkaTopic":
        parts = s.split(".")
        if (
            len(parts) < 3 or len(parts) > 4
        ):  # We expect at least rag_pipeline.category.operation.response
            raise ValueError(f"Invalid topic string: {s}")

        category = parts[1]
        operation = parts[2] if len(parts) >= 3 else None
        response_or_error = (
            TopicResponseType.from_string(parts[-1])
            if parts[-1] in ["response", "error"]
            else None
        )

        for topic in cls:
            if topic.category.value == category and topic.operation == operation:
                output = KafkaTopic(category, operation)
                output.response = response_or_error
                return output

        raise ValueError(f"No KafkaTopic found for '{s}'")

    def is_error(self) -> bool:
        if self.response is None:
            return False
        return self.response == TopicResponseType.ERROR

    def is_response(self) -> bool:
        if self.response is None:
            return False
        return self.response == TopicResponseType.RESPONSE

    # def __str__(self) -> str:
    #     if self.operation:
    #         return f"rag_pipeline.{self.category}.{self.operation}"
    #     return f"rag_pipeline.{self.category}"


__all__ = ["KafkaTopic", "KafkaTopicCategory"]
