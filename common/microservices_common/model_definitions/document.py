import logging
from typing import Any, ClassVar, Literal, Optional, Self

from pydantic import (
    BaseModel,
    Field,
    model_validator,
)

from microservices_common.logging import setup_logger


class ChunkingParams(BaseModel):
    chunking_strategy: Literal["basic", "by_title"]
    combine_under_n_chars: Optional[int] = Field(
        None, alias="combine_text_under_n_chars"
    )
    include_orig_elements: Optional[bool] = None
    max_characters: Optional[int] = None
    multipage_sections: Optional[bool] = None
    new_after_n_chars: Optional[int] = None
    overlap: Optional[int] = None
    overlap_all: Optional[bool] = None

    logger: ClassVar[logging.Logger] = setup_logger("chunking_params")

    @model_validator(mode="after")
    def validate_chunking_strategy(self) -> Self:
        if self.chunking_strategy == "basic":
            if self.combine_under_n_chars is not None:
                raise ValueError(
                    "combine_under_n_chars is not a valid parameter for basic chunking strategy"
                )
            if self.multipage_sections is not None:
                raise ValueError(
                    "multipage_sections is not a valid parameter for basic chunking strategy"
                )

        return self

    def update(self, other: "ChunkingParams") -> None:
        for key, value in other.model_dump(exclude_unset=True).items():
            current_value = getattr(self, key, None)
            self.logger.debug(f"Updating {key}:{current_value} to {value}")
            setattr(self, key, value)

    # @classmethod
    # def from_parameters(cls, parameters: dict[str, Any]) -> Optional[Self]:
    #     # Extract the chunking parameters from the request, removing them from the parameters dict as we want to chunk locally and not send them to the unstructured service.
    #     chunking_keys = cls.model_fields.keys()
    #     chunking_params = {
    #         key: parameters.pop(key, None) for key in chunking_keys if key in parameters
    #     }
    #     try:
    #         return cls(**chunking_params)
    #     except ValidationError:
    #         logger = setup_logger("chunking_params")
    #         logger.warning(f"Invalid chunking parameters: {chunking_params}")
    #         return None


class Document(BaseModel):
    filename: str
    file_id: Optional[str] = None
    parameters: Optional[dict[str, Any]] = None
    chunking_parameters: Optional[ChunkingParams] = None


class DocumentParseRequest(BaseModel):
    documents: list[Document]
    parameters: Optional[dict[str, Any]] = None
    chunking_parameters: Optional[ChunkingParams] = None


class Metadata(BaseModel):
    filetype: str
    languages: list[str]
    page_number: int
    filename: str
    parent_id: Optional[str] = None


class Chunk(BaseModel):
    type: str
    element_id: str
    text: str
    metadata: Metadata


class ParsedDocument(BaseModel):
    filename: str
    file_hash: str
    chunks: Optional[list[Chunk]] = None
    parameters: Optional[dict[str, Any]] = None
    chunking_parameters: Optional[ChunkingParams] = None


class DocumentParseResponse(BaseModel):
    documents: list[ParsedDocument]
