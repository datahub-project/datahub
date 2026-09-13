"""Shared value objects for the PII pipeline.

Pydantic rather than dataclasses because `Verdict` is also the parse target for model
output: an out-of-taxonomy label or a confidence of 1.7 is rejected at the boundary
instead of travelling as far as the write.
"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pii_taxonomy import BY_NAME, is_taxonomy_tag


class Source(str, Enum):
    RULE = "rule"
    MODEL = "model"


class Column(BaseModel):
    model_config = ConfigDict(frozen=True)

    field_path: str
    native_type: str = ""
    description: str = ""
    existing_tags: tuple[str, ...] = ()

    @property
    def already_labelled(self) -> bool:
        return any(is_taxonomy_tag(tag) for tag in self.existing_tags)


class Verdict(BaseModel):
    model_config = ConfigDict(frozen=True)

    field: str
    label: str
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    source: Source

    @field_validator("label")
    @classmethod
    def _known_label(cls, value: str) -> str:
        if value and value not in BY_NAME:
            raise ValueError(f"unknown label {value!r}")
        return value

    @property
    def is_pii(self) -> bool:
        return bool(self.label)


class Decision(BaseModel):
    """The rule pass split: what it settled, and what it is handing to the model."""

    model_config = ConfigDict(frozen=True)

    verdicts: list[Verdict] = []
    residual: list[Column] = []
