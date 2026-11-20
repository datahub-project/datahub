from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _FlinkApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class FlinkStatementSpec(_FlinkApiModel):
    statement: Optional[str] = None
    compute_pool_id: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)

    @field_validator("properties", mode="before")
    @classmethod
    def null_properties_are_empty(cls, value: object) -> object:
        return {} if value is None else value


class FlinkStatementStatus(_FlinkApiModel):
    phase: Optional[str] = None


class FlinkStatement(_FlinkApiModel):
    """A sql/v1 Statement. `name` is top level; `metadata` only holds `self`, `uid`, ..."""

    name: Optional[str] = None
    spec: Optional[FlinkStatementSpec] = None
    status: Optional[FlinkStatementStatus] = None


class FlinkPageMetadata(_FlinkApiModel):
    next: Optional[str] = None


class FlinkStatementsPage(_FlinkApiModel):
    # Items stay raw so one malformed statement does not drop the whole page.
    data: List[object] = Field(default_factory=list)
    metadata: Optional[FlinkPageMetadata] = None

    @field_validator("data", mode="before")
    @classmethod
    def null_data_is_empty(cls, value: object) -> object:
        return [] if value is None else value


class FlinkJob(BaseModel):
    """Topic-level lineage of one Flink statement in this recipe's Kafka cluster."""

    name: str
    input_topics: List[str] = Field(default_factory=list)
    output_topics: List[str] = Field(default_factory=list)
    query: str
    # The statement rewritten with bare topic names, for column-level lineage. None
    # when it reads or writes another cluster or holds more than one INSERT.
    parse_query: Optional[str] = None
    custom_properties: Dict[str, str] = Field(default_factory=dict)
