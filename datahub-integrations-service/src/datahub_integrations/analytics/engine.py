from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from pydantic import field_validator
from pydantic.fields import Field
from pydantic.main import BaseModel


class RowType(str, Enum):
    """Type of the row"""

    HEADER = "header"
    DATA = "data"
    ERROR = "error"


class Row(BaseModel):
    type: RowType
    values: List[Optional[str]]


class DataFormat(str, Enum):
    """Data format to return the data in."""

    JSON = "json"
    CSV = "csv"


class Filter(BaseModel):
    """Filter to apply to the data"""

    column: str
    value: Any = None
    condition: Optional[str] = None
    negated: bool = Field(default=False)

    @field_validator("condition", mode="before")
    @classmethod
    def default_condition_is_equals(cls, v: str) -> str:
        if v is None:
            return "="
        return v


class Predicate(BaseModel):
    """Collection of filters to apply to the data"""

    andFilters: List[Filter]


class ColumnAggregation(BaseModel):
    """Single aggregation to apply to the data"""

    column: str
    type: str
    alias: Optional[str] = None


class AggregationSpec(BaseModel):
    """Aggregation to apply to the data"""

    groupBy: Optional[List[str]] = None
    aggregations: Optional[List[ColumnAggregation]] = None


class AnalyticsEngine:
    """Base class for analytics engines"""

    def __init__(self) -> None:
        pass

    def get_preview(
        self, params: Dict[str, Any], limit: int, format: DataFormat
    ) -> Iterator[Row]:
        """Return a preview of the data at the physical location"""
        raise NotImplementedError

    def get_schema(self, params: Dict[str, str]) -> Dict[str, str]:
        """Return the schema of the data at the physical location"""
        raise NotImplementedError

    def get_data(
        self,
        params: Dict[str, Any],
        format: DataFormat,
        project: Optional[List[str]],
        filter: Optional[Predicate],
        aggregation: Optional[AggregationSpec],
        sql_query_fragment: Optional[str],
    ) -> Iterator[Row]:
        """Return the data at the physical location"""
        raise NotImplementedError
