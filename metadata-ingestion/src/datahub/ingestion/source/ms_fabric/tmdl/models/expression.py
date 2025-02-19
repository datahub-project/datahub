from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ExpressionType(str, Enum):
    """Types of expressions."""

    DAX = "DAX"
    M = "M"


class ExpressionKind(str, Enum):
    """Kinds of expressions."""

    CALCULATED_COLUMN = "calculatedColumn"
    CALCULATED_TABLE = "calculatedTable"
    MEASURE = "measure"
    PARTITION_QUERY = "partitionQuery"
    SECURITY_FILTER = "securityFilter"
    DETAIL_ROWS = "detailRows"


class Expression(BaseModel):
    """Base expression model."""

    expression_type: ExpressionType = Field(description="Type of expression (DAX or M)")
    expression: str = Field(description="Expression text")
    kind: Optional[ExpressionKind] = Field(None, description="Kind of expression")
