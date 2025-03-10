from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, validator


class TMDLBase(BaseModel):
    """Base class for all TMDL models."""

    name: str = Field(..., description="Name of the model element")
    description: Optional[str] = Field(
        None, description="Description of the model element"
    )
    annotations: Optional[Dict[str, Any]] = Field(
        default=None, description="Custom annotations"
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate name field."""
        if not v or not v.strip():
            raise ValueError("Name cannot be empty")
        if len(v) > 128:
            raise ValueError("Name cannot be longer than 128 characters")
        return v.strip()


class TMDLMetadata(BaseModel):
    """Metadata for TMDL models."""

    version: str = Field(..., description="TMDL version")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    modified_at: datetime = Field(default_factory=datetime.utcnow)
    compatibility_level: int = Field(
        1600, description="Compatibility level for the model"
    )


class DataType(BaseModel):
    """Data type definition."""

    name: str = Field(description="Data type name")
    length: Optional[int] = Field(None, description="Length for string types")
    precision: Optional[int] = Field(None, description="Precision for numeric types")
    scale: Optional[int] = Field(None, description="Scale for numeric types")

    @validator("name")
    def validate_data_type(cls, v: str) -> str:
        """Validate data type name."""
        valid_types = {
            "string",
            "int64",
            "double",
            "datetime",
            "boolean",
            "decimal",
            "binary",
            "variant",
        }
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid data type. Must be one of: {valid_types}")
        return v.lower()


class Expression(BaseModel):
    """Base class for DAX and M expressions."""

    expression_type: str = Field(..., description="Type of expression (DAX or M)")
    expression: str = Field(..., description="The expression text")

    @validator("expression_type")
    def validate_expression_type(cls, v: str) -> str:
        """Validate expression type."""
        if v not in ("DAX", "M"):
            raise ValueError("Expression type must be either 'DAX' or 'M'")
        return v
