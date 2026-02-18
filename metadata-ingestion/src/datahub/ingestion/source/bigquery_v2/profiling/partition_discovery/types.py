from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class PartitionInfo(BaseModel):
    required_columns: List[str] = Field(default_factory=list)
    partition_values: Dict[str, Union[str, int]] = Field(default_factory=dict)

    @field_validator("required_columns")
    @classmethod
    def validate_required_columns(cls, v: List[str]) -> List[str]:
        if v:
            for col in v:
                if not col or not isinstance(col, str):
                    raise ValueError(f"Invalid column name: {col}")
        return v

    class Config:
        frozen = False


class PartitionResult(BaseModel):
    partition_values: Dict[str, Union[str, int, float]] = Field(default_factory=dict)
    row_count: Optional[int] = Field(default=None)

    @field_validator("row_count")
    @classmethod
    def validate_row_count(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError(f"row_count must be non-negative, got: {v}")
        return v

    class Config:
        frozen = False
