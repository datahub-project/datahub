from datetime import date
from typing import Dict, List, TypedDict, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

# A partition column's value (date, numeric, or string) instead of a bare Any.
PartitionValue = Union[str, int, float, date]


class CachedPartitionMetadata(TypedDict):
    # Per-table partition metadata pre-fetched once per dataset from
    # INFORMATION_SCHEMA.COLUMNS, then threaded through discovery instead of a bare dict.
    partition_columns: List[str]
    column_types: Dict[str, str]


class ExtractedPartitionInfo(BaseModel):
    # Partition columns recovered from a BigQuery partition-filter error message.
    # Distinct from bigquery_schema.PartitionInfo (the structural definition from metadata).

    # Callers build this empty and populate required_columns afterwards, so the validator
    # must also run on assignment (not just construction) to actually check parsed values.
    model_config = ConfigDict(validate_assignment=True)

    required_columns: List[str] = Field(default_factory=list)

    @field_validator("required_columns")
    @classmethod
    def validate_required_columns(cls, v: List[str]) -> List[str]:
        # Pydantic already guarantees str items; only reject blank names.
        for col in v:
            if not col:
                raise ValueError("Partition column name must be non-empty")
        return v
