from typing import List

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
        if v:
            for col in v:
                if not col or not isinstance(col, str):
                    raise ValueError(f"Invalid column name: {col}")
        return v
