from typing import Optional

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source_config.operation_config import OperationConfig


class GlueProfilingConfig(ConfigModel):
    row_count: Optional[str] = Field(
        default=None,
        description="The parameter name for row count in glue table.",
    )
    column_count: Optional[str] = Field(
        default=None,
        description="The parameter name for column count in glue table.",
    )
    unique_count: Optional[str] = Field(
        default=None,
        description="The parameter name for the count of unique value in a column.",
    )
    unique_proportion: Optional[str] = Field(
        default=None,
        description="The parameter name for the proportion of unique values in a column.",
    )
    null_count: Optional[str] = Field(
        default=None,
        description="The parameter name for the count of null values in a column.",
    )
    null_proportion: Optional[str] = Field(
        default=None,
        description="The parameter name for the proportion of null values in a column.",
    )
    min: Optional[str] = Field(
        default=None,
        description="The parameter name for the min value of a column.",
    )
    max: Optional[str] = Field(
        default=None,
        description="The parameter name for the max value of a column.",
    )
    mean: Optional[str] = Field(
        default=None,
        description="The parameter name for the mean value of a column.",
    )
    median: Optional[str] = Field(
        default=None,
        description="The parameter name for the median value of a column.",
    )
    stdev: Optional[str] = Field(
        default=None,
        description="The parameter name for the standard deviation of a column.",
    )
    partition_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="""Regex patterns for filtering partitions for profile. The pattern should be a string like: "{'key':'value'}".""",
    )

    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )
