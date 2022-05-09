from pydantic.fields import Field

from datahub.configuration.common import ConfigModel


class GlueProfilingConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Whether to ingest data profiles stored in glue table parameters.",
    )
    row_count: str = Field(
        default=None,
        description="The parameter name for row count in glue table.",
    )
    column_count: str = Field(
        default=None,
        description="The parameter name for column count in glue table.",
    )
    unique_count: str = Field(
        default=None,
        description="The parameter name for the count of unique value in a column.",
    )
    unique_proportion: str = Field(
        default=None,
        description="The parameter name for the proportion of unique values in a column.",
    )
    null_count: int = Field(
        default=None,
        description="The parameter name for the count of null values in a column.",
    )
    null_proportion: str = Field(
        default=None,
        description="The parameter name for the proportion of null values in a column.",
    )
    min: str = Field(
        default=None,
        description="The parameter name for the min value of a column.",
    )
    max: str = Field(
        default=None,
        description="The parameter name for the max value of a column.",
    )
    mean: str = Field(
        default=None,
        description="The parameter name for the mean value of a column.",
    )
    median: str = Field(
        default=None,
        description="The parameter name for the median value of a column.",
    )
    stdev: str = Field(
        default=None,
        description="The parameter name for the standard deviation of a column.",
    )

    # partitioning
    partitioned: bool = Field(
        default=False,
        description="Whether to ingested partitioned glue profiles.",
    )
    partition_key: str = Field(
        default=None,
        description="The name of the partition column. It only supports single-key partitioning.",
    )
