from typing import Optional

import pydantic
from pydantic import model_validator
from pydantic.fields import Field

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source_config.operation_config import OperationConfig


class DataLakeProfilerConfig(ConfigModel):
    enabled: bool = Field(
        default=False, description="Whether profiling should be done."
    )
    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )

    # These settings will override the ones below.
    profile_table_level_only: bool = Field(
        default=False,
        description="Whether to perform profiling at table-level only or include column-level profiling as well.",
    )

    _allow_deny_patterns: AllowDenyPattern = pydantic.PrivateAttr(
        default=AllowDenyPattern.allow_all(),
    )

    max_number_of_fields_to_profile: Optional[pydantic.PositiveInt] = Field(
        default=None,
        description="A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
    )

    include_field_null_count: bool = Field(
        default=True,
        description="Whether to profile for the number of nulls for each column.",
    )
    include_field_min_value: bool = Field(
        default=True,
        description="Whether to profile for the min value of numeric columns.",
    )
    include_field_max_value: bool = Field(
        default=True,
        description="Whether to profile for the max value of numeric columns.",
    )
    include_field_mean_value: bool = Field(
        default=True,
        description="Whether to profile for the mean value of numeric columns.",
    )
    include_field_median_value: bool = Field(
        default=True,
        description="Whether to profile for the median value of numeric columns.",
    )
    include_field_stddev_value: bool = Field(
        default=True,
        description="Whether to profile for the standard deviation of numeric columns.",
    )
    include_field_quantiles: bool = Field(
        default=True,
        description="Whether to profile for the quantiles of numeric columns.",
    )
    include_field_distinct_value_frequencies: bool = Field(
        default=True, description="Whether to profile for distinct value frequencies."
    )
    include_field_histogram: bool = Field(
        default=True,
        description="Whether to profile for the histogram for numeric fields.",
    )
    include_field_sample_values: bool = Field(
        default=True,
        description="Whether to profile for the sample values for all columns.",
    )

    @model_validator(mode="after")
    def ensure_field_level_settings_are_normalized(self) -> "DataLakeProfilerConfig":
        max_num_fields_to_profile = self.max_number_of_fields_to_profile

        # Disable all field-level metrics.
        if self.profile_table_level_only:
            for field_name in self.__class__.model_fields:
                if field_name.startswith("include_field_"):
                    setattr(self, field_name, False)

            assert max_num_fields_to_profile is None, (
                "max_number_of_fields_to_profile should be set to None"
            )

        return self
