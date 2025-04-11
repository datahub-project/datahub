from typing import Any, Dict, Optional

import pydantic
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

    @pydantic.root_validator()
    def ensure_field_level_settings_are_normalized(
        cls: "DataLakeProfilerConfig", values: Dict[str, Any]
    ) -> Dict[str, Any]:
        max_num_fields_to_profile_key = "max_number_of_fields_to_profile"
        max_num_fields_to_profile = values.get(max_num_fields_to_profile_key)

        # Disable all field-level metrics.
        if values.get("profile_table_level_only"):
            for field_level_metric in cls.__fields__:
                if field_level_metric.startswith("include_field_"):
                    values.setdefault(field_level_metric, False)

            assert max_num_fields_to_profile is None, (
                f"{max_num_fields_to_profile_key} should be set to None"
            )

        return values
