import datetime
import os
from typing import Any, Dict, List, Optional

import pydantic
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel

_PROFILING_FLAGS_TO_REPORT = {
    "turn_off_expensive_profiling_metrics",
    "profile_table_level_only",
    "query_combiner_enabled",
    # all include_field_ flags are reported.
}


class GEProfilingConfig(ConfigModel):
    enabled: bool = Field(
        default=False, description="Whether profiling should be done."
    )
    limit: Optional[int] = Field(
        default=None,
        description="Max number of documents to profile. By default, profiles all documents.",
    )
    offset: Optional[int] = Field(
        default=None,
        description="Offset in documents to profile. By default, uses no offset.",
    )
    report_dropped_profiles: bool = Field(
        default=False,
        description="Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
    )

    turn_off_expensive_profiling_metrics: bool = Field(
        default=False,
        description="Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
    )
    profile_table_level_only: bool = Field(
        default=False,
        description="Whether to perform profiling at table-level only, or include column-level profiling as well.",
    )

    include_field_null_count: bool = Field(
        default=True,
        description="Whether to profile for the number of nulls for each column.",
    )
    include_field_distinct_count: bool = Field(
        default=True,
        description="Whether to profile for the number of distinct values for each column.",
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
        default=False,
        description="Whether to profile for the quantiles of numeric columns.",
    )
    include_field_distinct_value_frequencies: bool = Field(
        default=False, description="Whether to profile for distinct value frequencies."
    )
    include_field_histogram: bool = Field(
        default=False,
        description="Whether to profile for the histogram for numeric fields.",
    )
    include_field_sample_values: bool = Field(
        default=True,
        description="Whether to profile for the sample values for all columns.",
    )

    _allow_deny_patterns: AllowDenyPattern = pydantic.PrivateAttr(
        default=AllowDenyPattern.allow_all(),
    )
    max_number_of_fields_to_profile: Optional[pydantic.PositiveInt] = Field(
        default=None,
        description="A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
    )

    profile_if_updated_since_days: Optional[pydantic.PositiveFloat] = Field(
        default=1,
        description="Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`.",
    )

    profile_table_size_limit: Optional[int] = Field(
        default=1,
        description="Profile tables only if their size is less then specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `snowflake` and `BigQuery`",
    )

    profile_table_row_limit: Optional[int] = Field(
        default=50000,
        description="Profile tables only if their row count is less then specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `snowflake` and `BigQuery`",
    )

    # The default of (5 * cpu_count) is adopted from the default max_workers
    # parameter of ThreadPoolExecutor. Given that profiling is often an I/O-bound
    # task, it may make sense to increase this default value in the future.
    # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for profiling. Set to 1 to disable.",
    )

    # The query combiner enables us to combine multiple queries into a single query,
    # reducing the number of round-trips to the database and speeding up profiling.
    query_combiner_enabled: bool = Field(
        default=True,
        description="*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
    )

    # Hidden option - used for debugging purposes.
    catch_exceptions: bool = Field(default=True, description="")

    partition_profiling_enabled: bool = Field(default=True, description="")
    bigquery_temp_table_schema: Optional[str] = Field(
        default=None,
        description="On bigquery for profiling partitioned tables needs to create temporary views. You have to define a dataset where these will be created. Views will be cleaned up after profiler runs. (Great expectation tech details about this (https://legacy.docs.greatexpectations.io/en/0.9.0/reference/integrations/bigquery.html#custom-queries-with-sql-datasource).",
    )
    partition_datetime: Optional[datetime.datetime] = Field(
        default=None,
        description="For partitioned datasets profile only the partition which matches the datetime or profile the latest one if not set. Only Bigquery supports this.",
    )

    @pydantic.root_validator()
    def ensure_field_level_settings_are_normalized(
        cls: "GEProfilingConfig", values: Dict[str, Any]
    ) -> Dict[str, Any]:
        max_num_fields_to_profile_key = "max_number_of_fields_to_profile"
        max_num_fields_to_profile = values.get(max_num_fields_to_profile_key)

        # Disable all field-level metrics.
        if values.get("profile_table_level_only"):
            for field_level_metric in cls.__fields__:
                if field_level_metric.startswith("include_field_"):
                    values.setdefault(field_level_metric, False)

            assert (
                max_num_fields_to_profile is None
            ), f"{max_num_fields_to_profile_key} should be set to None"

        # Disable expensive queries.
        if values.get("turn_off_expensive_profiling_metrics"):
            expensive_field_level_metrics: List[str] = [
                "include_field_quantiles",
                "include_field_distinct_value_frequencies",
                "include_field_histogram",
                "include_field_sample_values",
            ]
            for expensive_field_metric in expensive_field_level_metrics:
                values.setdefault(expensive_field_metric, False)

            # By default, we profile at most 10 non-filtered columns in this mode.
            values.setdefault(max_num_fields_to_profile_key, 10)

        return values

    def any_field_level_metrics_enabled(self) -> bool:
        return any(
            getattr(self, field_name)
            for field_name in self.__fields__
            if field_name.startswith("include_field_")
        )

    def config_for_telemetry(self) -> Dict[str, Any]:
        config_dict = self.dict()

        return {
            flag: config_dict[flag]
            for flag in config_dict
            if flag in _PROFILING_FLAGS_TO_REPORT or flag.startswith("include_field_")
        }
