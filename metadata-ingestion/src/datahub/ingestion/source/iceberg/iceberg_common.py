import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from humanfriendly import format_timespan
from pydantic import Field, validator
from pyiceberg.catalog import Catalog, load_catalog
from sortedcontainers import SortedList

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import (
    OperationConfig,
    is_profiling_enabled,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict

logger = logging.getLogger(__name__)


class IcebergProfilingConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Whether profiling should be done.",
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
    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )
    # Stats we cannot compute without looking at data
    # include_field_mean_value: bool = True
    # include_field_median_value: bool = True
    # include_field_stddev_value: bool = True
    # include_field_quantiles: bool = False
    # include_field_distinct_value_frequencies: bool = False
    # include_field_histogram: bool = False
    # include_field_sample_values: bool = True


class IcebergSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    # Override the stateful_ingestion config param with the Iceberg custom stateful ingestion config in the IcebergSourceConfig
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Iceberg Stateful Ingestion Config."
    )
    # The catalog configuration is using a dictionary to be open and flexible.  All the keys and values are handled by pyiceberg.  This will future-proof any configuration change done by pyiceberg.
    catalog: Dict[str, Dict[str, Any]] = Field(
        description="Catalog configuration where to find Iceberg tables.  Only one catalog specification is supported.  The format is the same as [pyiceberg's catalog configuration](https://py.iceberg.apache.org/configuration/), where the catalog name is specified as the object name and attributes are set as key-value pairs.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion.",
    )
    namespace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for namespaces to filter in ingestion.",
    )
    user_ownership_property: Optional[str] = Field(
        default="owner",
        description="Iceberg table property to look for a `CorpUser` owner.  Can only hold a single user value.  If property has no value, no owner information will be emitted.",
    )
    group_ownership_property: Optional[str] = Field(
        default=None,
        description="Iceberg table property to look for a `CorpGroup` owner.  Can only hold a single group value.  If property has no value, no owner information will be emitted.",
    )
    profiling: IcebergProfilingConfig = IcebergProfilingConfig()
    processing_threads: int = Field(
        default=1, description="How many threads will be processing tables"
    )

    @validator("catalog", pre=True, always=True)
    def handle_deprecated_catalog_format(cls, value):
        # Once support for deprecated format is dropped, we can remove this validator.
        if (
            isinstance(value, dict)
            and "name" in value
            and "type" in value
            and "config" in value
        ):
            # This looks like the deprecated format
            logger.warning(
                "The catalog configuration format you are using is deprecated and will be removed in a future version. Please update to the new format.",
            )
            catalog_name = value["name"]
            catalog_type = value["type"]
            catalog_config = value["config"]
            new_catalog_config = {
                catalog_name: {"type": catalog_type, **catalog_config}
            }
            return new_catalog_config
        # In case the input is already the new format or is invalid
        return value

    @validator("catalog")
    def validate_catalog_size(cls, value):
        if len(value) != 1:
            raise ValueError("The catalog must contain exactly one entry.")

        # Retrieve the dict associated with the one catalog entry
        catalog_name, catalog_config = next(iter(value.items()))

        # Check if that dict is not empty
        if not catalog_config or not isinstance(catalog_config, dict):
            raise ValueError(
                f"The catalog configuration for '{catalog_name}' must not be empty and should be a dictionary with at least one key-value pair."
            )

        return value

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    def get_catalog(self) -> Catalog:
        """Returns the Iceberg catalog instance as configured by the `catalog` dictionary.

        Returns:
            Catalog: Iceberg catalog instance.
        """
        if not self.catalog:
            raise ValueError("No catalog configuration found")

        # Retrieve the dict associated with the one catalog entry
        catalog_name, catalog_config = next(iter(self.catalog.items()))
        logger.debug(
            "Initializing the catalog %s with config: %s", catalog_name, catalog_config
        )
        return load_catalog(name=catalog_name, **catalog_config)


class TopTableTimings:
    _VALUE_FIELD: str = "timing"
    top_entites: SortedList
    _size: int

    def __init__(self, size: int = 10):
        self._size = size
        self.top_entites = SortedList(key=lambda x: -x.get(self._VALUE_FIELD, 0))

    def add(self, entity: Dict[str, Any]) -> None:
        if self._VALUE_FIELD not in entity:
            return
        self.top_entites.add(entity)
        if len(self.top_entites) > self._size:
            self.top_entites.pop()

    def __str__(self) -> str:
        if len(self.top_entites) == 0:
            return "no timings reported"
        return str(list(self.top_entites))


class TimingClass:
    times: SortedList

    def __init__(self):
        self.times = SortedList()

    def add_timing(self, t: float) -> None:
        self.times.add(t)

    def __str__(self) -> str:
        if len(self.times) == 0:
            return "no timings reported"
        total = sum(self.times)
        avg = total / len(self.times)
        return str(
            {
                "average_time": format_timespan(avg, detailed=True, max_units=3),
                "min_time": format_timespan(self.times[0], detailed=True, max_units=3),
                "max_time": format_timespan(self.times[-1], detailed=True, max_units=3),
                # total_time does not provide correct information in case we run in more than 1 thread
                "total_time": format_timespan(total, detailed=True, max_units=3),
            }
        )


@dataclass
class IcebergSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)
    load_table_timings: TimingClass = field(default_factory=TimingClass)
    processing_table_timings: TimingClass = field(default_factory=TimingClass)
    profiling_table_timings: TimingClass = field(default_factory=TimingClass)
    tables_load_timings: TopTableTimings = field(default_factory=TopTableTimings)
    tables_profile_timings: TopTableTimings = field(default_factory=TopTableTimings)
    tables_process_timings: TopTableTimings = field(default_factory=TopTableTimings)
    listed_namespaces: int = 0
    total_listed_tables: int = 0
    tables_listed_per_namespace: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    def report_listed_tables_for_namespace(
        self, namespace: str, no_tables: int
    ) -> None:
        self.tables_listed_per_namespace[namespace] = no_tables
        self.total_listed_tables += no_tables

    def report_no_listed_namespaces(self, amount: int) -> None:
        self.listed_namespaces = amount

    def report_table_scanned(self, name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_table_load_time(
        self, t: float, table_name: str, table_metadata_location: str
    ) -> None:
        self.load_table_timings.add_timing(t)
        self.tables_load_timings.add(
            {"table": table_name, "timing": t, "metadata_file": table_metadata_location}
        )

    def report_table_processing_time(
        self, t: float, table_name: str, table_metadata_location: str
    ) -> None:
        self.processing_table_timings.add_timing(t)
        self.tables_process_timings.add(
            {"table": table_name, "timing": t, "metadata_file": table_metadata_location}
        )

    def report_table_profiling_time(
        self, t: float, table_name: str, table_metadata_location: str
    ) -> None:
        self.profiling_table_timings.add_timing(t)
        self.tables_profile_timings.add(
            {"table": table_name, "timing": t, "metadata_file": table_metadata_location}
        )
