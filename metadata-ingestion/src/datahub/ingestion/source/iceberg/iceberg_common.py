import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pydantic import Field, validator
from pyiceberg.catalog import Catalog, load_catalog

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
    user_ownership_property: Optional[str] = Field(
        default="owner",
        description="Iceberg table property to look for a `CorpUser` owner.  Can only hold a single user value.  If property has no value, no owner information will be emitted.",
    )
    group_ownership_property: Optional[str] = Field(
        default=None,
        description="Iceberg table property to look for a `CorpGroup` owner.  Can only hold a single group value.  If property has no value, no owner information will be emitted.",
    )
    profiling: IcebergProfilingConfig = IcebergProfilingConfig()

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
        return load_catalog(name=catalog_name, **catalog_config)


@dataclass
class IcebergSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    entities_profiled: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)
