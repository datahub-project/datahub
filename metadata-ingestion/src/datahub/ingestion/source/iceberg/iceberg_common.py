from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pydantic import Field
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


class IcebergCatalogConfig(ConfigModel):
    """
    Iceberg catalog config.

    https://py.iceberg.apache.org/configuration/
    """

    name: str = Field(
        default="default",
        description="Name of catalog",
    )
    type: str = Field(
        description="Type of catalog.  See [PyIceberg](https://py.iceberg.apache.org/configuration/) for list of possible values.",
    )
    config: Dict[str, str] = Field(
        description="Catalog specific configuration.  See [PyIceberg documentation](https://py.iceberg.apache.org/configuration/) for details.",
    )


class IcebergSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    # Override the stateful_ingestion config param with the Iceberg custom stateful ingestion config in the IcebergSourceConfig
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Iceberg Stateful Ingestion Config."
    )
    catalog: IcebergCatalogConfig = Field(
        description="Catalog configuration where to find Iceberg tables.  See [pyiceberg's catalog configuration details](https://py.iceberg.apache.org/configuration/).",
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

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    def get_catalog(self) -> Catalog:
        """Returns the Iceberg catalog instance as configured by the `catalog` dictionary.

        Returns:
            Catalog: Iceberg catalog instance.
        """
        return load_catalog(
            name=self.catalog.name, **{"type": self.catalog.type, **self.catalog.config}
        )


@dataclass
class IcebergSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    entities_profiled: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)
