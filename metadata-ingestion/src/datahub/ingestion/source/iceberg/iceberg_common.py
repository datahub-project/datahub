import os
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import pydantic
from azure.storage.filedatalake import FileSystemClient, PathProperties
from iceberg.core.filesystem.abfss_filesystem import AbfssFileSystem
from iceberg.core.filesystem.filesystem_tables import FilesystemTables
from pydantic import Field, root_validator

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.ingestion.source.azure.azure_common import AdlsSourceConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
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
    # Stats we cannot compute without looking at data
    # include_field_mean_value: bool = True
    # include_field_median_value: bool = True
    # include_field_stddev_value: bool = True
    # include_field_quantiles: bool = False
    # include_field_distinct_value_frequencies: bool = False
    # include_field_histogram: bool = False
    # include_field_sample_values: bool = True


class IcebergSourceConfig(StatefulIngestionConfigBase):
    # Override the stateful_ingestion config param with the Iceberg custom stateful ingestion config in the IcebergSourceConfig
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Iceberg Stateful Ingestion Config."
    )
    adls: Optional[AdlsSourceConfig] = Field(
        default=None,
        description="[Azure Data Lake Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) to crawl for Iceberg tables.  This is one filesystem type supported by this source and **only one can be configured**.",
    )
    localfs: Optional[str] = Field(
        default=None,
        description="Local path to crawl for Iceberg tables. This is one filesystem type supported by this source and **only one can be configured**.",
    )
    max_path_depth: int = Field(
        default=2,
        description="Maximum folder depth to crawl for Iceberg tables.  Folders deeper than this value will be silently ignored.",
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

    @pydantic.root_validator
    def validate_platform_instance(cls: "IcebergSourceConfig", values: Dict) -> Dict:
        stateful_ingestion = values.get("stateful_ingestion")
        if (
            stateful_ingestion
            and stateful_ingestion.enabled
            and not values.get("platform_instance")
        ):
            raise ConfigurationError(
                "Enabling Iceberg stateful ingestion requires to specify a platform instance."
            )
        return values

    @root_validator()
    def _ensure_one_filesystem_is_configured(
        cls: "IcebergSourceConfig", values: Dict
    ) -> Dict:
        if values.get("adls") and values.get("localfs"):
            raise ConfigurationError(
                "Only one filesystem can be configured: adls or localfs"
            )
        elif not values.get("adls") and not values.get("localfs"):
            raise ConfigurationError(
                "One filesystem (adls or localfs) needs to be configured."
            )
        return values

    @property
    def adls_filesystem_client(self) -> FileSystemClient:
        """Azure Filesystem client if configured.

        Raises:
            ConfigurationError: If ADLS is not configured.

        Returns:
            FileSystemClient: Azure Filesystem client instance to access storage account files and folders.
        """
        if self.adls:  # TODO Use local imports for abfss
            AbfssFileSystem.get_instance().set_conf(self.adls.dict())
            return self.adls.get_filesystem_client()
        raise ConfigurationError("No ADLS filesystem client configured")

    @property
    def filesystem_tables(self) -> FilesystemTables:
        """Iceberg FilesystemTables abstraction to access tables on a filesystem.
        Currently supporting ADLS (Azure Storage Account) and local filesystem.

        Raises:
            ConfigurationError: If no filesystem was configured.

        Returns:
            FilesystemTables: An Iceberg FilesystemTables abstraction instance to access tables on a filesystem
        """
        if self.adls:
            return FilesystemTables(self.adls.dict())
        elif self.localfs:
            return FilesystemTables()
        raise ConfigurationError("No filesystem client configured")

    def _get_adls_paths(self, root_path: str, depth: int) -> Iterable[Tuple[str, str]]:
        if self.adls and depth < self.max_path_depth:
            sub_paths = self.adls_filesystem_client.get_paths(
                path=root_path, recursive=False
            )
            sub_path: PathProperties
            for sub_path in sub_paths:
                if sub_path.is_directory:
                    dataset_name = ".".join(
                        sub_path.name[len(self.adls.base_path) + 1 :].split("/")
                    )
                    yield self.adls.get_abfss_url(sub_path.name), dataset_name
                    yield from self._get_adls_paths(sub_path.name, depth + 1)

    def _get_localfs_paths(
        self, root_path: str, depth: int
    ) -> Iterable[Tuple[str, str]]:
        if self.localfs and depth < self.max_path_depth:
            for f in os.scandir(root_path):
                if f.is_dir():
                    dataset_name = ".".join(f.path[len(self.localfs) + 1 :].split("/"))
                    yield f.path, dataset_name
                    yield from self._get_localfs_paths(f.path, depth + 1)

    def get_paths(self) -> Iterable[Tuple[str, str]]:
        """Generates a sequence of data paths and dataset names.

        Raises:
            ConfigurationError: If no filesystem configured.

        Yields:
            Iterator[Iterable[Tuple[str, str]]]: A sequence of tuples where the first item is the location of the dataset
            and the second item is the associated dataset name.
        """
        if self.adls:
            yield from self._get_adls_paths(self.adls.base_path, 0)
        elif self.localfs:
            yield from self._get_localfs_paths(self.localfs, 0)
        else:
            raise ConfigurationError("No filesystem client configured")


@dataclass
class IcebergSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    entities_profiled: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1
