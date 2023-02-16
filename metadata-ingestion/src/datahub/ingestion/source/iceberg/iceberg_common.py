from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pydantic
from fsspec import AbstractFileSystem, filesystem
from pydantic import Field, root_validator
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchIcebergTableError
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.configuration.source_common import DatasetSourceConfigMixin
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


class IcebergSourceStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """Iceberg custom stateful ingestion config definition(overrides _entity_types of StatefulStaleMetadataRemovalConfig)."""

    _entity_types: List[str] = pydantic.Field(default=["table"])


class IcebergCatalogConfig(ConfigModel):
    """
    Iceberg catalog config.

    https://py.iceberg.apache.org/configuration/
    """

    name: str = Field(
        description="Name of catalog",
    )
    conf: Dict[str, str] = Field(
        description="Catalog specific configuration.  See [PyIceberg documentation](https://py.iceberg.apache.org/configuration/) for details.",
    )


class IcebergSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
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
    catalog: Optional[IcebergCatalogConfig] = Field(
        default=None,
        description="Catalog configuration where to find Iceberg tables.",
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

    @root_validator()
    def _ensure_one_filesystem_is_configured(
        cls: "IcebergSourceConfig", values: Dict
    ) -> Dict:
        count = sum(
            [
                1
                for x in [
                    values.get("catalog"),
                    values.get("adls"),
                    values.get("localfs"),
                ]
                if x is not None
            ]
        )
        if count == 0:
            raise ConfigurationError(
                "One filesystem (catalog or adls or localfs) needs to be configured."
            )
        elif count > 1:
            raise ConfigurationError(
                "Only one filesystem can be configured: catalog or adls or localfs"
            )
        return values

    def load_table(self, table_name: str, table_location: str) -> Table:
        """Now that Iceberg catalog support has been added to this source, this method can be removed when we migrate away from HadoopCatalog.

        Args:
            table_name (str): Name of the Iceberg table
            table_location (str): Location of Iceberg table

        Raises:
            NoSuchIcebergTableError: If an Iceberg table could not be loaded from the specified location

        Returns:
            Table: An Iceberg table instance
        """
        table_location = (
            self.adls.get_abfss_url(table_location) if self.adls else table_location
        )
        io = load_file_io(
            properties={**vars(self.adls)} if self.adls else {},
            location=table_location,
        )
        try:
            table_version = self._read_version_hint(table_location, io)
            metadata_location = (
                f"{table_location}/metadata/v{table_version}.metadata.json"
            )
            metadata_file = io.new_input(metadata_location)
            metadata = FromInputFile.table_metadata(metadata_file)
            return Table(
                identifier=table_name,
                metadata=metadata,
                metadata_location=metadata_location,
                io=io,
            )
        except FileNotFoundError as e:
            raise NoSuchIcebergTableError() from e

    # Temporary until we migrate away from HadoopCatalog (or pyiceberg implements https://github.com/apache/iceberg/issues/6430).
    def _read_version_hint(self, location: str, io: FileIO) -> int:
        version_hint_file = io.new_input(f"{location}/metadata/version-hint.text")

        if not version_hint_file.exists():
            raise FileNotFoundError()
        else:
            with version_hint_file.open() as f:
                return int(f.read())

    def _get_paths(
        self,
        fs: AbstractFileSystem,
        root_path: str,
        path: str,
        depth: int,
        fix_path: Callable[[str], str] = lambda path: path,
    ) -> Iterable[Tuple[str, str]]:
        if depth < self.max_path_depth:
            for sub_path in fs.ls(path, detail=True):
                if sub_path["type"] == "directory":
                    dataset_name = ".".join(
                        s for s in strltrim(sub_path["name"], root_path).split("/") if s
                    )
                    yield fix_path(sub_path["name"]), dataset_name
                    yield from self._get_paths(
                        fs, root_path, sub_path["name"], depth + 1
                    )

    def get_paths(self) -> Iterable[Tuple[str, str]]:
        """Generates a sequence of data paths and dataset names.

        Raises:
            ConfigurationError: If no filesystem configured.

        Yields:
            Iterator[Iterable[Tuple[str, str]]]: A sequence of tuples where the first item is the location of the dataset
            and the second item is the associated dataset name.
        """
        if self.adls:
            yield from self._get_paths(
                filesystem("abfs", **vars(self.adls)),
                f"{self.adls.container_name}/{self.adls.base_path}",
                f"{self.adls.container_name}/{self.adls.base_path}",
                0,
                self.adls.get_abfss_url,
            )
        elif self.localfs:
            yield from self._get_paths(
                filesystem("file"), self.localfs, self.localfs, 0
            )
        else:
            raise ConfigurationError("No filesystem client configured")

    def get_catalog(self) -> Catalog:
        """Returns the Iceberg catalog instance as configured by the `catalog` dictionary.

        Returns:
            Catalog: Iceberg catalog instance, `None` is not configured.
        """
        return (
            load_catalog(name=self.catalog.name, **self.catalog.conf)
            if self.catalog
            else None
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

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1


def strltrim(to_trim: str, prefix: str) -> str:
    return to_trim[len(prefix) :] if to_trim.startswith(prefix) else to_trim
