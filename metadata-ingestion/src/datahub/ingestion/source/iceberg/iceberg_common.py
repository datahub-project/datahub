import os
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Tuple

from azure.storage.filedatalake import FileSystemClient, PathProperties
from iceberg.core.filesystem.abfss_filesystem import AbfssFileSystem
from iceberg.core.filesystem.filesystem_tables import FilesystemTables

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.azure.azure_common import AdlsSourceConfig


class IcebergProfilingConfig(ConfigModel):
    enabled: bool = False
    include_field_null_count: bool = True
    include_field_min_value: bool = True
    include_field_max_value: bool = True
    # Stats we cannot compute without looking at data
    # include_field_mean_value: bool = True
    # include_field_median_value: bool = True
    # include_field_stddev_value: bool = True
    # include_field_quantiles: bool = False
    # include_field_distinct_value_frequencies: bool = False
    # include_field_histogram: bool = False
    # include_field_sample_values: bool = True


class IcebergSourceConfig(DatasetSourceConfigBase):
    adls: Optional[AdlsSourceConfig]
    localfs: Optional[str]
    max_path_depth: int = 2
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    user_ownership_property: Optional[str]
    group_ownership_property: Optional[str]
    profiling: IcebergProfilingConfig = IcebergProfilingConfig()

    @property
    def adls_filesystem_client(self) -> FileSystemClient:
        """Azure Filesystem client if configured.

        Raises:
            ValueError: If ADLS is not configured.

        Returns:
            FileSystemClient: Azure Filesystem client instance to access storage account files and folders.
        """
        if self.adls:  # Use local imports for abfss
            AbfssFileSystem.get_instance().set_conf(self.adls.dict())
            return self.adls.get_filesystem_client()
        raise ValueError("No ADLS filesystem client configured")

    @property
    def filesystem_tables(self) -> FilesystemTables:
        """Iceberg FilesystemTables abstraction to access tables on a filesystem.
        Currently supporting ADLS (Azure Storage Account) and local filesystem.

        Raises:
            ValueError: If no filesystem was configured.

        Returns:
            FilesystemTables: An Iceberg FilesystemTables abstraction instance to access tables on a filesystem
        """
        if self.adls:
            return FilesystemTables(self.adls.dict())
        elif self.localfs:
            return FilesystemTables()
        raise ValueError("No filesystem client configured")

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
            ValueError: If no filesystem configured.

        Yields:
            Iterator[Iterable[Tuple[str, str]]]: A sequence of tuples where the first item is the location of the dataset
            and the second item is the associated dataset name.
        """
        if self.adls:
            yield from self._get_adls_paths(self.adls.base_path, 0)
        elif self.localfs:
            yield from self._get_localfs_paths(self.localfs, 0)
        else:
            raise ValueError("No filesystem client configured")


@dataclass
class IcebergSourceReport(SourceReport):
    tables_scanned: int = 0
    entities_profiled: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1
