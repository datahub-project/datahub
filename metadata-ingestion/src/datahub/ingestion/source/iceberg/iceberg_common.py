from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from azure.storage.filedatalake import FileSystemClient, PathProperties
from iceberg.core.filesystem.abfss_filesystem import AbfssFileSystem
from iceberg.core.filesystem.filesystem_tables import FilesystemTables

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
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


class IcebergSourceConfig(ConfigModel):
    env: str = DEFAULT_ENV
    adls: Optional[AdlsSourceConfig]
    base_path: str = "/"
    max_path_depth: int = 2
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    profiling: IcebergProfilingConfig = IcebergProfilingConfig()

    @property
    def filesystem_client(self) -> FileSystemClient:
        if self.adls:
            AbfssFileSystem.get_instance().set_conf(self.adls.dict())
            return self.adls.get_filesystem_client()
        raise ValueError("No filesystem client configured")

    @property
    def filesystem_tables(self) -> FilesystemTables:
        if self.adls:
            return FilesystemTables(self.adls.dict())
        raise ValueError("No filesystem client configured")

    @property
    def filesystem_url(self) -> str:
        if self.adls:
            return self.adls.get_abfss_url()
        raise ValueError("No filesystem client configured")

    def _get_paths(self, root_path: str, depth: int) -> Iterable[str]:
        if depth < self.max_path_depth:
            sub_paths = self.filesystem_client.get_paths(
                path=root_path, recursive=False
            )
            sub_path: PathProperties
            for sub_path in sub_paths:
                if sub_path.is_directory:
                    yield sub_path.name
                    yield from self._get_paths(sub_path.name, depth + 1)

    def get_paths(self) -> Iterable[str]:
        yield from self._get_paths(self.base_path, 0)


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
