from dataclasses import dataclass, field
from typing import List, Optional

from azure.storage.filedatalake import FileSystemClient
from iceberg.core.filesystem.abfss_filesystem import AbfssFileSystem
from iceberg.core.filesystem.filesystem_tables import FilesystemTables

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.azure.azure_common import AdlsSourceConfig


class IcebergProfilingConfig(ConfigModel):
    enabled: bool = False


class IcebergSourceConfig(ConfigModel):
    env: str = DEFAULT_ENV
    adls: Optional[AdlsSourceConfig]
    base_path: str = "/"
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
