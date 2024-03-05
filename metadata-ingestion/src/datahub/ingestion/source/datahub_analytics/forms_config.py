from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from pydantic import validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DatahubClientConfig


class PartitioningStrategy(str, Enum):
    DATE = "date"
    SNAPSHOT = "snapshot"


class DataHubAnalyticsFormSourceConfig(ConfigModel):
    server: DatahubClientConfig
    reporting_dataset_name: str = "reporting.forms"
    reporting_bucket_prefix: str = "s3://reporting-experiments/"
    reporting_store_platform: str = "s3"
    reporting_file_name: str = "forms"
    reporting_file_extension: str = "parquet"
    reporting_file_compression: str = "snappy"
    reporting_file_overwrite_existing: bool = False
    reporting_snapshot_partitioning_strategy: str = PartitioningStrategy.DATE
    allowed_forms: Optional[List[str]] = None

    @validator("reporting_snapshot_partitioning_strategy")
    def validate_partitioning_strategy(cls, v):
        if v not in PartitioningStrategy:
            raise ValueError(f"Unsupported partitioning strategy: {v}")
        return v


@dataclass
class DataHubAnalyticsFormSourceReport(SourceReport):
    feature_enabled: bool = True
    assets_scanned: int = 0
    forms_scanned: int = 0
    files_created: int = 0
    rows_created: int = 0
    reporting_store_connection_status: str = "Not connected"
    reporting_store_file_uri: str = ""

    def increment_assets_scanned(self):
        self.assets_scanned += 1

    def increment_forms_scanned(self):
        self.forms_scanned += 1

    def as_string(self) -> str:
        return super().as_string()
