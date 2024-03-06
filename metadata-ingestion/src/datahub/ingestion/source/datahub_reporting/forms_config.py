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


class DataHubReportingFormSourceConfig(ConfigModel):
    server: Optional[DatahubClientConfig] = None
    reporting_dataset_name: str = "reporting.forms"
    reporting_dataset_register_soft_deleted: bool = (
        True  # True by default because we want to keep the dataset hidden in the UI
    )
    reporting_bucket_prefix: Optional[str] = None
    reporting_store_platform: str = "s3"
    reporting_file_name: str = "forms"
    reporting_file_extension: str = "parquet"
    reporting_file_compression: str = "snappy"
    reporting_file_overwrite_existing: bool = True  # True by default because we want users to re-run reporting and get fresh data
    reporting_snapshot_partitioning_strategy: str = PartitioningStrategy.DATE
    forms_include: Optional[List[str]] = None  # If None, include all forms
    forms_exclude: Optional[List[str]] = None  # If None, exclude no forms
    generate_presigned_url: bool = True
    presigned_url_expiry_days: int = 7

    @validator("reporting_snapshot_partitioning_strategy")
    def validate_partitioning_strategy(cls, v):
        if v not in PartitioningStrategy:
            raise ValueError(f"Unsupported partitioning strategy: {v}")
        return v


@dataclass
class DataHubReportingFormSourceReport(SourceReport):
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
