import os
from typing import Dict, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled


class GoogleSheetsSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    """Configuration for the Google Sheets connector."""

    # Google API authentication
    credentials: str = Field(
        description="Path to the Google service account credentials JSON file."
    )

    # Configuration options
    folder_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Patterns to filter Google Drive folders to scan for sheets.",
    )

    sheet_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Patterns to filter Google Sheets by name.",
    )

    sheets_as_datasets: bool = Field(
        default=False,
        description="If True, each sheet within a Google Sheets document will be treated as a separate dataset. The Google Sheets document itself will be represented as a container. If False, each Google Sheets document will be treated as a dataset, with sheets as fields within that dataset.",
    )

    extract_usage_stats: bool = Field(
        default=True, description="Whether to extract usage statistics for sheets."
    )

    extract_tags: bool = Field(
        default=True,
        description="Whether to extract labels from Google Drive as tags. Fetches labelInfo from Drive API v3 and creates tags from label IDs and field values (text and selection types).",
    )

    usage_stats_lookback_days: int = Field(
        default=30,
        description="Number of days to look back for usage statistics.",
    )

    profiling: GEProfilingConfig = Field(
        default=GEProfilingConfig(),
        description="Configuration for profiling. Uses DataHub's standard Great Expectations profiler.",
    )

    extract_lineage_from_formulas: bool = Field(
        default=True,
        description="Whether to extract lineage information from sheet formulas. This includes connections to other Google Sheets and external data sources.",
    )

    parse_sql_for_lineage: bool = Field(
        default=True,
        description="Whether to use DataHub's SQL parser (sqlglot) to extract lineage from SQL queries in formulas. If False, falls back to basic regex-based extraction. Requires graph context.",
    )

    enable_cross_platform_lineage: bool = Field(
        default=True,
        description="Whether to extract cross-platform lineage (e.g., connections to BigQuery, etc.). Only applicable if extract_lineage_from_formulas is True.",
    )

    database_hostname_to_platform_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mappings to change platform instance in generated dataset urns for cross-platform lineage based on database hostname. "
        "Format: {'hostname': 'platform_instance'}. Use only if you really know what you are doing.",
    )

    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description="Whether to convert the urns of cross-platform lineage datasets to lowercase. "
        "This is recommended for consistency with most database platforms that use case-insensitive identifiers.",
    )

    extract_column_level_lineage: bool = Field(
        default=True,
        description="Whether to extract column-level lineage from Google Sheets formulas.",
    )

    scan_shared_drives: bool = Field(
        default=False,
        description="Whether to scan sheets in shared drives in addition to 'My Drive'.",
    )

    shared_drive_patterns: Optional[AllowDenyPattern] = Field(
        default=None,
        description="Patterns to filter which Shared Drives to scan by name. Only applicable when scan_shared_drives=True. If not set, all accessible shared drives are scanned. Use this to include/exclude specific shared drives by name.",
    )

    header_detection_mode: str = Field(
        default="first_row",
        description="How to detect headers: 'first_row' (default, always use row 1), 'auto_detect' (score-based detection), or 'none' (no headers, use column numbers like 'A', 'B', 'C').",
    )

    header_row_index: Optional[int] = Field(
        default=None,
        description="Explicit header row index (0-based). Overrides header_detection_mode if set. Use this when headers are always on a specific row (e.g., row 3 = index 2).",
    )

    skip_empty_leading_rows: bool = Field(
        default=True,
        description="Skip empty rows at the beginning before applying header detection. Recommended to avoid treating empty rows as headers.",
    )

    max_retries: int = Field(
        default=3, description="Number of retries for failed API requests."
    )

    retry_delay: int = Field(default=1, description="Delay in seconds between retries.")

    requests_per_second: Optional[float] = Field(
        default=None,
        description="Maximum number of Google API requests per second. If not set, no rate limiting is applied.",
    )

    extract_named_ranges: bool = Field(
        default=False,
        description="Whether to extract named ranges from Google Sheets as custom properties.",
    )

    enable_incremental_ingestion: bool = Field(
        default=True,
        description="Whether to enable incremental ingestion based on modifiedTime. Only sheets modified since the last run will be processed.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @field_validator("credentials")
    @classmethod
    def credentials_exist(cls, v: str) -> str:
        if not os.path.exists(v):
            raise ValueError(f"Credentials file {v} does not exist")
        return v

    @field_validator("header_detection_mode")
    @classmethod
    def validate_header_detection_mode(cls, v: str) -> str:
        valid_modes = {"first_row", "auto_detect", "none"}
        if v not in valid_modes:
            raise ValueError(
                f"header_detection_mode must be one of {valid_modes}, got '{v}'"
            )
        return v

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )
