import logging
import os
from typing import Dict, List, Literal, Optional

import certifi
from pydantic import Field, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingBaseConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.operation_config import is_profiling_enabled

logger = logging.getLogger(__name__)


class DremioConnectionConfig(ConfigModel):
    hostname: Optional[str] = Field(
        default=None,
        description="Hostname or IP Address of the Dremio server",
    )

    port: int = Field(
        default=9047,
        description="Port of the Dremio REST API",
    )

    username: Optional[str] = Field(
        default=None,
        description="Dremio username",
    )

    authentication_method: Optional[str] = Field(
        default="PAT",
        description="Authentication method: 'password' or 'PAT' (Personal Access Token)",
    )

    password: Optional[str] = Field(
        default=None,
        description="Dremio password or Personal Access Token",
    )

    tls: bool = Field(
        default=True,
        description="Whether the Dremio REST API port is encrypted",
    )

    disable_certificate_verification: Optional[bool] = Field(
        default=False,
        description="Disable TLS certificate verification",
    )

    path_to_certificates: str = Field(
        default=certifi.where(),
        description="Path to SSL certificates",
    )

    is_dremio_cloud: bool = Field(
        default=False,
        description="Whether this is a Dremio Cloud instance",
    )

    # https://docs.dremio.com/cloud/reference/api/#api-endpoint
    dremio_cloud_region: Literal["US", "EU"] = Field(
        default="US",
        description="Dremio Cloud region ('US' or 'EU')",
    )

    dremio_cloud_project_id: Optional[str] = Field(
        default=None,
        description="ID of Dremio Cloud Project. Found in Project Settings in the Dremio Cloud UI",
    )

    @validator("authentication_method")
    def validate_auth_method(cls, value):
        allowed_methods = ["password", "PAT"]
        if value not in allowed_methods:
            raise ValueError(
                f"authentication_method must be one of {allowed_methods}",
            )
        return value

    @validator("password")
    def validate_password(cls, value, values):
        if values.get("authentication_method") == "PAT" and not value:
            raise ValueError(
                "Password (Personal Access Token) is required when using PAT authentication",
            )
        return value


class ProfileConfig(GEProfilingBaseConfig):
    query_timeout: int = Field(
        default=300, description="Time before cancelling Dremio profiling query"
    )
    include_field_median_value: bool = Field(
        default=False,
        hidden_from_docs=True,
        description="Median causes a number of issues in Dremio.",
    )


class DremioSourceMapping(EnvConfigMixin, PlatformInstanceConfigMixin, ConfigModel):
    platform: str = Field(
        description="Source connection made by Dremio (e.g. S3, Snowflake)",
    )
    source_name: str = Field(
        description="Alias of platform in Dremio connection",
    )


class DremioSourceConfig(
    DremioConnectionConfig,
    StatefulIngestionConfigBase,
    BaseTimeWindowConfig,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    """
    Configuration for the Dremio metadata source.

    This configuration class defines all available options for connecting to and
    extracting metadata from Dremio deployments (Cloud, Software, Enterprise).
    It supports advanced features like format-based platform URNs, query lineage
    extraction, data profiling, and performance optimization settings.

    The configuration inherits from multiple mixins to provide:
    - Connection settings (DremioConnectionConfig)
    - Stateful ingestion capabilities (StatefulIngestionConfigBase)
    - Time window filtering (BaseTimeWindowConfig)
    - Environment and platform instance settings (EnvConfigMixin, PlatformInstanceConfigMixin)

    Key Features:
    - Multiple authentication methods (password, PAT)
    - Flexible filtering with AllowDenyPattern
    - Format-based platform URN generation for Delta Lake, Iceberg, etc.
    - Memory management with file-backed caching
    - SQL chunking for large view definitions
    - Cross-platform lineage with source mappings
    - Domain assignment based on dataset paths

    Example:
        >>> config = DremioSourceConfig(
        ...     host="dremio.company.com",
        ...     username="datahub_user",
        ...     password="secure_password",
        ...     use_format_based_platform_urns=True,
        ...     include_query_lineage=True
        ... )
    """

    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to Dremio sources, schemas, or datasets during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing". If you provide strings, then DataHub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified. Patterns are matched against the full dataset path in format "source.schema.table".',
    )

    source_mappings: Optional[List[DremioSourceMapping]] = Field(
        default=None,
        description="Mappings from Dremio sources to DataHub platforms and datasets.",
    )

    # Entity Filters (consistent with other DataHub connectors)
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for datasets (tables and views) to filter in ingestion. Specify regex to match the entire dataset name in source.schema.dataset format. e.g. to match all datasets starting with customer in s3 source and public schema, use the regex 's3.public.customer.*'",
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    # Profiling
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to profile",
    )
    profiling: ProfileConfig = Field(
        default=ProfileConfig(),
        description="Configuration for profiling",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    # Advanced Configs
    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for parallel processing",
    )

    include_query_lineage: bool = Field(
        default=False,
        description="Whether to include query-based lineage information.",
    )

    ingest_owner: bool = Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )

    # Format-based platform URN generation
    use_format_based_platform_urns: bool = Field(
        default=False,
        description="Generate platform-specific URNs based on table format (e.g., delta-lake, iceberg). When enabled, Delta Lake tables will use 'delta-lake' platform, Iceberg tables will use 'iceberg' platform, etc. When disabled, all tables use 'dremio' platform.",
    )

    format_platform_mapping: Dict[str, str] = Field(
        default_factory=lambda: {
            "delta": "delta-lake",
            "iceberg": "iceberg",
            "hudi": "hudi",
        },
        description="Mapping of format types to DataHub platforms. Keys are format type substrings (case-insensitive), values are DataHub platform names.",
    )

    # File-backed caching for OOM prevention (opt-in)
    enable_file_backed_cache: bool = Field(
        default=False,
        description="Enable file-backed caching to prevent OOM errors when processing large datasets. Uses SQLite for intermediate storage.",
    )
    file_backed_cache_size: int = Field(
        default=1000,
        description="Maximum number of items to keep in memory cache before writing to disk. Higher values use more memory but may be faster.",
    )
    cache_eviction_batch_size: int = Field(
        default=100,
        description="Number of items to evict from cache at once when memory limit is reached. Should be smaller than file_backed_cache_size.",
    )
    max_containers_per_batch: int = Field(
        default=10,
        description="Maximum number of containers to process in a single batch. Helps prevent OOM when discovering large numbers of containers.",
    )

    # Query retrieval optimization
    query_batch_size: int = Field(
        default=1000,
        description="Number of queries to fetch in each batch to prevent OOM errors. Smaller values use less memory but may be slower.",
    )

    max_query_duration_hours: int = Field(
        default=24,
        description="Maximum time window (in hours) for query retrieval in a single batch. Helps prevent OOM when there are many queries in a short time period.",
    )

    # View definition handling
    max_view_definition_length: int = Field(
        default=50000,
        description="Maximum length of view definition to retrieve. Longer definitions will be chunked into 32KB pieces for processing.",
        hidden_from_docs=True,
    )
    enable_sql_chunking: bool = Field(
        default=True,
        description="Enable chunking of large SQL statements (view definitions, queries) into 32KB pieces to prevent OOM while preserving SQL integrity.",
    )
    truncate_large_view_definitions: bool = Field(
        default=False,
        description="Whether to truncate view definitions that exceed max_view_definition_length. Deprecated - use enable_sql_chunking instead.",
    )

    # System tables configuration
    include_system_tables: bool = Field(
        default=False,
        description="Whether to include Dremio system tables (sys.*, information_schema.*) in ingestion. "
        "By default, system tables are excluded to focus on business data. "
        "Enable this if you need to catalog Dremio's internal metadata tables for governance or analysis purposes. ",
    )
