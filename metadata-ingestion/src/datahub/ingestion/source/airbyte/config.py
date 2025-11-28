import datetime
from typing import Dict, Optional

from pydantic import Field, SecretStr, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    EnvConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.str_enum import StrEnum


class AirbyteDeploymentType(StrEnum):
    """Enumeration of Airbyte deployment types"""

    OPEN_SOURCE = "oss"
    CLOUD = "cloud"


class PlatformInstanceConfig(ConfigModel):
    """Configuration for mapping Airbyte datasets to DataHub dataset URNs."""

    platform: Optional[str] = Field(
        default=None,
        description="Target DataHub platform name (overrides the source platform name)",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment to use for dataset URNs (e.g., PROD, DEV, STAGING)",
    )
    platform_instance: Optional[str] = Field(
        default=None, description="Platform instance to use for dataset URNs"
    )
    database_mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping from Airbyte database names to DataHub database names",
    )
    schema_mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping from Airbyte schema names to DataHub schema names",
    )
    database_prefix: Optional[str] = Field(
        default=None, description="Prefix to add to all database names"
    )
    schema_prefix: Optional[str] = Field(
        default=None, description="Prefix to add to all schema names"
    )


class PlatformMappingConfig(ConfigModel):
    default: PlatformInstanceConfig = Field(
        default_factory=PlatformInstanceConfig,
        description="Default mapping configuration to use when no specific mapping is found",
    )

    mappings: Dict[str, PlatformInstanceConfig] = Field(
        default_factory=dict,
        description="Mapping configurations for each platform (key: platform_name, value: mapping_config)",
    )

    def get_dataset_mapping(self, platform_name: str) -> PlatformInstanceConfig:
        if not platform_name:
            return self.default

        # Normalize the platform name
        normalized_name = platform_name.lower().replace(" ", "_").replace("-", "_")

        # Check if we have a mapping for this platform
        if normalized_name in self.mappings:
            return self.mappings[normalized_name]

        # Return the default mapping
        return self.default

    # Enable setting arbitrary platform mappings
    def __getitem__(self, key: str) -> PlatformInstanceConfig:
        return self.get_dataset_mapping(key)

    def __setitem__(self, key: str, value: PlatformInstanceConfig) -> None:
        normalized_key = key.lower().replace(" ", "_").replace("-", "_")
        self.mappings[normalized_key] = value


class AirbyteClientConfig(ConfigModel):
    """Base Airbyte Client Configuration"""

    # Deployment type to differentiate between Open Source and Cloud
    deployment_type: AirbyteDeploymentType = Field(
        default=AirbyteDeploymentType.OPEN_SOURCE,
        description="Type of Airbyte deployment ('oss' or 'cloud')",
    )

    # Connection settings
    host_port: Optional[str] = Field(
        default=None,
        description="Airbyte API host and port (e.g., http://localhost:8000) - required for OSS deployment",
    )

    # Authentication settings - Basic Auth
    username: Optional[str] = Field(
        default=None,
        description="Username for basic authentication (OSS deployment)",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Password for basic authentication (OSS deployment)",
    )

    # Authentication settings - API Key (for OSS)
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key or Personal Access Token for authentication (OSS deployment)",
    )

    # Authentication settings - OAuth2 (for cloud)
    oauth2_client_id: Optional[str] = Field(
        default=None,
        description="OAuth2 client ID (cloud deployment)",
    )
    oauth2_client_secret: Optional[SecretStr] = Field(
        default=None,
        description="OAuth2 client secret (cloud deployment)",
    )
    oauth2_refresh_token: Optional[SecretStr] = Field(
        default=None,
        description="OAuth2 refresh token (cloud deployment)",
    )

    # SSL verification settings
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates",
    )
    ssl_ca_cert: Optional[str] = Field(
        default=None,
        description="Path to CA certificate file (.pem) for SSL verification",
    )

    # Request configuration
    extra_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional HTTP headers to send with each request",
    )
    request_timeout: int = Field(
        default=30,
        description="Timeout for API requests in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for failed API requests",
    )
    retry_backoff_factor: float = Field(
        default=0.5,
        description="Backoff factor for retries (wait time is {factor} * (2 ^ retry_number))",
    )

    # Pagination
    page_size: int = Field(
        default=20,
        description="Number of items to fetch per page in API requests",
    )

    # Cloud specific configuration
    cloud_workspace_id: Optional[str] = Field(
        default=None,
        description="Workspace ID for Airbyte Cloud (required for cloud deployment)",
    )

    @validator("host_port")
    def validate_host_port_for_open_source(cls, v, values):
        if values.get("deployment_type") == AirbyteDeploymentType.OPEN_SOURCE and not v:
            raise ValueError("host_port is required for oss deployment")
        return v

    @validator("cloud_workspace_id")
    def validate_workspace_for_cloud(cls, v, values):
        if values.get("deployment_type") == AirbyteDeploymentType.CLOUD and not v:
            raise ValueError("cloud_workspace_id is required for cloud deployment")
        return v


class AirbyteSourceConfig(
    AirbyteClientConfig,
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    EnvConfigMixin,
):
    """Airbyte source configuration for metadata ingestion"""

    extract_column_level_lineage: bool = Field(
        default=True,
        description="Extract column-level lineage",
    )

    # Filters using standard DataHub allow/deny patterns
    workspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter workspaces. Use the pattern format as in other DataHub sources.",
    )

    connection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter connections. Use the pattern format as in other DataHub sources.",
    )

    source_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter sources. Use the pattern format as in other DataHub sources.",
    )

    destination_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter destinations. Use the pattern format as in other DataHub sources.",
    )

    # Jobs and status configuration
    include_statuses: bool = Field(
        default=True,
        description="Whether to ingest run statuses",
    )
    job_status_start_date: Optional[str] = Field(
        default=None,
        description="Start date for job status retrieval (format: yyyy-mm-ddTHH:MM:SSZ). Default is 7 days ago.",
    )
    job_status_end_date: Optional[str] = Field(
        default=None,
        description="End date for job status retrieval (format: yyyy-mm-ddTHH:MM:SSZ). Default is current time.",
    )
    job_statuses_limit: int = Field(
        default=100,
        description="Maximum number of job statuses to retrieve per connection",
    )

    # Owner extraction pattern
    extract_owners: bool = Field(
        default=False,
        description="Extract owners from connection names",
    )
    owner_extraction_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern with a capture group to extract owner from connection name",
    )

    # Tags extraction
    extract_tags: bool = Field(
        default=False,
        description="Extract tags from Airbyte metadata",
    )

    # Platform mapping
    platform_mapping: PlatformMappingConfig = Field(
        default_factory=PlatformMappingConfig,
        description="Platform mapping configuration for controlling dataset URN components",
    )

    # Workspace name as platform instance
    use_workspace_name_as_platform_instance: bool = Field(
        default=False,
        description="Use the workspace name as the platform instance in URNs. If enabled, this will override any platform_instance set in config.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("job_status_start_date", always=True)
    def set_default_start_date(cls, v):
        if v is None:
            # Default to 7 days ago
            seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            return seven_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
        return v

    @validator("job_status_end_date", always=True)
    def set_default_end_date(cls, v):
        if v is None:
            # Default to current time
            return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        return v
