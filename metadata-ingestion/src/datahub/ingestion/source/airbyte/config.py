import datetime
from typing import Dict, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    EnvConfigMixin,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
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


# Default Airbyte Cloud URLs
DEFAULT_CLOUD_API_URL = "https://api.airbyte.com/v1"
DEFAULT_CLOUD_OAUTH_TOKEN_URL = "https://auth.airbyte.com/oauth/token"

# Known source type to DataHub platform mapping
KNOWN_SOURCE_TYPE_MAPPING = {
    # Relational Databases
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mssql": "mssql",
    "sql-server": "mssql",
    "sqlserver": "mssql",
    "oracle": "oracle",
    "db2": "db2",
    # Cloud Data Warehouses
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "synapse": "mssql",
    # NoSQL Databases
    "mongodb": "mongodb",
    "mongo": "mongodb",
    "cassandra": "cassandra",
    "dynamodb": "dynamodb",
    "elasticsearch": "elasticsearch",
    "opensearch": "opensearch",
    "clickhouse": "clickhouse",
    # Big Data & Analytics
    "hive": "hive",
    "presto": "presto",
    "trino": "trino",
    "athena": "athena",
    "vertica": "vertica",
    "teradata": "teradata",
    "druid": "druid",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "google-cloud-storage": "gcs",
    "azure-blob-storage": "abs",
    "abs": "abs",
    # Streaming & Messaging
    "kafka": "kafka",
    "pulsar": "pulsar",
    "kinesis": "kinesis",
    # File Formats & Data Lakes
    "delta-lake": "delta-lake",
    "iceberg": "iceberg",
    "hudi": "hudi",
    # Other
    "glue": "glue",
    "salesforce": "salesforce",
    "netsuite": "netsuite",
    "sap-hana": "hana",
    "hana": "hana",
}


class PlatformDetail(ConfigModel):
    """Configuration for mapping a specific Airbyte source/destination to DataHub URNs."""

    platform: Optional[str] = Field(
        default=None,
        description="Override the platform type detection (e.g., 'postgres', 'mysql')",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets belong to",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment to use for dataset URNs (e.g., PROD, DEV, STAGING)",
    )
    database: Optional[str] = Field(
        default=None,
        description="Override the database name for all datasets from this source/destination",
    )
    include_schema_in_urn: bool = Field(
        default=True,
        description="Include schema in the dataset URN. Set to false for two-tier sources like MySQL.",
    )


class AirbyteClientConfig(ConfigModel):
    """Base Airbyte Client Configuration"""

    deployment_type: AirbyteDeploymentType = Field(
        default=AirbyteDeploymentType.OPEN_SOURCE,
        description="Type of Airbyte deployment ('oss' or 'cloud')",
    )

    host_port: Optional[str] = Field(
        default=None,
        description="Airbyte API host and port (e.g., http://localhost:8000) - required for OSS deployment",
    )

    username: Optional[str] = Field(
        default=None,
        description="Username for basic authentication (OSS deployment)",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Password for basic authentication (OSS deployment)",
    )

    api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key or Personal Access Token for authentication (OSS deployment)",
    )

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

    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates",
    )
    ssl_ca_cert: Optional[str] = Field(
        default=None,
        description="Path to CA certificate file (.pem) for SSL verification",
    )

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

    page_size: int = Field(
        default=20,
        description="Number of items to fetch per page in API requests",
    )

    cloud_workspace_id: Optional[str] = Field(
        default=None,
        description="Workspace ID for Airbyte Cloud (required for cloud deployment)",
    )

    cloud_api_url: str = Field(
        default=DEFAULT_CLOUD_API_URL,
        description="Base URL for Airbyte Cloud API (defaults to production URL)",
    )
    cloud_oauth_token_url: str = Field(
        default=DEFAULT_CLOUD_OAUTH_TOKEN_URL,
        description="OAuth token URL for Airbyte Cloud (defaults to production URL)",
    )

    @model_validator(mode="after")
    def validate_deployment_requirements(self) -> "AirbyteClientConfig":
        """Validate deployment-specific required fields."""
        if (
            self.deployment_type == AirbyteDeploymentType.OPEN_SOURCE
            and not self.host_port
        ):
            raise ValueError("host_port is required for oss deployment")

        if (
            self.deployment_type == AirbyteDeploymentType.CLOUD
            and not self.cloud_workspace_id
        ):
            raise ValueError("cloud_workspace_id is required for cloud deployment")

        return self


class AirbyteSourceConfig(
    AirbyteClientConfig,
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    EnvConfigMixin,
    IncrementalLineageConfigMixin,
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

    source_type_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from Airbyte sourceType/destinationType to DataHub platform names. "
        "Use this to normalize Airbyte's source types to DataHub platform names. "
        "Example: {'PostgreSQL': 'postgres', 'MySQL': 'mysql'}. "
        "If not specified, the sourceType/destinationType from Airbyte is sanitized and used directly.",
    )

    sources_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default_factory=dict,
        description="A mapping from Airbyte source ID to its platform/instance/env/database details. "
        "Use this to override platform details for specific sources. "
        "Example: {'11111111-1111-1111-1111-111111111111': {'platform': 'postgres', 'platform_instance': 'prod-postgres', 'env': 'PROD'}}",
    )

    destinations_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default_factory=dict,
        description="A mapping from Airbyte destination ID to its platform/instance/env/database details. "
        "Use this to override platform details for specific destinations.",
    )

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

    extract_owners: bool = Field(
        default=False,
        description="Extract owners from connection names",
    )
    owner_extraction_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern with a capture group to extract owner from connection name",
    )

    extract_tags: bool = Field(
        default=False,
        description="Extract tags from Airbyte metadata",
    )

    use_workspace_name_as_platform_instance: bool = Field(
        default=False,
        description="Use the workspace name as the platform instance in URNs. If enabled, this will override any platform_instance set in config.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @field_validator("job_status_start_date", mode="before")
    @classmethod
    def set_default_start_date(cls, v: Optional[str]) -> str:
        if v is None:
            # Default to 7 days ago
            seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            return seven_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
        return v

    @field_validator("job_status_end_date", mode="before")
    @classmethod
    def set_default_end_date(cls, v: Optional[str]) -> str:
        if v is None:
            # Default to current time
            return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        return v
