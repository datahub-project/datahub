from typing import Dict, Optional

from pydantic import Field, SecretStr, validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class PlatformConnectionConfig(
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    """Platform connection configuration for mapping Grafana datasources to their actual platforms."""

    platform: str = Field(
        description="The platform name (e.g., 'postgres', 'mysql', 'snowflake')"
    )
    database: Optional[str] = Field(default=None, description="Default database name")
    database_schema: Optional[str] = Field(
        default=None, description="Default schema name"
    )


class GrafanaSourceConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for Grafana source"""

    platform: str = Field(default="grafana", hidden_from_docs=True)
    url: str = Field(
        description="Grafana URL in the format http://your-grafana-instance with no trailing slash"
    )
    service_account_token: SecretStr = Field(
        description="Service account token for Grafana"
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates when connecting to Grafana",
    )

    # API pagination configuration
    page_size: int = Field(
        default=100,
        description="Number of items to fetch per API call when paginating through folders and dashboards",
    )

    # Extraction mode configuration
    basic_mode: bool = Field(
        default=False,
        description="Enable basic extraction mode for users with limited permissions. "
        "In basic mode, only dashboard metadata is extracted without detailed panel information, "
        "lineage, or folder hierarchy. This requires only basic dashboard read permissions.",
    )

    # Content filtering
    dashboard_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern to filter dashboards for ingestion",
    )
    folder_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern to filter folders for ingestion",
    )

    # Feature toggles
    ingest_tags: bool = Field(
        default=True, description="Whether to ingest dashboard and chart tags"
    )
    ingest_owners: bool = Field(
        default=True, description="Whether to ingest dashboard ownership information"
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage between charts and data sources. "
        "When enabled, the source will parse SQL queries and datasource configurations "
        "to build lineage relationships.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Whether to extract column-level lineage from SQL queries. "
        "Only applicable when include_lineage is enabled.",
    )

    # Platform connection mappings
    connection_to_platform_map: Dict[str, PlatformConnectionConfig] = Field(
        default_factory=dict,
        description="Map of Grafana datasource types/UIDs to platform connection configs for lineage extraction",
    )

    @validator("url", allow_reuse=True)
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)
