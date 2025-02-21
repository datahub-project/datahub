from typing import Dict, Optional

from pydantic import Field, SecretStr, validator

from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class PlatformConnectionConfig(
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    platform: str = Field(description="Upstream platform code (e.g. postgres, ms-sql)")
    database: Optional[str] = Field(default=None, description="Database name")
    database_schema: Optional[str] = Field(default=None, description="Schema name")


class GrafanaSourceConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    platform: str = Field(default="grafana", hidden_from_docs=True)
    url: str = Field(
        description="URL of Grafana instance (e.g. https://grafana.company.com)"
    )
    service_account_token: SecretStr = Field(description="Grafana API token")
    verify_ssl: bool = Field(
        default=True,
        description="Verify SSL certificate for secure connections (https)",
    )
    ingest_tags: bool = Field(
        default=True,
        description="Whether to ingest tags from Grafana dashboards and charts",
    )
    ingest_owners: bool = Field(
        default=True,
        description="Whether to ingest owners from Grafana dashboards and charts",
    )
    page_size: int = Field(
        default=100,
        description="Number of grafana entities to query through each Grafana API call",
    )
    connection_to_platform_map: Dict[str, PlatformConnectionConfig] = Field(
        default={},
        description="Map of Grafana connection names to their upstream platform details",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("url", allow_reuse=True)
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)
