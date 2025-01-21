from typing import Dict, Optional

from pydantic import Field, SecretStr, validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class PlatformConnectionConfig(ConfigModel):
    platform: str = Field(description="Platform to connect to")
    database: Optional[str] = Field(default=None, description="Database name")
    database_schema: Optional[str] = Field(default=None, description="Schema name")
    platform_instance: Optional[str] = Field(
        default=None, description="Platform instance"
    )
    env: str = Field(default="PROD", description="Environment")


class GrafanaSourceConfig(
    StatefulIngestionConfigBase, DatasetLineageProviderConfigBase
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
    platform_instance: Optional[str] = Field(
        default=None, description="Platform instance for DataHub"
    )
    connection_to_platform_map: Dict[str, PlatformConnectionConfig] = Field(
        default={},
        description="Map of Grafana connection names to their upstream platform details",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("url")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)
