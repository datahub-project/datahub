import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.sql.snowflake import BaseSnowflakeConfig

logger = logging.getLogger(__name__)


class Constant:
    """
    keys used in fivetran plugin
    """

    ORCHESTRATOR = "fivetran"
    # table column name
    SOURCE_SCHEMA_NAME = "SOURCE_SCHEMA_NAME"
    SOURCE_TABLE_NAME = "SOURCE_TABLE_NAME"
    DESTINATION_SCHEMA_NAME = "DESTINATION_SCHEMA_NAME"
    DESTINATION_TABLE_NAME = "DESTINATION_TABLE_NAME"
    SYNC_ID = "SYNC_ID"
    MESSAGE_DATA = "MESSAGE_DATA"
    TIME_STAMP = "TIME_STAMP"
    STATUS = "status"
    USER_ID = "USER_ID"
    GIVEN_NAME = "GIVEN_NAME"
    FAMILY_NAME = "FAMILY_NAME"
    EMAIL = "EMAIL"
    EMAIL_DISABLED = "EMAIL_DISABLED"
    VERIFIED = "VERIFIED"
    CREATED_AT = "CREATED_AT"
    CONNECTOR_ID = "CONNECTOR_ID"
    CONNECTOR_NAME = "CONNECTOR_NAME"
    CONNECTOR_TYPE_ID = "CONNECTOR_TYPE_ID"
    PAUSED = "PAUSED"
    SYNC_FREQUENCY = "SYNC_FREQUENCY"
    DESTINATION_ID = "DESTINATION_ID"
    CONNECTING_USER_ID = "CONNECTING_USER_ID"

    # Job status constants
    SUCCESSFUL = "SUCCESSFUL"
    FAILURE_WITH_TASK = "FAILURE_WITH_TASK"
    CANCELED = "CANCELED"


SUPPORTED_DATA_PLATFORM_MAPPING = {
    "postgres": "postgres",
    "snowflake": "snowflake",
    "mysql": "mysql",
}


class SnowflakeDestinationConfig(BaseSnowflakeConfig):
    database: str = Field(
        default=None, description="The fivetran connector log database."
    )
    log_schema: Optional[str] = Field(
        default="FIVETRAN_LOG", description="The fivetran connector log schema."
    )


class FivetranLogConfig(ConfigModel):
    destination_platform: str = pydantic.Field(
        default=None,
        description="The destination platform where fivetran connector log tables are dumped.",
    )
    snowflake_destination_config: Optional[SnowflakeDestinationConfig] = pydantic.Field(
        default=None,
        description="If destination platform is 'snowflake', provide snowflake configuration.",
    )


@dataclass
class FivetranSourceReport(StaleEntityRemovalSourceReport):
    connectors_scanned: int = 0
    filtered_connectors: List[str] = dataclass_field(default_factory=list)

    def report_connectors_scanned(self, count: int = 1) -> None:
        self.connectors_scanned += count

    def report_connectors_dropped(self, model: str) -> None:
        self.filtered_connectors.append(model)


class PlatformDetail(ConfigModel):
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    env: str = pydantic.Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )


class FivetranSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    fivetran_log_config: FivetranLogConfig = pydantic.Field(
        description="Fivetran log connector destination server configurations.",
    )
    connector_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for connectors to filter in ingestion.",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Airbyte Stateful Ingestion Config."
    )
    # Fivetran connector all sources to platform instance mapping
    sources_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of connector all sources to Data platform instance. Provide connector id as key.",
    )
    # Fivetran destination to platform instance mapping
    destination_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of fivetran destination to Data platform instance. Provide destination id as key.",
    )
