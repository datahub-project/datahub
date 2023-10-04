import logging
from dataclasses import dataclass, field as dataclass_field
from typing import List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
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


class FivetranSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    fivetran_log_config: FivetranLogConfig = pydantic.Field(
        description="Fivetran log connector destination server configurations.",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Airbyte Stateful Ingestion Config."
    )
