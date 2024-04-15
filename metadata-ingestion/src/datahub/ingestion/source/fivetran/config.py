import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional

import pydantic
from pydantic import Field, root_validator
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigMixin
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryConnectionConfig,
)
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
    SOURCE_SCHEMA_NAME = "source_schema_name"
    SOURCE_TABLE_NAME = "source_table_name"
    SOURCE_TABLE_ID = "source_table_id"
    SOURCE_COLUMN_NAME = "source_column_name"
    DESTINATION_SCHEMA_NAME = "destination_schema_name"
    DESTINATION_TABLE_NAME = "destination_table_name"
    DESTINATION_TABLE_ID = "destination_table_id"
    DESTINATION_COLUMN_NAME = "destination_column_name"
    SYNC_ID = "sync_id"
    MESSAGE_DATA = "message_data"
    TIME_STAMP = "time_stamp"
    STATUS = "status"
    USER_ID = "user_id"
    GIVEN_NAME = "given_name"
    FAMILY_NAME = "family_name"
    CONNECTOR_ID = "connector_id"
    CONNECTOR_NAME = "connector_name"
    CONNECTOR_TYPE_ID = "connector_type_id"
    PAUSED = "paused"
    SYNC_FREQUENCY = "sync_frequency"
    DESTINATION_ID = "destination_id"
    CONNECTING_USER_ID = "connecting_user_id"
    # Job status constants
    SUCCESSFUL = "SUCCESSFUL"
    FAILURE_WITH_TASK = "FAILURE_WITH_TASK"
    CANCELED = "CANCELED"


KNOWN_DATA_PLATFORM_MAPPING = {
    "postgres": "postgres",
    "snowflake": "snowflake",
}


class SnowflakeDestinationConfig(BaseSnowflakeConfig):
    database: str = Field(description="The fivetran connector log database.")
    log_schema: str = Field(description="The fivetran connector log schema.")


class BigQueryDestinationConfig(BigQueryConnectionConfig):
    dataset: str = Field(description="The fivetran connector log dataset.")


class FivetranLogConfig(ConfigModel):
    destination_platform: Literal["snowflake", "bigquery"] = pydantic.Field(
        default="snowflake",
        description="The destination platform where fivetran connector log tables are dumped.",
    )
    snowflake_destination_config: Optional[SnowflakeDestinationConfig] = pydantic.Field(
        default=None,
        description="If destination platform is 'snowflake', provide snowflake configuration.",
    )
    bigquery_destination_config: Optional[BigQueryDestinationConfig] = pydantic.Field(
        default=None,
        description="If destination platform is 'bigquery', provide bigquery configuration.",
    )
    _rename_destination_config = pydantic_renamed_field(
        "destination_config", "snowflake_destination_config"
    )

    @root_validator(pre=True)
    def validate_destination_platfrom_and_config(cls, values: Dict) -> Dict:
        destination_platform = values["destination_platform"]
        if destination_platform == "snowflake":
            if "snowflake_destination_config" not in values:
                raise ValueError(
                    "If destination platform is 'snowflake', user must provide snowflake destination configuration in the recipe."
                )
        elif destination_platform == "bigquery":
            if "bigquery_destination_config" not in values:
                raise ValueError(
                    "If destination platform is 'bigquery', user must provide bigquery destination configuration in the recipe."
                )
        else:
            raise ValueError(
                f"Destination platform '{destination_platform}' is not yet supported."
            )
        return values


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
    include_column_lineage: bool = Field(
        default=True,
        description="Populates table->table column lineage.",
    )
    sources_to_database: Dict[str, str] = pydantic.Field(
        default={},
        description="A mapping of the connector's all sources to its database. Use connector id as key.",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Airbyte Stateful Ingestion Config."
    )
    # Fivetran connector all sources to platform instance mapping
    sources_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of the connector's all sources dataset to platform instance. Use connector id as key.",
    )
    # Fivetran destination to platform instance mapping
    destination_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of destination dataset to platform instance. Use destination id as key.",
    )
