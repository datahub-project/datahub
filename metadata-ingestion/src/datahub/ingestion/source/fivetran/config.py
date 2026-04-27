import dataclasses
import logging
import warnings
from typing import Any, Dict, Optional

import pydantic
from pydantic import Field, field_validator, model_validator
from typing_extensions import Literal

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationWarning,
    TransparentSecretStr,
)
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.report import Report
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.unity.connection import UnityCatalogConnectionConfig
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

# Default safeguards to prevent fetching massive amounts of data.
MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT = 120
MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT = 1000
MAX_JOBS_PER_CONNECTOR_DEFAULT = 500


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
    EMAIL = "email"
    CONNECTOR_ID = "connection_id"
    CONNECTOR_NAME = "connection_name"
    CONNECTOR_TYPE_ID = "connector_type_id"
    PAUSED = "paused"
    SYNC_FREQUENCY = "sync_frequency"
    DESTINATION_ID = "destination_id"
    CONNECTING_USER_ID = "connecting_user_id"
    # Job status constants
    SUCCESSFUL = "SUCCESSFUL"
    FAILURE_WITH_TASK = "FAILURE_WITH_TASK"
    CANCELED = "CANCELED"
    GOOGLE_SHEETS_CONNECTOR_TYPE = "google_sheets"


# Key: Connector Type, Value: Platform ID/Name
KNOWN_DATA_PLATFORM_MAPPING = {
    "google_cloud_postgresql": "postgres",
    "postgres": "postgres",
    "snowflake": "snowflake",
    Constant.GOOGLE_SHEETS_CONNECTOR_TYPE: Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
}

# Note: (As of Oct 2025) Fivetran Platform Connector has stale lineage metadata for Google Sheets column data (deleted/renamed).
# Ref: https://fivetran.com/docs/connectors/files/google-sheets#deletingdata
# TODO: Remove Google Sheets connector type from DISABLE_LINEAGE_FOR_CONNECTOR_TYPES
DISABLE_COL_LINEAGE_FOR_CONNECTOR_TYPES = [Constant.GOOGLE_SHEETS_CONNECTOR_TYPE]


class SnowflakeDestinationConfig(SnowflakeConnectionConfig):
    database: str = Field(description="The fivetran connector log database.")
    log_schema: str = Field(description="The fivetran connector log schema.")
    preserve_case: bool = Field(
        default=False,
        description=(
            "Preserve the case of database and schema identifiers when querying "
            "the Fivetran log. Set this to True when the log lives in a Snowflake "
            "catalog-linked database (CLD) — e.g., when the Fivetran Managed Data "
            "Lake Service surfaces logs through Snowflake — where Snowflake's "
            "default identifier uppercasing breaks lookup against case-preserving "
            "Glue/Iceberg-backed schemas."
        ),
    )


class BigQueryDestinationConfig(BigQueryConnectionConfig):
    dataset: str = Field(description="The fivetran connector log dataset.")


class DatabricksDestinationConfig(UnityCatalogConnectionConfig):
    catalog: str = Field(description="The fivetran connector log catalog.")
    log_schema: str = Field(description="The fivetran connector log schema.")

    @field_validator("warehouse_id", mode="after")
    @classmethod
    def warehouse_id_should_not_be_empty(cls, warehouse_id: Optional[str]) -> str:
        if warehouse_id is None or (warehouse_id and warehouse_id.strip() == ""):
            raise ValueError("Fivetran requires warehouse_id to be set")
        return warehouse_id


class ManagedDataLakeDestinationConfig(SnowflakeConnectionConfig):
    """Configuration for the Fivetran Managed Data Lake Service.

    The Fivetran Platform Connector log is read through a Snowflake
    catalog-linked database (CLD), so this config inherits Snowflake
    connection details. The data tables themselves live in the
    cloud-native catalog (AWS Glue) and emitted URNs target that catalog
    rather than Snowflake.
    """

    database: str = Field(
        description=(
            "The Snowflake catalog-linked database that surfaces the "
            "Fivetran Platform Connector logs (e.g., `LH_SOURCE_FIVETRAN_USW2`)."
        )
    )
    log_schema: str = Field(
        description=(
            "The schema within the catalog-linked database that holds the "
            "Fivetran log tables (e.g., `fivetran_metadata_<suffix>`)."
        )
    )
    preserve_case: bool = Field(
        default=True,
        description=(
            "Preserve the case of database and schema identifiers when "
            "querying the Fivetran log. Defaults to True for Managed Data "
            "Lake recipes because catalog-linked databases are case-preserving."
        ),
    )
    catalog_type: Literal["glue", "iceberg_rest", "unity", "biglake", "onelake"] = (
        Field(
            default="glue",
            description=(
                "The cloud-native catalog backing the Fivetran Managed Data Lake "
                "destination. Currently only `glue` is implemented; other values "
                "are accepted by the type but rejected at config-load time until "
                "those branches are wired up."
            ),
        )
    )
    glue_database_prefix: str = Field(
        default="fivetran_",
        description=(
            "Prefix Fivetran applies when auto-creating Glue databases. "
            "Glue database name is `<glue_database_prefix><connector_schema>`. "
            "Only consulted when `catalog_type='glue'`."
        ),
    )

    @field_validator("catalog_type", mode="after")
    @classmethod
    def validate_catalog_type(cls, value: str) -> str:
        # The Literal is forward-compatible (new catalogs can be added without a
        # config-shape migration), but the URN-construction branches don't exist
        # yet. Reject at recipe load time so users see the failure before any
        # ingestion work runs, not partway through a connector loop.
        if value != "glue":
            raise ValueError(
                f"`catalog_type='{value}'` is not implemented yet; "
                "only `catalog_type='glue'` is currently supported."
            )
        return value


class FivetranAPIConfig(ConfigModel):
    api_key: TransparentSecretStr = Field(description="Fivetran API key")
    api_secret: TransparentSecretStr = Field(description="Fivetran API secret")
    base_url: str = Field(
        default="https://api.fivetran.com", description="Fivetran API base URL"
    )
    request_timeout_sec: int = Field(
        default=30, description="Request timeout in seconds"
    )


class FivetranLogConfig(ConfigModel):
    destination_platform: Literal[
        "snowflake", "bigquery", "databricks", "managed_data_lake"
    ] = pydantic.Field(
        default="snowflake",
        description=(
            "The destination platform where fivetran connector log tables are dumped. "
            "`managed_data_lake` covers the Fivetran Managed Data Lake Service when "
            "logs are surfaced through a Snowflake catalog-linked database; "
            "emitted URNs target the underlying AWS Glue catalog."
        ),
    )
    snowflake_destination_config: Optional[SnowflakeDestinationConfig] = pydantic.Field(
        default=None,
        description="If destination platform is 'snowflake', provide snowflake configuration.",
    )
    bigquery_destination_config: Optional[BigQueryDestinationConfig] = pydantic.Field(
        default=None,
        description="If destination platform is 'bigquery', provide bigquery configuration.",
    )
    databricks_destination_config: Optional[DatabricksDestinationConfig] = (
        pydantic.Field(
            default=None,
            description="If destination platform is 'databricks', provide databricks configuration.",
        )
    )
    managed_data_lake_destination_config: Optional[ManagedDataLakeDestinationConfig] = (
        pydantic.Field(
            default=None,
            description=(
                "If destination platform is 'managed_data_lake', provide the Fivetran "
                "Managed Data Lake Service configuration (Snowflake CLD connection + "
                "Glue catalog settings)."
            ),
        )
    )
    _rename_destination_config = pydantic_renamed_field(
        "destination_config", "snowflake_destination_config"
    )

    max_jobs_per_connector: int = pydantic.Field(
        default=MAX_JOBS_PER_CONNECTOR_DEFAULT,
        gt=0,
        description="Maximum number of sync jobs to retrieve per connector. This acts as a safety net to prevent excessive data ingestion. Increase cautiously if you need to see more historical sync runs.",
    )

    max_table_lineage_per_connector: int = pydantic.Field(
        default=MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
        gt=0,
        description="Maximum number of table lineage entries to retrieve per connector. This acts as a safety net to prevent excessive data ingestion. When this limit is exceeded, only the most recent entries are ingested.",
    )

    max_column_lineage_per_connector: int = pydantic.Field(
        default=MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
        gt=0,
        description="Maximum number of column lineage entries to retrieve per connector. This acts as a safety net to prevent excessive data ingestion. When this limit is exceeded, only the most recent entries are ingested.",
    )

    @model_validator(mode="after")
    def validate_destination_platform_and_config(self) -> "FivetranLogConfig":
        if self.destination_platform == "snowflake":
            if self.snowflake_destination_config is None:
                raise ValueError(
                    "If destination platform is 'snowflake', user must provide snowflake destination configuration in the recipe."
                )
        elif self.destination_platform == "bigquery":
            if self.bigquery_destination_config is None:
                raise ValueError(
                    "If destination platform is 'bigquery', user must provide bigquery destination configuration in the recipe."
                )
        elif self.destination_platform == "databricks":
            if self.databricks_destination_config is None:
                raise ValueError(
                    "If destination platform is 'databricks', user must provide databricks destination configuration in the recipe."
                )
        elif self.destination_platform == "managed_data_lake":
            if self.managed_data_lake_destination_config is None:
                raise ValueError(
                    "If destination platform is 'managed_data_lake', user must provide "
                    "managed_data_lake destination configuration in the recipe."
                )
        else:
            raise ValueError(
                f"Destination platform '{self.destination_platform}' is not yet supported."
            )
        return self


@dataclasses.dataclass
class MetadataExtractionPerfReport(Report):
    connectors_metadata_extraction_sec: PerfTimer = dataclasses.field(
        default_factory=PerfTimer
    )
    connectors_lineage_extraction_sec: PerfTimer = dataclasses.field(
        default_factory=PerfTimer
    )
    connectors_jobs_extraction_sec: PerfTimer = dataclasses.field(
        default_factory=PerfTimer
    )


@dataclasses.dataclass
class FivetranSourceReport(StaleEntityRemovalSourceReport):
    connectors_scanned: int = 0
    fivetran_rest_api_call_count: int = 0
    filtered_connectors: LossyList[str] = dataclasses.field(default_factory=LossyList)
    metadata_extraction_perf: MetadataExtractionPerfReport = dataclasses.field(
        default_factory=MetadataExtractionPerfReport
    )

    def report_connectors_scanned(self, count: int = 1) -> None:
        self.connectors_scanned += count

    def report_connectors_dropped(self, connector: str) -> None:
        self.filtered_connectors.append(connector)

    def report_fivetran_rest_api_call_count(self) -> None:
        self.fivetran_rest_api_call_count += 1


class PlatformDetail(ConfigModel):
    platform: Optional[str] = pydantic.Field(
        default=None,
        description="Override the platform type detection.",
    )
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    env: str = pydantic.Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )
    database: Optional[str] = pydantic.Field(
        default=None,
        description="The database that all assets produced by this connector belong to. "
        "For destinations, this defaults to the fivetran log config's database.",
    )
    include_schema_in_urn: bool = pydantic.Field(
        default=True,
        description="Include schema in the dataset URN. In some cases, the schema is not relevant to the dataset URN and Fivetran sets it to the source and destination table names in the connector.",
    )


class FivetranSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    fivetran_log_config: FivetranLogConfig = pydantic.Field(
        description="Fivetran log connector destination server configurations.",
    )
    connector_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filtering regex patterns for connector names.",
    )
    destination_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for destination ids to filter in ingestion. "
        "Fivetran destination IDs are usually two word identifiers e.g. canyon_tolerable, and are not the same as the destination database name. "
        "They're visible in the Fivetran UI under Destinations -> Overview -> Destination Group ID.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Populates table->table column lineage.",
    )

    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Fivetran Stateful Ingestion Config."
    )

    # Fivetran connector all sources to platform instance mapping
    sources_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping from connector id to its platform/instance/env/database details.",
    )
    # Fivetran destination to platform instance mapping
    destination_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of destination id to its platform/instance/env details.",
    )

    """
    Use Fivetran REST API to get :
    - Google Sheets Connector details and emit related entities
    Fivetran Platform Connector syncs limited information about the Google Sheets Connector.
    """
    api_config: Optional[FivetranAPIConfig] = Field(
        default=None,
        description="Fivetran REST API configuration, used to provide wider support for connections.",
    )

    @model_validator(mode="before")
    @classmethod
    def compat_sources_to_database(cls, values: Any) -> Any:
        if "sources_to_database" in values:
            warnings.warn(
                "The sources_to_database field is deprecated, please use sources_to_platform_instance instead.",
                ConfigurationWarning,
                stacklevel=2,
            )
            mapping = values.pop("sources_to_database")

            values.setdefault("sources_to_platform_instance", {})
            for key, value in mapping.items():
                values["sources_to_platform_instance"].setdefault(key, {})
                values["sources_to_platform_instance"][key].setdefault(
                    "database", value
                )

        return values

    history_sync_lookback_period: int = pydantic.Field(
        7,
        description="The number of days to look back when extracting connectors' sync history.",
    )
