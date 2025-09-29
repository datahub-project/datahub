import dataclasses
import logging
import warnings
from typing import Dict, Optional

from pydantic import Field, root_validator
from typing_extensions import Literal

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationWarning,
)
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
)
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.api.report import Report
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.fivetran.fivetran_constants import (
    DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR,
    DataJobMode,
    FivetranMode,
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
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.str_enum import StrEnum

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
    EMAIL = "email"
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


class SnowflakeDestinationConfig(SnowflakeConnectionConfig):
    database: str = Field(description="The fivetran connector log database.")
    log_schema: str = Field(description="The fivetran connector log schema.")


class BigQueryDestinationConfig(BigQueryConnectionConfig):
    dataset: str = Field(description="The fivetran connector log dataset.")


class FivetranLogConfig(ConfigModel):
    destination_platform: Literal["snowflake", "bigquery"] = Field(
        default="snowflake",
        description="The destination platform where fivetran connector log tables are dumped.",
    )
    snowflake_destination_config: Optional[SnowflakeDestinationConfig] = Field(
        default=None,
        description="If destination platform is 'snowflake', provide snowflake configuration.",
    )
    bigquery_destination_config: Optional[BigQueryDestinationConfig] = Field(
        default=None,
        description="If destination platform is 'bigquery', provide bigquery configuration.",
    )
    _rename_destination_config = pydantic_renamed_field(
        "destination_config", "snowflake_destination_config"
    )

    @root_validator(skip_on_failure=True)
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


class FivetranAPIConfig(ConfigModel):
    """Configuration for the Fivetran API client."""

    api_key: str = Field(description="Fivetran API key")
    api_secret: str = Field(description="Fivetran API secret")
    base_url: str = Field(
        default="https://api.fivetran.com", description="Fivetran API base URL"
    )
    request_timeout_sec: int = Field(
        default=30, description="Request timeout in seconds"
    )
    max_workers: int = Field(
        default=5, description="Maximum number of parallel workers for API requests"
    )


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
    filtered_connectors: LossyList[str] = dataclasses.field(default_factory=LossyList)
    metadata_extraction_perf: MetadataExtractionPerfReport = dataclasses.field(
        default_factory=MetadataExtractionPerfReport
    )

    def report_connectors_scanned(self, count: int = 1) -> None:
        self.connectors_scanned += count

    def report_connectors_dropped(self, connector: str) -> None:
        self.filtered_connectors.append(connector)


class ColumnNamingPattern(StrEnum):
    SNAKE_CASE = "snake_case"  # e.g., first_name
    CAMEL_CASE = "camelCase"  # e.g., firstName
    PASCAL_CASE = "PascalCase"  # e.g., FirstName
    UPPER_CASE = "UPPER_CASE"  # e.g., FIRST_NAME
    LOWER_CASE = "lower_case"  # e.g., firstname
    AUTO = "auto"  # Auto-detect (default)


class PlatformDetail(ConfigModel):
    platform: Optional[str] = Field(
        default=None,
        description="Override the platform type detection.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )
    database: Optional[str] = Field(
        default=None,
        description="The database that all assets produced by this connector belong to. "
        "For destinations, this defaults to the fivetran log config's database.",
    )
    include_schema_in_urn: bool = Field(
        default=True,
        description="Include schema in the dataset URN. In some cases, the schema is not relevant to the dataset URN and Fivetran sets it to the source and destination table names in the connector.",
    )
    column_naming_pattern: ColumnNamingPattern = Field(
        default=ColumnNamingPattern.AUTO,
        description="The casing pattern used for column names in this platform. "
        "If set to 'auto', the system will attempt to detect the pattern.",
    )


class FivetranSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, IncrementalLineageConfigMixin
):
    fivetran_mode: FivetranMode = Field(
        default=FivetranMode.AUTO,
        description="Mode of operation: 'enterprise' for log tables access (Snowflake/BigQuery destinations only), "
        "'standard' for REST API (supports all destination platforms), or 'auto' to automatically detect the best approach.",
    )

    datajob_mode: DataJobMode = Field(
        default=DataJobMode.CONSOLIDATED,
        description="DataJob generation mode: 'consolidated' to create one job per connector, 'per_table' to create one job per table.",
    )

    # Enterprise version configuration
    fivetran_log_config: Optional[FivetranLogConfig] = Field(
        default=None,
        description="Fivetran log connector destination server configurations for Enterprise mode. "
        "Requires access to Fivetran log tables in your Snowflake or BigQuery destination. "
        "Provides faster performance but limited to these two destination platforms.",
    )

    # Standard version configuration
    api_config: Optional[FivetranAPIConfig] = Field(
        default=None,
        description="Fivetran REST API configuration for Standard mode. "
        "Uses Fivetran REST API and supports all destination platforms. "
        "Slower than Enterprise mode but more universally compatible.",
    )

    connector_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filtering regex patterns for connector names or connector IDs. Supports filtering by either the connector display name or the connector ID for maximum flexibility.",
    )
    destination_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for destination IDs or destination names to filter in ingestion. "
        "Supports filtering by either the destination ID (e.g. canyon_tolerable) or the destination name for maximum flexibility. "
        "Destination IDs are visible in the Fivetran UI under Destinations -> Overview -> Destination Group ID. "
        "Note: Destination name filtering in Enterprise (log table) mode works best when api_config is also provided for enhanced metadata retrieval, but is not required.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Populates table->table column lineage.",
    )

    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Fivetran Stateful Ingestion Config."
    )

    # Fivetran connector all sources to platform instance mapping
    sources_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping from connector ID to its source platform/instance/env/database details. "
        "Use this to override auto-detected source metadata or ensure connectors are included regardless of connector_patterns. "
        "Particularly useful for filling gaps when API auto-detection is insufficient.",
    )
    # Fivetran destination to platform instance mapping
    destination_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping of destination ID to its platform/instance/env/database details. "
        "Use this to override auto-detected destination metadata. "
        "More important in Enterprise (log table) mode where destination platform detection is limited to Snowflake and BigQuery.",
    )

    history_sync_lookback_period: int = Field(
        7,
        description="The number of days to look back when extracting connectors' sync history.",
    )

    max_table_lineage_per_connector: int = Field(
        default=DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR,
        description="Maximum number of table lineage entries to extract per connector. "
        "Increase this value if you have connectors with many table lineage entries and want to capture all of them. "
        "Set to -1 to disable the limit entirely (not recommended for very large connectors).",
    )

    @root_validator(skip_on_failure=True)
    def validate_config_based_on_mode(cls, values: Dict) -> Dict:
        """Validate configuration based on the selected mode."""
        mode = values.get("fivetran_mode", FivetranMode.AUTO)
        log_config = values.get("fivetran_log_config")
        api_config = values.get("api_config")

        if mode == FivetranMode.ENTERPRISE:
            if not log_config:
                raise ValueError(
                    "Enterprise mode requires 'fivetran_log_config' to be specified."
                )
        elif mode == FivetranMode.STANDARD:
            if not api_config:
                raise ValueError("Standard mode requires 'api_config' to be specified.")
        elif mode == FivetranMode.AUTO:
            # For AUTO mode, we'll validate this at runtime, allowing either config to be provided
            if not log_config and not api_config:
                raise ValueError(
                    "Either 'fivetran_log_config' (for enterprise) or 'api_config' (for standard) must be specified."
                )

        return values

    @root_validator(pre=True)
    def compat_sources_to_database(cls, values: Dict) -> Dict:
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
