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
            "Pass `database` and `log_schema` identifiers verbatim when "
            "issuing `USE DATABASE` / `USE SCHEMA`, instead of Snowflake's "
            "default uppercasing of unquoted identifiers. Useful when the "
            "log lives in a Snowflake schema created with quoted "
            "lowercase names, or any other case-preserving setup where "
            "the uppercasing path would query identifiers that don't "
            "exist. **For Managed Data Lake destinations specifically: "
            "prefer `log_source: rest_api` over a Snowflake "
            "catalog-linked database (CLD) — REST mode reads the log "
            "directly via API and avoids the identifier-casing issue "
            "altogether.**"
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
    destination_platform: Literal["snowflake", "bigquery", "databricks"] = (
        pydantic.Field(
            default="snowflake",
            description=(
                "The destination platform where fivetran connector log tables "
                "are dumped. For Managed Data Lake destinations use "
                "`log_source: rest_api` instead (no `fivetran_log_config` block "
                "needed)."
            ),
        )
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
    _rename_destination_config = pydantic_renamed_field(
        "destination_config", "snowflake_destination_config"
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
    database_lowercase: bool = pydantic.Field(
        default=True,
        description=(
            "Lowercase the `database` segment when constructing the dataset "
            "URN. Defaults to True to match DataHub's standard lowercase URN "
            "convention (and to preserve the long-standing Fivetran connector "
            "behaviour). Set False to keep the case Fivetran reports — useful "
            "when aligning with another DataHub source whose URN preserves "
            "the database casing (e.g. some Glue or Iceberg setups). "
            "Schema and table segments are always passed through unchanged."
        ),
    )

    @property
    def database_for_urn(self) -> Optional[str]:
        """The `database` value formatted for inclusion in a dataset URN.

        Centralises the case rule so both URN-construction sites
        (`_extend_lineage` for source URNs, `build_destination_urn` for
        destination URNs) lower-case identically — and so the per-record
        opt-out via `database_lowercase=False` only has to be wired in
        one place. Exposed as a `@property` (not a Pydantic field /
        `@computed_field`) so it doesn't leak into `model_dump()` and
        thus into `_compose_custom_properties` — the customProperties
        aspect intentionally surfaces the user-typed `database` verbatim.
        """
        if self.database is None:
            return None
        return self.database.lower() if self.database_lowercase else self.database


class FivetranSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    fivetran_log_config: Optional[FivetranLogConfig] = pydantic.Field(
        default=None,
        description=(
            "Fivetran Platform Connector log destination configuration. "
            "Required for `log_database` mode (the inferred default whenever "
            "this block is present). Optional in `rest_api` mode — when "
            "supplied alongside `api_config`, the REST reader uses the DB "
            "log only for per-run sync history (which the REST API doesn't "
            "expose)."
        ),
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
    log_source: Optional[Literal["log_database", "rest_api"]] = pydantic.Field(
        default=None,
        description=(
            "Where to read the Fivetran log from. Leave unset to let the "
            "connector infer this from which credential blocks you provide:\n"
            "  - Only `fivetran_log_config` → `log_database`.\n"
            "  - Only `api_config` → `rest_api`.\n"
            "  - Both → `log_database` (DB-primary; REST still owns "
            "destination routing and Google Sheets details).\n"
            "Set this explicitly to override the default routing — e.g. "
            "`rest_api` with a `fivetran_log_config` block also present runs "
            "REST-primary with the DB log only providing per-run sync history."
        ),
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

    @model_validator(mode="before")
    @classmethod
    def compat_max_limits_lifted_from_log_config(cls, values: Any) -> Any:
        # `max_jobs_per_connector`, `max_table_lineage_per_connector`, and
        # `max_column_lineage_per_connector` previously lived on
        # `FivetranLogConfig`. They were lifted to the top-level config
        # because they govern the lineage payload size regardless of which
        # log source (DB log, REST API, or hybrid) the connector reads
        # from. Rewrite old recipes silently by hoisting the values from
        # `fivetran_log_config` to the top level and emitting a
        # deprecation warning. Top-level values always win if both are set.
        log_cfg = values.get("fivetran_log_config")
        if not isinstance(log_cfg, dict):
            return values
        for legacy_field in (
            "max_jobs_per_connector",
            "max_table_lineage_per_connector",
            "max_column_lineage_per_connector",
        ):
            if legacy_field not in log_cfg:
                continue
            old_value = log_cfg.pop(legacy_field)
            warnings.warn(
                f"`fivetran_log_config.{legacy_field}` is deprecated; move it "
                f"to the top-level `{legacy_field}` config field. This shim "
                f"will be removed in a future release.",
                ConfigurationWarning,
                stacklevel=2,
            )
            # Top-level value wins on conflict.
            values.setdefault(legacy_field, old_value)
        return values

    history_sync_lookback_period: int = pydantic.Field(
        7,
        description="The number of days to look back when extracting connectors' sync history.",
    )

    # These limits apply equally to log_database and rest_api modes — they
    # govern how many sync runs / lineage rows we ingest per connector,
    # regardless of where the data comes from.
    max_jobs_per_connector: int = pydantic.Field(
        default=MAX_JOBS_PER_CONNECTOR_DEFAULT,
        gt=0,
        description="Maximum number of sync jobs to retrieve per connector.",
    )
    max_table_lineage_per_connector: int = pydantic.Field(
        default=MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
        gt=0,
        description="Maximum number of table lineage entries to retrieve per connector.",
    )
    max_column_lineage_per_connector: int = pydantic.Field(
        default=MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
        gt=0,
        description="Maximum number of column lineage entries to retrieve per connector.",
    )
    rest_api_max_workers: int = pydantic.Field(
        default=4,
        ge=1,
        le=32,
        description=(
            "Number of worker threads used to fetch per-connector data "
            "(schemas + sync history) in parallel when `log_source: rest_api`. "
            "Values >1 issue concurrent HTTP calls to the Fivetran REST API "
            "and meaningfully speed up ingestion for accounts with hundreds "
            "of connectors. Set to 1 for fully sequential behaviour. "
            "Lower this (not raise it) if you hit Fivetran rate limits. "
            "Ignored in `log_database` mode."
        ),
    )
    rest_api_per_connector_timeout_sec: int = pydantic.Field(
        default=300,
        gt=0,
        description=(
            "Hard wall-clock timeout (seconds) for fetching a single "
            "connector's schema + sync history when `log_source: rest_api`. "
            "If exceeded, that connector is emitted without lineage / run "
            "history and a warning is recorded — the rest of the ingest "
            "continues. Guards against a single hung HTTP call stalling "
            "the whole run. Healthy connectors finish in seconds; bump only "
            "if you have very large connectors that legitimately need more."
        ),
    )

    @model_validator(mode="after")
    def validate_log_source_credentials(self) -> "FivetranSourceConfig":
        # Infer the log_source if not set explicitly. With both blocks
        # present the DB log is preferred because it's the canonical source
        # for table+column lineage with `name_in_source` (REST's metadata
        # endpoint is staleness-bound to the last successful sync) and for
        # DPI run-history events. Users who want REST-primary in a hybrid
        # setup must opt in by setting `log_source: rest_api` explicitly.
        if self.log_source is None:
            if self.api_config is not None and self.fivetran_log_config is None:
                self.log_source = "rest_api"
            elif self.fivetran_log_config is not None:
                self.log_source = "log_database"
            else:
                raise ValueError(
                    "Fivetran source requires either `fivetran_log_config` "
                    "(read logs from the destination warehouse) or `api_config` "
                    "(read everything via the Fivetran REST API). At least one "
                    "must be provided."
                )

        if self.log_source == "rest_api":
            if self.api_config is None:
                raise ValueError(
                    "log_source='rest_api' requires `api_config` (Fivetran API "
                    "key + secret) to be configured. The REST mode does not "
                    "use the destination database connection."
                )
        else:
            # log_database mode — fivetran_log_config must be supplied
            if self.fivetran_log_config is None:
                raise ValueError(
                    "log_source='log_database' requires `fivetran_log_config` "
                    "describing the destination warehouse where the Fivetran "
                    "Platform Connector log lives. To skip this and read logs "
                    "via the Fivetran REST API instead, set "
                    "`log_source: rest_api` and provide `api_config`."
                )
        return self
