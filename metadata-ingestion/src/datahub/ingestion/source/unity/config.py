import logging
import os
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Union

import pydantic
from pydantic import Field, field_validator, model_validator
from typing_extensions import Literal

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigEnum,
    ConfigModel,
    HiddenFromDocs,
)
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mce_builder import ALL_ENV_TYPES
from datahub.ingestion.api.incremental_ownership_helper import (
    IncrementalOwnershipConfigMixin,
)
from datahub.ingestion.api.incremental_properties_helper import (
    IncrementalPropertiesConfigMixin,
)
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulProfilingConfigMixin,
)
from datahub.ingestion.source.unity.connection import UnityCatalogConnectionConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.operation_config import (
    OperationConfig,
    is_profiling_enabled,
)
from datahub.utilities.global_warning_util import add_global_warning

logger = logging.getLogger(__name__)

# Configuration default constants
INCLUDE_TAGS_DEFAULT = True
INCLUDE_HIVE_METASTORE_DEFAULT = True


class LineageDataSource(ConfigEnum):
    AUTO = "AUTO"
    SYSTEM_TABLES = "SYSTEM_TABLES"
    API = "API"


class UsageDataSource(ConfigEnum):
    AUTO = "AUTO"
    SYSTEM_TABLES = "SYSTEM_TABLES"
    API = "API"


class UnityCatalogProfilerConfig(ConfigModel):
    method: str = Field(
        description=(
            "Profiling method to use."
            " Options supported are `ge`, `analyze`, and `sqlalchemy`."
            " `ge` uses Great Expectations and runs SELECT SQL queries on profiled tables."
            " `analyze` calls ANALYZE TABLE on profiled tables. Only works for delta tables."
            " `sqlalchemy` uses the custom SQLAlchemy-based profiler (no GE dependency)."
        ),
    )

    # TODO: Support cluster compute as well, for ge profiling
    warehouse_id: Optional[str] = Field(
        default=None, description="SQL Warehouse id, for running profiling queries."
    )

    pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter tables for profiling during ingestion. "
            "Specify regex to match the `catalog.schema.table` format. "
            "Note that only tables allowed by the `table_pattern` will be considered."
        ),
    )


class DeltaLakeDetails(ConfigModel):
    platform_instance_name: Optional[str] = Field(
        default=None, description="Delta-lake platform instance name"
    )
    env: str = Field(default="PROD", description="Delta-lake environment")


class UnityCatalogAnalyzeProfilerConfig(UnityCatalogProfilerConfig):
    method: Literal["analyze"] = "analyze"

    # TODO: Reduce duplicate code with DataLakeProfilerConfig, GEProfilingConfig, SQLAlchemyConfig
    enabled: bool = Field(
        default=False, description="Whether profiling should be done."
    )
    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )

    profile_table_level_only: bool = Field(
        default=False,
        description="Whether to perform profiling at table-level only or include column-level profiling as well.",
    )

    call_analyze: bool = Field(
        default=True,
        description=(
            "Whether to call ANALYZE TABLE as part of profile ingestion."
            "If false, will ingest the results of the most recent ANALYZE TABLE call, if any."
        ),
    )

    max_wait_secs: int = Field(
        default=int(timedelta(hours=1).total_seconds()),
        description="Maximum time to wait for an ANALYZE TABLE query to complete.",
    )

    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for profiling. Set to 1 to disable.",
    )

    @property
    def include_columns(self):
        return not self.profile_table_level_only


# TODO: should this max_wait_secs had been implemented as a global profiler feature instead of keeping it specific to Unity Catalog?
class UnityCatalogGEProfilerConfig(UnityCatalogProfilerConfig, GEProfilingConfig):
    method: Literal["ge"] = "ge"

    max_wait_secs: Optional[int] = Field(
        default=None,
        description="Maximum time to wait for a table to be profiled.",
    )


class UnityCatalogSQLAlchemyProfilerConfig(
    UnityCatalogProfilerConfig, GEProfilingConfig
):
    method: Literal["sqlalchemy"] = "sqlalchemy"

    max_wait_secs: Optional[int] = Field(
        default=None,
        description="Maximum time to wait for a table to be profiled.",
    )


class FederationConnectionDetail(ConfigModel):
    platform: Optional[str] = pydantic.Field(
        default=None,
        description="Override the DataHub platform auto-detected from the Unity Catalog "
        "connection type (e.g. 'mssql', 'postgres').",
    )
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="platform_instance the external source was ingested under. Must match "
        "for the lineage link to resolve.",
    )
    env: Optional[str] = pydantic.Field(
        default=None,
        description="env of the external dataset (defaults to the source env).",
    )
    database: Optional[str] = pydantic.Field(
        default=None,
        description="Override the remote database name (falls back to the foreign catalog's "
        "connection options).",
    )
    convert_urns_to_lowercase: Optional[bool] = pydantic.Field(
        default=None,
        description="Override case-folding of the external URN (defaults to the source's "
        "convert_urns_to_lowercase). Must match how the external source was ingested.",
    )

    @field_validator("env", mode="after")
    @classmethod
    def env_must_be_one_of(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v.upper() not in ALL_ENV_TYPES:
            raise ValueError(f"env must be one of {ALL_ENV_TYPES}, found {v}")
        return v.upper() if v is not None else v


class UnityCatalogSourceConfig(
    UnityCatalogConnectionConfig,
    SQLCommonConfig,
    StatefulIngestionConfigBase,
    BaseUsageConfig,
    DatasetSourceConfigMixin,
    StatefulProfilingConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    IncrementalOwnershipConfigMixin,
    IncrementalPropertiesConfigMixin,
):
    include_metastore: bool = pydantic.Field(
        default=False,
        description=(
            "Whether to ingest the workspace's metastore as a container and include it in all urns."
            " Changing this will affect the urns of all entities in the workspace."
            " This config is deprecated and will be removed in the future,"
            " so it is recommended to not set this to `True` for new ingestions."
            " If you have an existing unity catalog ingestion, you'll want to avoid duplicates by soft deleting existing data."
            " If stateful ingestion is enabled, running with `include_metastore: false` should be sufficient."
            " Otherwise, we recommend deleting via the cli: `datahub delete --platform databricks` and re-ingesting with `include_metastore: false`."
        ),
    )

    ingest_data_platform_instance_aspect: Optional[bool] = pydantic.Field(
        default=False,
        description=(
            "Option to enable/disable ingestion of the data platform instance aspect."
            " The default data platform instance id for a dataset is workspace_name"
        ),
    )

    _only_ingest_assigned_metastore_removed = pydantic_removed_field(
        "only_ingest_assigned_metastore", month="June", year=2023
    )

    _metastore_id_pattern_removed = pydantic_removed_field(
        "metastore_id_pattern", month="June", year=2023
    )

    catalogs: Optional[List[str]] = pydantic.Field(
        default=None,
        description=(
            "Fixed list of catalogs to ingest."
            " If not specified, catalogs will be ingested based on `catalog_pattern`."
        ),
    )

    catalog_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for catalogs to filter in ingestion. Specify regex to match the full `metastore.catalog` name.",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to the full `metastore.catalog.schema` name. e.g. to match all tables in schema analytics, use the regex `^mymetastore\\.mycatalog\\.analytics$`.",
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in `catalog.schema.table` format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex `Customer\\.public\\.customer.*`.",
    )

    notebook_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for notebooks to filter in ingestion, based on notebook *path*."
            " Specify regex to match the entire notebook path in `/<dir>/.../<name>` format."
            " e.g. to match all notebooks in the root Shared directory, use the regex `/Shared/.*`."
        ),
    )

    metric_view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for Unity Catalog Metric Views to filter in ingestion."
            " Specify regex to match the full `catalog.schema.metric_view_name`."
            " Only applies when `include_metric_views` is True."
        ),
    )

    include_metric_views: bool = pydantic.Field(
        default=False,
        description=(
            "Enable enriched ingestion of Unity Catalog Metric Views: subtype"
            " 'Metric View', YAML body as ViewProperties, upstream and column-level"
            " lineage from `source` / `joins` / `dimensions.expr` / `measures.expr`,"
            " `Dimension` / `Measure` tags on matching columns, `materialization`"
            " → `ViewProperties.materialized`, and `filter` as a custom property."
            " Default `false` keeps metric views as plain Tables. Requires a"
            " `databricks-sdk` recent enough to expose `TableType.METRIC_VIEW`."
        ),
    )

    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to catalogs, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    include_table_lineage: bool = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation.",
    )

    include_external_lineage: bool = pydantic.Field(
        default=True,
        description=(
            "Option to enable/disable lineage generation for external tables."
            " Only external S3 tables are supported at the moment."
        ),
    )

    include_notebooks: bool = pydantic.Field(
        default=False,
        description="Ingest notebooks, represented as DataHub datasets.",
    )

    include_ownership: bool = pydantic.Field(
        default=False,
        description="Option to enable/disable ownership generation for metastores, catalogs, schemas, and tables.",
    )

    include_tags: bool = pydantic.Field(
        default=INCLUDE_TAGS_DEFAULT,
        description=(
            "Option to enable/disable column/table tag extraction. "
            "Requires warehouse_id to be set since tag extraction needs to query system.information_schema.tags. "
            "If warehouse_id is not provided, this will be automatically disabled to allow ingestion to continue."
        ),
    )

    _rename_table_ownership = pydantic_renamed_field(
        "include_table_ownership", "include_ownership"
    )

    include_column_lineage: bool = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion. ",
    )

    include_federation_lineage: bool = pydantic.Field(
        default=True,
        description="Emit an upstream COPY lineage edge from each Lakehouse Federation "
        "foreign catalog table to its external source dataset (with column-level lineage "
        "when include_column_lineage is set). Disable to skip the cross-platform link.",
    )
    emit_federation_structured_properties: bool = pydantic.Field(
        default=True,
        description="Define and assign structured properties marking foreign catalogs as "
        "federated (facetable in the UI).",
    )
    federation_structured_property_namespace: str = pydantic.Field(
        default="databricks.federation",
        description="Qualified-name prefix for the federation structured properties; "
        "each property is this prefix plus its suffix (e.g. 'databricks.federation.platform').",
    )
    federation_connection_details: Dict[str, FederationConnectionDetail] = (
        pydantic.Field(
            default_factory=dict,
            description="Per-connection overrides keyed by Unity Catalog connection name.",
        )
    )
    include_federation_column_backfill: bool = pydantic.Field(
        default=True,
        description="For foreign (Lakehouse Federation) catalog tables whose columns "
        "Unity Catalog has not synced yet, resolve the schema from the already-ingested "
        "external source dataset via the DataHub graph. Requires a graph connection "
        "(e.g. a datahub-rest sink) and the external source to be ingested; otherwise "
        "it is a no-op.",
    )

    include_table_constraints: bool = pydantic.Field(
        default=False,
        description=(
            "If enabled, fetches primary key and foreign key constraints for each table "
            "via an additional tables.get() API call per table (one call per table). "
            "Disabled by default to avoid unexpected API load on large catalogs. "
            "Enables PK/FK visualisation in the DataHub schema view when set to true."
        ),
    )

    include_partition_keys: bool = pydantic.Field(
        default=False,
        description=(
            "If enabled, the `isPartitioningKey` field is populated on schema fields "
            "for columns that are part of the table's partition key. "
            "Partition key information is already present in the tables.list() response "
            "so no additional API calls are made."
        ),
    )

    lineage_data_source: LineageDataSource = pydantic.Field(
        default=LineageDataSource.AUTO,
        description=(
            "Source for lineage data extraction. Options: "
            f"'{LineageDataSource.AUTO.value}' - Use system tables when SQL warehouse is available, fallback to API; "
            f"'{LineageDataSource.SYSTEM_TABLES.value}' - Force use of system.access.table_lineage and system.access.column_lineage tables (requires SQL warehouse); "
            f"'{LineageDataSource.API.value}' - Force use of REST API endpoints for lineage data"
        ),
    )

    ignore_start_time_lineage: bool = pydantic.Field(
        default=False,
        description="Option to ignore the start_time and retrieve all available lineage. When enabled, the start_time filter will be set to zero to extract all lineage events regardless of the configured time window.",
    )

    column_lineage_column_limit: int = pydantic.Field(
        default=300,
        description="Limit the number of columns to get column level lineage. ",
    )

    lineage_max_workers: HiddenFromDocs[int] = pydantic.Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for column lineage thread pool executor. Set to 1 to disable.",
    )

    databricks_api_page_size: int = pydantic.Field(
        default=0,
        ge=0,
        description=(
            "Page size for Databricks API calls when listing resources (catalogs, schemas, tables, etc.). "
            "When set to 0 (default), uses server-side configured page length (recommended). "
            "When set to a positive value, the page length is the minimum of this value and the server configured value. "
            "Must be a non-negative integer."
        ),
    )

    include_usage_statistics: bool = Field(
        default=True,
        description="Generate usage statistics.",
    )

    usage_data_source: UsageDataSource = pydantic.Field(
        default=UsageDataSource.AUTO,
        description=(
            "Source for usage/query history data extraction. Options: "
            f"'{UsageDataSource.AUTO.value}' (default) - Automatically use system.query.history table when SQL warehouse is configured, otherwise fall back to REST API. "
            "This provides better performance for multi-workspace setups and large query volumes when warehouse_id is set. "
            f"'{UsageDataSource.SYSTEM_TABLES.value}' - Force use of system.query.history table (requires SQL warehouse and SELECT permission on system.query.history). "
            f"'{UsageDataSource.API.value}' - Force use of REST API endpoints for query history (legacy method, may have limitations with multiple workspaces)."
        ),
    )

    include_queries: bool = pydantic.Field(
        default=True,
        description=(
            "If enabled, emit DataHub Query entities for the SQL statements seen in query "
            "history (the statement text and the datasets it reads/writes). Only effective "
            "on the system-tables usage path; identical statements are de-duplicated by "
            "fingerprint."
        ),
    )
    include_query_usage_statistics: bool = pydantic.Field(
        default=True,
        description=(
            "If enabled, emit per-query usage/popularity statistics (queryUsageStatistics) "
            "for the emitted Query entities. Only effective when include_queries is enabled."
        ),
    )

    push_down_database_pattern_access_history: bool = Field(
        default=False,
        description=(
            "If enabled, pushes down catalog pattern filtering to system.access.table_lineage "
            "for improved performance during usage extraction. This filters on source and target "
            "catalogs in table_lineage. Maps to Snowflake's database_pattern semantics via "
            "catalog_pattern (Unity catalog = Snowflake database). Only applies when usage is "
            "fetched via system tables (usage_data_source AUTO with warehouse or SYSTEM_TABLES). "
            "Also adds a statement_id semi-join against table_lineage, so only queries that "
            "have at least one lineage row in the configured time window are fetched; queries "
            "without system.access.table_lineage rows are omitted entirely (not sqlglot-fallbacked)."
        ),
    )

    skip_sqlglot_when_system_table_lineage_missing: bool = Field(
        default=False,
        description=(
            "If enabled on the system-tables usage path, queries with no matching rows "
            "in system.access.table_lineage (for their statement_id within the configured "
            "time window) are skipped instead of parsed with sqlglot. Only applies when "
            "usage is fetched via system.query.history joined with system.access.table_lineage. "
            "Queries that have lineage but unresolvable dataset URNs still fall back to sqlglot."
        ),
    )

    include_column_usage_stats: bool = Field(
        default=False,
        description=(
            "If enabled, force full sqlglot parsing of usage queries so column-level "
            "usage statistics (fieldCounts) are produced. This bypasses the faster "
            "system-table preparsed lineage path, so usage extraction is slower. Only "
            "changes behavior on the system-tables usage path (the REST API path already "
            "parses every query). Takes precedence over "
            "push_down_database_pattern_access_history and "
            "skip_sqlglot_when_system_table_lineage_missing, which are ignored when set."
        ),
    )

    local_temp_path: HiddenFromDocs[Optional[pathlib.Path]] = pydantic.Field(
        default=None,
        description=(
            "Advanced/dev only. Local directory in which to persist the drained "
            "query-history audit log (SQLite). When set, the audit log is kept across "
            "runs so the next run over the same time window reloads it and skips "
            "re-fetching (including after a crash). The file name is keyed by the usage "
            "source and time window so it never serves a stale window; files for other "
            "windows are not pruned automatically. When unset, an ephemeral, "
            "self-cleaning buffer is used and no caching occurs."
        ),
    )

    # TODO: Remove `type:ignore` by refactoring config
    profiling: Union[
        UnityCatalogGEProfilerConfig,
        UnityCatalogAnalyzeProfilerConfig,
        UnityCatalogSQLAlchemyProfilerConfig,
    ] = Field(  # type: ignore
        default=UnityCatalogSQLAlchemyProfilerConfig(),
        description="Data profiling configuration",
        discriminator="method",
    )

    emit_siblings: bool = pydantic.Field(
        default=True,
        description="Whether to emit siblings relation with corresponding delta-lake platform's table. If enabled, this will also ingest the corresponding delta-lake table.",
    )

    delta_lake_options: DeltaLakeDetails = Field(
        default=DeltaLakeDetails(),
        description="Details about the delta lake, incase to emit siblings",
    )

    include_ml_model_aliases: bool = pydantic.Field(
        default=False,
        description="Whether to include ML model aliases in the ingestion.",
    )

    ml_model_max_results: int = pydantic.Field(
        default=1000,
        ge=0,
        description="Maximum number of ML models to ingest.",
    )

    _forced_disable_tag_extraction: bool = pydantic.PrivateAttr(default=False)
    _forced_disable_hive_metastore_extraction = pydantic.PrivateAttr(default=False)

    include_hive_metastore: bool = pydantic.Field(
        default=INCLUDE_HIVE_METASTORE_DEFAULT,
        description="Whether to ingest legacy `hive_metastore` catalog. This requires executing queries on SQL warehouse.",
    )

    workspace_name: Optional[str] = pydantic.Field(
        default=None,
        description="Name of the workspace. Default to deployment name present in workspace_url",
    )

    def __init__(self, **data):
        # First, let the parent handle the root validators and field processing
        super().__init__(**data)

        # After model creation, check if we need to auto-disable features
        # based on the final warehouse_id value (which may have been set by root validators)
        include_tags_original = data.get("include_tags", INCLUDE_TAGS_DEFAULT)
        include_hive_metastore_original = data.get(
            "include_hive_metastore", INCLUDE_HIVE_METASTORE_DEFAULT
        )

        # Track what we're force-disabling
        forced_disable_tag_extraction = False
        forced_disable_hive_metastore_extraction = False

        # Check if features should be auto-disabled based on final warehouse_id
        if include_tags_original and not self.warehouse_id:
            forced_disable_tag_extraction = True
            self.include_tags = False  # Modify the model attribute directly
            logger.warning(
                "warehouse_id is not set but include_tags=True. "
                "Automatically disabling tag extraction since it requires SQL queries. "
                "Set warehouse_id to enable tag extraction."
            )

        if include_hive_metastore_original and not self.warehouse_id:
            forced_disable_hive_metastore_extraction = True
            self.include_hive_metastore = False  # Modify the model attribute directly
            logger.warning(
                "warehouse_id is not set but include_hive_metastore=True. "
                "Automatically disabling hive metastore extraction since it requires SQL queries. "
                "Set warehouse_id to enable hive metastore extraction."
            )

        # Set private attributes
        self._forced_disable_tag_extraction = forced_disable_tag_extraction
        self._forced_disable_hive_metastore_extraction = (
            forced_disable_hive_metastore_extraction
        )

    def usage_uses_system_tables(self, warehouse_id: Optional[str]) -> bool:
        """Return True when usage data should come from the system-tables join path."""
        src = self.usage_data_source
        if src == UsageDataSource.SYSTEM_TABLES:
            return True
        if src == UsageDataSource.AUTO:
            return bool(warehouse_id)
        return False

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    def is_ge_profiling(self) -> bool:
        return self.profiling.method == "ge"

    def is_sqlalchemy_profiling(self) -> bool:
        return self.profiling.method == "sqlalchemy"

    def uses_table_level_profiler(self) -> bool:
        return self.is_ge_profiling() or self.is_sqlalchemy_profiling()

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Unity Catalog Stateful Ingestion Config."
    )

    def _validate_start_time_window(self) -> None:
        # Called at the end of set_warehouse_id_from_profiling so self.warehouse_id is
        # already resolved from profiling before this check runs.
        # Intentionally checks BOTH usage and lineage data sources: either path using
        # system tables extends the allowed history window from 30 to 365 days.
        uses_system_tables = bool(self.warehouse_id) and (
            self.usage_data_source != UsageDataSource.API
            or self.lineage_data_source != LineageDataSource.API
        )
        # system.query.history / system.access.* retain ~365 days; the REST API is limited to 30.
        max_days = 365 if uses_system_tables else 30
        age_days = (datetime.now(timezone.utc) - self.start_time).days
        if age_days > max_days:
            raise ValueError(
                f"start_time is {age_days} days old; the configured source retains at "
                f"most {max_days} days of history."
            )

    @field_validator("workspace_url", mode="after")
    @classmethod
    def workspace_url_should_start_with_http_scheme(cls, workspace_url: str) -> str:
        if not workspace_url.lower().startswith(("http://", "https://")):
            raise ValueError(
                "Workspace URL must start with http scheme. e.g. https://my-workspace.cloud.databricks.com"
            )
        return workspace_url

    @field_validator("local_temp_path", mode="after")
    @classmethod
    def local_temp_path_must_be_dir(
        cls, v: Optional[pathlib.Path]
    ) -> Optional[pathlib.Path]:
        # Fail fast with a clear message instead of a confusing extraction error when
        # the audit-log cache directory doesn't exist (matches Snowflake/BigQuery).
        if v is not None and not v.is_dir():
            raise ValueError(f"local_temp_path must be an existing directory, got: {v}")
        return v

    @field_validator("include_metastore", mode="after")
    @classmethod
    def include_metastore_warning(cls, v: bool) -> bool:
        if v:
            msg = (
                "`include_metastore` is enabled."
                " This is not recommended and this option will be removed in the future, which is a breaking change."
                " All databricks urns will change if you re-ingest with this disabled."
                " We recommend soft deleting all databricks data and re-ingesting with `include_metastore` set to `False`."
            )
            logger.warning(msg)
            add_global_warning(msg)
        return v

    @model_validator(mode="after")
    def set_warehouse_id_from_profiling(self):
        profiling = self.profiling
        if not self.warehouse_id and profiling and profiling.warehouse_id:
            self.warehouse_id = profiling.warehouse_id
        if (
            self.warehouse_id
            and profiling
            and profiling.warehouse_id
            and self.warehouse_id != profiling.warehouse_id
        ):
            raise ValueError(
                "When `warehouse_id` is set, it must match the `warehouse_id` in `profiling`."
            )

        if self.warehouse_id and profiling and not profiling.warehouse_id:
            profiling.warehouse_id = self.warehouse_id

        if profiling and profiling.enabled and not profiling.warehouse_id:
            raise ValueError("warehouse_id must be set when profiling is enabled.")

        # Run after warehouse_id is resolved so the 30 vs 365-day cap is correct.
        self._validate_start_time_window()

        return self

    @model_validator(mode="after")
    def validate_lineage_data_source_with_warehouse(self):
        lineage_data_source = self.lineage_data_source or LineageDataSource.AUTO

        if (
            lineage_data_source == LineageDataSource.SYSTEM_TABLES
            and not self.warehouse_id
        ):
            raise ValueError(
                f"lineage_data_source='{LineageDataSource.SYSTEM_TABLES.value}' requires warehouse_id to be set"
            )

        return self

    @model_validator(mode="after")
    def validate_usage_data_source_with_warehouse(self):
        usage_data_source = self.usage_data_source or UsageDataSource.AUTO

        if usage_data_source == UsageDataSource.SYSTEM_TABLES and not self.warehouse_id:
            raise ValueError(
                f"usage_data_source='{UsageDataSource.SYSTEM_TABLES.value}' requires warehouse_id to be set"
            )

        return self

    @model_validator(mode="after")
    def warn_system_table_usage_flags_without_system_tables(self):
        # These flags only affect the system.query.history + table_lineage path.
        # warehouse_id is already resolved here (set_warehouse_id_from_profiling runs
        # first), so we can tell whether usage will actually use system tables and
        # warn — rather than fail — when the flags would be silent no-ops.
        if self.usage_uses_system_tables(self.warehouse_id):
            return self

        set_flags = [
            name
            for name, enabled in (
                (
                    "push_down_database_pattern_access_history",
                    self.push_down_database_pattern_access_history,
                ),
                (
                    "skip_sqlglot_when_system_table_lineage_missing",
                    self.skip_sqlglot_when_system_table_lineage_missing,
                ),
            )
            if enabled
        ]
        if set_flags:
            msg = (
                f"{', '.join(set_flags)} only affect the system-tables usage path "
                "but usage is not configured to use system tables "
                f"(usage_data_source={self.usage_data_source.value}, "
                f"warehouse_id={'set' if self.warehouse_id else 'unset'}). "
                "These options will have no effect."
            )
            logger.warning(msg)
            add_global_warning(msg)
        return self

    @model_validator(mode="after")
    def warn_column_usage_stats_overrides_system_table_flags(self):
        if not self.include_column_usage_stats:
            return self

        overridden = [
            name
            for name, enabled in (
                (
                    "push_down_database_pattern_access_history",
                    self.push_down_database_pattern_access_history,
                ),
                (
                    "skip_sqlglot_when_system_table_lineage_missing",
                    self.skip_sqlglot_when_system_table_lineage_missing,
                ),
            )
            if enabled
        ]
        if overridden:
            msg = (
                f"{', '.join(overridden)} are ignored because include_column_usage_stats "
                "is enabled: usage queries are fully sqlglot-parsed for column-level "
                "statistics."
            )
            logger.warning(msg)
            add_global_warning(msg)
        return self

    @field_validator("schema_pattern", mode="after")
    @classmethod
    def schema_pattern_should__always_deny_information_schema(
        cls, v: AllowDenyPattern
    ) -> AllowDenyPattern:
        v.deny.append(".*\\.information_schema")
        return v

    @field_validator("profiling", mode="before")
    @classmethod
    def _default_profiling_method(cls, v: object) -> object:
        if isinstance(v, dict) and "method" not in v:
            return {**v, "method": "sqlalchemy"}
        return v
