import json
import re
import textwrap
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import cached_property
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import clickhouse_driver
import clickhouse_sqlalchemy.types as custom_types
import pydantic
from clickhouse_sqlalchemy.drivers import base
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from pydantic import field_validator, model_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import reflection
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import BOOLEAN, DATE, DATETIME, INTEGER

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import HiddenFromDocs, LaxStr
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    get_time_bucket,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.sql_common import (
    SqlWorkUnit,
    logger,
    register_custom_type,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    MapTypeClass,
    NumberTypeClass,
    StringTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    UpstreamClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

assert clickhouse_driver

# adding extra types not handled by clickhouse-sqlalchemy 0.1.8
base.ischema_names["DateTime64(0)"] = DATETIME
base.ischema_names["DateTime64(1)"] = DATETIME
base.ischema_names["DateTime64(2)"] = DATETIME
base.ischema_names["DateTime64(3)"] = DATETIME
base.ischema_names["DateTime64(4)"] = DATETIME
base.ischema_names["DateTime64(5)"] = DATETIME
base.ischema_names["DateTime64(6)"] = DATETIME
base.ischema_names["DateTime64(7)"] = DATETIME
base.ischema_names["DateTime64(8)"] = DATETIME
base.ischema_names["DateTime64(9)"] = DATETIME
base.ischema_names["Date32"] = DATE
base.ischema_names["Bool"] = BOOLEAN
base.ischema_names["Nothing"] = sqltypes.NullType
base.ischema_names["Int128"] = INTEGER
base.ischema_names["Int256"] = INTEGER
base.ischema_names["UInt128"] = INTEGER
base.ischema_names["UInt256"] = INTEGER
# This is needed for clickhouse-sqlalchemy 0.2.3
base.ischema_names["DateTime"] = DATETIME
base.ischema_names["DateTime64"] = DATETIME

register_custom_type(custom_types.common.Array, ArrayTypeClass)
register_custom_type(custom_types.ip.IPv4, NumberTypeClass)
register_custom_type(custom_types.ip.IPv6, StringTypeClass)
register_custom_type(custom_types.common.Map, MapTypeClass)
register_custom_type(custom_types.common.Tuple, UnionTypeClass)


def _is_valid_username(value: str) -> bool:
    """Check if value is a safe username (alphanumeric, underscores, hyphens)."""
    if not value:
        return False
    allowed_chars = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    )
    return all(c in allowed_chars for c in value)


class LineageCollectorType(Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"


class LineageDatasetPlatform(Enum):
    CLICKHOUSE = "clickhouse"


@dataclass(frozen=True, eq=True)
class LineageDataset:
    platform: LineageDatasetPlatform
    path: str


@dataclass
class LineageItem:
    dataset: LineageDataset
    upstreams: Set[LineageDataset]
    collector_type: LineageCollectorType
    dataset_lineage_type: str = field(init=False)

    def __post_init__(self):
        if self.collector_type == LineageCollectorType.TABLE:
            self.dataset_lineage_type = DatasetLineageTypeClass.COPY
        elif self.collector_type in [
            LineageCollectorType.VIEW,
        ]:
            self.dataset_lineage_type = DatasetLineageTypeClass.VIEW
        else:
            self.dataset_lineage_type = DatasetLineageTypeClass.TRANSFORMED


class ClickHouseConfig(
    TwoTierSQLAlchemyConfig, BaseTimeWindowConfig, DatasetLineageProviderConfigBase
):
    # defaults
    host_port: str = Field(default="localhost:8123", description="ClickHouse host URL.")
    scheme: HiddenFromDocs[str] = Field(default="clickhouse")
    password: pydantic.SecretStr = Field(
        default=pydantic.SecretStr(""), description="password"
    )
    secure: Optional[bool] = Field(
        default=None, description="[deprecated] Use uri_opts instead."
    )
    protocol: Optional[str] = Field(
        default=None, description="[deprecated] Use uri_opts instead."
    )
    _deprecate_secure = pydantic_field_deprecated("secure")
    _deprecate_protocol = pydantic_field_deprecated("protocol")

    uri_opts: Dict[str, LaxStr] = Field(
        default={},
        description="The part of the URI and it's used to provide additional configuration options or parameters for the database connection.",
    )
    include_table_lineage: Optional[bool] = Field(
        default=True, description="Whether table lineage should be ingested."
    )
    include_materialized_views: Optional[bool] = Field(default=True, description="")

    # Query log extraction options
    include_query_log_lineage: bool = Field(
        default=False,
        description="Whether to extract lineage from query_log (INSERT/CREATE queries). "
        "This complements the definition-based lineage from views/materialized views.",
    )
    include_usage_statistics: bool = Field(
        default=False,
        description="Whether to extract usage statistics from query_log. "
        "Tracks which tables and columns are queried by SELECT statements.",
    )
    include_query_log_operations: bool = Field(
        default=False,
        description="Whether to show recent write activity on datasets. "
        "Displays INSERT, UPDATE, DELETE history in the dataset's activity tab.",
    )
    query_log_deny_usernames: List[str] = Field(
        default=[],
        description="List of ClickHouse usernames to exclude from query log extraction.",
    )
    temporary_tables_pattern: List[str] = Field(
        default=[
            r"^_.*",  # Tables starting with underscore
            r".*\.tmp_.*",  # Tables with tmp_ prefix
            r".*\.temp_.*",  # Tables with temp_ prefix
            r".*\._inner.*",  # Inner tables for materialized views
        ],
        description="Regex patterns for temporary tables to filter from lineage.",
    )
    top_n_queries: pydantic.PositiveInt = Field(
        default=10,
        description="Number of top queries to save to each table for usage statistics.",
    )

    @field_validator("query_log_deny_usernames")
    @classmethod
    def validate_query_log_deny_usernames(cls, v: List[str]) -> List[str]:
        """Validate usernames to prevent SQL injection."""
        for username in v:
            if not _is_valid_username(username):
                raise ValueError(
                    f"Invalid username '{username}' in query_log_deny_usernames. "
                    "Usernames must contain only alphanumeric characters, "
                    "underscores, and hyphens."
                )
        return v

    @cached_property
    def _compiled_temporary_tables_pattern(self) -> List[re.Pattern[str]]:
        return [
            re.compile(pattern, re.IGNORECASE)
            for pattern in self.temporary_tables_pattern
        ]

    def is_temp_table(self, name: str) -> bool:
        """Check if a table name matches temporary table patterns."""
        for pattern in self._compiled_temporary_tables_pattern:
            if pattern.match(name):
                return True
        return False

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        current_db: Optional[str] = None,
    ) -> str:
        url = make_url(
            super().get_sql_alchemy_url(uri_opts=self.uri_opts, current_db=current_db)
        )
        if url.drivername == "clickhouse+native" and url.query.get("protocol"):
            logger.debug(f"driver = {url.drivername}, query = {url.query}")
            raise Exception(
                "You cannot use a schema clickhouse+native and clickhouse+http at the same time"
            )

        # We can setup clickhouse ingestion in sqlalchemy_uri form and config form.
        # Why we need to update database in uri at all?
        # Because we get database from sqlalchemy inspector and inspector we form from url inherited from
        # TwoTierSQLAlchemySource and SQLAlchemySource
        if self.sqlalchemy_uri and current_db:
            url = url.set(database=current_db)

        return str(url)

    # pre = True because we want to take some decision before pydantic initialize the configuration to default values
    @model_validator(mode="before")
    @classmethod
    def projects_backward_compatibility(cls, values: Dict) -> Dict:
        secure = values.get("secure")
        protocol = values.get("protocol")
        uri_opts = values.get("uri_opts")
        if (secure or protocol) and not uri_opts:
            logger.warning(
                "uri_opts is not set but protocol or secure option is set."
                " secure and  protocol options is deprecated, please use "
                "uri_opts instead."
            )
            logger.info(
                "Initializing uri_opts from deprecated secure or protocol options"
            )
            values["uri_opts"] = {}
            if secure is not None:
                values["uri_opts"]["secure"] = str(secure)
            if protocol is not None:
                values["uri_opts"]["protocol"] = protocol
            logger.debug(f"uri_opts: {uri_opts}")
        elif (secure or protocol) and uri_opts:
            raise ValueError(
                "secure and protocol options is deprecated. Please use uri_opts only."
            )

        return values


PROPERTIES_COLUMNS = (
    "engine, partition_key, sorting_key, primary_key, sampling_key, storage_policy, "
    + "metadata_modification_time, total_rows, total_bytes, data_paths, metadata_path"
)


# reflection.cache uses eval and other magic to partially rewrite the function.
# mypy can't handle it, so we ignore it for now.
@reflection.cache  # type: ignore
def _get_all_table_comments_and_properties(self, connection, **kw):
    properties_clause = (
        "formatRow('JSONEachRow', {properties_columns})".format(
            properties_columns=PROPERTIES_COLUMNS
        )
        if PROPERTIES_COLUMNS
        else "null"
    )
    comment_sql = textwrap.dedent(
        """\
        SELECT database
             , name                AS table_name
             , comment
             , {properties_clause} AS properties
          FROM system.tables
         WHERE name NOT LIKE '.inner%'""".format(properties_clause=properties_clause)
    )

    all_table_comments: Dict[Tuple[str, str], Dict[str, Any]] = {}

    result = connection.execute(text(comment_sql))
    for table in result:
        all_table_comments[(table.database, table.table_name)] = {
            "text": table.comment,
            "properties": (
                {k: str(v) for k, v in json.loads(table.properties).items()}
                if table.properties
                else {}
            ),
        }
    return all_table_comments


@reflection.cache  # type: ignore
def get_table_comment(self, connection, table_name, schema=None, **kw):
    all_table_comments = self._get_all_table_comments_and_properties(connection, **kw)
    return all_table_comments.get((schema, table_name), {"text": None})


@reflection.cache  # type: ignore
def _get_all_relation_info(self, connection, **kw):
    result = connection.execute(
        text(
            textwrap.dedent(
                """\
        SELECT database
             , if(engine LIKE '%View', 'v', 'r') AS relkind
             , name                              AS relname
          FROM system.tables
         WHERE name NOT LIKE '.inner%'"""
            )
        )
    )
    relations = {}
    for rel in result:
        relations[(rel.database, rel.relname)] = rel
    return relations


def _get_table_or_view_names(self, relkind, connection, schema=None, **kw):
    info_cache = kw.get("info_cache")
    all_relations = self._get_all_relation_info(connection, info_cache=info_cache)
    relation_names = []
    for _, relation in all_relations.items():
        if relation.database == schema and relation.relkind == relkind:
            relation_names.append(relation.relname)
    return relation_names


@reflection.cache  # type: ignore
def get_table_names(self, connection, schema=None, **kw):
    return self._get_table_or_view_names("r", connection, schema, **kw)


@reflection.cache  # type: ignore
def get_view_names(self, connection, schema=None, **kw):
    return self._get_table_or_view_names("v", connection, schema, **kw)


# We fetch column info an entire schema at a time to improve performance
# when reflecting schema for multiple tables at once.
@reflection.cache  # type: ignore
def _get_schema_column_info(self, connection, schema=None, **kw):
    all_columns = defaultdict(list)

    where_clause = "WHERE database = :schema" if schema else ""
    params = {"schema": schema} if schema else {}

    query = text(f"""
        SELECT database
             , table AS table_name
             , name
             , type
             , comment
          FROM system.columns
         {where_clause}
         ORDER BY database, table, position
    """)
    result = connection.execute(query, params)

    for col in result:
        key = (col.database, col.table_name)
        all_columns[key].append(col)
    return dict(all_columns)


def _get_clickhouse_columns(self, connection, table_name, schema=None, **kw):
    info_cache = kw.get("info_cache")
    all_schema_columns = self._get_schema_column_info(
        connection, schema, info_cache=info_cache
    )
    key = (schema, table_name)
    return all_schema_columns.get(key, [])


def _get_column_info(self, name, format_type, comment):
    try:
        col_type = self._get_column_type(name, format_type)
    except TypeError as e:
        # clickhouse_sqlalchemy can fail on complex types like Map with nested types
        # that it doesn't fully support (e.g., Map(String, String) parsing issues).
        # Fall back to NullType to allow schema discovery to continue.
        logger.warning(
            f"Failed to parse column type for '{name}' with type '{format_type}': {e}. "
            "Using NullType as fallback."
        )
        col_type = sqltypes.NullType()

    nullable = False

    # extract nested_type from LowCardinality type
    if isinstance(col_type, custom_types.common.LowCardinality):
        col_type = col_type.nested_type

    # extract nested_type from Nullable type
    if isinstance(col_type, custom_types.common.Nullable):
        col_type = col_type.nested_type
        nullable = True

    result = {
        "name": name,
        "type": col_type,
        "nullable": nullable,
        "comment": comment,
        "full_type": format_type,
    }
    return result


@reflection.cache  # type: ignore
def get_columns(self, connection, table_name, schema=None, **kw):
    if not schema:
        query = f"DESCRIBE TABLE {self._quote_table_name(table_name)}"
        cols = self._execute(connection, query)
    else:
        cols = self._get_clickhouse_columns(connection, table_name, schema, **kw)

    return [
        self._get_column_info(name=col.name, format_type=col.type, comment=col.comment)
        for col in cols
    ]


@reflection.cache  # type: ignore
def get_view_definition(self, connection, view_name, schema=None, **kw):
    """Extract view definition from ClickHouse system tables.

    ClickHouse stores the full CREATE statement in system.tables.create_table_query.
    We return the full statement and let the SQL parser extract the SELECT portion.
    """
    schema_clause = "AND database = :schema" if schema else ""
    params = (
        {"view_name": view_name, "schema": schema}
        if schema
        else {"view_name": view_name}
    )

    query = text(f"""
        SELECT create_table_query
          FROM system.tables
         WHERE name = :view_name
           AND engine LIKE '%View'
           {schema_clause}
    """)
    result = connection.execute(query, params).fetchone()

    if result and result[0]:
        return result[0]
    return ""


# This monkey-patching enables us to batch fetch the table descriptions, rather than
# fetching them one at a time.
ClickHouseDialect._get_all_table_comments_and_properties = (
    _get_all_table_comments_and_properties
)
ClickHouseDialect.get_table_comment = get_table_comment
ClickHouseDialect._get_all_relation_info = _get_all_relation_info
ClickHouseDialect._get_table_or_view_names = _get_table_or_view_names
ClickHouseDialect.get_table_names = get_table_names
ClickHouseDialect.get_view_names = get_view_names
ClickHouseDialect._get_schema_column_info = _get_schema_column_info
ClickHouseDialect._get_clickhouse_columns = _get_clickhouse_columns
ClickHouseDialect._get_column_info = _get_column_info
ClickHouseDialect.get_columns = get_columns
ClickHouseDialect.get_view_definition = get_view_definition

clickhouse_datetime_format = "%Y-%m-%d %H:%M:%S"


@platform_name("ClickHouse")
@config_class(ClickHouseConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default to get lineage for views via `include_view_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.VIEW,
        SourceCapabilityModifier.TABLE,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default via `include_view_column_lineage`",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Optionally enabled via `include_usage_statistics`",
)
class ClickHouseSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for tables, views, materialized views and dictionaries
    - Column types associated with each table(except *AggregateFunction and DateTime with timezone)
    - Table, row, and column statistics via optional SQL profiling.
    - Table, view, materialized view and dictionary(with CLICKHOUSE source_type) lineage

    ### Query Log Extraction

    Enable `include_query_log_lineage` and/or `include_usage_statistics` to extract
    additional metadata from ClickHouse's `system.query_log`:

    - **Query-based lineage**: Table and column-level lineage from INSERT/CREATE queries
    - **Usage statistics**: Dataset usage from SELECT queries
    - **Operations**: Operation aspects (INSERT, UPDATE, etc.)

    ```yaml
    source:
      type: clickhouse
      config:
        host_port: "localhost:8123"
        include_query_log_lineage: true
        include_usage_statistics: true
        start_time: "2024-01-01T00:00:00Z"
        end_time: "2024-01-08T00:00:00Z"
    ```

    """

    config: ClickHouseConfig

    def __init__(self, config: ClickHouseConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "clickhouse")
        self._lineage_map: Optional[Dict[str, LineageItem]] = None
        self._all_tables_set: Optional[Set[str]] = None
        self._query_log_aggregator: Optional[SqlParsingAggregator] = None

        # Initialize query log aggregator if needed
        if self._should_extract_query_log():
            self._init_query_log_aggregator()

    @classmethod
    def create(cls, config_dict, ctx):
        config = ClickHouseConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _add_view_to_aggregator(
        self,
        view_urn: str,
        view_definition: str,
        default_db: Optional[str],
        default_schema: Optional[str],
    ) -> None:
        """Override to handle ClickHouse materialized views with TO clause.

        ClickHouse materialized views can specify a separate target table using the
        TO clause: CREATE MATERIALIZED VIEW view_name TO target_table AS SELECT ...
        The target table stores the actual data, while the materialized view is just a trigger.
        For lineage, we want source → target_table (not source → materialized view).
        """
        to_table_urn = self._extract_to_table_urn(view_urn, view_definition, default_db)
        if to_table_urn:
            # Register lineage to the TO table instead of the MV
            self.aggregator.add_view_definition(
                view_urn=to_table_urn,
                view_definition=view_definition,
                default_db=default_db,
                default_schema=default_schema,
            )
            # Also add MV → TO table relationship
            self.aggregator.add_known_lineage_mapping(
                upstream_urn=view_urn,
                downstream_urn=to_table_urn,
                lineage_type=DatasetLineageTypeClass.VIEW,
            )
        else:
            super()._add_view_to_aggregator(
                view_urn=view_urn,
                view_definition=view_definition,
                default_db=default_db,
                default_schema=default_schema,
            )

    def _extract_to_table_urn(
        self,
        view_urn: str,
        view_definition: str,
        default_db: Optional[str],
    ) -> Optional[str]:
        """Extract the TO table URN from a ClickHouse materialized view definition."""
        if "MATERIALIZED VIEW" not in view_definition.upper():
            return None

        # Extract target table from TO clause (case-insensitive)
        match = re.search(r"\bTO\s+(\S+)", view_definition, re.IGNORECASE)
        if not match:
            return None

        target_table = match.group(1)
        # Remove backticks or quotes if present
        target_table = target_table.strip("`\"'")

        # Build the target table URN
        if "." in target_table:
            # Fully qualified: db.table
            target_dataset_name = target_table
        else:
            # Unqualified: use the default database
            if default_db:
                target_dataset_name = f"{default_db}.{target_table}"
            else:
                target_dataset_name = target_table

        return builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=target_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _should_extract_query_log(self) -> bool:
        """Check if any query log extraction feature is enabled."""
        return (
            self.config.include_query_log_lineage
            or self.config.include_usage_statistics
            or self.config.include_query_log_operations
        )

    def _init_query_log_aggregator(self) -> None:
        """Initialize the SQL parsing aggregator for query log extraction."""
        start_time, end_time = self._get_query_log_time_window()

        self._query_log_aggregator = SqlParsingAggregator(
            platform="clickhouse",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            eager_graph_load=False,
            generate_lineage=self.config.include_query_log_lineage,
            generate_queries=self.config.include_query_log_lineage,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_query_usage_statistics=self.config.include_usage_statistics,
            usage_config=BaseUsageConfig(
                bucket_duration=self.config.bucket_duration,
                start_time=start_time,
                end_time=end_time,
                top_n_queries=self.config.top_n_queries,
            ),
            generate_operations=self.config.include_query_log_operations,
            is_temp_table=self.config.is_temp_table,
            format_queries=False,
        )

    def _get_query_log_time_window(self) -> Tuple[datetime, datetime]:
        """Get the time window for query log extraction."""
        start_time = self.config.start_time
        end_time = self.config.end_time

        # Align to bucket boundaries for accurate aggregation
        if self.config.include_usage_statistics:
            start_time = get_time_bucket(start_time, self.config.bucket_duration)

        return start_time, end_time

    def _build_query_log_query(self) -> str:
        """Build SQL query to fetch relevant entries from query_log."""
        start_time, end_time = self._get_query_log_time_window()
        start_time_str = start_time.strftime(clickhouse_datetime_format)
        end_time_str = end_time.strftime(clickhouse_datetime_format)

        # Build user exclusion filter
        user_filters = []
        for username in self.config.query_log_deny_usernames:
            user_filters.append(f"user != '{username}'")
        user_filter_clause = " AND ".join(user_filters) if user_filters else "1=1"

        # Query kinds that produce lineage (INSERT, CREATE TABLE AS)
        # For usage, we also include SELECT
        query_kinds = ["'Insert'", "'Create'", "'Select'"]
        query_kinds_clause = ", ".join(query_kinds)

        # Security: usernames are validated by Pydantic field validator
        # (validate_query_log_deny_usernames) to only allow safe characters [a-zA-Z0-9_-],
        # preventing SQL injection.
        return f"""
SELECT
    query_id,
    query,
    query_kind,
    user,
    event_time,
    query_duration_ms,
    read_rows,
    written_rows,
    current_database,
    normalized_query_hash
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query = 1
  AND event_time >= '{start_time_str}'
  AND event_time < '{end_time_str}'
  AND query_kind IN ({query_kinds_clause})
  AND {user_filter_clause}
  AND query NOT LIKE '%system.%'
ORDER BY event_time ASC
"""

    def _extract_query_log(self) -> Iterable[MetadataWorkUnit]:
        """Extract lineage and usage from query_log and yield workunits."""
        if not self._query_log_aggregator:
            return

        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)

        query = self._build_query_log_query()
        logger.info("Fetching query log from ClickHouse...")
        logger.debug(f"Query log SQL: {query[:200]}...")

        try:
            result = engine.execute(text(query))
            rows = list(result)
            logger.info(f"Fetched {len(rows)} queries from query_log")
        except Exception as e:
            self.report.report_failure(
                "query_log_extraction",
                f"Failed to fetch query log: {e}",
            )
            return

        num_lineage = 0
        num_usage = 0
        for row in rows:
            row_dict = dict(row._mapping)
            observed_query = self._parse_query_log_row(row_dict)
            if observed_query:
                query_kind = row_dict.get("query_kind", "")
                if query_kind in ("Insert", "Create"):
                    num_lineage += 1
                elif query_kind == "Select":
                    num_usage += 1
                self._query_log_aggregator.add(observed_query)

        logger.info(
            f"Query log processing complete: {num_lineage} lineage queries, "
            f"{num_usage} usage queries"
        )

        yield from auto_workunit(self._query_log_aggregator.gen_metadata())

    def _parse_query_log_row(self, row: Dict) -> Optional[ObservedQuery]:
        """Parse a query_log row into an ObservedQuery."""
        try:
            timestamp = row["event_time"]
            if isinstance(timestamp, datetime):
                timestamp = timestamp.astimezone(timezone.utc)

            query = row["query"]
            user = row.get("user", "")

            return ObservedQuery(
                query=query,
                session_id=row.get("query_id"),
                timestamp=timestamp,
                user=CorpUserUrn(user) if user else None,
                default_db=row.get("current_database"),
                query_hash=str(row.get("normalized_query_hash", "")),
            )
        except Exception as e:
            logger.warning(f"Failed to parse query log row: {e}")
            return None

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # Emit schema and definition-based lineage workunits
        for wu in super().get_workunits_internal():
            if (
                self.config.include_table_lineage
                and isinstance(wu, SqlWorkUnit)
                and isinstance(wu.metadata, MetadataChangeEvent)
                and isinstance(wu.metadata.proposedSnapshot, DatasetSnapshot)
            ):
                dataset_snapshot: DatasetSnapshotClass = wu.metadata.proposedSnapshot
                assert dataset_snapshot

                lineage_mcp = self.get_lineage_mcp(wu.metadata.proposedSnapshot.urn)

                if lineage_mcp is not None:
                    yield lineage_mcp.as_workunit()

            # Emit the work unit from super.
            yield wu

        # Emit query log based lineage and usage workunits
        if self._should_extract_query_log():
            yield from self._extract_query_log()

    def _get_all_tables(self) -> Set[str]:
        all_tables_query: str = textwrap.dedent(
            """\
        SELECT database, name AS table_name
          FROM system.tables
         WHERE name NOT LIKE '.inner%'"""
        )

        all_tables_set = set()

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        for db_row in engine.execute(text(all_tables_query)):
            all_tables_set.add(f"{db_row['database']}.{db_row['table_name']}")

        return all_tables_set

    def _populate_lineage_map(
        self, query: str, lineage_type: LineageCollectorType
    ) -> None:
        """
        This method generate table level lineage based with the given query.
        The query should return the following columns: target_schema, target_table, source_table, source_schema

        :param query: The query to run to extract lineage.
        :type query: str
        :param lineage_type: The way the lineage should be processed
        :type lineage_type: LineageType
        return: The method does not return with anything as it directly modify the self._lineage_map property.
        :rtype: None
        """
        assert self._lineage_map is not None

        if not self._all_tables_set:
            self._all_tables_set = self._get_all_tables()

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        try:
            for db_row in engine.execute(text(query)):
                dataset_name = f"{db_row['target_schema']}.{db_row['target_table']}"
                if not self.config.database_pattern.allowed(
                    db_row["target_schema"]
                ) or not self.config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                # Target
                target_path = (
                    f"{self.config.platform_instance + '.' if self.config.platform_instance else ''}"
                    f"{dataset_name}"
                )
                target = LineageItem(
                    dataset=LineageDataset(
                        platform=LineageDatasetPlatform.CLICKHOUSE, path=target_path
                    ),
                    upstreams=set(),
                    collector_type=lineage_type,
                )

                # Source
                platform = LineageDatasetPlatform.CLICKHOUSE
                path = f"{db_row['source_schema']}.{db_row['source_table']}"

                sources = [
                    LineageDataset(
                        platform=platform,
                        path=path,
                    )
                ]

                for source in sources:
                    # Filtering out tables which does not exist in ClickHouse
                    # It was deleted in the meantime
                    if (
                        source.platform == LineageDatasetPlatform.CLICKHOUSE
                        and source.path not in self._all_tables_set
                    ):
                        logger.warning(f"{source.path} missing table")
                        continue

                    target.upstreams.add(source)

                # Merging downstreams if dataset already exists and has downstreams
                if target.dataset.path in self._lineage_map:
                    self._lineage_map[
                        target.dataset.path
                    ].upstreams = self._lineage_map[
                        target.dataset.path
                    ].upstreams.union(target.upstreams)

                else:
                    self._lineage_map[target.dataset.path] = target

                logger.info(
                    f"Lineage[{target}]:{self._lineage_map[target.dataset.path]}"
                )

        except Exception as e:
            logger.warning(
                f"Extracting {lineage_type.name} lineage from ClickHouse failed."
                f"Continuing...\nError was {e}."
            )

    def _populate_lineage(self) -> None:
        # only dictionaries with clickhouse as a source are supported
        table_lineage_query = textwrap.dedent(
            """\
        SELECT extractAll(engine_full, '''(.*?)''')[2] AS source_schema
             , extractAll(engine_full, '''(.*?)''')[3] AS source_table
             , database                                AS target_schema
             , name                                    AS target_table
          FROM system.tables
         WHERE engine IN ('Distributed')
        UNION ALL
        SELECT extract(create_table_query, 'DB ''(.*?)''')    AS source_schema
             , extract(create_table_query, 'TABLE ''(.*?)''') AS source_table
             , database                                       AS target_schema
             , name                                           AS target_table
          FROM system.tables
         WHERE engine IN ('Dictionary')
           AND create_table_query LIKE '%SOURCE(CLICKHOUSE(%'
         ORDER BY target_schema, target_table, source_schema, source_table"""
        )

        view_lineage_query = textwrap.dedent(
            """\
          WITH
              (SELECT groupUniqArray(concat(database, '.', name))
                 FROM system.tables
              ) AS tables
        SELECT substring(source, 1, position(source, '.') - 1) AS source_schema
             , substring(source, position(source, '.') + 1)    AS source_table
             , database                                        AS target_schema
             , name                                            AS target_table
          FROM system.tables
         ARRAY JOIN arrayIntersect(splitByRegexp('[\\s()'']+', create_table_query), tables) AS source
         WHERE engine IN ('View')
           AND NOT (source_schema = target_schema AND source_table = target_table)
         ORDER BY target_schema, target_table, source_schema, source_table"""
        )

        # get materialized view downstream and upstream
        materialized_view_lineage_query = textwrap.dedent(
            """\
        SELECT source_schema, source_table, target_schema, target_table
          FROM (
                  WITH
                      (SELECT groupUniqArray(concat(database, '.', name))
                         FROM system.tables
                      ) AS tables
                SELECT substring(source, 1, position(source, '.') - 1) AS source_schema
                     , substring(source, position(source, '.') + 1)    AS source_table
                     , database                                        AS target_schema
                     , name                                            AS target_table
                     , extract(create_table_query, 'TO (\\S+)')     AS extract_to
                  FROM system.tables
                 ARRAY JOIN arrayIntersect(splitByRegexp('[\\s()'']+', create_table_query), tables) AS source
                 WHERE engine IN ('MaterializedView')
                   AND NOT (source_schema = target_schema AND source_table = target_table)
                   AND source <> extract_to
                 UNION ALL
                SELECT database                                                AS source_schema
                     , name                                                    AS source_table
                     , substring(extract_to, 1, position(extract_to, '.') - 1) AS target_schema
                     , substring(extract_to, position(extract_to, '.') + 1)    AS target_table
                     , extract(create_table_query, 'TO (\\S+)')             AS extract_to
                  FROM system.tables
                 WHERE engine IN ('MaterializedView')
                   AND extract_to <> '')
         ORDER BY target_schema, target_table, source_schema, source_table"""
        )

        if not self._lineage_map:
            self._lineage_map = defaultdict()

        if self.config.include_tables:
            # Populate table level lineage for dictionaries and distributed tables
            self._populate_lineage_map(
                query=table_lineage_query, lineage_type=LineageCollectorType.TABLE
            )

        if self.config.include_views:
            # Populate table level lineage for views
            self._populate_lineage_map(
                query=view_lineage_query, lineage_type=LineageCollectorType.VIEW
            )

        if self.config.include_materialized_views:
            # Populate table level lineage for materialized_views
            self._populate_lineage_map(
                query=materialized_view_lineage_query,
                lineage_type=LineageCollectorType.MATERIALIZED_VIEW,
            )

    def get_lineage_mcp(
        self, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        dataset_key = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return None

        if not self._lineage_map:
            self._populate_lineage()
        assert self._lineage_map is not None

        upstream_lineage: List[UpstreamClass] = []

        if dataset_key.name in self._lineage_map:
            item = self._lineage_map[dataset_key.name]
            for upstream in item.upstreams:
                upstream_table = UpstreamClass(
                    dataset=builder.make_dataset_urn_with_platform_instance(
                        upstream.platform.value,
                        upstream.path,
                        self.config.platform_instance,
                        self.config.env,
                    ),
                    type=item.dataset_lineage_type,
                )
                upstream_lineage.append(upstream_table)

        if not upstream_lineage:
            return None

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineage(upstreams=upstream_lineage),
        )

        return mcp

    def close(self) -> None:
        if self._query_log_aggregator:
            self._query_log_aggregator.close()
        super().close()
