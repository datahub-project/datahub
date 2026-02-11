import logging
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from threading import Lock
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

# This import verifies that the dependencies are available.
import teradatasqlalchemy.types as custom_types
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql.expression import text
from teradatasqlalchemy.dialect import TeradataDialect
from teradatasqlalchemy.options import configure

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp_builder import add_owner_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.stats_collections import TopKDict

logger: logging.Logger = logging.getLogger(__name__)

# Precompiled regex pattern for case-insensitive "(not casespecific)" removal
NOT_CASESPECIFIC_PATTERN = re.compile(r"\(not casespecific\)", re.IGNORECASE)

# Teradata uses a two-tier database.table naming approach without default database prefixing
DEFAULT_NO_DATABASE_TERADATA = None

# Common excluded databases used in multiple places
EXCLUDED_DATABASES = [
    "All",
    "Crashdumps",
    "Default",
    "DemoNow_Monitor",
    "EXTUSER",
    "External_AP",
    "GLOBAL_FUNCTIONS",
    "LockLogShredder",
    "PUBLIC",
    "SQLJ",
    "SYSBAR",
    "SYSJDBC",
    "SYSLIB",
    "SYSSPATIAL",
    "SYSUDTLIB",
    "SYSUIF",
    "SysAdmin",
    "Sys_Calendar",
    "SystemFe",
    "TDBCMgmt",
    "TDMaps",
    "TDPUSER",
    "TDQCD",
    "TDStats",
    "TD_ANALYTICS_DB",
    "TD_SERVER_DB",
    "TD_SYSFNLIB",
    "TD_SYSGPL",
    "TD_SYSXML",
    "TDaaS_BAR",
    "TDaaS_DB",
    "TDaaS_Maint",
    "TDaaS_Monitor",
    "TDaaS_Support",
    "TDaaS_TDBCMgmt1",
    "TDaaS_TDBCMgmt2",
    "dbcmngr",
    "mldb",
    "system",
    "tapidb",
    "tdwm",
    "val",
    "dbc",
]

register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.INTERVAL_DAY, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MONTH, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR_TO_MONTH, TimeTypeClass)
register_custom_type(custom_types.MBB, BytesTypeClass)
register_custom_type(custom_types.MBR, BytesTypeClass)
register_custom_type(custom_types.GEOMETRY, BytesTypeClass)
register_custom_type(custom_types.TDUDT, BytesTypeClass)
register_custom_type(custom_types.XML, BytesTypeClass)
register_custom_type(custom_types.PERIOD_TIME, TimeTypeClass)
register_custom_type(custom_types.PERIOD_DATE, TimeTypeClass)
register_custom_type(custom_types.PERIOD_TIMESTAMP, TimeTypeClass)


@dataclass
class TeradataTable:
    database: str
    name: str
    description: Optional[str]
    object_type: str
    create_timestamp: datetime
    last_alter_name: Optional[str]
    last_alter_timestamp: Optional[datetime]
    request_text: Optional[str]


# Cache size of 1 is sufficient since schemas are processed sequentially
# Note: This cache is per-process and helps when processing multiple tables in the same schema
@lru_cache(maxsize=1)
def get_schema_columns(
    self: Any, connection: Connection, dbc_columns: str, schema: str
) -> Dict[str, List[Any]]:
    start_time = time.time()
    columns: Dict[str, List[Any]] = {}
    columns_query = f"select * from dbc.{dbc_columns} where DatabaseName (NOT CASESPECIFIC) = :schema (NOT CASESPECIFIC) order by TableName, ColumnId"
    rows = connection.execute(text(columns_query), {"schema": schema}).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.TableName not in columns:
            columns[row_mapping.TableName] = []

        columns[row_mapping.TableName].append(row_mapping)

    end_time = time.time()
    extraction_time = end_time - start_time
    logger.info(
        f"Column extraction for schema '{schema}' completed in {extraction_time:.2f} seconds"
    )

    # Update report if available
    if hasattr(self, "report"):
        self.report.column_extraction_duration_seconds += extraction_time

    return columns


# Cache size of 1 is sufficient since schemas are processed sequentially
# Note: This cache is per-process and helps when processing multiple tables in the same schema
@lru_cache(maxsize=1)
def get_schema_pk_constraints(
    self: Any, connection: Connection, schema: str
) -> Dict[str, List[Any]]:
    dbc_indices = "IndicesV" + "X" if configure.usexviews else "IndicesV"
    primary_keys: Dict[str, List[Any]] = {}
    stmt = f"select * from dbc.{dbc_indices} where DatabaseName (NOT CASESPECIFIC) = :schema (NOT CASESPECIFIC) and IndexType = 'K' order by IndexNumber"
    rows = connection.execute(text(stmt), {"schema": schema}).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.TableName not in primary_keys:
            primary_keys[row_mapping.TableName] = []

        primary_keys[row_mapping.TableName].append(row_mapping)

    return primary_keys


def optimized_get_pk_constraint(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    **kw: Dict[str, Any],
) -> Dict:
    """
    Override
    TODO: Check if we need PRIMARY Indices or PRIMARY KEY Indices
    TODO: Check for border cases (No PK Indices)
    """

    if schema is None:
        schema = self.default_schema_name

    # Default value for 'usexviews' is False so use dbc.IndicesV by default
    # dbc_indices = self.__get_xviews_obj("IndicesV")

    # table_obj = table(
    #    dbc_indices, column("ColumnName"), column("IndexName"), schema="dbc"
    # )

    res = []
    pk_keys = self.get_schema_pk_constraints(connection, schema)
    res = pk_keys.get(table_name, [])

    index_columns = list()
    index_name = None

    for index_column in res:
        index_columns.append(self.normalize_name(index_column.ColumnName))
        index_name = self.normalize_name(
            index_column.IndexName
        )  # There should be just one IndexName

        # Update counter if available
        if hasattr(self, "report"):
            self.report.num_primary_keys_processed += 1

    return {"constrained_columns": index_columns, "name": index_name}


def optimized_get_columns(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    tables_cache: Optional[MutableMapping[str, List[TeradataTable]]] = None,
    use_qvci: bool = False,
    **kw: Dict[str, Any],
) -> List[Dict]:
    tables_cache = tables_cache or {}
    if schema is None:
        schema = self.default_schema_name

    # Using 'help schema.table.*' statements has been considered.
    # The DBC.ColumnsV provides the default value which is not available
    # with the 'help column' commands result.

    td_table: Optional[TeradataTable] = None
    # Check if the object is a view
    for t in tables_cache[schema]:
        if t.name == table_name:
            td_table = t
            break

    if td_table is None:
        logger.warning(
            f"Table {table_name} not found in cache for schema {schema}, not getting columns"
        )
        return []

    res = []
    if td_table.object_type == "View" and not use_qvci:
        # Volatile table definition is not stored in the dictionary.
        # We use the 'help schema.table.*' command instead to get information for all columns.
        # We have to do the same for views since we need the type information
        # which is not available in dbc.ColumnsV.
        res = self._get_column_help(connection, schema, table_name, column_name=None)

        # If this is a view, get types for individual columns (dbc.ColumnsV won't have types for view columns).
        # For a view or a volatile table, we have to set the default values as the 'help' command does not have it.
        col_info_list = []
        for r in res:
            updated_column_info_dict = self._update_column_help_info(r._mapping)
            col_info_list.append(dict(r._mapping, **(updated_column_info_dict)))
        res = col_info_list
    else:
        # Default value for 'usexviews' is False so use dbc.ColumnsV by default
        dbc_columns = "columnsQV" if use_qvci else "columnsV"
        dbc_columns = dbc_columns + "X" if configure.usexviews else dbc_columns
        res = self.get_schema_columns(connection, dbc_columns, schema).get(
            table_name, []
        )

    start_time = time.time()

    final_column_info = []
    # Don't care about ART tables now
    # Ignore the non-functional column in a PTI table
    for row in res:
        try:
            col_info = self._get_column_info(row)

            # Add CommentString as comment field for column description
            if hasattr(row, "CommentString") and row.CommentString:
                col_info["comment"] = row.CommentString.strip()
            elif (
                isinstance(row, dict)
                and "CommentString" in row
                and row["CommentString"]
            ):
                col_info["comment"] = row["CommentString"].strip()

            if "TSColumnType" in col_info and col_info["TSColumnType"] is not None:
                if (
                    col_info["ColumnName"] == "TD_TIMEBUCKET"
                    and col_info["TSColumnType"].strip() == "TB"
                ):
                    continue
            final_column_info.append(col_info)

            # Update counter - access report through self from the connection context
            if hasattr(self, "report"):
                self.report.num_columns_processed += 1

        except Exception as e:
            logger.error(
                f"Failed to process column {getattr(row, 'ColumnName', 'unknown')}: {e}"
            )
            if hasattr(self, "report"):
                self.report.num_column_extraction_failures += 1
            continue

    # Update timing
    if hasattr(self, "report"):
        end_time = time.time()
        self.report.column_extraction_duration_seconds += end_time - start_time

    return final_column_info


# Cache size of 1 is sufficient since schemas are processed sequentially
# Note: This cache is per-process and helps when processing multiple tables in the same schema
@lru_cache(maxsize=1)
def get_schema_foreign_keys(
    self: Any, connection: Connection, schema: str
) -> Dict[str, List[Any]]:
    dbc_child_parent_table = (
        "All_RI_ChildrenV" + "X" if configure.usexviews else "All_RI_ChildrenV"
    )
    foreign_keys: Dict[str, List[Any]] = {}
    stmt = f"""
    SELECT dbc."All_RI_ChildrenV"."ChildDB",  dbc."All_RI_ChildrenV"."ChildTable", dbc."All_RI_ChildrenV"."IndexID", dbc."{dbc_child_parent_table}"."IndexName", dbc."{dbc_child_parent_table}"."ChildKeyColumn", dbc."{dbc_child_parent_table}"."ParentDB", dbc."{dbc_child_parent_table}"."ParentTable", dbc."{dbc_child_parent_table}"."ParentKeyColumn"
        FROM dbc."{dbc_child_parent_table}"
    WHERE ChildDB = '{schema}' ORDER BY "IndexID" ASC
    """
    rows = connection.execute(text(stmt)).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.ChildTable not in foreign_keys:
            foreign_keys[row_mapping.ChildTable] = []

        foreign_keys[row_mapping.ChildTable].append(row_mapping)

    return foreign_keys


def optimized_get_foreign_keys(self, connection, table_name, schema=None, **kw):
    """
    Overrides base class method
    """

    if schema is None:
        schema = self.default_schema_name
    # Default value for 'usexviews' is False so use DBC.All_RI_ChildrenV by default
    res = self.get_schema_foreign_keys(connection, schema).get(table_name, [])

    def grouper(fk_row):
        return {
            "name": fk_row.IndexName or fk_row.IndexID,  # ID if IndexName is None
            "schema": fk_row.ParentDB,
            "table": fk_row.ParentTable,
        }

    # TODO: Check if there's a better way
    fk_dicts = list()
    for constraint_info, constraint_cols in groupby_unsorted(res, grouper):
        fk_dict = {
            "name": str(constraint_info["name"]),
            "constrained_columns": list(),
            "referred_table": constraint_info["table"],
            "referred_schema": constraint_info["schema"],
            "referred_columns": list(),
        }

        for constraint_col in constraint_cols:
            fk_dict["constrained_columns"].append(
                self.normalize_name(constraint_col.ChildKeyColumn)
            )
            fk_dict["referred_columns"].append(
                self.normalize_name(constraint_col.ParentKeyColumn)
            )

        fk_dicts.append(fk_dict)

    return fk_dicts


def optimized_get_view_definition(
    self: Any,
    connection: Connection,
    view_name: str,
    schema: Optional[str] = None,
    tables_cache: Optional[MutableMapping[str, List[TeradataTable]]] = None,
    **kw: Dict[str, Any],
) -> Optional[str]:
    tables_cache = tables_cache or {}
    if schema is None:
        schema = self.default_schema_name

    if schema not in tables_cache:
        return None

    for table in tables_cache[schema]:
        if table.name == view_name:
            return self.normalize_name(table.request_text)

    return None


@dataclass
class TeradataReport(SQLSourceReport, BaseTimeWindowReport):
    # View processing metrics (actively used)
    num_views_processed: int = 0
    num_view_processing_failures: int = 0
    view_extraction_total_time_seconds: float = 0.0
    view_extraction_average_time_seconds: float = 0.0
    slowest_view_processing_time_seconds: float = 0.0
    slowest_view_name: TopKDict[str, float] = field(default_factory=TopKDict)

    # Connection pool performance metrics (actively used)
    connection_pool_wait_time_seconds: float = 0.0
    connection_pool_max_wait_time_seconds: float = 0.0

    # Database-level metrics similar to BigQuery's approach (actively used)
    num_database_tables_to_scan: TopKDict[str, int] = field(default_factory=TopKDict)
    num_database_views_to_scan: TopKDict[str, int] = field(default_factory=TopKDict)

    # Global metadata extraction timing (single query for all databases)
    metadata_extraction_total_sec: float = 0.0

    # Lineage extraction query time range (actively used)
    lineage_start_time: Optional[datetime] = None
    lineage_end_time: Optional[datetime] = None

    # Audit query processing statistics
    num_audit_query_entries_processed: int = 0


class BaseTeradataConfig(TwoTierSQLAlchemyConfig):
    scheme: str = Field(default="teradatasql", description="database scheme")


class TeradataConfig(BaseTeradataConfig, BaseTimeWindowConfig):
    databases: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of databases to ingest. If not specified, all databases will be ingested."
            " Even if this is specified, databases will still be filtered by `database_pattern`."
        ),
    )

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=EXCLUDED_DATABASES),
        description="Regex patterns for databases to filter in ingestion.",
    )
    include_table_lineage: bool = Field(
        default=False,
        description="Whether to include table lineage in the ingestion. "
        "This requires to have the table lineage feature enabled.",
    )

    include_view_lineage: bool = Field(
        default=True,
        description="Whether to include view lineage in the ingestion. "
        "This requires to have the view lineage feature enabled.",
    )

    include_queries: bool = Field(
        default=True,
        description="Whether to generate query entities for SQL queries. "
        "Query entities provide metadata about individual SQL queries including "
        "execution timestamps, user information, and query text.",
    )
    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    default_db: Optional[str] = Field(
        default=None,
        description="The default database to use for unqualified table names",
    )

    include_usage_statistics: bool = Field(
        default=False,
        description="Generate usage statistic.",
    )

    use_file_backed_cache: bool = Field(
        default=True,
        description="Whether to use a file backed cache for the view definitions.",
    )

    use_qvci: bool = Field(
        default=False,
        description="Whether to use QVCI to get column information. This is faster but requires to have QVCI enabled.",
    )

    include_historical_lineage: bool = Field(
        default=False,
        description="Whether to include historical lineage data from PDCRINFO.DBQLSqlTbl_Hst in addition to current DBC.QryLogV data. "
        "This provides access to historical query logs that may have been archived. "
        "The historical table existence is checked automatically and gracefully falls back to current data only if not available.",
    )

    use_server_side_cursors: bool = Field(
        default=True,
        description="Enable server-side cursors for large result sets using SQLAlchemy's stream_results. "
        "This reduces memory usage by streaming results from the database server. "
        "Automatically falls back to client-side batching if server-side cursors are not supported.",
    )

    max_workers: int = Field(
        default=10,
        description="Maximum number of worker threads to use for parallel processing. "
        "Controls the level of concurrency for operations like view processing.",
    )

    extract_ownership: bool = Field(
        default=False,
        description=(
            "Whether to extract ownership information for tables and views based on their creator. "
            "When enabled, the table/view creator from Teradata's system tables "
            "will be added as an owner with DATAOWNER type. "
            "Ownership is applied using OVERWRITE mode, meaning any existing ownership "
            "information (including manually added or modified owners from the UI) "
            "will be replaced. Use with caution."
        ),
    )


@platform_name("Teradata")
@config_class(TeradataConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.DATABASE,
    ],
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on",
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.OWNERSHIP,
    "Optionally enabled via configuration (extract_ownership)",
)
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_FINE, "Optionally enabled via configuration")
@capability(SourceCapability.USAGE_STATS, "Optionally enabled via configuration")
class TeradataSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    """

    config: TeradataConfig

    QUERY_TEXT_CURRENT_QUERIES: str = """
    SELECT
        s.QueryID as "query_id",
        UserName as "user",
        StartTime AT TIME ZONE 'GMT' as "timestamp",
        DefaultDatabase as default_database,
        s.SqlTextInfo as "query_text",
        s.SqlRowNo as "row_no"
    FROM "DBC".QryLogV as l
    JOIN "DBC".QryLogSqlV as s on s.QueryID = l.QueryID
    WHERE
        l.ErrorCode = 0
        AND l.statementtype not in (
        'Unrecognized type',
        'Create Database/User',
        'Help',
        'Modify Database',
        'Drop Table',
        'Show',
        'Not Applicable',
        'Grant',
        'Abort',
        'Database',
        'Flush Query Logging',
        'Null',
        'Begin/End DBQL',
        'Revoke'
    )
        and "timestamp" >= TIMESTAMP '{start_time}'
        and "timestamp" < TIMESTAMP '{end_time}'
        and s.CollectTimeStamp >= TIMESTAMP '{start_time}'
        and default_database not in ('DEMONOW_MONITOR')
        {databases_filter}
    ORDER BY "timestamp", "query_id", "row_no"
    """.strip()

    QUERY_TEXT_HISTORICAL_UNION: str = """
    SELECT
        "query_id",
        "user",
        "timestamp",
        default_database,
        "query_text",
        "row_no"
    FROM (
        SELECT
            h.QueryID as "query_id",
            h.UserName as "user",
            h.StartTime AT TIME ZONE 'GMT' as "timestamp",
            h.DefaultDatabase as default_database,
            h.SqlTextInfo as "query_text",
            h.SqlRowNo as "row_no"
        FROM "PDCRINFO".DBQLSqlTbl_Hst as h
        WHERE
            h.ErrorCode = 0
            AND h.statementtype not in (
            'Unrecognized type',
            'Create Database/User',
            'Help',
            'Modify Database',
            'Drop Table',
            'Show',
            'Not Applicable',
            'Grant',
            'Abort',
            'Database',
            'Flush Query Logging',
            'Null',
            'Begin/End DBQL',
            'Revoke'
        )
            and h.StartTime AT TIME ZONE 'GMT' >= TIMESTAMP '{start_time}'
            and h.StartTime AT TIME ZONE 'GMT' < TIMESTAMP '{end_time}'
            and h.CollectTimeStamp >= TIMESTAMP '{start_time}'
            and h.DefaultDatabase not in ('DEMONOW_MONITOR')
            {databases_filter_history}

        UNION

        SELECT
            s.QueryID as "query_id",
            l.UserName as "user",
            l.StartTime AT TIME ZONE 'GMT' as "timestamp",
            l.DefaultDatabase as default_database,
            s.SqlTextInfo as "query_text",
            s.SqlRowNo as "row_no"
        FROM "DBC".QryLogV as l
        JOIN "DBC".QryLogSqlV as s on s.QueryID = l.QueryID
        WHERE
            l.ErrorCode = 0
            AND l.statementtype not in (
            'Unrecognized type',
            'Create Database/User',
            'Help',
            'Modify Database',
            'Drop Table',
            'Show',
            'Not Applicable',
            'Grant',
            'Abort',
            'Database',
            'Flush Query Logging',
            'Null',
            'Begin/End DBQL',
            'Revoke'
        )
            and l.StartTime AT TIME ZONE 'GMT' >= TIMESTAMP '{start_time}'
            and l.StartTime AT TIME ZONE 'GMT' < TIMESTAMP '{end_time}'
            and s.CollectTimeStamp >= TIMESTAMP '{start_time}'
            and l.DefaultDatabase not in ('DEMONOW_MONITOR')
            {databases_filter}
    ) as combined_results
    ORDER BY "timestamp", "query_id", "row_no"
    """.strip()

    TABLES_AND_VIEWS_QUERY: str = f"""
SELECT
    t.DataBaseName,
    t.TableName as name,
    t.CommentString as description,
    CASE t.TableKind
         WHEN 'I' THEN 'Join index'
         WHEN 'N' THEN 'Hash index'
         WHEN 'T' THEN 'Table'
         WHEN 'V' THEN 'View'
         WHEN 'O' THEN 'NoPI Table'
         WHEN 'Q' THEN 'Queue table'
    END AS object_type,
    t.CreateTimeStamp,
    t.LastAlterName,
    t.LastAlterTimeStamp,
    t.RequestText,
    t.CreatorName  -- User who created the table/view, used for ownership extraction
FROM dbc.TablesV t
WHERE DataBaseName NOT IN ({",".join([f"'{db}'" for db in EXCLUDED_DATABASES])})
AND t.TableKind in ('T', 'V', 'Q', 'O')
ORDER by DataBaseName, TableName;
     """.strip()

    _tables_cache: MutableMapping[str, List[TeradataTable]] = defaultdict(list)
    # Cache mapping (schema, entity_name) -> creator_name for table/view ownership
    _table_creator_cache: MutableMapping[Tuple[str, str], str] = {}
    _tables_cache_lock = Lock()  # Protect shared cache from concurrent access
    _pooled_engine: Optional[Engine] = None  # Reusable pooled engine
    _pooled_engine_lock = Lock()  # Protect engine creation

    def __init__(self, config: TeradataConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "teradata")

        self.report: TeradataReport = TeradataReport()
        self.graph: Optional[DataHubGraph] = ctx.graph
        self._report_lock = Lock()  # Thread safety for report counters

        self.schema_resolver = self._init_schema_resolver()

        # Initialize SqlParsingAggregator for modern lineage processing
        logger.info("Initializing SqlParsingAggregator for enhanced lineage processing")
        self.aggregator = SqlParsingAggregator(
            platform="teradata",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=self.schema_resolver,
            graph=self.ctx.graph,
            generate_lineage=self.config.include_view_lineage
            or self.config.include_table_lineage,
            generate_queries=self.config.include_queries,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_query_usage_statistics=self.config.include_usage_statistics,
            generate_operations=self.config.usage.include_operational_stats
            if self.config.include_usage_statistics
            else False,
            usage_config=self.config.usage
            if self.config.include_usage_statistics
            else None,
            eager_graph_load=False,
        )
        self.report.sql_aggregator = self.aggregator.report

        if self.config.include_tables or self.config.include_views:
            with self.report.new_stage("Table and view discovery"):
                self.cache_tables_and_views()
                logger.info(f"Found {len(self._tables_cache)} tables and views")
            setattr(self, "loop_tables", self.cached_loop_tables)  # noqa: B010
            setattr(self, "loop_views", self.cached_loop_views)  # noqa: B010
            setattr(  # noqa: B010
                self, "get_table_properties", self.cached_get_table_properties
            )

            tables_cache = self._tables_cache
            setattr(  # noqa: B010
                TeradataDialect,
                "get_columns",
                lambda self,
                connection,
                table_name,
                schema=None,
                use_qvci=self.config.use_qvci,
                **kw: optimized_get_columns(
                    self,
                    connection,
                    table_name,
                    schema,
                    tables_cache=tables_cache,
                    use_qvci=use_qvci,
                    **kw,
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_pk_constraint",
                lambda self,
                connection,
                table_name,
                schema=None,
                **kw: optimized_get_pk_constraint(
                    self, connection, table_name, schema, **kw
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_foreign_keys",
                lambda self,
                connection,
                table_name,
                schema=None,
                **kw: optimized_get_foreign_keys(
                    self, connection, table_name, schema, **kw
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_schema_columns",
                lambda self, connection, dbc_columns, schema: get_schema_columns(
                    self, connection, dbc_columns, schema
                ),
            )

            # Disabling the below because the cached view definition is not the view definition the column in tablesv actually holds the last statement executed against the object... not necessarily the view definition
            # setattr(
            #   TeradataDialect,
            #    "get_view_definition",
            #   lambda self, connection, view_name, schema=None, **kw: optimized_get_view_definition(
            #        self, connection, view_name, schema, tables_cache=tables_cache, **kw
            #    ),
            # )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_schema_pk_constraints",
                lambda self, connection, schema: get_schema_pk_constraints(
                    self, connection, schema
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_schema_foreign_keys",
                lambda self, connection, schema: get_schema_foreign_keys(
                    self, connection, schema
                ),
            )
        else:
            logger.info(
                "Disabling stale entity removal as tables and views are disabled"
            )
            if self.config.stateful_ingestion:
                self.config.stateful_ingestion.remove_stale_metadata = False

    def _add_default_options(self, sql_config: SQLCommonConfig) -> None:
        """Add Teradata-specific default options"""
        super()._add_default_options(sql_config)
        if sql_config.is_profiling_enabled():
            # Sqlalchemy uses QueuePool by default however Teradata uses SingletonThreadPool.
            # SingletonThreadPool does not support parellel connections. For using profiling, we need to use QueuePool.
            # https://docs.sqlalchemy.org/en/20/core/pooling.html#connection-pool-configuration
            # https://github.com/Teradata/sqlalchemy-teradata/issues/96
            sql_config.options.setdefault("poolclass", QueuePool)

    @classmethod
    def create(cls, config_dict, ctx):
        config = TeradataConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _init_schema_resolver(self) -> SchemaResolver:
        if not self.config.include_tables or not self.config.include_views:
            if self.ctx.graph:
                return self.ctx.graph.initialize_schema_resolver_from_datahub(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
            else:
                logger.warning(
                    "Failed to load schema info from DataHub as DataHubGraph is missing.",
                )
        return SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def get_inspectors(self):
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.
        url = self.config.get_sql_alchemy_url()

        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        # Get list of databases first
        with engine.connect() as conn:
            inspector = inspect(conn)
            if self.config.database and self.config.database != "":
                databases = [self.config.database]
            elif self.config.databases:
                databases = self.config.databases
            else:
                databases = inspector.get_schema_names()

        # Create separate connections for each database to avoid connection lifecycle issues
        for db in databases:
            if self.config.database_pattern.allowed(db):
                with engine.connect() as conn:
                    db_inspector = inspect(conn)
                    db_inspector._datahub_database = db
                    yield db_inspector

    def get_db_name(self, inspector: Inspector) -> str:
        if hasattr(inspector, "_datahub_database"):
            return inspector._datahub_database

        engine = inspector.engine

        if engine and hasattr(engine, "url") and hasattr(engine.url, "database"):
            return str(engine.url.database).strip('"')
        else:
            raise Exception("Unable to get database name from Sqlalchemy inspector")

    def cached_loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[MetadataWorkUnit]:
        setattr(  # noqa: B010
            inspector,
            "get_table_names",
            lambda schema: [
                i.name
                for i in filter(
                    lambda t: t.object_type != "View",
                    self._tables_cache.get(schema, []),
                )
            ],
        )
        yield from super().loop_tables(inspector, schema, sql_config)

    def cached_get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        cache_entries = self._tables_cache.get(schema, [])
        for entry in cache_entries:
            if entry.name == table:
                description = entry.description
                if entry.object_type == "View" and entry.request_text:
                    properties["view_definition"] = entry.request_text
                break
        return description, properties, location

    def _get_creator_for_entity(self, schema: str, entity_name: str) -> Optional[str]:
        """Get creator name for a table or view."""
        with self._tables_cache_lock:
            return self._table_creator_cache.get((schema, entity_name))

    def _emit_ownership_if_available(
        self,
        dataset_name: str,
        schema: str,
        entity_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit ownership metadata for a dataset if creator information is available."""
        if not self.config.extract_ownership:
            return

        creator_name = self._get_creator_for_entity(schema, entity_name)
        if creator_name:
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            owner_urn = make_user_urn(creator_name)
            yield from add_owner_to_entity_wu(
                entity_type="dataset",
                entity_urn=dataset_urn,
                owner_urn=owner_urn,
            )

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader] = None,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """Override to add ownership metadata based on table creator."""
        yield from self._emit_ownership_if_available(dataset_name, schema, table)
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config, data_reader
        )

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """Override to add ownership metadata based on view creator."""
        yield from self._emit_ownership_if_available(dataset_name, schema, view)
        yield from super()._process_view(
            dataset_name, inspector, schema, view, sql_config
        )

    def cached_loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[MetadataWorkUnit]:
        start_time = time.time()

        # Get view names from cache
        view_names = [
            i.name
            for i in filter(
                lambda t: t.object_type == "View", self._tables_cache.get(schema, [])
            )
        ]
        actual_view_count = len(view_names)

        if actual_view_count == 0:
            end_time = time.time()
            processing_time = end_time - start_time
            logger.info(
                f"View processing for schema '{schema}' completed in {processing_time:.2f} seconds (0 views, 0 work units)"
            )
            return

        # Use custom threading implementation with connection pooling
        work_unit_count = 0

        for work_unit in self._loop_views_with_connection_pool(
            view_names, schema, sql_config
        ):
            work_unit_count += 1
            yield work_unit

        end_time = time.time()
        processing_time = end_time - start_time

        logger.info(
            f"View processing for schema '{schema}' completed in {processing_time:.2f} seconds ({actual_view_count} views, {work_unit_count} work units)"
        )

        # Update report timing metrics
        if hasattr(self, "report"):
            self.report.view_extraction_total_time_seconds += processing_time
            self.report.num_views_processed += actual_view_count

            # Track slowest view processing at view level (will be updated by individual view processing)
            # Note: slowest_view_name now tracks individual views, not schemas

            # Calculate average processing time per view
            if self.report.num_views_processed > 0:
                self.report.view_extraction_average_time_seconds = (
                    self.report.view_extraction_total_time_seconds
                    / self.report.num_views_processed
                )

    def _loop_views_with_connection_pool(
        self, view_names: List[str], schema: str, sql_config: SQLCommonConfig
    ) -> Iterable[Union[MetadataWorkUnit, Any]]:
        """
        Process views using individual database connections per thread for true parallelization.

        Each thread gets its own connection from a QueuePool, enabling true concurrent processing.
        """
        if self.config.max_workers == 1:
            # Single-threaded processing - no need for complexity
            yield from self._process_views_single_threaded(
                view_names, schema, sql_config
            )
            return

        logger.info(
            f"Processing {len(view_names)} views with {self.config.max_workers} worker threads"
        )

        # Get or create reusable pooled engine
        engine = self._get_or_create_pooled_engine()

        try:
            # Thread-safe result collection
            report_lock = Lock()

            def process_single_view(
                view_name: str,
            ) -> List[Union[MetadataWorkUnit, Any]]:
                """Process a single view with its own database connection."""
                results: List[Union[MetadataWorkUnit, Any]] = []

                # Detailed timing measurements for bottleneck analysis
                timings = {
                    "connection_acquire": 0.0,
                    "view_processing": 0.0,
                    "work_unit_generation": 0.0,
                    "total": 0.0,
                }

                total_start = time.time()
                try:
                    # Measure connection acquisition time
                    conn_start = time.time()
                    with engine.connect() as conn:
                        timings["connection_acquire"] = time.time() - conn_start

                        # Update connection pool metrics
                        with report_lock:
                            pool_wait_time = timings["connection_acquire"]
                            self.report.connection_pool_wait_time_seconds += (
                                pool_wait_time
                            )
                            if (
                                pool_wait_time
                                > self.report.connection_pool_max_wait_time_seconds
                            ):
                                self.report.connection_pool_max_wait_time_seconds = (
                                    pool_wait_time
                                )

                        # Measure view processing setup
                        processing_start = time.time()
                        thread_inspector = inspect(conn)
                        # Inherit database information for Teradata two-tier architecture
                        thread_inspector._datahub_database = schema  # type: ignore

                        dataset_name = self.get_identifier(
                            schema=schema, entity=view_name, inspector=thread_inspector
                        )

                        # Thread-safe reporting
                        with report_lock:
                            self.report.report_entity_scanned(
                                dataset_name, ent_type="view"
                            )

                        if not sql_config.view_pattern.allowed(dataset_name):
                            with report_lock:
                                self.report.report_dropped(dataset_name)
                            return results

                        timings["view_processing"] = time.time() - processing_start

                        # Measure work unit generation
                        wu_start = time.time()
                        for work_unit in self._process_view(
                            dataset_name=dataset_name,
                            inspector=thread_inspector,
                            schema=schema,
                            view=view_name,
                            sql_config=sql_config,
                        ):
                            results.append(work_unit)
                        timings["work_unit_generation"] = time.time() - wu_start

                    # Track individual view timing
                    timings["total"] = time.time() - total_start

                    with report_lock:
                        self.report.slowest_view_name[f"{schema}.{view_name}"] = (
                            timings["total"]
                        )

                except Exception as e:
                    with report_lock:
                        self.report.num_view_processing_failures += 1
                        # Log full exception details for debugging
                        import traceback

                        full_traceback = traceback.format_exc()
                        logger.error(
                            f"Failed to process view {schema}.{view_name}: {str(e)}"
                        )
                        logger.error(f"Full traceback: {full_traceback}")
                        self.report.warning(
                            f"Error processing view {schema}.{view_name}",
                            context=f"View: {schema}.{view_name}, Error: {str(e)}",
                            exc=e,
                        )

                return results

            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                # Submit all view processing tasks
                future_to_view = {
                    executor.submit(process_single_view, view_name): view_name
                    for view_name in view_names
                }

                # Process completed tasks as they finish
                for future in as_completed(future_to_view):
                    view_name = future_to_view[future]
                    try:
                        results = future.result()
                        # Yield all results from this view
                        for result in results:
                            yield result
                    except Exception as e:
                        with report_lock:
                            self.report.warning(
                                "Error in thread processing view",
                                context=f"{schema}.{view_name}",
                                exc=e,
                            )

        finally:
            # Don't dispose the reusable engine here - it will be cleaned up in close()
            pass

    def _process_views_single_threaded(
        self, view_names: List[str], schema: str, sql_config: SQLCommonConfig
    ) -> Iterable[Union[MetadataWorkUnit, Any]]:
        """Process views sequentially with a single connection."""
        engine = self.get_metadata_engine()

        try:
            with engine.connect() as conn:
                inspector = inspect(conn)

                for view_name in view_names:
                    view_start_time = time.time()
                    try:
                        dataset_name = self.get_identifier(
                            schema=schema, entity=view_name, inspector=inspector
                        )

                        self.report.report_entity_scanned(dataset_name, ent_type="view")

                        if not sql_config.view_pattern.allowed(dataset_name):
                            self.report.report_dropped(dataset_name)
                            continue

                        # Process the view and yield results
                        for work_unit in self._process_view(
                            dataset_name=dataset_name,
                            inspector=inspector,
                            schema=schema,
                            view=view_name,
                            sql_config=sql_config,
                        ):
                            yield work_unit

                        # Track individual view timing
                        view_end_time = time.time()
                        view_processing_time = view_end_time - view_start_time
                        self.report.slowest_view_name[f"{schema}.{view_name}"] = (
                            view_processing_time
                        )

                    except Exception as e:
                        # Log full exception details for debugging
                        import traceback

                        full_traceback = traceback.format_exc()
                        logger.error(
                            f"Failed to process view {schema}.{view_name}: {str(e)}"
                        )
                        logger.error(f"Full traceback: {full_traceback}")
                        self.report.warning(
                            f"Error processing view {schema}.{view_name}",
                            context=f"View: {schema}.{view_name}, Error: {str(e)}",
                            exc=e,
                        )

        finally:
            engine.dispose()

    def _get_or_create_pooled_engine(self) -> Engine:
        """Get or create a reusable SQLAlchemy engine with QueuePool for concurrent connections."""
        with self._pooled_engine_lock:
            if self._pooled_engine is None:
                url = self.config.get_sql_alchemy_url()

                # Optimal connection pool sizing to match max_workers exactly
                # Teradata driver can be sensitive to high connection counts, so cap at reasonable limit
                max_safe_connections = (
                    13  # Conservative limit: 8 base + 5 overflow for Teradata stability
                )

                # Adjust max_workers to match available connection pool capacity
                effective_max_workers = min(
                    self.config.max_workers, max_safe_connections
                )

                # Set pool size to match effective workers for optimal performance
                base_connections = min(
                    effective_max_workers, 8
                )  # Reasonable base connections
                max_overflow = (
                    effective_max_workers - base_connections
                )  # Remaining as overflow

                # Log adjustment if max_workers was reduced
                if effective_max_workers < self.config.max_workers:
                    logger.warning(
                        f"Reduced max_workers from {self.config.max_workers} to {effective_max_workers} to match Teradata connection pool capacity"
                    )

                # Update the config to reflect the effective value used
                self.config.max_workers = effective_max_workers

                pool_options = {
                    **self.config.options,
                    "poolclass": QueuePool,
                    "pool_size": base_connections,
                    "max_overflow": max_overflow,
                    "pool_pre_ping": True,  # Validate connections
                    "pool_recycle": 1800,  # Recycle connections after 30 mins (more frequent)
                    "pool_timeout": 60,  # Longer timeout for connection acquisition
                    "pool_reset_on_return": "rollback",  # Explicit rollback on connection return
                }

                # Add Teradata-specific connection options for stability
                if "connect_args" not in pool_options:
                    pool_options["connect_args"] = {}

                # Teradata-specific connection arguments for better stability
                pool_options["connect_args"].update(
                    {
                        "connect_timeout": "30000",  # Connection timeout in ms (30 seconds)
                        "request_timeout": "120000",  # Request timeout in ms (2 minutes)
                    }
                )

                self._pooled_engine = create_engine(url, **pool_options)
                logger.info(
                    f"Created optimized Teradata connection pool: {base_connections} base + {max_overflow} overflow = {base_connections + max_overflow} max connections (matching {effective_max_workers} workers)"
                )

            return self._pooled_engine

    def cache_tables_and_views(self) -> None:
        with self.report.new_stage("Cache tables and views"):
            engine = self.get_metadata_engine()
            try:
                database_counts: Dict[str, Dict[str, int]] = defaultdict(
                    lambda: {"tables": 0, "views": 0}
                )

                for entry in engine.execute(self.TABLES_AND_VIEWS_QUERY):
                    table = TeradataTable(
                        database=entry.DataBaseName.strip(),
                        name=entry.name.strip(),
                        description=entry.description.strip()
                        if entry.description
                        else None,
                        object_type=entry.object_type,
                        create_timestamp=entry.CreateTimeStamp,
                        last_alter_name=entry.LastAlterName,
                        last_alter_timestamp=entry.LastAlterTimeStamp,
                        request_text=(
                            entry.RequestText.strip()
                            if entry.object_type == "View" and entry.RequestText
                            else None
                        ),
                    )

                    # Count objects per database for metrics
                    if table.object_type == "View":
                        database_counts[table.database]["views"] += 1
                    else:
                        database_counts[table.database]["tables"] += 1

                    with self._tables_cache_lock:
                        self._tables_cache[table.database].append(table)
                        creator_name = (entry.CreatorName or "").strip()
                        if creator_name:
                            self._table_creator_cache[(table.database, table.name)] = (
                                creator_name
                            )

                for database, counts in database_counts.items():
                    self.report.num_database_tables_to_scan[database] = counts["tables"]
                    self.report.num_database_views_to_scan[database] = counts["views"]

            finally:
                engine.dispose()

    def _reconstruct_queries_streaming(
        self, entries: Iterable[Any]
    ) -> Iterable[ObservedQuery]:
        """Reconstruct complete queries from database entries in streaming fashion.

        This method processes entries in order and reconstructs multi-row queries
        by concatenating rows with the same query_id.
        """
        current_query_id = None
        current_query_parts = []
        current_query_metadata = None

        for entry in entries:
            # Count each audit query entry processed
            self.report.num_audit_query_entries_processed += 1

            query_id = getattr(entry, "query_id", None)
            query_text = str(getattr(entry, "query_text", ""))

            if query_id != current_query_id:
                # New query started - yield the previous one if it exists
                if current_query_id is not None and current_query_parts:
                    yield self._create_observed_query_from_parts(
                        current_query_parts, current_query_metadata
                    )

                # Start new query
                current_query_id = query_id
                current_query_parts = [query_text] if query_text else []
                current_query_metadata = entry
            else:
                # Same query - append the text
                if query_text:
                    current_query_parts.append(query_text)

        # Yield the last query if it exists
        if current_query_id is not None and current_query_parts:
            yield self._create_observed_query_from_parts(
                current_query_parts, current_query_metadata
            )

    def _create_observed_query_from_parts(
        self, query_parts: List[str], metadata_entry: Any
    ) -> ObservedQuery:
        """Create ObservedQuery from reconstructed query parts and metadata."""
        # Join all parts to form the complete query
        # Teradata fragments are split at fixed lengths without artificial breaks
        full_query_text = "".join(query_parts)

        # Extract metadata
        session_id = getattr(metadata_entry, "session_id", None)
        timestamp = getattr(metadata_entry, "timestamp", None)
        user = getattr(metadata_entry, "user", None)
        default_database = getattr(metadata_entry, "default_database", None)

        # Apply Teradata-specific query transformations
        cleaned_query = NOT_CASESPECIFIC_PATTERN.sub("", full_query_text)

        # For Teradata's two-tier architecture (database.table), we should not set default_db
        # to avoid incorrect URN generation like "dbc.database.table" instead of "database.table"
        # The SQL parser will treat database.table references correctly without default_db
        return ObservedQuery(
            query=cleaned_query,
            session_id=session_id,
            timestamp=timestamp,
            user=CorpUserUrn(user) if user else None,
            default_db=DEFAULT_NO_DATABASE_TERADATA,  # Teradata uses two-tier database.table naming without default database prefixing
            default_schema=default_database,
        )

    def _convert_entry_to_observed_query(self, entry: Any) -> ObservedQuery:
        """Convert database query entry to ObservedQuery for SqlParsingAggregator.

        DEPRECATED: This method is deprecated in favor of _reconstruct_queries_streaming
        which properly handles multi-row queries. This method does not handle queries
        that span multiple rows correctly and should not be used.
        """
        # Extract fields from database result
        query_text = str(entry.query_text).strip()
        session_id = getattr(entry, "session_id", None)
        timestamp = getattr(entry, "timestamp", None)
        user = getattr(entry, "user", None)
        default_database = getattr(entry, "default_database", None)

        # Apply Teradata-specific query transformations
        cleaned_query = NOT_CASESPECIFIC_PATTERN.sub("", query_text)

        # For Teradata's two-tier architecture (database.table), we should not set default_db
        # to avoid incorrect URN generation like "dbc.database.table" instead of "database.table"
        # However, we should set default_schema for unqualified table references
        return ObservedQuery(
            query=cleaned_query,
            session_id=session_id,
            timestamp=timestamp,
            user=CorpUserUrn(user) if user else None,
            default_db=DEFAULT_NO_DATABASE_TERADATA,  # Teradata uses two-tier database.table naming without default database prefixing
            default_schema=default_database,  # Set default_schema for unqualified table references
        )

    def _fetch_lineage_entries_chunked(self) -> Iterable[Any]:
        """Fetch lineage entries using server-side cursor to handle large result sets efficiently."""
        queries = self._make_lineage_queries()

        fetch_engine = self.get_metadata_engine()
        try:
            with fetch_engine.connect() as conn:
                cursor_type = (
                    "server-side"
                    if self.config.use_server_side_cursors
                    else "client-side"
                )

                total_count_all_queries = 0

                for query_index, query in enumerate(queries, 1):
                    logger.info(
                        f"Executing lineage query {query_index}/{len(queries)} for time range {self.config.start_time} to {self.config.end_time} with {cursor_type} cursor..."
                    )

                    # Use helper method to try server-side cursor with fallback
                    result = self._execute_with_cursor_fallback(conn, query)

                    # Stream results in batches to avoid memory issues
                    batch_size = 5000
                    batch_count = 0
                    query_total_count = 0

                    while True:
                        # Fetch a batch of rows
                        batch = result.fetchmany(batch_size)
                        if not batch:
                            break

                        batch_count += 1
                        query_total_count += len(batch)
                        total_count_all_queries += len(batch)

                        logger.info(
                            f"Query {query_index} - Fetched batch {batch_count}: {len(batch)} lineage entries (query total: {query_total_count})"
                        )
                        yield from batch

                    logger.info(
                        f"Completed query {query_index}: {query_total_count} lineage entries in {batch_count} batches"
                    )

                logger.info(
                    f"Completed fetching all queries: {total_count_all_queries} total lineage entries from {len(queries)} queries"
                )

        except Exception as e:
            logger.error(f"Error fetching lineage entries: {e}")
            raise
        finally:
            fetch_engine.dispose()

    def _check_historical_table_exists(self) -> bool:
        """
        Check if the PDCRINFO.DBQLSqlTbl_Hst table exists and is accessible.
        DBQL rows are periodically moved to history table and audit queries might not exist in DBC already.
        There is not guarantee that the historical table exists, so we need to check it.

        Returns:
            bool: True if the historical table exists and is accessible, False otherwise.
        """
        engine = self.get_metadata_engine()
        try:
            # Use a simple query to check if the table exists and is accessible
            check_query = """
                SELECT TOP 1 QueryID 
                FROM PDCRINFO.DBQLSqlTbl_Hst 
                WHERE 1=0
            """
            with engine.connect() as conn:
                conn.execute(text(check_query))
                logger.info(
                    "Historical lineage table PDCRINFO.DBQLSqlTbl_Hst is available"
                )
                return True
        except Exception as e:
            logger.info(
                f"Historical lineage table PDCRINFO.DBQLSqlTbl_Hst is not available: {e}"
            )
            return False
        finally:
            engine.dispose()

    def _make_lineage_queries(self) -> List[str]:
        databases_filter = (
            ""
            if not self.config.databases
            else "and l.DefaultDatabase in ({databases})".format(
                databases=",".join([f"'{db}'" for db in self.config.databases])
            )
        )

        queries = []

        # Check if historical lineage is configured and available
        if (
            self.config.include_historical_lineage
            and self._check_historical_table_exists()
        ):
            logger.info(
                "Using UNION query to combine historical and current lineage data to avoid duplicates"
            )
            # For historical query, we need the database filter for historical part
            databases_filter_history = (
                databases_filter.replace("l.DefaultDatabase", "h.DefaultDatabase")
                if databases_filter
                else ""
            )

            union_query = self.QUERY_TEXT_HISTORICAL_UNION.format(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                databases_filter=databases_filter,
                databases_filter_history=databases_filter_history,
            )
            queries.append(union_query)
        else:
            if self.config.include_historical_lineage:
                logger.warning(
                    "Historical lineage was requested but PDCRINFO.DBQLSqlTbl_Hst table is not available. Falling back to current data only."
                )

            # Use current-only query when historical data is not available
            current_query = self.QUERY_TEXT_CURRENT_QUERIES.format(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                databases_filter=databases_filter,
            )
            queries.append(current_query)

        return queries

    def get_metadata_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self.config.options)

    def _execute_with_cursor_fallback(
        self, connection: Connection, query: str, params: Optional[Dict] = None
    ) -> Any:
        """
        Execute query with server-side cursor if enabled and supported, otherwise fall back to regular execution.

        Args:
            connection: Database connection
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query result object
        """
        if self.config.use_server_side_cursors:
            try:
                # Try server-side cursor first
                if params:
                    result = connection.execution_options(stream_results=True).execute(
                        text(query), params
                    )
                else:
                    result = connection.execution_options(stream_results=True).execute(
                        text(query)
                    )

                logger.debug(
                    "Successfully using server-side cursor for query execution"
                )
                return result

            except Exception as e:
                logger.warning(
                    f"Server-side cursor failed, falling back to client-side execution: {e}"
                )
                # Fall through to regular execution

        # Regular execution (client-side)
        if params:
            return connection.execute(text(query), params)
        else:
            return connection.execute(text(query))

    def _generate_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Override to prevent parent class from generating aggregator work units during schema extraction.

        We handle aggregator generation manually after populating it with audit log data.
        """
        # Do nothing - we'll call the parent implementation manually after populating the aggregator
        return iter([])

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting Teradata metadata extraction")

        # Step 1: Schema extraction first (parent class will skip aggregator generation due to our override)
        with self.report.new_stage("Schema metadata extraction"):
            yield from super().get_workunits_internal()
            logger.info("Completed schema metadata extraction")

        # Step 2: Lineage extraction after schema extraction
        # This allows lineage processing to have access to all discovered schema information
        with self.report.new_stage("Audit log extraction and lineage processing"):
            self._populate_aggregator_from_audit_logs()
            # Call parent implementation directly to generate aggregator work units
            yield from super()._generate_aggregator_workunits()
            logger.info("Completed lineage processing")

    def _populate_aggregator_from_audit_logs(self) -> None:
        """SqlParsingAggregator-based lineage extraction with enhanced capabilities."""
        with self.report.new_stage("Lineage extraction from Teradata audit logs"):
            # Record the lineage query time range in the report
            self.report.lineage_start_time = self.config.start_time
            self.report.lineage_end_time = self.config.end_time

            logger.info(
                f"Starting lineage extraction from Teradata audit logs (time range: {self.config.start_time} to {self.config.end_time})"
            )

            if (
                self.config.include_table_lineage
                or self.config.include_usage_statistics
            ):
                # Step 1: Stream query entries from database with memory-efficient processing
                with self.report.new_stage("Fetching lineage entries from Audit Logs"):
                    queries_processed = 0

                    # Use streaming query reconstruction for memory efficiency
                    for observed_query in self._reconstruct_queries_streaming(
                        self._fetch_lineage_entries_chunked()
                    ):
                        self.aggregator.add(observed_query)

                        queries_processed += 1
                        if queries_processed % 10000 == 0:
                            logger.info(
                                f"Processed {queries_processed} queries to aggregator"
                            )

                    if queries_processed == 0:
                        logger.info("No lineage entries found")
                        return

                    logger.info(
                        f"Completed adding {queries_processed} queries to SqlParsingAggregator"
                    )

            logger.info("Completed lineage extraction from Teradata audit logs")

    def close(self) -> None:
        """Clean up resources when source is closed."""
        logger.info("Closing SqlParsingAggregator")
        self.aggregator.close()

        # Clean up pooled engine
        with self._pooled_engine_lock:
            if self._pooled_engine is not None:
                logger.info("Disposing pooled engine")
                self._pooled_engine.dispose()
                self._pooled_engine = None

        # Report failed views summary
        super().close()
