import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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
    Set,
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
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source_helpers import auto_lowercase_urns
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import SchemaMetadataClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage
from datahub.utilities.groupby import groupby_unsorted

logger: logging.Logger = logging.getLogger(__name__)

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
    columns: Dict[str, List[Any]] = {}
    columns_query = f"select * from dbc.{dbc_columns} where DatabaseName (NOT CASESPECIFIC) = '{schema}' (NOT CASESPECIFIC) order by TableName, ColumnId"
    rows = connection.execute(text(columns_query)).fetchall()
    for row in rows:
        row_mapping = row._mapping
        if row_mapping.TableName not in columns:
            columns[row_mapping.TableName] = []

        columns[row_mapping.TableName].append(row_mapping)

    return columns


# Cache size of 1 is sufficient since schemas are processed sequentially
# Note: This cache is per-process and helps when processing multiple tables in the same schema
@lru_cache(maxsize=1)
def get_schema_pk_constraints(
    self: Any, connection: Connection, schema: str
) -> Dict[str, List[Any]]:
    dbc_indices = "IndicesV" + "X" if configure.usexviews else "IndicesV"
    primary_keys: Dict[str, List[Any]] = {}
    stmt = f"select * from dbc.{dbc_indices} where DatabaseName (NOT CASESPECIFIC) = '{schema}' (NOT CASESPECIFIC) and IndexType = 'K' order by IndexNumber"
    rows = connection.execute(text(stmt)).fetchall()
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
class TeradataReport(SQLSourceReport, IngestionStageReport, BaseTimeWindowReport):
    num_queries_parsed: int = 0
    num_view_ddl_parsed: int = 0
    num_table_parse_failures: int = 0
    num_views_processed: int = 0
    num_view_processing_failures: int = 0


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

    database_pattern = Field(
        default=AllowDenyPattern(deny=EXCLUDED_DATABASES),
        description="Regex patterns for databases to filter in ingestion.",
    )
    include_table_lineage = Field(
        default=False,
        description="Whether to include table lineage in the ingestion. "
        "This requires to have the table lineage feature enabled.",
    )

    include_view_lineage = Field(
        default=True,
        description="Whether to include view lineage in the ingestion. "
        "This requires to have the view lineage feature enabled.",
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
        description="Whether to include historical lineage data from PDCRDATA.DBQLSqlTbl_Hst in addition to current DBC.QryLogV data. "
        "This provides access to historical query logs that may have been archived. "
        "The historical table existence is checked automatically and gracefully falls back to current data only if not available.",
    )

    lineage_processing_workers: int = Field(
        default=1,
        description="Number of worker threads to use for parallel SQL parsing during lineage processing. "
        "Uses single database query followed by parallel SQL parsing (no additional DB connections). "
        "Set to 1 for single-threaded processing. Higher values can significantly improve performance "
        "for large volumes of lineage queries without increasing database load.",
    )

    lineage_batch_size: int = Field(
        default=1000,
        description="Number of SQL statements to parse in each batch when using multi-threading. "
        "Only used when lineage_processing_workers > 1. "
        "Larger batches reduce threading overhead but may increase memory usage.",
    )

    view_processing_workers: int = Field(
        default=1,
        description="Number of worker threads to use for parallel view processing. "
        "Multi-threading is currently EXPERIMENTAL and may cause stability issues. "
        "Set to 1 (default) for stable single-threaded processing. "
        "Higher values can improve performance but may increase database load and cause crashes.",
    )

    view_batch_size: int = Field(
        default=50,
        description="Number of views to process in each batch when using multi-threading. "
        "Only used when view_processing_workers > 1. "
        "Smaller batches are recommended for view processing due to the overhead of help commands.",
    )

    enable_experimental_threading: bool = Field(
        default=False,
        description="Enable multi-threading features for lineage and view processing. "
        "Lineage processing uses optimized approach with single DB query + parallel SQL parsing. "
        "View processing may still have stability issues due to help command execution. "
        "Required when lineage_processing_workers > 1 or view_processing_workers > 1.",
    )


@platform_name("Teradata")
@config_class(TeradataConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Optionally enabled via configuration")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
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

    LINEAGE_QUERY_DATABASE_FILTER: str = """and default_database IN ({databases})"""

    LINEAGE_TIMESTAMP_BOUND_QUERY: str = """
    SELECT MIN(CollectTimeStamp) as "min_ts", MAX(CollectTimeStamp) as "max_ts" from DBC.QryLogV
    """.strip()

    QUERY_TEXT_QUERY: str = """
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
    ORDER BY "query_id", "row_no"
    """.strip()

    QUERY_TEXT_QUERY_WITH_HISTORY: str = """
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
    UNION ALL
    SELECT
        h.QueryID as "query_id",
        h.UserName as "user",
        h.StartTime AT TIME ZONE 'GMT' as "timestamp",
        h.DefaultDatabase as default_database,
        h.SqlTextInfo as "query_text",
        h.SqlRowNo as "row_no"
    FROM "PDCRDATA".DBQLSqlTbl_Hst as h
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
    ORDER BY "query_id", "row_no"
    """.strip()

    LINEAGE_TIMESTAMP_BOUND_QUERY_WITH_HISTORY: str = """
    SELECT 
        MIN("min_ts") as "min_ts", 
        MAX("max_ts") as "max_ts" 
    FROM (
        SELECT MIN(CollectTimeStamp) as "min_ts", MAX(CollectTimeStamp) as "max_ts" 
        FROM DBC.QryLogV
        UNION ALL
        SELECT MIN(CollectTimeStamp) as "min_ts", MAX(CollectTimeStamp) as "max_ts" 
        FROM PDCRDATA.DBQLSqlTbl_Hst
    ) combined_timestamps
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
    t.RequestText
FROM dbc.TablesV t
WHERE DataBaseName NOT IN ({",".join([f"'{db}'" for db in EXCLUDED_DATABASES])})
AND t.TableKind in ('T', 'V', 'Q', 'O')
ORDER by DataBaseName, TableName;
     """.strip()

    _tables_cache: MutableMapping[str, List[TeradataTable]] = defaultdict(list)

    def __init__(self, config: TeradataConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "teradata")

        self.report: TeradataReport = TeradataReport()
        self.graph: Optional[DataHubGraph] = ctx.graph
        self._report_lock = Lock()  # Thread safety for report counters

        self.builder: SqlParsingBuilder = SqlParsingBuilder(
            usage_config=self.config.usage,  # Always pass usage config
            generate_lineage=True,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_operations=self.config.usage.include_operational_stats
            if self.config.include_usage_statistics
            else False,
        )

        self.schema_resolver = self._init_schema_resolver()

        if self.config.include_tables or self.config.include_views:
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
        config = TeradataConfig.parse_obj(config_dict)
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
        with engine.connect() as conn:
            inspector = inspect(conn)
            if self.config.database and self.config.database != "":
                databases = [self.config.database]
            elif self.config.databases:
                databases = self.config.databases
            else:
                databases = inspector.get_schema_names()
            for db in databases:
                if self.config.database_pattern.allowed(db):
                    # url = self.config.get_sql_alchemy_url(current_db=db)
                    # with create_engine(url, **self.config.options).connect() as conn:
                    #    inspector = inspect(conn)
                    inspector._datahub_database = db
                    yield inspector

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
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        setattr(  # noqa: B010
            inspector,
            "get_table_names",
            lambda schema: [
                i.name
                for i in filter(
                    lambda t: t.object_type != "View", self._tables_cache[schema]
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

        for entry in self._tables_cache[schema]:
            if entry.name == table:
                description = entry.description
                if entry.object_type == "View" and entry.request_text:
                    properties["view_definition"] = entry.request_text
                break
        return description, properties, location

    def cached_loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        view_names = [
            i.name
            for i in filter(
                lambda t: t.object_type == "View", self._tables_cache[schema]
            )
        ]

        setattr(  # noqa: B010
            inspector,
            "get_view_names",
            lambda schema: view_names,
        )

        # Check if we should use multi-threading for view processing
        # Only use multi-threading when explicitly enabled and help commands are needed (not using QVCI)
        if (
            self.config.view_processing_workers > 1
            and self.config.enable_experimental_threading
            and not self.config.use_qvci
            and len(view_names) > 1
        ):
            logger.warning(
                "Using EXPERIMENTAL multi-threaded view processing. "
                "This may cause crashes and is not recommended for production."
            )
            yield from self._loop_views_multi_threaded(
                inspector, schema, sql_config, view_names
            )
        else:
            if (
                self.config.view_processing_workers > 1
                and not self.config.enable_experimental_threading
            ):
                logger.info(
                    "Multi-threaded view processing requested but experimental threading not enabled. "
                    "Using single-threaded processing. Set enable_experimental_threading=true to enable."
                )
            for work_unit in super().loop_views(inspector, schema, sql_config):
                # Count processed views in single-threaded mode
                with self._report_lock:
                    self.report.num_views_processed += 1
                    if self.report.num_views_processed % 10 == 0:
                        logger.info(f"Processed {self.report.num_views_processed} views")
                yield work_unit

    def _loop_views_multi_threaded(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
        view_names: List[str],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """Multi-threaded view processing to parallelize help command execution."""
        logger.info(
            f"Using {self.config.view_processing_workers} worker threads for view processing"
        )

        total_views = len(view_names)
        logger.info(
            f"Processing {total_views} views in schema '{schema}' with multi-threading"
        )

        if not view_names:
            return

        # Group views into batches
        batch_size = self.config.view_batch_size
        view_batches = [
            view_names[i : i + batch_size]
            for i in range(0, len(view_names), batch_size)
        ]

        logger.info(
            f"Processing {len(view_batches)} view batches with {self.config.view_processing_workers} workers"
        )

        def process_view_batch(
            batch_info: Tuple[int, List[str]],
        ) -> Tuple[int, List[Union[SqlWorkUnit, MetadataWorkUnit]]]:
            """Process a batch of views with improved thread safety."""
            batch_idx, view_batch = batch_info
            work_units = []

            # Create a new engine connection for this thread
            thread_engine = self.get_metadata_engine()

            try:
                with thread_engine.connect() as connection:
                    # Create a thread-local inspector
                    thread_inspector = inspect(connection)
                    thread_inspector._datahub_database = getattr(  # type: ignore[attr-defined]
                        inspector, "_datahub_database", None
                    )

                    # Set view names for this batch
                    setattr(  # noqa: B010
                        thread_inspector,
                        "get_view_names",
                        lambda schema: view_batch,
                    )

                    for view_name in view_batch:
                        try:
                            # Process single view using parent method logic
                            # Note: _process_view requires dataset_name as first parameter
                            dataset_name = self.get_identifier(
                                schema=schema,
                                entity=view_name,
                                inspector=thread_inspector,
                            )
                            for work_unit in self._process_view(
                                dataset_name,
                                thread_inspector,
                                schema,
                                view_name,
                                sql_config,
                            ):
                                work_units.append(work_unit)

                            # Thread-safe counter update
                            with self._report_lock:
                                if hasattr(self.report, "num_views_processed"):
                                    self.report.num_views_processed += 1
                                    current_count = self.report.num_views_processed
                                else:
                                    # Initialize counter if it doesn't exist
                                    self.report.num_views_processed = 1
                                    current_count = 1

                            if current_count % 10 == 0:
                                logger.info(f"Processed {current_count} views")

                        except Exception as e:
                            logger.error(
                                f"Error processing view {view_name} in batch {batch_idx}: {e}"
                            )
                            with self._report_lock:
                                if hasattr(self.report, "num_view_processing_failures"):
                                    self.report.num_view_processing_failures += 1
                                else:
                                    self.report.num_view_processing_failures = 1
                            continue

            except Exception as e:
                logger.error(f"Error in view batch {batch_idx} processing: {e}")
            finally:
                # Ensure engine is properly disposed
                thread_engine.dispose()

            return batch_idx, work_units

        # Process batches in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(
            max_workers=self.config.view_processing_workers
        ) as executor:
            # Submit all batches to the thread pool with batch index
            future_to_batch = {
                executor.submit(process_view_batch, (i, batch)): i
                for i, batch in enumerate(view_batches)
            }

            # Collect results as they complete
            completed_batches = {}
            for future in as_completed(future_to_batch):
                original_batch_idx = future_to_batch[future]
                try:
                    batch_idx, work_units = future.result()
                    completed_batches[batch_idx] = work_units
                    logger.debug(
                        f"Completed view batch {batch_idx + 1}/{len(view_batches)} with {len(work_units)} work units"
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing view batch {original_batch_idx}: {e}"
                    )
                    with self._report_lock:
                        if hasattr(self.report, "num_view_processing_failures"):
                            self.report.num_view_processing_failures += 1
                        else:
                            self.report.num_view_processing_failures = 1
                    completed_batches[
                        original_batch_idx
                    ] = []  # Empty result for failed batch

            # Yield results in original order to maintain consistency
            for i in range(len(view_batches)):
                if i in completed_batches:
                    for work_unit in completed_batches[i]:
                        yield work_unit

        logger.info(f"Completed multi-threaded view processing of {total_views} views")

    def cache_tables_and_views(self) -> None:
        engine = self.get_metadata_engine()
        for entry in engine.execute(self.TABLES_AND_VIEWS_QUERY):
            table = TeradataTable(
                database=entry.DataBaseName.strip(),
                name=entry.name.strip(),
                description=entry.description.strip() if entry.description else None,
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
            if table.database not in self._tables_cache:
                self._tables_cache[table.database] = []

            self._tables_cache[table.database].append(table)

    def get_audit_log_mcps(self, urns: Set[str]) -> Iterable[MetadataWorkUnit]:
        """
        Extract lineage metadata from audit logs with optional multi-threading support.
        Uses optimized approach: single DB query + parallel SQL parsing.
        """
        if (
            self.config.lineage_processing_workers <= 1
            or not self.config.enable_experimental_threading
        ):
            if (
                self.config.lineage_processing_workers > 1
                and not self.config.enable_experimental_threading
            ):
                logger.info(
                    "Multi-threaded lineage processing requested but threading not enabled. "
                    "Using single-threaded processing. Set enable_experimental_threading=true to enable."
                )
            yield from self._get_audit_log_mcps_single_threaded(urns)
        else:
            logger.info(
                "Using optimized multi-threaded lineage processing: single DB query + parallel SQL parsing."
            )
            yield from self._get_audit_log_mcps_multi_threaded(urns)

    def _get_audit_log_mcps_single_threaded(
        self, urns: Set[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Single-threaded lineage extraction (original implementation)."""
        engine = self.get_metadata_engine()
        for entry in engine.execute(self._make_lineage_query()):
            with self._report_lock:
                self.report.num_queries_parsed += 1
                if self.report.num_queries_parsed % 1000 == 0:
                    logger.info(f"Parsed {self.report.num_queries_parsed} queries")

            yield from self.gen_lineage_from_query(
                query=entry.query_text,
                default_database=entry.default_database,
                timestamp=entry.timestamp,
                user=entry.user,
                urns=urns,
            )

    def _get_audit_log_mcps_multi_threaded(
        self, urns: Set[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Optimized multi-threaded lineage extraction with chunked database fetching."""
        logger.info(
            f"Using {self.config.lineage_processing_workers} worker threads for lineage processing"
        )

        # Step 1: Fetch SQL statements in chunks to avoid connection timeouts
        logger.info("Fetching lineage queries from database in chunks...")
        try:
            lineage_entries = list(self._fetch_lineage_entries_chunked())
        except Exception as e:
            logger.error(f"Failed to fetch lineage entries: {e}")
            logger.info("Falling back to single-threaded processing...")
            yield from self._get_audit_log_mcps_single_threaded(urns)
            return

        total_queries = len(lineage_entries)
        logger.info(
            f"Retrieved {total_queries} lineage queries for parallel processing"
        )

        if not lineage_entries:
            return

        # Step 2: Group SQL statements into batches for parallel processing
        batch_size = self.config.lineage_batch_size
        batches = [
            lineage_entries[i : i + batch_size]
            for i in range(0, len(lineage_entries), batch_size)
        ]

        logger.info(
            f"Processing {len(batches)} batches with {self.config.lineage_processing_workers} workers"
        )

        def process_sql_batch(
            batch_info: Tuple[int, List[Any]],
        ) -> Tuple[int, List[MetadataWorkUnit]]:
            """Process a batch of SQL statements with thread isolation."""
            batch_idx, batch_entries = batch_info
            work_units = []

            # Create completely isolated thread-local instances
            try:
                # Create thread-local schema resolver copy
                thread_schema_resolver = SchemaResolver(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )

                # Create thread-local SQL parsing builder
                thread_builder = SqlParsingBuilder(
                    usage_config=self.config.usage,  # Always pass usage config, even if not generating stats
                    generate_lineage=True,
                    generate_usage_statistics=self.config.include_usage_statistics,
                    generate_operations=self.config.usage.include_operational_stats
                    if self.config.include_usage_statistics
                    else False,
                )

                for entry in batch_entries:
                    try:
                        # Thread-safe counter update
                        with self._report_lock:
                            self.report.num_queries_parsed += 1
                            current_count = self.report.num_queries_parsed

                        if current_count % 1000 == 0:
                            logger.info(f"Parsed {current_count} queries")

                        # Use thread-local schema resolver for lineage parsing
                        result = sqlglot_lineage(
                            sql=str(entry.query_text).replace("(NOT CASESPECIFIC)", ""),
                            schema_resolver=thread_schema_resolver,  # Use thread-local schema resolver
                            default_db=None,
                            default_schema=str(entry.default_database)
                            if entry.default_database
                            else self.config.default_db,
                        )

                        if not result.debug_info.table_error:
                            # Process with thread-local builder
                            lineage_work_units = list(
                                thread_builder.process_sql_parsing_result(
                                    result,
                                    query=str(entry.query_text),
                                    is_view_ddl=False,
                                    query_timestamp=entry.timestamp,
                                    user=f"urn:li:corpuser:{entry.user}"
                                    if entry.user
                                    else None,
                                    include_urns=urns,
                                )
                            )
                            work_units.extend(lineage_work_units)
                        else:
                            with self._report_lock:
                                self.report.num_table_parse_failures += 1

                    except Exception as e:
                        logger.debug(f"Error parsing SQL in batch {batch_idx}: {e}")
                        with self._report_lock:
                            self.report.num_table_parse_failures += 1
                        continue

            except Exception as e:
                logger.error(f"Error in SQL parsing batch {batch_idx}: {e}")
                with self._report_lock:
                    self.report.num_table_parse_failures += 1

            return batch_idx, work_units

        # Step 3: Process SQL statements in parallel (using shared schema resolver)
        try:
            with ThreadPoolExecutor(
                max_workers=self.config.lineage_processing_workers,
                thread_name_prefix="lineage_worker",
            ) as executor:
                future_to_batch = {
                    executor.submit(process_sql_batch, (i, batch)): i
                    for i, batch in enumerate(batches)
                }

                # Collect results in order
                completed_batches = {}
                for future in as_completed(future_to_batch):
                    original_batch_idx = future_to_batch[future]
                    try:
                        batch_idx, work_units = future.result(
                            timeout=300
                        )  # 5 minute timeout per batch
                        completed_batches[batch_idx] = work_units
                        logger.debug(
                            f"Completed SQL parsing batch {batch_idx + 1}/{len(batches)} with {len(work_units)} work units"
                        )

                    except Exception as e:
                        logger.error(
                            f"Error processing SQL parsing batch {original_batch_idx}: {e}"
                        )
                        with self._report_lock:
                            self.report.num_table_parse_failures += 1
                        completed_batches[original_batch_idx] = []

                # Yield results in original order
                for i in range(len(batches)):
                    if i in completed_batches:
                        for work_unit in completed_batches[i]:
                            yield work_unit

        except Exception as e:
            logger.error(f"Critical error in multi-threaded lineage processing: {e}")
            logger.info("Falling back to single-threaded processing...")
            yield from self._get_audit_log_mcps_single_threaded(urns)
            return

        logger.info(
            f"Completed optimized multi-threaded lineage processing of {total_queries} queries"
        )

    def _fetch_lineage_entries_chunked(self) -> Iterable[Any]:
        """Fetch lineage entries using server-side cursor to handle large result sets efficiently."""
        base_query = self._make_lineage_query()

        fetch_engine = self.get_metadata_engine()
        try:
            with fetch_engine.connect() as conn:
                logger.info("Executing lineage query with server-side cursor...")

                # Use server-side cursor for streaming large result sets
                # This avoids loading all data into memory at once
                result = conn.execute(text(base_query))

                # Stream results in batches to avoid memory issues
                batch_size = 5000
                batch_count = 0
                total_count = 0

                while True:
                    # Fetch a batch of rows
                    batch = result.fetchmany(batch_size)
                    if not batch:
                        break

                    batch_count += 1
                    total_count += len(batch)

                    logger.info(
                        f"Fetched batch {batch_count}: {len(batch)} lineage entries (total: {total_count})"
                    )
                    yield from batch

                logger.info(
                    f"Completed fetching {total_count} lineage entries in {batch_count} batches"
                )

        except Exception as e:
            logger.error(f"Error fetching lineage entries: {e}")
            raise
        finally:
            fetch_engine.dispose()

    def _check_historical_table_exists(self) -> bool:
        """
        Check if the PDCRDATA.DBQLSqlTbl_Hst table exists and is accessible.

        Returns:
            bool: True if the historical table exists and is accessible, False otherwise.
        """
        try:
            engine = self.get_metadata_engine()
            # Use a simple query to check if the table exists and is accessible
            check_query = """
                SELECT TOP 1 QueryID 
                FROM PDCRDATA.DBQLSqlTbl_Hst 
                WHERE 1=0
            """
            with engine.connect() as conn:
                conn.execute(text(check_query))
                logger.info(
                    "Historical lineage table PDCRDATA.DBQLSqlTbl_Hst is available"
                )
                return True
        except Exception as e:
            logger.info(
                f"Historical lineage table PDCRDATA.DBQLSqlTbl_Hst is not available: {e}"
            )
            return False

    def _make_lineage_query(self) -> str:
        databases_filter = (
            ""
            if not self.config.databases
            else "and default_database in ({databases})".format(
                databases=",".join([f"'{db}'" for db in self.config.databases])
            )
        )

        # Determine which query to use based on configuration and table availability
        if (
            self.config.include_historical_lineage
            and self._check_historical_table_exists()
        ):
            logger.info(
                "Using historical lineage data from both DBC.QryLogV and PDCRDATA.DBQLSqlTbl_Hst"
            )
            # For historical query, we need the database filter for both current and historical parts
            databases_filter_history = (
                databases_filter.replace("default_database", "h.DefaultDatabase")
                if databases_filter
                else ""
            )

            query = self.QUERY_TEXT_QUERY_WITH_HISTORY.format(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                databases_filter=databases_filter,
                databases_filter_history=databases_filter_history,
            )
        else:
            if self.config.include_historical_lineage:
                logger.warning(
                    "Historical lineage was requested but PDCRDATA.DBQLSqlTbl_Hst table is not available. Falling back to current data only."
                )

            query = self.QUERY_TEXT_QUERY.format(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                databases_filter=databases_filter,
            )
        return query

    def gen_lineage_from_query(
        self,
        query: str,
        default_database: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        user: Optional[str] = None,
        view_urn: Optional[str] = None,
        urns: Optional[Set[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        result = sqlglot_lineage(
            # With this clever hack we can make the query parser to not fail on queries with CASESPECIFIC
            sql=query.replace("(NOT CASESPECIFIC)", ""),
            schema_resolver=self.schema_resolver,
            default_db=None,
            default_schema=(
                default_database if default_database else self.config.default_db
            ),
        )
        if result.debug_info.table_error:
            logger.debug(
                f"Error parsing table lineage ({view_urn}):\n{result.debug_info.table_error}"
            )
            self.report.num_table_parse_failures += 1
        else:
            yield from self.builder.process_sql_parsing_result(
                result,
                query=query,
                is_view_ddl=view_urn is not None,
                query_timestamp=timestamp,
                user=f"urn:li:corpuser:{user}",
                include_urns=urns,
            )

    def _parse_sql_for_lineage(
        self,
        query_text: str,
        default_database: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        user: Optional[str] = None,
        view_urn: Optional[str] = None,
        urns: Optional[Set[str]] = None,
        schema_resolver: Optional[SchemaResolver] = None,
        thread_builder: Optional[SqlParsingBuilder] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Pure SQL parsing for lineage extraction - no database connections needed."""
        if schema_resolver is None:
            schema_resolver = self.schema_resolver

        # Pure SQL parsing using sqlglot - completely independent of database connections
        result = sqlglot_lineage(
            sql=query_text.replace(
                "(NOT CASESPECIFIC)", ""
            ),  # Teradata-specific cleanup
            schema_resolver=schema_resolver,
            default_db=None,
            default_schema=default_database
            if default_database
            else self.config.default_db,
        )

        if result.debug_info.table_error:
            logger.debug(
                f"Error parsing table lineage ({view_urn}):\n{result.debug_info.table_error}"
            )
            # Error counting handled by caller
        else:
            # Use thread-local builder if provided, otherwise use main builder instance
            builder_to_use = (
                thread_builder if thread_builder is not None else self.builder
            )
            yield from builder_to_use.process_sql_parsing_result(
                result,
                query=query_text,
                is_view_ddl=view_urn is not None,
                query_timestamp=timestamp,
                user=f"urn:li:corpuser:{user}" if user else None,
                include_urns=urns,
            )

    def get_metadata_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self.config.options)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        logger.info("Starting Teradata metadata extraction")

        # Add all schemas to the schema resolver
        # Sql parser operates on lowercase urns so we need to lowercase the urns
        with self.report.new_stage("Schema metadata extraction"):
            schema_wu_count = 0
            for wu in auto_lowercase_urns(super().get_workunits_internal()):
                urn = wu.get_urn()
                schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
                if schema_metadata:
                    self.schema_resolver.add_schema_metadata(urn, schema_metadata)
                schema_wu_count += 1
                if schema_wu_count % 100 == 0:
                    logger.info(f"Processed {schema_wu_count} schema work units")
                yield wu
            logger.info(
                f"Completed schema metadata extraction: {schema_wu_count} work units"
            )

        urns = self.schema_resolver.get_urns()
        if self.config.include_table_lineage or self.config.include_usage_statistics:
            with self.report.new_stage("Audit log extraction"):
                logger.info(f"Starting lineage extraction for {len(urns)} URNs")
                yield from self.get_audit_log_mcps(urns=urns)

        with self.report.new_stage("SQL parsing builder"):
            builder_wu_count = 0
            for wu in self.builder.gen_workunits():
                builder_wu_count += 1
                if builder_wu_count % 100 == 0:
                    logger.info(f"Generated {builder_wu_count} builder work units")
                yield wu
            logger.info(f"Completed SQL parsing builder: {builder_wu_count} work units")
