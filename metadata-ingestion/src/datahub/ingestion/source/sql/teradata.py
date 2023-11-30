import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from itertools import groupby
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
import teradatasqlalchemy  # noqa: F401
import teradatasqlalchemy.types as custom_types
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
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
from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.ingestion.source_report.time_window import BaseTimeWindowReport
from datahub.metadata._schema_classes import SchemaMetadataClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)
from datahub.utilities.sqlglot_lineage import SchemaResolver, sqlglot_lineage

logger: logging.Logger = logging.getLogger(__name__)

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


# lru cache is set to 1 which work only in single threaded environment but it keeps the memory footprint lower
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


# lru cache is set to 1 which work only in single threaded environment but it keeps the memory footprint lower
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

    return {"constrained_columns": index_columns, "name": index_name}


def optimized_get_columns(
    self: Any,
    connection: Connection,
    table_name: str,
    schema: Optional[str] = None,
    tables_cache: MutableMapping[str, List[TeradataTable]] = {},
    use_qvci: bool = False,
    **kw: Dict[str, Any],
) -> List[Dict]:
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

    final_column_info = []
    # Don't care about ART tables now
    # Ignore the non-functional column in a PTI table
    for row in res:
        col_info = self._get_column_info(row)
        if "TSColumnType" in col_info and col_info["TSColumnType"] is not None:
            if (
                col_info["ColumnName"] == "TD_TIMEBUCKET"
                and col_info["TSColumnType"].strip() == "TB"
            ):
                continue
        final_column_info.append(col_info)

    return final_column_info


# lru cache is set to 1 which work only in single threaded environment but it keeps the memory footprint lower
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
    for constraint_info, constraint_cols in groupby(res, grouper):
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
    tables_cache: MutableMapping[str, List[TeradataTable]] = {},
    **kw: Dict[str, Any],
) -> Optional[str]:
    if schema is None:
        schema = self.default_schema_name

    if schema not in tables_cache:
        return None

    for table in tables_cache[schema]:
        if table.name == view_name:
            return self.normalize_name(table.request_text)

    return None


@dataclass
class TeradataReport(ProfilingSqlReport, IngestionStageReport, BaseTimeWindowReport):
    num_queries_parsed: int = 0
    num_view_ddl_parsed: int = 0
    num_table_parse_failures: int = 0


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
        default=AllowDenyPattern(
            deny=[
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
        ),
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
    SELECT MIN(CollectTimeStamp) as "min_ts", MAX(CollectTimeStamp) as "max_ts" from DBC.DBQLogTbl
    """.strip()

    QUERY_TEXT_QUERY: str = """
    SELECT
        s.QueryID as "query_id",
        UserName as "user",
        StartTime AT TIME ZONE 'GMT' as "timestamp",
        DefaultDatabase as default_database,
        s.SqlTextInfo as "query_text",
        s.SqlRowNo as "row_no"
    FROM "DBC".DBQLogTbl as l
    JOIN "DBC".DBQLSqlTbl as s on s.QueryID = l.QueryID
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

    TABLES_AND_VIEWS_QUERY: str = """
SELECT
    t.DatabaseName,
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
FROM dbc.Tables t
WHERE DatabaseName NOT IN (
                'All',
                'Crashdumps',
                'Default',
                'DemoNow_Monitor',
                'EXTUSER',
                'External_AP',
                'GLOBAL_FUNCTIONS',
                'LockLogShredder',
                'PUBLIC',
                'SQLJ',
                'SYSBAR',
                'SYSJDBC',
                'SYSLIB',
                'SYSSPATIAL',
                'SYSUDTLIB',
                'SYSUIF',
                'SysAdmin',
                'Sys_Calendar',
                'SystemFe',
                'TDBCMgmt',
                'TDMaps',
                'TDPUSER',
                'TDQCD',
                'TDStats',
                'TD_ANALYTICS_DB',
                'TD_SERVER_DB',
                'TD_SYSFNLIB',
                'TD_SYSGPL',
                'TD_SYSXML',
                'TDaaS_BAR',
                'TDaaS_DB',
                'TDaaS_Maint',
                'TDaaS_Monitor',
                'TDaaS_Support',
                'TDaaS_TDBCMgmt1',
                'TDaaS_TDBCMgmt2',
                'dbcmngr',
                'mldb',
                'system',
                'tapidb',
                'tdwm',
                'val',
                'dbc'
)
AND t.TableKind in ('T', 'V', 'Q', 'O')
ORDER by DatabaseName, TableName;
     """.strip()

    _tables_cache: MutableMapping[str, List[TeradataTable]] = defaultdict(list)

    def __init__(self, config: TeradataConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "teradata")

        self.report: TeradataReport = TeradataReport()
        self.graph: Optional[DataHubGraph] = ctx.graph

        self.builder: SqlParsingBuilder = SqlParsingBuilder(
            usage_config=self.config.usage
            if self.config.include_usage_statistics
            else None,
            generate_lineage=True,
            generate_usage_statistics=self.config.include_usage_statistics,
            generate_operations=self.config.usage.include_operational_stats,
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
                lambda self, connection, table_name, schema=None, use_qvci=self.config.use_qvci, **kw: optimized_get_columns(
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
                lambda self, connection, table_name, schema=None, **kw: optimized_get_pk_constraint(
                    self, connection, table_name, schema, **kw
                ),
            )

            setattr(  # noqa: B010
                TeradataDialect,
                "get_foreign_keys",
                lambda self, connection, table_name, schema=None, **kw: optimized_get_foreign_keys(
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

            setattr(  # noqa: B010
                TeradataDialect,
                "get_view_definition",
                lambda self, connection, view_name, schema=None, **kw: optimized_get_view_definition(
                    self, connection, view_name, schema, tables_cache=tables_cache, **kw
                ),
            )

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

    def cached_loop_tables(  # noqa: C901
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

    def cached_loop_views(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        setattr(  # noqa: B010
            inspector,
            "get_view_names",
            lambda schema: [
                i.name
                for i in filter(
                    lambda t: t.object_type == "View", self._tables_cache[schema]
                )
            ],
        )
        yield from super().loop_views(inspector, schema, sql_config)

    def cache_tables_and_views(self) -> None:
        engine = self.get_metadata_engine()
        for entry in engine.execute(self.TABLES_AND_VIEWS_QUERY):
            table = TeradataTable(
                database=entry.DatabaseName.strip(),
                name=entry.name.strip(),
                description=entry.description.strip() if entry.description else None,
                object_type=entry.object_type,
                create_timestamp=entry.CreateTimeStamp,
                last_alter_name=entry.LastAlterName,
                last_alter_timestamp=entry.LastAlterTimeStamp,
                request_text=entry.RequestText.strip()
                if entry.object_type == "View" and entry.RequestText
                else None,
            )
            if table.database not in self._tables_cache:
                self._tables_cache[table.database] = []

            self._tables_cache[table.database].append(table)

    def get_audit_log_mcps(self, urns: Set[str]) -> Iterable[MetadataWorkUnit]:
        engine = self.get_metadata_engine()
        for entry in engine.execute(self._make_lineage_query()):
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

    def _make_lineage_query(self) -> str:
        databases_filter = (
            ""
            if not self.config.databases
            else "and default_database in ({databases})".format(
                databases=",".join([f"'{db}'" for db in self.config.databases])
            )
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
            default_schema=default_database
            if default_database
            else self.config.default_db,
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

    def get_metadata_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self.config.options)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # Add all schemas to the schema resolver
        # Sql parser operates on lowercase urns so we need to lowercase the urns
        for wu in auto_lowercase_urns(super().get_workunits_internal()):
            urn = wu.get_urn()
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata:
                self.schema_resolver.add_schema_metadata(urn, schema_metadata)
            yield wu

        urns = self.schema_resolver.get_urns()
        if self.config.include_table_lineage or self.config.include_usage_statistics:
            self.report.report_ingestion_stage_start("audit log extraction")
            yield from self.get_audit_log_mcps(urns=urns)

        yield from self.builder.gen_workunits()
