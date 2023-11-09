import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, MutableMapping, Optional, Tuple, Union

# This import verifies that the dependencies are available.
import teradatasqlalchemy  # noqa: F401
import teradatasqlalchemy.types as custom_types
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from teradatasqlalchemy.dialect import TeradataDialect

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
from datahub.metadata._schema_classes import SchemaMetadataClass, ViewPropertiesClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.sqlglot_lineage import SchemaResolver, sqlglot_lineage
from datahub.utilities.urns.dataset_urn import DatasetUrn

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
                "dbc",
                "All",
                "Crashdumps",
                "DBC",
                "dbcmngr",
                "Default",
                "External_AP",
                "EXTUSER",
                "LockLogShredder",
                "PUBLIC",
                "Sys_Calendar",
                "SysAdmin",
                "SYSBAR",
                "SYSJDBC",
                "SYSLIB",
                "SystemFe",
                "SYSUDTLIB",
                "SYSUIF",
                "TD_SERVER_DB",
                "TDStats",
                "TD_SYSGPL",
                "TD_SYSXML",
                "TDMaps",
                "TDPUSER",
                "TDQCD",
                "tdwm",
                "SQLJ",
                "TD_SYSFNLIB",
                "SYSSPATIAL",
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

    disable_schema_metadata: bool = Field(
        default=False,
        description="Whether to disable schema metadata ingestion. Only table names and database names will be ingested.",
    )

    use_cached_metadata: bool = Field(
        default=True,
        description="Whether to use cached metadata. This reduce the number of queries to the database but requires to have cached metadata.",
    )


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

    LINEAGE_QUERY: str = """
    SELECT
        QueryId as "query_id",
        UserName as "user",
        StartTime AT TIME ZONE 'GMT' as "timestamp",
        DefaultDatabase as default_database,
        QueryText as "query_text"
    FROM "DBC".DBQLogTbl
    where ErrorCode = 0
    and "timestamp" >= TIMESTAMP '{start_time}'
    and "timestamp" < TIMESTAMP '{end_time}'
    """.strip()

    LINEAGE_QUERY_DATABASE_FILTER: str = """and default_database IN ({databases})"""

    LINEAGE_TIMESTAMP_BOUND_QUERY: str = """
    SELECT MIN(CollectTimeStamp) as "min_ts", MAX(CollectTimeStamp) as "max_ts" from DBC.DBQLogTbl
    """.strip()

    QUERY_TEXT_QUERY: str = """
    SELECT
        QueryID as "query_id",
        SqlTextInfo as "query_text",
        SqlRowNo as "row_no"
        FROM "DBC".DBQLSqlTbl
        ORDER BY "query_id", "row_no"
    WHERE
        CollectTimeStamp >= TIMESTAMP '{min_ts}'
        and CollectTimeStamp < TIMESTAMP '{max_ts}'
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
WHERE DatabaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
        'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
        'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
        'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB', 'TDStats',
        'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
        'tdwm', 'SQLJ', 'TD_SYSFNLIB', 'SYSSPATIAL')
and t.TableKind in ('T', 'V', 'Q', 'O')
ORDER by DatabaseName, TableName;
     """.strip()

    _tables_cache: MutableMapping[str, List[TeradataTable]] = defaultdict(list)

    _view_definition_cache: MutableMapping[str, str]
    _query_cache: MutableMapping[str, str] = {}

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

        if self.config.use_file_backed_cache:
            self._view_definition_cache = FileBackedDict[str]()
            self._query_cache = FileBackedDict[str]()
        else:
            self._view_definition_cache = {}
            self._query_cache = {}

        if self.config.use_cached_metadata:
            self.cache_tables_and_views()
            logger.info(f"Found {len(self._tables_cache)} tables and views")
            setattr(self, "loop_tables", self.cached_loop_tables)  # noqa: B010
            setattr(self, "loop_views", self.cached_loop_views)  # noqa: B010
            setattr(  # noqa: B010
                self, "get_table_properties", self.cached_get_table_properties
            )
            # self.loop_tables = self.cached_loop_tables
            # self.loop_views = self.cached_loop_views
            # self.get_table_properties = self.cached_get_table_properties
        if self.config.disable_schema_metadata:
            # self._get_columns = lambda dataset_name, inspector, schema, table: []
            # self._get_foreign_keys = lambda dataset_name, inspector, schema, table: []
            setattr(  # noqa: B010
                self, "_get_columns", lambda dataset_name, inspector, schema, table: []
            )
            setattr(  # noqa: B010
                TeradataDialect,
                "get_columns",
                lambda self, connection, table_name, schema=None, **kw: [],
            )
            setattr(  # noqa: B010
                TeradataDialect,
                "get_pk_constraint",
                lambda self, connection, table_name, schema=None, **kw: {},
            )
            setattr(  # noqa: B010
                self,
                "_get_foreign_keys",
                lambda dataset_name, inspector, schema, table: [],
            )

    @classmethod
    def create(cls, config_dict, ctx):
        config = TeradataConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _init_schema_resolver(self) -> SchemaResolver:
        if (
            self.config.disable_schema_metadata
            or not self.config.include_tables
            or not self.config.include_views
        ):
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

    def get_view_lineage(self) -> Iterable[MetadataWorkUnit]:
        for key in self._view_definition_cache.keys():
            view_definition = self._view_definition_cache[key]
            dataset_urn = DatasetUrn.create_from_string(key)

            db_name: Optional[str] = None
            # We need to get the default db from the dataset urn otherwise the builder generates the wrong urns
            if "." in dataset_urn.get_dataset_name():
                db_name = dataset_urn.get_dataset_name().split(".", 1)[0]

            self.report.num_view_ddl_parsed += 1
            if self.report.num_view_ddl_parsed % 1000 == 0:
                logger.info(f"Parsed {self.report.num_view_ddl_parsed} view ddl")

            yield from self.gen_lineage_from_query(
                query=view_definition, default_database=db_name, view_urn=key
            )

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

    def get_audit_log_mcps(self) -> Iterable[MetadataWorkUnit]:
        engine = self.get_metadata_engine()
        bounds = engine.execute(self.LINEAGE_TIMESTAMP_BOUND_QUERY).fetchone()
        query = self.QUERY_TEXT_QUERY.format(
            min_ts=getattr(bounds, "min_ts", self.config.start_time),
            max_ts=getattr(bounds, "max_ts", self.config.end_time),
        )
        for entry in engine.execute(query):
            key = str(entry.query_id)
            self._query_cache[key] = self._query_cache.get(key, "") + entry.query_text
        for entry in engine.execute(self._make_lineage_query()):
            self.report.num_queries_parsed += 1
            if self.report.num_queries_parsed % 1000 == 0:
                logger.info(f"Parsed {self.report.num_queries_parsed} queries")
            yield from self.gen_lineage_from_query(
                query=self._query_cache.get(str(entry.query_id), entry.query_text),
                default_database=entry.default_database,
                timestamp=entry.timestamp,
                user=entry.user,
            )

    def _make_lineage_query(self) -> str:
        query = self.LINEAGE_QUERY.format(
            start_time=self.config.start_time, end_time=self.config.end_time
        )
        if self.config.databases:
            query += self.LINEAGE_QUERY_DATABASE_FILTER.format(
                databases=",".join([f"'{db}'" for db in self.config.databases])
            )
        return query

    def gen_lineage_from_query(
        self,
        query: str,
        default_database: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        user: Optional[str] = None,
        view_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        result = sqlglot_lineage(
            sql=query,
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
                include_urns=self.schema_resolver.get_urns(),
            )

    def get_metadata_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self.config.options)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # Add all schemas to the schema resolver
        for wu in super().get_workunits_internal():
            urn = wu.get_urn()
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata:
                self.schema_resolver.add_schema_metadata(urn, schema_metadata)
            view_properties = wu.get_aspect_of_type(ViewPropertiesClass)
            if view_properties and self.config.include_view_lineage:
                self._view_definition_cache[urn] = view_properties.viewLogic
            yield wu

        if self.config.include_view_lineage:
            if not self.config.include_views and self.graph is not None:
                d = {}

                entries = self.graph.bulk_fetch_view_definitions(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )

                for (urn, view_definition) in entries:
                    self._view_definition_cache[urn] = view_definition
                    d[urn] = view_definition

                logger.info(
                    f"Loaded {len(self._view_definition_cache)} view definitions."
                )
                logger.info(
                    f"Sample view urns: {list(self._view_definition_cache.keys())[:10]}"
                )
            elif not self.config.include_views:
                self.report.report_failure(
                    "view_lineage", "Missing DataHubGraph to read view definitions."
                )

            self.report.report_ingestion_stage_start("view lineage extraction")
            yield from self.get_view_lineage()
            yield from self.builder._gen_lineage_workunits()

        if self.config.include_table_lineage or self.config.include_usage_statistics:
            self.report.report_ingestion_stage_start("audit log extraction")
            yield from self.get_audit_log_mcps()

        yield from self.builder.gen_workunits()
