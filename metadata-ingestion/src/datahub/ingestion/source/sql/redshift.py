import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

# These imports verify that the dependencies are available.
import psycopg2  # noqa: F401
import pydantic  # noqa: F401
import sqlalchemy
import sqlalchemy_redshift  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection, reflection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy_redshift.dialect import RedshiftDialect, RelationKey
from sqllineage.runner import LineageRunner

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.configuration.time_window_config import BaseTimeWindowConfig
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
    SqlWorkUnit,
)

# TRICKY: it's necessary to import the Postgres source because
# that module has some side effects that we care about here.
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    UpstreamClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class LineageMode(Enum):
    SQL_BASED = "sql_based"
    STL_SCAN_BASED = "stl_scan_based"
    MIXED = "mixed"


class LineageCollectorType(Enum):
    QUERY_SCAN = "query_scan"
    QUERY_SQL_PARSER = "query_sql_parser"
    VIEW = "view"
    NON_BINDING_VIEW = "non-binding-view"
    COPY = "copy"
    UNLOAD = "unload"


class LineageDatasetPlatform(Enum):
    S3 = "s3"
    REDSHIFT = "redshift"


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
    query_parser_failed_sqls: List[str]

    def __post_init__(self):
        if self.collector_type == LineageCollectorType.COPY:
            self.dataset_lineage_type = DatasetLineageTypeClass.COPY
        elif self.collector_type in [
            LineageCollectorType.VIEW,
            LineageCollectorType.NON_BINDING_VIEW,
        ]:
            self.dataset_lineage_type = DatasetLineageTypeClass.VIEW
        else:
            self.dataset_lineage_type = DatasetLineageTypeClass.TRANSFORMED


class RedshiftConfig(
    PostgresConfig, BaseTimeWindowConfig, DatasetLineageProviderConfigBase
):
    # Although Amazon Redshift is compatible with Postgres's wire format,
    # we actually want to use the sqlalchemy-redshift package and dialect
    # because it has better caching behavior. In particular, it queries
    # the full table, column, and constraint information in a single larger
    # query, and then simply pulls out the relevant information as needed.
    # Because of this behavior, it uses dramatically fewer round trips for
    # large Redshift warehouses. As an example, see this query for the columns:
    # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/60b4db04c1d26071c291aeea52f1dcb5dd8b0eb0/sqlalchemy_redshift/dialect.py#L745.
    scheme = Field(
        default="redshift+psycopg2",
        description="",
        exclude=True,
    )

    default_schema: str = Field(
        default="public",
        description="The default schema to use if the sql parser fails to parse the schema with `sql_based` lineage collector",
    )

    include_table_lineage: Optional[bool] = Field(
        default=True, description="Whether table lineage should be ingested."
    )
    include_copy_lineage: Optional[bool] = Field(
        default=True,
        description="Whether lineage should be collected from copy commands",
    )
    capture_lineage_query_parser_failures: Optional[bool] = Field(
        default=False,
        description="Whether to capture lineage query parser errors with dataset properties for debuggings",
    )

    table_lineage_mode: Optional[LineageMode] = Field(
        default=LineageMode.STL_SCAN_BASED,
        description="Which table lineage collector mode to use. Available modes are: [stl_scan_based, sql_based, mixed]",
    )

    @pydantic.validator("platform")
    def platform_is_always_redshift(cls, v):
        return "redshift"


# reflection.cache uses eval and other magic to partially rewrite the function.
# mypy can't handle it, so we ignore it for now.
@reflection.cache  # type: ignore
def _get_all_table_comments(self, connection, **kw):
    COMMENT_SQL = """
        SELECT n.nspname as schema,
               c.relname as table_name,
               pgd.description as table_comment
        FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_description pgd ON pgd.objsubid = 0 AND pgd.objoid = c.oid
        WHERE c.relkind in ('r', 'v', 'm', 'f', 'p')
          AND pgd.description IS NOT NULL
        ORDER BY "schema", "table_name";
    """

    all_table_comments: Dict[RelationKey, str] = {}

    result = connection.execute(COMMENT_SQL)
    for table in result:
        key = RelationKey(table.table_name, table.schema, connection)
        all_table_comments[key] = table.table_comment

    return all_table_comments


@reflection.cache  # type: ignore
def get_table_comment(self, connection, table_name, schema=None, **kw):
    all_table_comments = self._get_all_table_comments(connection, **kw)
    key = RelationKey(table_name, schema, connection)
    if key not in all_table_comments.keys():
        key = key.unquoted()
    return {"text": all_table_comments.get(key)}


# gets all the relations for internal schemas and external schemas
# by UNION of internal schemas (excluding namespaces starting with pg_)
# and external schemas
@reflection.cache  # type: ignore
def _get_all_relation_info(self, connection, **kw):
    result = connection.execute(
        """
        SELECT c.relkind,
            n.oid AS "schema_oid",
            n.nspname AS "schema",
            c.oid AS "rel_oid",
            c.relname,
            CASE c.reldiststyle
                WHEN 0 THEN 'EVEN'
                WHEN 1 THEN 'KEY'
                WHEN 8 THEN 'ALL'
            END AS "diststyle",
            c.relowner AS "owner_id",
            u.usename AS "owner_name",
            TRIM(TRAILING ';' FROM pg_catalog.pg_get_viewdef (c.oid,TRUE)) AS "view_definition",
            pg_catalog.array_to_string(c.relacl,'\n') AS "privileges"
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
        WHERE c.relkind IN ('r','v','m','S','f')
        AND   n.nspname !~ '^pg_'
        AND   n.nspname != 'information_schema'
        UNION
        SELECT 'r' AS "relkind",
            NULL AS "schema_oid",
            schemaname AS "schema",
            NULL AS "rel_oid",
            tablename AS "relname",
            NULL AS "diststyle",
            NULL AS "owner_id",
            NULL AS "owner_name",
            NULL AS "view_definition",
            NULL AS "privileges"
        FROM pg_catalog.svv_external_tables
        ORDER BY "schema",
                "relname";"""
    )
    relations = {}
    for rel in result:
        key = RelationKey(rel.relname, rel.schema, connection)
        relations[key] = rel
    return relations


# workaround to get external tables
# Rewriting some external table types to match redshift type based on
# this redshift-sqlalchemy pull request:
#   https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/pull/163/files
# The mapping of external types to redshift types:
#   (https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html):
# External type -> Redshift type
#   int -> integer
#   decimal -> numeric
#   char -> character
#   float -> real
#   double -> float
@reflection.cache  # type: ignore
def _get_schema_column_info(self, connection, schema=None, **kw):
    schema_clause = "AND schema = '{schema}'".format(schema=schema) if schema else ""
    all_columns = defaultdict(list)

    with connection.connect() as cc:
        result = cc.execute(
            """
            SELECT
              n.nspname as "schema",
              c.relname as "table_name",
              att.attname as "name",
              format_encoding(att.attencodingtype::integer) as "encode",
              format_type(att.atttypid, att.atttypmod) as "type",
              att.attisdistkey as "distkey",
              att.attsortkeyord as "sortkey",
              att.attnotnull as "notnull",
              pg_catalog.col_description(att.attrelid, att.attnum)
                as "comment",
              adsrc,
              attnum,
              pg_catalog.format_type(att.atttypid, att.atttypmod),
              pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS DEFAULT,
              n.oid as "schema_oid",
              c.oid as "table_oid"
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_attribute att
              ON att.attrelid = c.oid
            LEFT JOIN pg_catalog.pg_attrdef ad
              ON (att.attrelid, att.attnum) = (ad.adrelid, ad.adnum)
            WHERE n.nspname !~ '^pg_'
              AND att.attnum > 0
              AND NOT att.attisdropped
              {schema_clause}
            UNION
            SELECT
              view_schema as "schema",
              view_name as "table_name",
              col_name as "name",
              null as "encode",
              col_type as "type",
              null as "distkey",
              0 as "sortkey",
              null as "notnull",
              null as "comment",
              null as "adsrc",
              null as "attnum",
              col_type as "format_type",
              null as "default",
              null as "schema_oid",
              null as "table_oid"
            FROM pg_get_late_binding_view_cols() cols(
              view_schema name,
              view_name name,
              col_name name,
              col_type varchar,
              col_num int)
            WHERE 1 {schema_clause}
            UNION
            SELECT
              schemaname as "schema",
              tablename as "table_name",
              columnname as "name",
              null as "encode",
              -- Spectrum represents data types differently.
              -- Standardize, so we can infer types.
              CASE
                WHEN external_type = 'int' THEN 'integer'
                 ELSE
                   regexp_replace(
                   replace(
                   replace(
                   replace(
                   replace(
                   replace(
                   replace(external_type, 'decimal', 'numeric'),
                    'varchar', 'character varying'),
                    'string', 'character varying'),
                    'char(', 'character('),
                    'float', 'real'),
                    'double', 'float'),
                    '^array<(.*)>$', '$1[]', 1, 'p')
                 END AS "type",
              null as "distkey",
              0 as "sortkey",
              null as "notnull",
              null as "comment",
              null as "adsrc",
              null as "attnum",
              CASE
                 WHEN external_type = 'int' THEN 'integer'
                 ELSE
                   regexp_replace(
                   replace(
                   replace(
                   replace(
                   replace(
                   replace(
                   replace(external_type, 'decimal', 'numeric'),
                    'varchar', 'character varying'),
                    'string', 'character varying'),
                    'char(', 'character('),
                    'float', 'real'),
                    'double', 'float'),
                    '^array<(.*)>$', '$1[]', 1, 'p')
                 END AS "format_type",
              null as "default",
              null as "schema_oid",
              null as "table_oid"
            FROM SVV_EXTERNAL_COLUMNS
            WHERE 1 {schema_clause}
            ORDER BY "schema", "table_name", "attnum"
        """.format(
                schema_clause=schema_clause
            )
        )
        for col in result:
            key = RelationKey(col.table_name, col.schema, connection)
            all_columns[key].append(col)
    return dict(all_columns)


def _get_external_db_mapping(connection):
    # SQL query to get mapping of external schemas in redshift to its external database.
    return connection.execute(
        """
        select * from svv_external_schemas
        """
    )


# This monkey-patching enables us to batch fetch the table descriptions, rather than
# fetching them one at a time.
RedshiftDialect._get_all_table_comments = _get_all_table_comments
RedshiftDialect.get_table_comment = get_table_comment
RedshiftDialect._get_all_relation_info = _get_all_relation_info
RedshiftDialect._get_schema_column_info = _get_schema_column_info

redshift_datetime_format = "%Y-%m-%d %H:%M:%S"


@dataclass
class RedshiftReport(SQLSourceReport):
    # https://forums.aws.amazon.com/ann.jspa?annID=9105
    saas_version: str = ""
    upstream_lineage: Dict[str, List[str]] = field(default_factory=dict)


@platform_name("Redshift")
@config_class(RedshiftConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(
    SourceCapability.USAGE_STATS,
    "Not provided by this module, use `bigquery-usage` for that.",
    supported=False,
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class RedshiftSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views and tables
    - Column types associated with each table
    - Also supports PostGIS extensions
    - Table, row, and column statistics via optional SQL profiling
    - Table lineage

    :::tip

    You can also get fine-grained usage statistics for Redshift using the `redshift-usage` source described below.

    :::

    ### Prerequisites

    This source needs to access system tables that require extra permissions.
    To grant these permissions, please alter your datahub Redshift user the following way:
    ```sql
    ALTER USER datahub_user WITH SYSLOG ACCESS UNRESTRICTED;
    GRANT SELECT ON pg_catalog.svv_table_info to datahub_user;
    GRANT SELECT ON pg_catalog.svl_user_info to datahub_user;
    ```
    :::note

    Giving a user unrestricted access to system tables gives the user visibility to data generated by other users. For example, STL_QUERY and STL_QUERYTEXT contain the full text of INSERT, UPDATE, and DELETE statements.

    :::

    ### Lineage

    There are multiple lineage collector implementations as Redshift does not support table lineage out of the box.

    #### stl_scan_based
    The stl_scan based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) and [stl_scan](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_SCAN.html) system tables to
    discover lineage between tables.
    Pros:
    - Fast
    - Reliable

    Cons:
    - Does not work with Spectrum/external tables because those scans do not show up in stl_scan table.
    - If a table is depending on a view then the view won't be listed as dependency. Instead the table will be connected with the view's dependencies.

    #### sql_based
    The sql_based based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) to discover all the insert queries
    and uses sql parsing to discover the dependecies.

    Pros:
    - Works with Spectrum tables
    - Views are connected properly if a table depends on it

    Cons:
    - Slow.
    - Less reliable as the query parser can fail on certain queries

    #### mixed
    Using both collector above and first applying the sql based and then the stl_scan based one.

    Pros:
    - Works with Spectrum tables
    - Views are connected properly if a table depends on it
    - A bit more reliable than the sql_based one only

    Cons:
    - Slow
    - May be incorrect at times as the query parser can fail on certain queries

    :::note

    The redshift stl redshift tables which are used for getting data lineage only retain approximately two to five days of log history. This means you cannot extract lineage from queries issued outside that window.

    :::

    """

    eskind_to_platform = {1: "glue", 2: "hive", 3: "postgres", 4: "redshift"}

    def __init__(self, config: RedshiftConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "redshift")
        self.catalog_metadata: Dict = {}
        self.config: RedshiftConfig = config
        self._lineage_map: Optional[Dict[str, LineageItem]] = None
        self._all_tables_set: Optional[Set[str]] = None
        self.report: RedshiftReport = RedshiftReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = RedshiftConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_catalog_metadata(self, conn: Connection) -> None:
        try:
            catalog_metadata = _get_external_db_mapping(conn)
        except Exception as e:
            self.error(logger, "external-svv_external_schemas", f"Error was {e}")
            return

        db_name = self.get_db_name()

        external_schema_mapping = {}
        for rel in catalog_metadata:
            if rel.eskind != 1:
                logger.debug(
                    f"Skipping {rel.schemaname} for mapping to external database as currently we only "
                    f"support glue"
                )
                continue
            external_schema_mapping[rel.schemaname] = {
                "eskind": rel.eskind,
                "external_database": rel.databasename,
                "esoptions": rel.esoptions,
                "esoid": rel.esoid,
                "esowner": rel.esowner,
            }
        self.catalog_metadata[db_name] = external_schema_mapping

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.
        engine = self.get_metadata_engine()
        with engine.connect() as conn:
            self.get_catalog_metadata(conn)
            inspector = inspect(conn)
            yield inspector

    def get_metadata_engine(self) -> sqlalchemy.engine.Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        return create_engine(url, **self.config.options)

    def inspect_version(self) -> Any:
        db_engine = self.get_metadata_engine()
        logger.info("Checking current version")
        for db_row in db_engine.execute("select version()"):
            self.report.saas_version = db_row[0]

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        try:
            self.inspect_version()
        except Exception as e:
            self.report.report_failure("version", f"Error: {e}")
            return

        for wu in super().get_workunits():
            yield wu
            if (
                isinstance(wu, SqlWorkUnit)
                and isinstance(wu.metadata, MetadataChangeEvent)
                and isinstance(wu.metadata.proposedSnapshot, DatasetSnapshot)
            ):
                lineage_mcp = None
                lineage_properties_aspect: Optional[DatasetPropertiesClass] = None

                dataset_snapshot: DatasetSnapshotClass = wu.metadata.proposedSnapshot
                assert dataset_snapshot

                if self.config.include_table_lineage:
                    lineage_mcp, lineage_properties_aspect = self.get_lineage_mcp(
                        wu.metadata.proposedSnapshot.urn
                    )

                if lineage_mcp is not None:
                    lineage_wu = MetadataWorkUnit(
                        id=f"redshift-{lineage_mcp.entityUrn}-{lineage_mcp.aspectName}",
                        mcp=lineage_mcp,
                    )
                    self.report.report_workunit(lineage_wu)

                    yield lineage_wu

                if lineage_properties_aspect:
                    aspects = dataset_snapshot.aspects
                    if aspects is None:
                        aspects = []

                    dataset_properties_aspect: Optional[DatasetPropertiesClass] = None

                    for aspect in aspects:
                        if isinstance(aspect, DatasetPropertiesClass):
                            dataset_properties_aspect = aspect

                    if dataset_properties_aspect is None:
                        dataset_properties_aspect = DatasetPropertiesClass()
                        aspects.append(dataset_properties_aspect)

                    custom_properties = (
                        {
                            **dataset_properties_aspect.customProperties,
                            **lineage_properties_aspect.customProperties,
                        }
                        if dataset_properties_aspect.customProperties
                        else lineage_properties_aspect.customProperties
                    )
                    dataset_properties_aspect.customProperties = custom_properties
                    dataset_snapshot.aspects = aspects

                    dataset_snapshot.aspects.append(dataset_properties_aspect)

    def _get_all_tables(self) -> Set[str]:
        all_tables_query: str = """
        select
            table_schema as schemaname,
            table_name as tablename
        from
            pg_catalog.svv_tables
        where
            table_type = 'BASE TABLE'
            and table_schema not in ('information_schema', 'pg_catalog', 'pg_internal')
        union
        select
            distinct schemaname,
            tablename
        from
            svv_external_tables
        union
        SELECT
            n.nspname AS schemaname
            ,c.relname AS tablename
        FROM
            pg_catalog.pg_class AS c
        INNER JOIN
            pg_catalog.pg_namespace AS n
            ON c.relnamespace = n.oid
        WHERE relkind = 'v'
        and
        n.nspname not in ('pg_catalog', 'information_schema')

        """
        db_name = self.get_db_name()
        all_tables_set = set()

        engine = self.get_metadata_engine()
        for db_row in engine.execute(all_tables_query):
            all_tables_set.add(
                f'{db_name}.{db_row["schemaname"]}.{db_row["tablename"]}'
            )

        return all_tables_set

    def _get_sources_from_query(self, db_name: str, query: str) -> List[LineageDataset]:
        sources = list()

        parser = LineageRunner(query)

        for table in parser.source_tables:
            source_schema, source_table = str(table).split(".")
            if source_schema == "<default>":
                source_schema = str(self.config.default_schema)

            source = LineageDataset(
                platform=LineageDatasetPlatform.REDSHIFT,
                path=f"{db_name}.{source_schema}.{source_table}",
            )
            sources.append(source)

        return sources

    def get_db_name(self, inspector: Inspector = None) -> str:
        db_name = getattr(self.config, "database")
        db_alias = getattr(self.config, "database_alias")
        if db_alias:
            db_name = db_alias
        return db_name

    def _populate_lineage_map(
        self, query: str, lineage_type: LineageCollectorType
    ) -> None:
        """
        This method generate table level lineage based with the given query.
        The query should return the following columns: target_schema, target_table, source_table, source_schema
        source_table and source_schema can be omitted if the sql_field is set because then it assumes the source_table
        and source_schema will be extracted from the sql_field by sql parsing.

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

        engine = self.get_metadata_engine()

        db_name = self.get_db_name()

        try:
            for db_row in engine.execute(query):

                if not self.config.schema_pattern.allowed(
                    db_row["target_schema"]
                ) or not self.config.table_pattern.allowed(db_row["target_table"]):
                    continue

                # Target
                target_path = (
                    f'{db_name}.{db_row["target_schema"]}.{db_row["target_table"]}'
                )
                target = LineageItem(
                    dataset=LineageDataset(
                        platform=LineageDatasetPlatform.REDSHIFT, path=target_path
                    ),
                    upstreams=set(),
                    collector_type=lineage_type,
                    query_parser_failed_sqls=list(),
                )

                sources: List[LineageDataset] = list()
                # Source
                if lineage_type in [
                    lineage_type.QUERY_SQL_PARSER,
                    lineage_type.NON_BINDING_VIEW,
                ]:
                    try:
                        sources = self._get_sources_from_query(
                            db_name=db_name, query=db_row["ddl"]
                        )
                    except Exception as e:
                        target.query_parser_failed_sqls.append(db_row["ddl"])
                        self.warn(
                            logger,
                            "parsing-query",
                            f'Error parsing query {db_row["ddl"]} for getting lineage .'
                            f"\nError was {e}.",
                        )
                else:
                    if lineage_type == lineage_type.COPY:
                        platform = LineageDatasetPlatform.S3
                        path = db_row["filename"].strip()
                        if urlparse(path).scheme != "s3":
                            self.warn(
                                logger,
                                "non-s3-lineage",
                                f"Only s3 source supported with copy. The source was: {path}.",
                            )
                            continue
                    else:
                        platform = LineageDatasetPlatform.REDSHIFT
                        path = f'{db_name}.{db_row["source_schema"]}.{db_row["source_table"]}'

                    sources = [
                        LineageDataset(
                            platform=platform,
                            path=path,
                        )
                    ]

                for source in sources:
                    # Filtering out tables which does not exist in Redshift
                    # It was deleted in the meantime or query parser did not capture well the table name
                    if (
                        source.platform == LineageDatasetPlatform.REDSHIFT
                        and source.path not in self._all_tables_set
                    ):
                        self.warn(
                            logger, "missing-table", f"{source.path} missing table"
                        )
                        continue

                    target.upstreams.add(source)

                # Merging downstreams if dataset already exists and has downstreams
                if target.dataset.path in self._lineage_map:

                    self._lineage_map[
                        target.dataset.path
                    ].upstreams = self._lineage_map[
                        target.dataset.path
                    ].upstreams.union(
                        target.upstreams
                    )

                else:
                    self._lineage_map[target.dataset.path] = target

                logger.info(
                    f"Lineage[{target}]:{self._lineage_map[target.dataset.path]}"
                )

        except Exception as e:
            self.warn(logger, f"extract-{lineage_type.name}", f"Error was {e}")

    def _populate_lineage(self) -> None:

        stl_scan_based_lineage_query: str = """
            select
                distinct cluster,
                target_schema,
                target_table,
                username,
                source_schema,
                source_table
            from
                    (
                select
                    distinct tbl as target_table_id,
                    sti.schema as target_schema,
                    sti.table as target_table,
                    sti.database as cluster,
                    query,
                    starttime
                from
                    stl_insert
                join SVV_TABLE_INFO sti on
                    sti.table_id = tbl
                where starttime >= '{start_time}'
                and starttime < '{end_time}'
                and cluster = '{db_name}'
                    ) as target_tables
            join ( (
                select
                    pu.usename::varchar(40) as username,
                    ss.tbl as source_table_id,
                    sti.schema as source_schema,
                    sti.table as source_table,
                    scan_type,
                    sq.query as query
                from
                    (
                    select
                        distinct userid,
                        query,
                        tbl,
                        type as scan_type
                    from
                        stl_scan
                ) ss
                join SVV_TABLE_INFO sti on
                    sti.table_id = ss.tbl
                left join pg_user pu on
                    pu.usesysid = ss.userid
                left join stl_query sq on
                    ss.query = sq.query
                where
                    pu.usename <> 'rdsdb')
            ) as source_tables
                    using (query)
            where
                scan_type in (1, 2, 3)
            order by cluster, target_schema, target_table, starttime asc
        """.format(
            # We need the original database name for filtering
            db_name=self.config.database,
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )
        view_lineage_query = """
            select
                distinct
                srcnsp.nspname as source_schema
                ,
                srcobj.relname as source_table
                ,
                tgtnsp.nspname as target_schema
                ,
                tgtobj.relname as target_table
            from
                pg_catalog.pg_class as srcobj
            inner join
                pg_catalog.pg_depend as srcdep
                    on
                srcobj.oid = srcdep.refobjid
            inner join
                pg_catalog.pg_depend as tgtdep
                    on
                srcdep.objid = tgtdep.objid
            join
                pg_catalog.pg_class as tgtobj
                    on
                tgtdep.refobjid = tgtobj.oid
                and srcobj.oid <> tgtobj.oid
            left outer join
                pg_catalog.pg_namespace as srcnsp
                    on
                srcobj.relnamespace = srcnsp.oid
            left outer join
                pg_catalog.pg_namespace tgtnsp
                    on
                tgtobj.relnamespace = tgtnsp.oid
            where
                tgtdep.deptype = 'i'
                --dependency_internal
                and tgtobj.relkind = 'v'
                --i=index, v=view, s=sequence
                and tgtnsp.nspname not in ('pg_catalog', 'information_schema')
                order by target_schema, target_table asc
        """

        list_late_binding_views_query = """
        SELECT
            n.nspname AS target_schema
            ,c.relname AS target_table
            , COALESCE(pg_get_viewdef(c.oid, TRUE), '') AS ddl
        FROM
            pg_catalog.pg_class AS c
        INNER JOIN
            pg_catalog.pg_namespace AS n
            ON c.relnamespace = n.oid
        WHERE relkind = 'v'
        and ddl like '%%with no schema binding%%'
        and
        n.nspname not in ('pg_catalog', 'information_schema')
        """

        list_insert_create_queries_sql = """
        select
            distinct cluster,
            target_schema,
            target_table,
            username,
            querytxt as ddl
        from
                (
            select
                distinct tbl as target_table_id,
                sti.schema as target_schema,
                sti.table as target_table,
                sti.database as cluster,
                usename as username,
                querytxt,
                si.starttime as starttime
            from
                stl_insert as si
            join SVV_TABLE_INFO sti on
                sti.table_id = tbl
            left join pg_user pu on
                pu.usesysid = si.userid
            left join stl_query sq on
                si.query = sq.query
            left join stl_load_commits slc on
                slc.query = si.query
            where
                pu.usename <> 'rdsdb'
                and sq.aborted = 0
                and slc.query IS NULL
                and cluster = '{db_name}'
                and si.starttime >= '{start_time}'
                and si.starttime < '{end_time}'
            ) as target_tables
            order by cluster, target_schema, target_table, starttime asc
        """.format(
            # We need the original database name for filtering
            db_name=self.config.database,
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )

        list_copy_commands_sql = """
        select
            distinct
                "schema" as target_schema,
                "table" as target_table,
                filename
        from
            stl_insert as si
        join stl_load_commits as c on
            si.query = c.query
        join SVV_TABLE_INFO sti on
            sti.table_id = tbl
        where
            database = '{db_name}'
            and si.starttime >= '{start_time}'
            and si.starttime < '{end_time}'
        order by target_schema, target_table, starttime asc
        """.format(
            # We need the original database name for filtering
            db_name=self.config.database,
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )

        if not self._lineage_map:
            self._lineage_map = defaultdict()

        if self.config.table_lineage_mode == LineageMode.STL_SCAN_BASED:
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            self._populate_lineage_map(
                query=stl_scan_based_lineage_query,
                lineage_type=LineageCollectorType.QUERY_SCAN,
            )
        elif self.config.table_lineage_mode == LineageMode.SQL_BASED:
            # Populate table level lineage by parsing table creating sqls
            self._populate_lineage_map(
                query=list_insert_create_queries_sql,
                lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
            )
        elif self.config.table_lineage_mode == LineageMode.MIXED:
            # Populate table level lineage by parsing table creating sqls
            self._populate_lineage_map(
                query=list_insert_create_queries_sql,
                lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
            )
            # Populate table level lineage by getting upstream tables from stl_scan redshift table
            self._populate_lineage_map(
                query=stl_scan_based_lineage_query,
                lineage_type=LineageCollectorType.QUERY_SCAN,
            )

        if self.config.include_views:
            # Populate table level lineage for views
            self._populate_lineage_map(
                query=view_lineage_query, lineage_type=LineageCollectorType.VIEW
            )

            # Populate table level lineage for late binding views
            self._populate_lineage_map(
                query=list_late_binding_views_query,
                lineage_type=LineageCollectorType.NON_BINDING_VIEW,
            )
        if self.config.include_copy_lineage:
            self._populate_lineage_map(
                query=list_copy_commands_sql, lineage_type=LineageCollectorType.COPY
            )

    def get_lineage_mcp(
        self, dataset_urn: str
    ) -> Tuple[
        Optional[MetadataChangeProposalWrapper], Optional[DatasetPropertiesClass]
    ]:
        dataset_key = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return None, None

        if self._lineage_map is None:
            logger.debug("Populating lineage")
            self._populate_lineage()
        assert self._lineage_map is not None

        upstream_lineage: List[UpstreamClass] = []
        custom_properties: Dict[str, str] = {}

        if dataset_key.name in self._lineage_map:
            item = self._lineage_map[dataset_key.name]
            if (
                self.config.capture_lineage_query_parser_failures
                and item.query_parser_failed_sqls
            ):
                custom_properties["lineage_sql_parser_failed_queries"] = ",".join(
                    item.query_parser_failed_sqls
                )
            for upstream in item.upstreams:
                upstream_table = UpstreamClass(
                    dataset=builder.make_dataset_urn_with_platform_instance(
                        upstream.platform.value,
                        upstream.path,
                        platform_instance=self.config.platform_instance_map.get(
                            upstream.platform.value
                        )
                        if self.config.platform_instance_map
                        else None,
                        env=self.config.env,
                    ),
                    type=item.dataset_lineage_type,
                )
                upstream_lineage.append(upstream_table)

        dataset_params = dataset_key.name.split(".")
        db_name = dataset_params[0]
        schemaname = dataset_params[1]
        tablename = dataset_params[2]
        if db_name in self.catalog_metadata:
            if schemaname in self.catalog_metadata[db_name]:
                external_db_params = self.catalog_metadata[db_name][schemaname]
                upstream_platform = self.eskind_to_platform[
                    external_db_params["eskind"]
                ]
                catalog_upstream = UpstreamClass(
                    mce_builder.make_dataset_urn_with_platform_instance(
                        upstream_platform,
                        "{database}.{table}".format(
                            database=external_db_params["external_database"],
                            table=tablename,
                        ),
                        platform_instance=self.config.platform_instance_map.get(
                            upstream_platform
                        )
                        if self.config.platform_instance_map
                        else None,
                        env=self.config.env,
                    ),
                    DatasetLineageTypeClass.COPY,
                )
                upstream_lineage.append(catalog_upstream)

        properties = None
        if custom_properties:
            properties = DatasetPropertiesClass(customProperties=custom_properties)

        if upstream_lineage:
            self.report.upstream_lineage[dataset_urn] = [
                u.dataset for u in upstream_lineage
            ]
        else:
            return None, properties

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspectName="upstreamLineage",
            aspect=UpstreamLineage(upstreams=upstream_lineage),
        )

        return mcp, properties
