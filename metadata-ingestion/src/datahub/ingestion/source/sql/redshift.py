from collections import defaultdict
from typing import Dict, Iterable, Optional, Union

# These imports verify that the dependencies are available.
import psycopg2  # noqa: F401
import sqlalchemy_redshift  # noqa: F401
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Connection, reflection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy_redshift.dialect import RedshiftDialect, RelationKey

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    logger,
)

# TRICKY: it's necessary to import the Postgres source because
# that module has some side effects that we care about here.
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)


class RedshiftConfig(PostgresConfig):
    # Although Amazon Redshift is compatible with Postgres's wire format,
    # we actually want to use the sqlalchemy-redshift package and dialect
    # because it has better caching behavior. In particular, it queries
    # the full table, column, and constraint information in a single larger
    # query, and then simply pulls out the relevant information as needed.
    # Because of this behavior, it uses dramatically fewer round trips for
    # large Redshift warehouses. As an example, see this query for the columns:
    # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/60b4db04c1d26071c291aeea52f1dcb5dd8b0eb0/sqlalchemy_redshift/dialect.py#L745.
    scheme = "redshift+psycopg2"


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
                   replace(
                   replace(
                   replace(
                   replace(
                   replace(external_type, 'decimal', 'numeric'),
                    'varchar', 'character varying'),
                    'char(', 'character('),
                    'float', 'real'),
                    'double', 'float')
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
                   replace(
                   replace(
                   replace(
                   replace(
                   replace(external_type, 'decimal', 'numeric'),
                    'varchar', 'character varying'),
                    'char(', 'character('),
                    'float', 'real'),
                    'double', 'float')
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
    try:
        result = connection.execute(
            """
            select * from svv_external_schemas
            """
        )
        return result
    except Exception as e:
        logger.error(
            "Error querying svv_external_schemas to get external database mapping.", e
        )
        return None


# This monkey-patching enables us to batch fetch the table descriptions, rather than
# fetching them one at a time.
RedshiftDialect._get_all_table_comments = _get_all_table_comments
RedshiftDialect.get_table_comment = get_table_comment
RedshiftDialect._get_all_relation_info = _get_all_relation_info
RedshiftDialect._get_schema_column_info = _get_schema_column_info


class RedshiftSource(SQLAlchemySource):
    catalog_metadata: Dict = {}
    eskind_to_platform = {1: "glue", 2: "hive", 3: "postgres", 4: "redshift"}

    def __init__(self, config: RedshiftConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "redshift")

    @classmethod
    def create(cls, config_dict, ctx):
        config = RedshiftConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_catalog_metadata(self, conn: Connection) -> None:
        catalog_metadata = _get_external_db_mapping(conn)
        if catalog_metadata is None:
            return
        db_name = getattr(self.config, "database")
        db_alias = getattr(self.config, "database_alias")
        if db_alias:
            db_name = db_alias

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
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            self.get_catalog_metadata(conn)
            inspector = inspect(conn)
            yield inspector

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        for wu in super().get_workunits():
            yield wu
            if isinstance(wu, SqlWorkUnit) and isinstance(
                wu.metadata, MetadataChangeEvent
            ):
                lineage_mcp = self.get_lineage_mcp(wu.metadata.proposedSnapshot.urn)
                if lineage_mcp is not None:
                    lineage_wu = MetadataWorkUnit(
                        id=f"redshift-{lineage_mcp.entityUrn}-{lineage_mcp.aspectName}",
                        mcp=lineage_mcp,
                    )
                    self.report.report_workunit(lineage_wu)
                    yield lineage_wu

    def get_lineage_mcp(
        self, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        dataset_key = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return None

        dataset_params = dataset_key.name.split(".")
        db_name = dataset_params[0]
        schemaname = dataset_params[1]
        tablename = dataset_params[2]
        if db_name in self.catalog_metadata:
            if schemaname in self.catalog_metadata[db_name]:
                external_db_params = self.catalog_metadata[db_name][schemaname]
                upstream_lineage = UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            mce_builder.make_dataset_urn(
                                self.eskind_to_platform[external_db_params["eskind"]],
                                "{database}.{table}".format(
                                    database=external_db_params["external_database"],
                                    table=tablename,
                                ),
                                self.config.env,
                            ),
                            DatasetLineageTypeClass.COPY,
                        )
                    ]
                )
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                return mcp
        return None
