import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import redshift_connector

from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class RedshiftColumn(BaseColumn):
    dist_key: bool = False
    sort_key: bool = False
    default: Optional[str] = None
    encode: Optional[str] = None


@dataclass
class RedshiftTable(BaseTable):
    type: Optional[str] = None
    schema: Optional[str] = None
    dist_style: Optional[str] = None
    description: Optional[str] = None
    columns: List[RedshiftColumn] = field(default_factory=list)
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None
    location: Optional[str] = None
    parameters: Optional[str] = None
    input_parameters: Optional[str] = None
    output_parameters: Optional[str] = None
    serde_parameters: Optional[str] = None
    last_altered: Optional[datetime] = None


@dataclass
class RedshiftView:
    type: str
    name: str
    schema: str
    ddl: str
    columns: List[RedshiftColumn] = field(default_factory=list)
    description: Optional[str] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None


@dataclass
class RedshiftSchema:
    name: str
    database: str
    type: str
    owner: Optional[str] = None
    option: Optional[str] = None
    external_database: Optional[str] = None


@dataclass
class RedshiftExtraTableMeta:
    database: str
    schema: str
    table: str
    size: Optional[int] = None
    tbl_rows: Optional[int] = None
    estimated_visible_rows: Optional[int] = None
    skew_rows: Optional[float] = None
    last_accessed: Optional[datetime] = None


class RedshiftMetadatQueries:
    list_databases: str = """SELECT datname FROM pg_database
        WHERE (datname <> ('padb_harvest')::name)
        AND (datname <> ('template0')::name)
        AND (datname <> ('template1')::name)
        """

    list_schemas: str = """SELECT database_name,
        schema_name,
        schema_type,
        usename as schema_owner_name,
        schema_option,
        NULL::varchar(255) as external_database
        FROM SVV_REDSHIFT_SCHEMAS as s
        inner join pg_catalog.pg_user_info as i on i.usesysid = s.schema_owner
        where schema_name !~ '^pg_'
        AND   schema_name != 'information_schema'
UNION ALL
SELECT null as database_name,
        schemaname as schema_name,
        CASE s.eskind
            WHEN '1' THEN 'GLUE'
            WHEN '2' THEN 'HIVE'
            WHEN '3' THEN 'POSTGRES'
            WHEN '4' THEN 'REDSHIFT'
            ELSE 'OTHER'
        END as schema_type,
        usename as schema_owner_name,
        esoptions as schema_option,
        databasename as external_database
        FROM SVV_EXTERNAL_SCHEMAS as s
        inner join pg_catalog.pg_user_info as i on i.usesysid = s.esowner
        ORDER BY database_name,
            SCHEMA_NAME;
        """

    list_tables: str = """
 SELECT  CASE c.relkind
                WHEN 'r' THEN 'TABLE'
                WHEN 'v' THEN 'VIEW'
                WHEN 'm' THEN 'MATERIALIZED VIEW'
                WHEN 'f' THEN 'FOREIGN TABLE'
            END AS tabletype,
            n.oid AS "schema_oid",
            n.nspname AS "schema",
            c.oid AS "rel_oid",
            c.relname,
            ci.relcreationtime as creation_time,
            CASE c.reldiststyle
                WHEN 0 THEN 'EVEN'
                WHEN 1 THEN 'KEY'
                WHEN 8 THEN 'ALL'
            END AS "diststyle",
            c.relowner AS "owner_id",
            u.usename AS "owner_name",
            TRIM(TRAILING ';' FROM pg_catalog.pg_get_viewdef (c.oid,TRUE)) AS "view_definition",
            pg_catalog.array_to_string(c.relacl,'\n') AS "privileges",
            NULL as "location",
            NULL as parameters,
            NULL as input_format,
            NULL As output_format,
            NULL as serde_parameters,
            pgd.description as table_description
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_class_info as ci on c.oid = ci.reloid
        LEFT JOIN pg_catalog.pg_description pgd ON pgd.objsubid = 0 AND pgd.objoid = c.oid
        JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
        WHERE c.relkind IN ('r','v','m','S','f')
        AND   n.nspname !~ '^pg_'
        AND   n.nspname != 'information_schema'
        UNION
        SELECT 'EXTERNAL_TABLE' as tabletype,
            NULL AS "schema_oid",
            schemaname AS "schema",
            NULL AS "rel_oid",
            tablename AS "relname",
            NULL as "creation_time",
            NULL AS "diststyle",
            NULL AS "owner_id",
            NULL AS "owner_name",
            NULL AS "view_definition",
            NULL AS "privileges",
            "location",
            parameters,
            input_format,
            output_format,
            serde_parameters,
            NULL as table_description
        FROM pg_catalog.svv_external_tables
        ORDER BY "schema",
                "relname";
"""
    list_columns: str = """
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
              pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) as default,
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
              AND   n.nspname != 'information_schema'
              AND att.attnum > 0
              AND NOT att.attisdropped
              and schema = '{schema_name}'
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
            WHERE 1 and schema = '{schema_name}'
            UNION
            SELECT
              schemaname as "schema",
              tablename as "table_name",
              columnname as "name",
              null as "encode",
              -- Spectrum represents data types differently.
              -- Standardize, so we can infer types.
              external_type AS "type",
              null as "distkey",
              0 as "sortkey",
              null as "notnull",
              null as "comment",
              null as "adsrc",
              null as "attnum",
              external_type AS "format_type",
              null as "default",
              null as "schema_oid",
              null as "table_oid"
            FROM SVV_EXTERNAL_COLUMNS
            WHERE 1 and schema = '{schema_name}'
            ORDER BY "schema", "table_name", "attnum"
"""

    additional_table_metadata: str = """
        select
            database,
            schema,
            "table",
            size,
            tbl_rows,
            estimated_visible_rows,
            skew_rows,
            last_accessed
        from
            pg_catalog.svv_table_info as ti
        left join (
            select
                tbl,
                max(endtime) as last_accessed
            from
                pg_catalog.stl_insert
            group by
                tbl) as la on
            (la.tbl = ti.table_id)
       ;
    """


class RedshiftDataDictionary:
    @staticmethod
    def get_query_result(
        conn: redshift_connector.Connection, query: str
    ) -> redshift_connector.Cursor:
        cursor: redshift_connector.Cursor = conn.cursor()

        logger.debug(f"Query : {query}")
        cursor.execute(query)
        return cursor

    @staticmethod
    def get_databases(conn: redshift_connector.Connection) -> List[str]:

        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftMetadatQueries.list_databases,
        )

        dbs = cursor.fetchall()

        return [db[0] for db in dbs]

    @staticmethod
    def get_schemas(
        conn: redshift_connector.Connection, database: str
    ) -> List[RedshiftSchema]:

        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftMetadatQueries.list_schemas.format(database_name=database),
        )

        schemas = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]

        return [
            RedshiftSchema(
                database=database,
                name=schema[field_names.index("schema_name")],
                type=schema[field_names.index("schema_type")],
                owner=schema[field_names.index("schema_owner_name")],
                option=schema[field_names.index("schema_option")],
                external_database=schema[field_names.index("external_database")],
            )
            for schema in schemas
        ]

    @staticmethod
    def enrich_tables(
        conn: redshift_connector.Connection,
    ) -> (Dict[str, Dict[str, RedshiftExtraTableMeta]]):

        cur = RedshiftDataDictionary.get_query_result(
            conn, RedshiftMetadatQueries.additional_table_metadata
        )
        field_names = [i[0] for i in cur.description]
        db_table_metadata = cur.fetchall()

        table_enrich: Dict[str, Dict[str, RedshiftExtraTableMeta]] = {}
        for meta in db_table_metadata:
            table_meta: RedshiftExtraTableMeta = RedshiftExtraTableMeta(
                database=meta[field_names.index("database")],
                schema=meta[field_names.index("schema")],
                table=meta[field_names.index("table")],
                size=meta[field_names.index("size")],
                tbl_rows=meta[field_names.index("tbl_rows")],
                estimated_visible_rows=meta[
                    field_names.index("estimated_visible_rows")
                ],
                skew_rows=meta[field_names.index("skew_rows")],
                last_accessed=meta[field_names.index("last_accessed")],
            )
            if table_meta.schema not in table_enrich:
                table_enrich[table_meta.schema] = {}

            table_enrich[table_meta.schema][table_meta.table] = table_meta

        return table_enrich

    @staticmethod
    def get_tables_and_views(
        conn: redshift_connector.Connection,
    ) -> Tuple[Dict[str, List[RedshiftTable]], Dict[str, List[RedshiftView]]]:
        tables: Dict[str, List[RedshiftTable]] = {}
        views: Dict[str, List[RedshiftView]] = {}

        # This query needs to run separately as we can't join witht the main query because it works with
        # driver only functions.
        enrich_metada = RedshiftDataDictionary.enrich_tables(conn)

        cur = RedshiftDataDictionary.get_query_result(
            conn, RedshiftMetadatQueries.list_tables
        )
        field_names = [i[0] for i in cur.description]
        db_tables = cur.fetchall()

        for table in db_tables:
            schema = table[field_names.index("schema")]
            if table[field_names.index("tabletype")] not in [
                "MATERIALIZED VIEW",
                "VIEW",
            ]:
                if schema not in tables:
                    tables[schema] = []
                table_name = table[field_names.index("relname")]

                creation_time: Optional[datetime] = None
                if table[field_names.index("creation_time")]:
                    creation_time = table[field_names.index("creation_time")].replace(
                        tzinfo=timezone.utc
                    )

                last_altered: Optional[datetime] = None
                size_in_bytes: Optional[int] = None
                rows_count: Optional[int] = None
                if schema in enrich_metada and table_name in enrich_metada[schema]:

                    if enrich_metada[schema][table_name].last_accessed:
                        # Mypy seems to be not clever enough to understand the above check
                        last_accessed = enrich_metada[schema][table_name].last_accessed
                        assert last_accessed
                        last_altered = last_accessed.replace(tzinfo=timezone.utc)
                    elif creation_time:
                        last_altered = creation_time

                    if enrich_metada[schema][table_name].size:
                        # Mypy seems to be not clever enough to understand the above check
                        size = enrich_metada[schema][table_name].size
                        assert size
                        size_in_bytes = size * 1024 * 1024

                    if enrich_metada[schema][table_name].estimated_visible_rows:
                        rows = enrich_metada[schema][table_name].estimated_visible_rows
                        assert rows
                        rows_count = int(rows)

                tables[schema].append(
                    RedshiftTable(
                        type=table[field_names.index("tabletype")],
                        created=creation_time,
                        last_altered=last_altered,
                        name=table_name,
                        schema=table[field_names.index("schema")],
                        size_in_bytes=size_in_bytes,
                        rows_count=rows_count,
                        dist_style=table[field_names.index("diststyle")],
                        location=table[field_names.index("location")],
                        parameters=table[field_names.index("parameters")],
                        input_parameters=table[field_names.index("input_format")],
                        output_parameters=table[field_names.index("output_format")],
                        serde_parameters=table[field_names.index("serde_parameters")],
                        description=table[field_names.index("table_description")],
                    )
                )
            else:
                if schema not in views:
                    views[schema] = []

                views[schema].append(
                    RedshiftView(
                        type=table[field_names.index("tabletype")],
                        name=table[field_names.index("relname")],
                        schema=table[field_names.index("schema")],
                        ddl=table[field_names.index("view_definition")],
                        created=table[field_names.index("creation_time")],
                        description=table[field_names.index("table_description")],
                    )
                )

        return tables, views

    @staticmethod
    def get_columns_for_schema(
        conn: redshift_connector.Connection, schema: RedshiftSchema
    ) -> Dict[str, List[RedshiftColumn]]:

        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftMetadatQueries.list_columns.format(schema_name=schema.name),
        )

        table_columns: Dict[str, List[RedshiftColumn]] = {}

        field_names = [i[0] for i in cursor.description]
        columns = cursor.fetchmany()
        while columns:
            for column in columns:
                table = column[field_names.index("table_name")]
                if table not in table_columns:
                    table_columns[table] = []

                column = RedshiftColumn(
                    name=column[field_names.index("name")],
                    ordinal_position=column[field_names.index("attnum")],
                    data_type=str(column[field_names.index("type")]).upper(),
                    comment=column[field_names.index("comment")],
                    is_nullable=not column[field_names.index("notnull")],
                    default=column[field_names.index("default")],
                    dist_key=column[field_names.index("distkey")],
                    sort_key=column[field_names.index("sortkey")],
                    encode=column[field_names.index("encode")],
                )

                table_columns[table].append(column)
            columns = cursor.fetchmany()

        return table_columns
