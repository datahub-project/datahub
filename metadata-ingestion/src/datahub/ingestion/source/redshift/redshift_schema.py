import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import redshift_connector

from datahub.ingestion.source.redshift.query import RedshiftQuery
from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column

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
class RedshiftView(BaseTable):
    type: Optional[str] = None
    columns: List[RedshiftColumn] = field(default_factory=list)
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


@dataclass
class LineageRow:
    source_schema: Optional[str]
    source_table: Optional[str]
    target_schema: Optional[str]
    target_table: Optional[str]
    ddl: Optional[str]
    filename: Optional[str]


# this is a class to be a proxy to query Redshift
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
            RedshiftQuery.list_databases,
        )

        dbs = cursor.fetchall()

        return [db[0] for db in dbs]

    @staticmethod
    def get_schemas(
        conn: redshift_connector.Connection, database: str
    ) -> List[RedshiftSchema]:
        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftQuery.list_schemas.format(database_name=database),
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
    ) -> Dict[str, Dict[str, RedshiftExtraTableMeta]]:
        cur = RedshiftDataDictionary.get_query_result(
            conn, RedshiftQuery.additional_table_metadata
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
                table_enrich.setdefault(table_meta.schema, {})

            table_enrich[table_meta.schema][table_meta.table] = table_meta

        return table_enrich

    @staticmethod
    def get_tables_and_views(
        conn: redshift_connector.Connection,
    ) -> Tuple[Dict[str, List[RedshiftTable]], Dict[str, List[RedshiftView]]]:
        tables: Dict[str, List[RedshiftTable]] = {}
        views: Dict[str, List[RedshiftView]] = {}

        # This query needs to run separately as we can't join with the main query because it works with
        # driver only functions.
        enriched_table = RedshiftDataDictionary.enrich_tables(conn)

        cur = RedshiftDataDictionary.get_query_result(conn, RedshiftQuery.list_tables)
        field_names = [i[0] for i in cur.description]
        db_tables = cur.fetchall()
        logger.info(f"Fetched {len(db_tables)} tables/views from Redshift")
        for table in db_tables:
            schema = table[field_names.index("schema")]
            if table[field_names.index("tabletype")] not in [
                "MATERIALIZED VIEW",
                "VIEW",
            ]:
                if schema not in tables:
                    tables.setdefault(schema, [])
                table_name = table[field_names.index("relname")]

                creation_time: Optional[datetime] = None
                if table[field_names.index("creation_time")]:
                    creation_time = table[field_names.index("creation_time")].replace(
                        tzinfo=timezone.utc
                    )

                last_altered: Optional[datetime] = None
                size_in_bytes: Optional[int] = None
                rows_count: Optional[int] = None
                if schema in enriched_table and table_name in enriched_table[schema]:
                    if enriched_table[schema][table_name].last_accessed:
                        # Mypy seems to be not clever enough to understand the above check
                        last_accessed = enriched_table[schema][table_name].last_accessed
                        assert last_accessed
                        last_altered = last_accessed.replace(tzinfo=timezone.utc)
                    elif creation_time:
                        last_altered = creation_time

                    if enriched_table[schema][table_name].size:
                        # Mypy seems to be not clever enough to understand the above check
                        size = enriched_table[schema][table_name].size
                        if size:
                            size_in_bytes = size * 1024 * 1024

                    if enriched_table[schema][table_name].estimated_visible_rows:
                        rows = enriched_table[schema][table_name].estimated_visible_rows
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
                        comment=table[field_names.index("table_description")],
                    )
                )
            else:
                if schema not in views:
                    views[schema] = []

                views[schema].append(
                    RedshiftView(
                        type=table[field_names.index("tabletype")],
                        name=table[field_names.index("relname")],
                        ddl=table[field_names.index("view_definition")],
                        created=table[field_names.index("creation_time")],
                        comment=table[field_names.index("table_description")],
                    )
                )
        for schema_key, schema_tables in tables.items():
            logger.info(
                f"In schema: {schema_key} discovered {len(schema_tables)} tables"
            )
        for schema_key, schema_views in views.items():
            logger.info(f"In schema: {schema_key} discovered {len(schema_views)} views")

        return tables, views

    @staticmethod
    def get_schema_fields_for_column(
        column: RedshiftColumn,
    ) -> List[SchemaField]:
        return get_schema_fields_for_hive_column(
            column.name,
            column.data_type.lower(),
            description=column.comment,
            default_nullable=True,
        )

    @staticmethod
    def get_columns_for_schema(
        conn: redshift_connector.Connection, schema: RedshiftSchema
    ) -> Dict[str, List[RedshiftColumn]]:
        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftQuery.list_columns.format(schema_name=schema.name),
        )

        table_columns: Dict[str, List[RedshiftColumn]] = {}

        field_names = [i[0] for i in cursor.description]
        columns = cursor.fetchmany()
        while columns:
            for column in columns:
                table = column[field_names.index("table_name")]
                if table not in table_columns:
                    table_columns.setdefault(table, [])

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

    @staticmethod
    def get_lineage_rows(
        conn: redshift_connector.Connection,
        query: str,
    ) -> Iterable[LineageRow]:
        cursor = conn.cursor()
        cursor.execute(query)
        field_names = [i[0] for i in cursor.description]

        rows = cursor.fetchmany()
        while rows:
            for row in rows:
                yield LineageRow(
                    source_schema=row[field_names.index("source_schema")]
                    if "source_schema" in field_names
                    else None,
                    source_table=row[field_names.index("source_table")]
                    if "source_table" in field_names
                    else None,
                    target_schema=row[field_names.index("target_schema")]
                    if "target_schema" in field_names
                    else None,
                    target_table=row[field_names.index("target_table")]
                    if "target_table" in field_names
                    else None,
                    ddl=row[field_names.index("ddl")] if "ddl" in field_names else None,
                    filename=row[field_names.index("filename")]
                    if "filename" in field_names
                    else None,
                )
            rows = cursor.fetchmany()
