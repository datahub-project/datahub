import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple, Union

import redshift_connector

from datahub.ingestion.source.redshift.query import (
    RedshiftCommonQuery,
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
)
from datahub.ingestion.source.sql.sql_generic import BaseColumn, BaseTable
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult
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

    def is_external_table(self) -> bool:
        return self.type == "EXTERNAL_TABLE"


@dataclass
class RedshiftView(BaseTable):
    type: Optional[str] = None
    materialized: bool = False
    columns: List[RedshiftColumn] = field(default_factory=list)
    last_altered: Optional[datetime] = None
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None

    def is_external_table(self) -> bool:
        return self.type == "EXTERNAL_TABLE"


@dataclass
class RedshiftSchema:
    name: str
    database: str
    type: str
    owner: Optional[str] = None
    option: Optional[str] = None
    external_platform: Optional[str] = None
    external_database: Optional[str] = None

    def is_external_schema(self) -> bool:
        return self.type == "external"

    def get_upstream_schema_name(self) -> Optional[str]:
        """Gets the schema name from the external schema option.

        Returns:
            Optional[str]: The schema name from the external schema option
            if this is an external schema and has a valid option format, None otherwise.
        """

        if not self.is_external_schema() or not self.option:
            return None

        # For external schema on redshift, option is in form
        # {"SCHEMA":"tickit"}
        schema_match = re.search(r'"SCHEMA"\s*:\s*"([^"]*)"', self.option)
        if not schema_match:
            return None
        else:
            return schema_match.group(1)


@dataclass
class PartialInboundDatashare:
    share_name: str
    producer_namespace_prefix: str
    consumer_database: str

    def get_description(self) -> str:
        return (
            f"Namespace Prefix {self.producer_namespace_prefix} Share {self.share_name}"
        )


@dataclass
class OutboundDatashare:
    share_name: str
    producer_namespace: str
    source_database: str

    def get_key(self) -> str:
        return f"{self.producer_namespace}.{self.share_name}"


@dataclass
class InboundDatashare:
    share_name: str
    producer_namespace: str
    consumer_database: str

    def get_key(self) -> str:
        return f"{self.producer_namespace}.{self.share_name}"

    def get_description(self) -> str:
        return f"Namespace {self.producer_namespace} Share {self.share_name}"


@dataclass
class RedshiftDatabase:
    name: str
    type: str
    options: Optional[str] = None

    def is_shared_database(self) -> bool:
        return self.type == "shared"

    # NOTE: ideally options are in form
    # {"datashare_name":"xxx","datashare_producer_account":"1234","datashare_producer_namespace":"yyy"}
    # however due to varchar(128) type of database table that captures options
    # we may receive only partial information about inbound share
    def get_inbound_share(
        self,
    ) -> Optional[Union[InboundDatashare, PartialInboundDatashare]]:
        if not self.is_shared_database() or not self.options:
            return None

        # Convert into single regex ??
        share_name_match = re.search(r'"datashare_name"\s*:\s*"([^"]*)"', self.options)
        namespace_match = re.search(
            r'"datashare_producer_namespace"\s*:\s*"([^"]*)"', self.options
        )
        partial_namespace_match = re.search(
            r'"datashare_producer_namespace"\s*:\s*"([^"]*)$', self.options
        )

        if not share_name_match:
            # We will always at least get share name
            return None

        share_name = share_name_match.group(1)
        if namespace_match:
            return InboundDatashare(
                share_name=share_name,
                producer_namespace=namespace_match.group(1),
                consumer_database=self.name,
            )
        elif partial_namespace_match:
            return PartialInboundDatashare(
                share_name=share_name,
                producer_namespace_prefix=partial_namespace_match.group(1),
                consumer_database=self.name,
            )
        else:
            return PartialInboundDatashare(
                share_name=share_name,
                producer_namespace_prefix="",
                consumer_database=self.name,
            )


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
    is_materialized: bool = False


@dataclass
class LineageRow:
    source_schema: Optional[str]
    source_table: Optional[str]
    target_schema: Optional[str]
    target_table: Optional[str]
    ddl: Optional[str]
    filename: Optional[str]
    timestamp: Optional[datetime]
    session_id: Optional[str]


@dataclass
class TempTableRow:
    transaction_id: int
    session_id: str
    query_text: str
    create_command: str
    start_time: datetime
    urn: Optional[str]
    parsed_result: Optional[SqlParsingResult] = None


@dataclass
class AlterTableRow:
    # TODO unify this type with TempTableRow
    transaction_id: int
    session_id: str
    query_text: str
    start_time: datetime


def _stringy(x: Optional[int]) -> Optional[str]:
    if x is None:
        return None
    return str(x)


# this is a class to be a proxy to query Redshift
class RedshiftDataDictionary:
    def __init__(self, is_serverless):
        self.queries: RedshiftCommonQuery = RedshiftProvisionedQuery()
        if is_serverless:
            self.queries = RedshiftServerlessQuery()

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
            RedshiftCommonQuery.list_databases,
        )

        dbs = cursor.fetchall()

        return [db[0] for db in dbs]

    @staticmethod
    def get_database_details(
        conn: redshift_connector.Connection, database: str
    ) -> Optional[RedshiftDatabase]:
        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftCommonQuery.get_database_details(database),
        )

        row = cursor.fetchone()
        if row is None:
            return None
        return RedshiftDatabase(
            name=database,
            type=row[1],
            options=row[2],
        )

    @staticmethod
    def get_schemas(
        conn: redshift_connector.Connection, database: str
    ) -> List[RedshiftSchema]:
        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftCommonQuery.list_schemas(database),
        )

        schemas = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]

        return [
            RedshiftSchema(
                database=database,
                name=schema[field_names.index("schema_name")],
                type=schema[field_names.index("schema_type")],
                option=schema[field_names.index("schema_option")],
                external_platform=schema[field_names.index("external_platform")],
                external_database=schema[field_names.index("external_database")],
            )
            for schema in schemas
        ]

    def enrich_tables(
        self,
        conn: redshift_connector.Connection,
    ) -> Dict[str, Dict[str, RedshiftExtraTableMeta]]:
        # Warning: This table enrichment will not return anything for
        # external tables (spectrum) and for tables that have never been queried / written to.
        cur = RedshiftDataDictionary.get_query_result(
            conn, self.queries.additional_table_metadata_query()
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
                is_materialized=meta[field_names.index("is_materialized")],
            )
            if table_meta.schema not in table_enrich:
                table_enrich.setdefault(table_meta.schema, {})

            table_enrich[table_meta.schema][table_meta.table] = table_meta

        return table_enrich

    def get_tables_and_views(
        self,
        conn: redshift_connector.Connection,
        database: str,
        skip_external_tables: bool = False,
        is_shared_database: bool = False,
    ) -> Tuple[Dict[str, List[RedshiftTable]], Dict[str, List[RedshiftView]]]:
        tables: Dict[str, List[RedshiftTable]] = {}
        views: Dict[str, List[RedshiftView]] = {}

        # This query needs to run separately as we can't join with the main query because it works with
        # driver only functions.
        enriched_tables = self.enrich_tables(conn)

        cur = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftCommonQuery.list_tables(
                database=database,
                skip_external_tables=skip_external_tables,
                is_shared_database=is_shared_database,
            ),
        )
        field_names = [i[0] for i in cur.description]
        db_tables = cur.fetchall()
        logger.info(f"Fetched {len(db_tables)} tables/views from Redshift")

        for table in db_tables:
            schema = table[field_names.index("schema")]
            table_name = table[field_names.index("relname")]

            if table[field_names.index("tabletype")] not in [
                "MATERIALIZED VIEW",
                "VIEW",
            ]:
                if schema not in tables:
                    tables.setdefault(schema, [])

                (
                    creation_time,
                    last_altered,
                    rows_count,
                    size_in_bytes,
                ) = RedshiftDataDictionary.get_table_stats(
                    enriched_tables, field_names, schema, table
                )

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
                (
                    creation_time,
                    last_altered,
                    rows_count,
                    size_in_bytes,
                ) = RedshiftDataDictionary.get_table_stats(
                    enriched_tables=enriched_tables,
                    field_names=field_names,
                    schema=schema,
                    table=table,
                )

                materialized = False
                if schema in enriched_tables and table_name in enriched_tables[schema]:
                    if enriched_tables[schema][table_name].is_materialized:
                        materialized = True

                views[schema].append(
                    RedshiftView(
                        type=table[field_names.index("tabletype")],
                        name=table[field_names.index("relname")],
                        ddl=table[field_names.index("view_definition")],
                        created=creation_time,
                        comment=table[field_names.index("table_description")],
                        last_altered=last_altered,
                        size_in_bytes=size_in_bytes,
                        rows_count=rows_count,
                        materialized=materialized,
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
    def get_table_stats(enriched_tables, field_names, schema, table):
        table_name = table[field_names.index("relname")]

        creation_time: Optional[datetime] = None
        if table[field_names.index("creation_time")]:
            creation_time = table[field_names.index("creation_time")].replace(
                tzinfo=timezone.utc
            )
        last_altered: Optional[datetime] = None
        size_in_bytes: Optional[int] = None
        rows_count: Optional[int] = None
        if schema in enriched_tables and table_name in enriched_tables[schema]:
            if (
                last_accessed := enriched_tables[schema][table_name].last_accessed
            ) is not None:
                last_altered = last_accessed.replace(tzinfo=timezone.utc)
            elif creation_time:
                last_altered = creation_time

            if (size := enriched_tables[schema][table_name].size) is not None:
                size_in_bytes = size * 1024 * 1024

            if (
                rows := enriched_tables[schema][table_name].estimated_visible_rows
            ) is not None:
                rows_count = int(rows)
        else:
            # The object was not found in the enriched data.
            #
            # If we don't have enriched data, it may be either because:
            #   1 The table is empty (as per https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html) empty tables are omitted from svv_table_info.
            #   2. The table is external
            #   3. The table is a view (non-materialized)
            #
            # In case 1, we want to report an accurate profile suggesting that the table is empty.
            # In case 2, do nothing since we cannot cheaply profile
            # In case 3, do nothing since we cannot cheaply profile
            if table[field_names.index("tabletype")] == "TABLE":
                rows_count = 0
                size_in_bytes = 0
                logger.info("Found some tables with no profiles need to return 0")

        return creation_time, last_altered, rows_count, size_in_bytes

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
        conn: redshift_connector.Connection,
        database: str,
        schema: RedshiftSchema,
        is_shared_database: bool = False,
    ) -> Dict[str, List[RedshiftColumn]]:
        cursor = RedshiftDataDictionary.get_query_result(
            conn,
            RedshiftCommonQuery.list_columns(
                database_name=database,
                schema_name=schema.name,
                is_shared_database=is_shared_database,
            ),
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
                    source_schema=(
                        row[field_names.index("source_schema")]
                        if "source_schema" in field_names
                        else None
                    ),
                    source_table=(
                        row[field_names.index("source_table")]
                        if "source_table" in field_names
                        else None
                    ),
                    target_schema=(
                        row[field_names.index("target_schema")]
                        if "target_schema" in field_names
                        else None
                    ),
                    target_table=(
                        row[field_names.index("target_table")]
                        if "target_table" in field_names
                        else None
                    ),
                    # See https://docs.aws.amazon.com/redshift/latest/dg/r_STL_QUERYTEXT.html
                    # for why we need to remove the \\n.
                    ddl=(
                        row[field_names.index("ddl")].replace("\\n", "\n")
                        if "ddl" in field_names
                        else None
                    ),
                    filename=(
                        row[field_names.index("filename")]
                        if "filename" in field_names
                        else None
                    ),
                    timestamp=(
                        row[field_names.index("timestamp")]
                        if "timestamp" in field_names
                        else None
                    ),
                    session_id=(
                        _stringy(row[field_names.index("session_id")])
                        if "session_id" in field_names
                        else None
                    ),
                )
            rows = cursor.fetchmany()

    @staticmethod
    def get_temporary_rows(
        conn: redshift_connector.Connection,
        query: str,
    ) -> Iterable[TempTableRow]:
        cursor = conn.cursor()

        cursor.execute(query)

        field_names = [i[0] for i in cursor.description]

        rows = cursor.fetchmany()
        while rows:
            for row in rows:
                # Skipping roews with no session_id
                session_id = _stringy(row[field_names.index("session_id")])
                if session_id is None:
                    continue
                yield TempTableRow(
                    transaction_id=row[field_names.index("transaction_id")],
                    session_id=session_id,
                    # See https://docs.aws.amazon.com/redshift/latest/dg/r_STL_QUERYTEXT.html
                    # for why we need to replace the \n with a newline.
                    query_text=row[field_names.index("query_text")].replace(
                        r"\n", "\n"
                    ),
                    create_command=row[field_names.index("create_command")],
                    start_time=row[field_names.index("start_time")],
                    urn=None,
                )
            rows = cursor.fetchmany()

    @staticmethod
    def get_alter_table_commands(
        conn: redshift_connector.Connection,
        query: str,
    ) -> Iterable[AlterTableRow]:
        # TODO: unify this with get_temporary_rows
        cursor = RedshiftDataDictionary.get_query_result(conn, query)

        field_names = [i[0] for i in cursor.description]

        rows = cursor.fetchmany()
        while rows:
            for row in rows:
                session_id = _stringy(row[field_names.index("session_id")])
                if session_id is None:
                    continue
                yield AlterTableRow(
                    transaction_id=row[field_names.index("transaction_id")],
                    session_id=session_id,
                    # See https://docs.aws.amazon.com/redshift/latest/dg/r_STL_QUERYTEXT.html
                    # for why we need to replace the \n with a newline.
                    query_text=row[field_names.index("query_text")].replace(
                        r"\n", "\n"
                    ),
                    start_time=row[field_names.index("start_time")],
                )
            rows = cursor.fetchmany()

    @staticmethod
    def get_outbound_datashares(
        conn: redshift_connector.Connection,
    ) -> Iterable[OutboundDatashare]:
        cursor = conn.cursor()
        cursor.execute(RedshiftCommonQuery.list_outbound_datashares())
        for item in cursor.fetchall():
            yield OutboundDatashare(
                share_name=item[1],
                producer_namespace=item[2],
                source_database=item[3],
            )

    # NOTE: this is not used right now as it requires superuser privilege
    # We can use this in future if the permissions are lowered.
    @staticmethod
    def get_inbound_datashare(
        conn: redshift_connector.Connection,
        database: str,
    ) -> Optional[InboundDatashare]:
        cursor = conn.cursor()
        cursor.execute(RedshiftCommonQuery.get_inbound_datashare(database))
        item = cursor.fetchone()
        if item:
            return InboundDatashare(
                share_name=item[1],
                producer_namespace=item[2],
                consumer_database=item[3],
            )
        return None
