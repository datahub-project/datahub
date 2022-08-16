import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from google.cloud import bigquery

from datahub.ingestion.source.bigquery.bigquery_audit import BigqueryTableIdentifier

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class BigqueryColumn:
    name: str
    ordinal_position: int
    is_nullable: bool
    is_partition_column: bool
    data_type: str
    comment: str


@dataclass
class BigqueryTable:
    name: str
    created: datetime
    last_altered: datetime
    size_in_bytes: int
    rows_count: int
    comment: str
    ddl: str
    columns: List[BigqueryColumn] = field(default_factory=list)


@dataclass
class BigqueryView:
    name: str
    created: datetime
    last_altered: datetime
    comment: str
    ddl: str
    columns: List[BigqueryColumn] = field(default_factory=list)


@dataclass
class BigqueryDataset:
    name: str
    created: datetime
    last_altered: datetime
    location: str
    comment: str
    tables: List[BigqueryTable] = field(default_factory=list)
    views: List[BigqueryView] = field(default_factory=list)


@dataclass
class BigqueryProject:
    id: str
    name: str
    datasets: List[BigqueryDataset] = field(default_factory=list)


class BigqueryQuery:
    @staticmethod
    def show_databases(project_id: str) -> str:
        return f"select schema_name from {project_id}.INFORMATION_SCHEMA.SCHEMATA"

    @staticmethod
    def datasets_for_project_id(project_id: str) -> str:
        return f"""select s.CATALOG_NAME as catalog_name
        , s.schema_name as table_schema
        , s.location as location
        , s.CREATION_TIME as created
        , s.LAST_MODIFIED_TIME as last_altered
        , o.OPTION_VALUE as comment
        from {project_id}.INFORMATION_SCHEMA.SCHEMATA as s
        left join {project_id}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS as o on o.schema_name = s.schema_name and o.option_name = "description"
        order by s.schema_name"""

    @staticmethod
    def tables_for_project_id(project_id: str, location: str) -> str:
        # https://cloud.google.com/bigquery/docs/information-schema-table-storage?hl=en
        return f"""
        SELECT ts.table_catalog as table_catalog,
        ts.table_schema as table_schema,
        ts.table_name as table_name,
        ts.table_type as table_type,
        t.creation_time as created,
        last_altered as last_altered,
        tos.OPTION_VALUE as comment,
        ts.TOTAL_ROWS as row_count,
        ts.TOTAL_PARTITIONS = partition_count,
        ts.TOTAL_LOGICAL_BYTES as total_logical_bytes,
        ts.ACTIVE_LOGICAL_BYTES	as active_logical_bytes,
        ts.LONG_TERM_LOGICAL_BYTES as long_term_logical_bytes,
        ts.TOTAL_PHYSICAL_BYTES	as total_physical_bytes,
        ts.ACTIVE_PHYSICAL_BYTES as active_physical_bytes,
        ts.LONG_TERM_PHYSICAL_BYTES as long_term_physical_bytes,
        ts.TIME_TRAVEL_PHYSICAL_BYTES as time_travel_physical_bytes,
        is_insertable_into,
        ddl
        FROM `{project_id}`.{location}.INFORMATION_SCHEMA.TABLES t
        left join `{project_id}`.{location}.INFORMATION_SCHEMA.TABLE_STORAGE as ts on ts.project_id = t.project_id and ts.TABLE_SCHEMA = t.table_schema and ts.TABLE_NAME = t.TABLE_NAME
        left join `{project_id}`.{location}.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on tos.schema_name = ts.schema_name and tos.TABLE_NAME = ts.TABLE_NAME and tos.OPTION_NAME = "description"
        WHERE table_type in ( 'BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def tables_for_dataset(project_id: str, dataset_name: str) -> str:
        # https://cloud.google.com/bigquery/docs/information-schema-table-storage?hl=en
        return f"""
        SELECT t.table_catalog as table_catalog,
        t.table_schema as table_schema,
        t.table_name as table_name,
        t.table_type as table_type,
        t.creation_time as created,
        ts.last_modified_time as last_altered,
        tos.OPTION_VALUE as comment,
        is_insertable_into,
        ddl,
        row_count,
        size_bytes as bytes
        FROM `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLES t
        join `{project_id}`.{dataset_name}.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
        left join `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema and t.TABLE_NAME = tos.TABLE_NAME and tos.OPTION_NAME = "description"
        WHERE table_type in ('BASE TABLE', 'EXTERNAL TABLE')
        order by table_schema, table_name"""

    @staticmethod
    def views_for_dataset(project_id: str, dataset_name: str) -> str:
        return f"""
        SELECT t.table_catalog as table_catalog,
        t.table_schema as table_schema,
        t.table_name as table_name,
        t.table_type as table_type,
        t.creation_time as created,
        ts.last_modified_time as last_altered,
        tos.OPTION_VALUE as comment,
        is_insertable_into,
        ddl as view_definition,
        row_count,
        size_bytes
        FROM `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLES t
        join `{project_id}`.{dataset_name}.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
        left join `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema and t.TABLE_NAME = tos.TABLE_NAME and tos.OPTION_NAME = "description"
        WHERE table_type in ( 'VIEW MATERIALIZED', 'VIEW')
        order by table_schema, table_name"""

    @staticmethod
    def columns_for_dataset(project_id: str, dataset_name: str) -> str:
        return f"""
        select
        c.table_catalog as table_catalog,
        c.table_schema as table_schema,
        c.table_name as table_name,
        c.column_name as column_name,
        c.ordinal_position as ordinal_position,
        c.is_nullable as is_nullable,
        c.data_type as data_type,
        description as comment,
        c.is_hidden as is_hidden,
        c.is_partitioning_column as is_partitioning_column
        from `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMNS c
        join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name and cfp.column_name = c.column_name
        ORDER BY ordinal_position"""

    @staticmethod
    def columns_for_table(table_identifier: BigqueryTableIdentifier) -> str:
        return f"""select
        c.table_catalog as table_catalog,
        c.table_schema as table_schema,
        c.table_name as table_name,
        c.column_name as column_name,
        c.ordinal_position as ordinal_position,
        c.is_nullable as is_nullable,
        c.data_type as data_type,
        c.is_hidden as is_hidden,
        c.is_partitioning_column as is_partitioning_column,
        description as comment
        from `{table_identifier.project_id}`.{table_identifier.dataset}.INFORMATION_SCHEMA.COLUMNS as c
        join `{table_identifier.project_id}`.{table_identifier.dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name and cfp.column_name = c.column_name
        where c.table_name='{table_identifier.table}'
        ORDER BY ordinal_position"""


class BigQueryDataDictionary:
    def query(self, conn, query):
        logger.debug("Query : {}".format(query))
        resp = conn.query(query)
        return resp.result()

    def get_project_ids(self, conn: bigquery.Client) -> List[BigqueryProject]:
        projects = conn.list_projects()
        databases: List[BigqueryProject] = []

        for project in projects:
            bigquery_project = BigqueryProject(
                id=project.project_id, name=project.friendly_name
            )
            databases.append(bigquery_project)

        return databases

    def get_datasets_for_project_id(
        self, conn: bigquery.Client, project_id: str
    ) -> List[BigqueryDataset]:

        bigquery_datasets = []
        cur = self.query(
            conn,
            BigqueryQuery.datasets_for_project_id(project_id),
        )
        for schema in cur:
            bigquery_schema = BigqueryDataset(
                name=schema.table_schema,
                created=schema.created,
                location=schema.location,
                last_altered=schema.last_altered,
                comment=schema.comment,
            )
            bigquery_datasets.append(bigquery_schema)
        return bigquery_datasets

    def get_tables_for_project_id(
        self, conn: bigquery.Client, project_id: str, location: str
    ) -> Optional[Dict[str, List[BigqueryTable]]]:
        tables: Dict[str, List[BigqueryTable]] = {}
        try:
            cur = self.query(
                conn,
                BigqueryQuery.tables_for_project_id(project_id, location),
            )
        except Exception as e:
            logger.debug(e)
            # Error - Information schema query returned too much data. Please repeat query with more selective predicates.
            return None

        for table in cur:
            if table.table_schema not in tables:
                tables[table.table_schema] = []
            tables[table.table_schema].append(
                BigqueryTable(
                    name=table.table_name,
                    created=table.created,
                    last_altered=table.last_altered,
                    size_in_bytes=table.bytes,
                    rows_count=table.row_count,
                    comment=table.comment,
                    ddl=table.ddl,
                )
            )
        return tables

    def get_tables_for_dataset(
        self, conn: bigquery.Client, project_id: str, dataset_name: str
    ) -> List[BigqueryTable]:
        tables: List[BigqueryTable] = []

        cur = self.query(
            conn,
            BigqueryQuery.tables_for_dataset(project_id, dataset_name),
        )

        for table in cur:
            tables.append(
                BigqueryTable(
                    name=table.table_name,
                    created=table.created,
                    last_altered=table.last_altered,
                    size_in_bytes=table.bytes,
                    rows_count=table.row_count,
                    comment=table.comment,
                    ddl=table.ddl,
                )
            )
        return tables

    def get_views_for_dataset(
        self, conn: bigquery.Client, project_id: str, dataset_name: str
    ) -> List[BigqueryView]:
        views: List[BigqueryView] = []

        cur = self.query(
            conn, BigqueryQuery.views_for_dataset(project_id, dataset_name)
        )
        for table in cur:
            views.append(
                BigqueryView(
                    name=table.table_name,
                    created=table.created,
                    last_altered=table.last_altered,
                    comment=table.comment,
                    ddl=table.view_definition,
                )
            )
        return views

    def get_columns_for_dataset(
        self, conn: bigquery.Client, project_id: str, schema_name: str
    ) -> Optional[Dict[str, List[BigqueryColumn]]]:
        columns: Dict[str, List[BigqueryColumn]] = {}
        try:
            cur = self.query(
                conn, BigqueryQuery.columns_for_dataset(project_id, schema_name)
            )
        except Exception as e:
            logger.debug(e)
            # Error - Information schema query returned too much data.
            # Please repeat query with more selective predicates.
            return None

        for column in cur:
            if column.table_name not in columns:
                columns[column.table_name] = []
            columns[column.table_name].append(
                BigqueryColumn(
                    name=column.column_name,
                    ordinal_position=column.ordinal_position,
                    is_nullable=column.is_nullable == "YES",
                    data_type=column.data_type,
                    comment=column.comment,
                    is_partition_column=column.is_partitioning_column == "YES",
                )
            )
        return columns

    def get_columns_for_table(
        self, conn: bigquery.Client, table_identifier: BigqueryTableIdentifier
    ) -> List[BigqueryColumn]:
        columns: List[BigqueryColumn] = []

        cur = self.query(
            conn,
            BigqueryQuery.columns_for_table(table_identifier),
        )

        for column in cur:
            columns.append(
                BigqueryColumn(
                    name=column.column_name,
                    ordinal_position=column.ordinal_position,
                    is_nullable=column.is_nullable == "YES",
                    data_type=column.data_type,
                    comment=column.comment,
                    is_partition_column=column.is_partitioning_column == "YES",
                )
            )
        return columns
