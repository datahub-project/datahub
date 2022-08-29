import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier

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

    show_datasets: str = (
        "select schema_name from {project_id}.INFORMATION_SCHEMA.SCHEMATA"
    )

    datasets_for_project_id: str = """
select
  s.CATALOG_NAME as catalog_name,
  s.schema_name as table_schema,
  s.location as location,
  s.CREATION_TIME as created,
  s.LAST_MODIFIED_TIME as last_altered,
  o.OPTION_VALUE as comment
from
  {project_id}.INFORMATION_SCHEMA.SCHEMATA as s
  left join {project_id}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS as o on o.schema_name = s.schema_name
  and o.option_name = "description"
order by
  s.schema_name
"""

    # https://cloud.google.com/bigquery/docs/information-schema-table-storage?hl=en
    tables_for_dataset = """
SELECT
  t.table_catalog as table_catalog,
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
FROM
  `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLES t
  join `{project_id}`.{dataset_name}.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('BASE TABLE', 'EXTERNAL TABLE')
order by
  table_schema,
  table_name
"""

    views_for_dataset: str = """
SELECT
  t.table_catalog as table_catalog,
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
FROM
  `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLES t
  join `{project_id}`.{dataset_name}.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('VIEW MATERIALIZED', 'VIEW')
order by
  table_schema,
  table_name
"""

    columns_for_dataset: str = """
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
from
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMNS c
  join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
ORDER BY
  ordinal_position"""

    columns_for_table: str = """
select
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
from
  `{table_identifier.project_id}`.{table_identifier.dataset}.INFORMATION_SCHEMA.COLUMNS as c
  join `{table_identifier.project_id}`.{table_identifier.dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
where
  c.table_name = '{table_identifier.table}'
ORDER BY
  ordinal_position"""


class BigQueryDataDictionary:
    @staticmethod
    def get_query_result(conn: bigquery.Client, query: str) -> RowIterator:
        logger.debug(f"Query : {query}")
        resp = conn.query(query)
        return resp.result()

    @staticmethod
    def get_projects(conn: bigquery.Client) -> List[BigqueryProject]:
        projects = conn.list_projects()

        return [
            BigqueryProject(id=p.project_id, name=p.friendly_name) for p in projects
        ]

    @staticmethod
    def get_datasets_for_project_id(
        conn: bigquery.Client, project_id: str
    ) -> List[BigqueryDataset]:

        schemas = BigQueryDataDictionary.get_query_result(
            conn,
            BigqueryQuery.datasets_for_project_id.format(project_id=project_id),
        )
        return [
            BigqueryDataset(
                name=s.table_schema,
                created=s.created,
                location=s.location,
                last_altered=s.last_altered,
                comment=s.comment,
            )
            for s in schemas
        ]

    @staticmethod
    def get_tables_for_dataset(
        conn: bigquery.Client, project_id: str, dataset_name: str
    ) -> List[BigqueryTable]:

        cur = BigQueryDataDictionary.get_query_result(
            conn,
            BigqueryQuery.tables_for_dataset.format(
                project_id=project_id, dataset_name=dataset_name
            ),
        )

        return [
            BigqueryTable(
                name=table.table_name,
                created=table.created,
                last_altered=table.last_altered,
                size_in_bytes=table.bytes,
                rows_count=table.row_count,
                comment=table.comment,
                ddl=table.ddl,
            )
            for table in cur
        ]

    @staticmethod
    def get_views_for_dataset(
        conn: bigquery.Client, project_id: str, dataset_name: str
    ) -> List[BigqueryView]:

        cur = BigQueryDataDictionary.get_query_result(
            conn,
            BigqueryQuery.views_for_dataset.format(
                project_id=project_id, dataset_name=dataset_name
            ),
        )
        return [
            BigqueryView(
                name=table.table_name,
                created=table.created,
                last_altered=table.last_altered,
                comment=table.comment,
                ddl=table.view_definition,
            )
            for table in cur
        ]

    @staticmethod
    def get_columns_for_dataset(
        conn: bigquery.Client, project_id: str, dataset_name: str
    ) -> Optional[Dict[str, List[BigqueryColumn]]]:
        columns: Dict[str, List[BigqueryColumn]] = defaultdict(list)
        try:
            cur = BigQueryDataDictionary.get_query_result(
                conn,
                BigqueryQuery.columns_for_dataset.format(
                    project_id=project_id, dataset_name=dataset_name
                ),
            )
        except Exception as e:
            logger.warning(f"Columns for dataset query failed with exception: {e}")
            # Error - Information schema query returned too much data.
            # Please repeat query with more selective predicates.
            return None

        for column in cur:
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

    @staticmethod
    def get_columns_for_table(
        conn: bigquery.Client, table_identifier: BigqueryTableIdentifier
    ) -> List[BigqueryColumn]:

        cur = BigQueryDataDictionary.get_query_result(
            conn,
            BigqueryQuery.columns_for_table.format(table_identifier=table_identifier),
        )

        return [
            BigqueryColumn(
                name=column.column_name,
                ordinal_position=column.ordinal_position,
                is_nullable=column.is_nullable == "YES",
                data_type=column.data_type,
                comment=column.comment,
                is_partition_column=column.is_partitioning_column == "YES",
            )
            for column in cur
        ]
