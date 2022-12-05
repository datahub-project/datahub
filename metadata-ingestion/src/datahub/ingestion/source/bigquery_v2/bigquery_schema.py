import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, cast

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator, TableListItem, TimePartitioning

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=True)
class BigqueryColumn:
    name: str
    ordinal_position: int
    field_path: str
    is_nullable: bool
    is_partition_column: bool
    data_type: str
    comment: str


@dataclass
class BigqueryTable:
    name: str
    created: datetime
    last_altered: Optional[datetime]
    size_in_bytes: Optional[int]
    rows_count: Optional[int]
    expires: Optional[datetime]
    clustering_fields: Optional[List[str]]
    labels: Optional[Dict[str, str]]
    num_partitions: Optional[int]
    max_partition_id: Optional[str]
    max_shard_id: Optional[str]
    active_billable_bytes: Optional[int]
    long_term_billable_bytes: Optional[int]
    comment: str
    ddl: str
    time_partitioning: TimePartitioning
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
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    location: Optional[str] = None
    comment: Optional[str] = None
    tables: List[BigqueryTable] = field(default_factory=list)
    views: List[BigqueryView] = field(default_factory=list)


@dataclass
class BigqueryProject:
    id: str
    name: str
    datasets: List[BigqueryDataset] = field(default_factory=list)


class BigqueryQuery:

    show_datasets: str = (
        "select schema_name from `{project_id}`.INFORMATION_SCHEMA.SCHEMATA"
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
  `{project_id}`.INFORMATION_SCHEMA.SCHEMATA as s
  left join `{project_id}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS as o on o.schema_name = s.schema_name
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
  size_bytes as bytes,
  num_partitions,
  max_partition_id,
  active_billable_bytes,
  long_term_billable_bytes,
  REGEXP_EXTRACT(t.table_name, r".*_(\\d+)$") as table_suffix,
  REGEXP_REPLACE(t.table_name, r"_(\\d+)$", "") as table_base

FROM
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLES t
  join `{project_id}`.`{dataset_name}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
  left join (
    select
        table_name,
        sum(case when partition_id not in ('__NULL__', '__UNPARTITIONED__', '__STREAMING_UNPARTITIONED__') then 1 else 0 END) as num_partitions,
        max(case when partition_id not in ('__NULL__', '__UNPARTITIONED__', '__STREAMING_UNPARTITIONED__') then partition_id else NULL END) as max_partition_id,
        sum(total_rows) as total_rows,
        sum(case when storage_tier = 'LONG_TERM' then total_billable_bytes else 0 end) as long_term_billable_bytes,
        sum(case when storage_tier = 'ACTIVE' then total_billable_bytes else 0 end) as active_billable_bytes,
    from
        `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.PARTITIONS
    group by
        table_name) as p on
    t.table_name = p.table_name
WHERE
  table_type in ('BASE TABLE', 'EXTERNAL TABLE')
{table_filter}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
"""

    tables_for_dataset_without_partition_data = """
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  tos.OPTION_VALUE as comment,
  is_insertable_into,
  ddl,
  REGEXP_EXTRACT(t.table_name, r".*_(\\d+)$") as table_suffix,
  REGEXP_REPLACE(t.table_name, r"_(\\d+)$", "") as table_base

FROM
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLES t
  left join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('BASE TABLE', 'EXTERNAL TABLE')
{table_filter}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
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
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLES t
  join `{project_id}`.`{dataset_name}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('VIEW MATERIALIZED', 'VIEW')
order by
  table_schema ASC,
  table_name ASC
"""

    views_for_dataset_without_data_read: str = """
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  tos.OPTION_VALUE as comment,
  is_insertable_into,
  ddl as view_definition
FROM
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLES t
  left join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('VIEW MATERIALIZED', 'VIEW')
order by
  table_schema ASC,
  table_name ASC
"""

    columns_for_dataset: str = """
select
  c.table_catalog as table_catalog,
  c.table_schema as table_schema,
  c.table_name as table_name,
  c.column_name as column_name,
  c.ordinal_position as ordinal_position,
  cfp.field_path as field_path,
  c.is_nullable as is_nullable,
  CASE WHEN CONTAINS_SUBSTR(field_path, ".") THEN NULL ELSE c.data_type END as data_type,
  description as comment,
  c.is_hidden as is_hidden,
  c.is_partitioning_column as is_partitioning_column
from
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMNS c
  join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
ORDER BY
  table_catalog, table_schema, table_name, ordinal_position ASC, data_type DESC"""

    columns_for_table: str = """
select
  c.table_catalog as table_catalog,
  c.table_schema as table_schema,
  c.table_name as table_name,
  c.column_name as column_name,
  c.ordinal_position as ordinal_position,
  cfp.field_path as field_path,
  c.is_nullable as is_nullable,
  CASE WHEN CONTAINS_SUBSTR(field_path, ".") THEN NULL ELSE c.data_type END as data_type,
  c.is_hidden as is_hidden,
  c.is_partitioning_column as is_partitioning_column,
  description as comment
from
  `{table_identifier.project_id}`.`{table_identifier.dataset}`.INFORMATION_SCHEMA.COLUMNS as c
  join `{table_identifier.project_id}`.`{table_identifier.dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
where
  c.table_name = '{table_identifier.table}'
ORDER BY
  table_catalog, table_schema, table_name, ordinal_position ASC, data_type DESC"""


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
        conn: bigquery.Client, project_id: str, maxResults: Optional[int] = None
    ) -> List[BigqueryDataset]:
        # FIXME: Due to a bug in BigQuery's type annotations, we need to cast here.
        maxResults = cast(int, maxResults)
        datasets = conn.list_datasets(project_id, max_results=maxResults)

        return [BigqueryDataset(name=d.dataset_id) for d in datasets]

    @staticmethod
    def get_datasets_for_project_id_with_information_schema(
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
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
        tables: Dict[str, TableListItem],
        with_data_read_permission: bool = False,
    ) -> List[BigqueryTable]:

        filter: str = ", ".join(f"'{table}'" for table in tables.keys())

        if with_data_read_permission:
            # Tables are ordered by name and table suffix to make sure we always process the latest sharded table
            # and skip the others. Sharded tables are tables with suffix _20220102
            cur = BigQueryDataDictionary.get_query_result(
                conn,
                BigqueryQuery.tables_for_dataset.format(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    table_filter=f" and t.table_name in ({filter})" if filter else "",
                ),
            )
        else:
            # Tables are ordered by name and table suffix to make sure we always process the latest sharded table
            # and skip the others. Sharded tables are tables with suffix _20220102
            cur = BigQueryDataDictionary.get_query_result(
                conn,
                BigqueryQuery.tables_for_dataset_without_partition_data.format(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    table_filter=f" and t.table_name in ({filter})" if filter else "",
                ),
            )

        # Some property we want to capture only available from the TableListItem we get from an earlier query of
        # the list of tables.
        return [
            BigqueryTable(
                name=table.table_name,
                created=table.created,
                last_altered=datetime.fromtimestamp(
                    table.last_altered / 1000, tz=timezone.utc
                )
                if "last_altered" in table
                else None,
                size_in_bytes=table.get("bytes"),
                rows_count=table.get("row_count"),
                comment=table.comment,
                ddl=table.ddl,
                expires=tables[table.table_name].expires if tables else None,
                labels=tables[table.table_name].labels if tables else None,
                time_partitioning=tables[table.table_name].time_partitioning
                if tables
                else None,
                clustering_fields=tables[table.table_name].clustering_fields
                if tables
                else None,
                max_partition_id=table.get("max_partition_id"),
                max_shard_id=BigqueryTableIdentifier.get_table_and_shard(
                    table.table_name
                )[1]
                if len(BigqueryTableIdentifier.get_table_and_shard(table.table_name))
                == 2
                else None,
                num_partitions=table.get("num_partitions"),
                active_billable_bytes=table.get("active_billable_bytes"),
                long_term_billable_bytes=table.get("long_term_billable_bytes"),
            )
            for table in cur
        ]

    @staticmethod
    def get_views_for_dataset(
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
        has_data_read: bool,
    ) -> List[BigqueryView]:

        if has_data_read:
            cur = BigQueryDataDictionary.get_query_result(
                conn,
                BigqueryQuery.views_for_dataset.format(
                    project_id=project_id, dataset_name=dataset_name
                ),
            )
        else:
            cur = BigQueryDataDictionary.get_query_result(
                conn,
                BigqueryQuery.views_for_dataset_without_data_read.format(
                    project_id=project_id, dataset_name=dataset_name
                ),
            )

        return [
            BigqueryView(
                name=table.table_name,
                created=table.created,
                last_altered=table.last_altered if "last_altered" in table else None,
                comment=table.comment,
                ddl=table.view_definition,
            )
            for table in cur
        ]

    @staticmethod
    def get_columns_for_dataset(
        conn: bigquery.Client,
        project_id: str,
        dataset_name: str,
        column_limit: Optional[int] = None,
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

        last_seen_table: str = ""
        for column in cur:
            if (
                column_limit
                and column.table_name in columns
                and len(columns[column.table_name]) >= column_limit
            ):
                if last_seen_table != column.table_name:
                    logger.warning(
                        f"{project_id}.{dataset_name}.{column.table_name} contains more than {column_limit} columns, only processing {column_limit} columns"
                    )
                    last_seen_table = column.table_name
            else:
                columns[column.table_name].append(
                    BigqueryColumn(
                        name=column.column_name,
                        ordinal_position=column.ordinal_position,
                        field_path=column.field_path,
                        is_nullable=column.is_nullable == "YES",
                        data_type=column.data_type,
                        comment=column.comment,
                        is_partition_column=column.is_partitioning_column == "YES",
                    )
                )

        return columns

    @staticmethod
    def get_columns_for_table(
        conn: bigquery.Client,
        table_identifier: BigqueryTableIdentifier,
        column_limit: Optional[int],
    ) -> List[BigqueryColumn]:

        cur = BigQueryDataDictionary.get_query_result(
            conn,
            BigqueryQuery.columns_for_table.format(table_identifier=table_identifier),
        )

        columns: List[BigqueryColumn] = []
        last_seen_table: str = ""
        for column in cur:
            if (
                column_limit
                and column.table_name in columns
                and len(columns[column.table_name]) >= column_limit
            ):
                if last_seen_table != column.table_name:
                    logger.warning(
                        f"{table_identifier.project_id}.{table_identifier.dataset}.{column.table_name} contains more than {column_limit} columns, only processing {column_limit} columns"
                    )
                    last_seen_table = column.table_name
            else:
                columns.append(
                    BigqueryColumn(
                        name=column.column_name,
                        ordinal_position=column.ordinal_position,
                        is_nullable=column.is_nullable == "YES",
                        field_path=column.field_path,
                        data_type=column.data_type,
                        comment=column.comment,
                        is_partition_column=column.is_partitioning_column == "YES",
                    )
                )
            last_seen_table = column.table_name

        return columns
