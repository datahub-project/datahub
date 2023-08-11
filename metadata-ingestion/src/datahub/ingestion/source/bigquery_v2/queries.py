class BigqueryTableType:
    # See https://cloud.google.com/bigquery/docs/information-schema-tables#schema
    BASE_TABLE = "BASE TABLE"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED VIEW"
    CLONE = "CLONE"
    SNAPSHOT = "SNAPSHOT"


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
    tables_for_dataset = f"""
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
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  join `{{project_id}}`.`{{dataset_name}}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
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
        `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.PARTITIONS
    group by
        table_name) as p on
    t.table_name = p.table_name
WHERE
  table_type in ('{BigqueryTableType.BASE_TABLE}', '{BigqueryTableType.EXTERNAL}')
{{table_filter}}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
"""

    tables_for_dataset_without_partition_data = f"""
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
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('{BigqueryTableType.BASE_TABLE}', '{BigqueryTableType.EXTERNAL}')
{{table_filter}}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
"""

    views_for_dataset: str = f"""
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
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  join `{{project_id}}`.`{{dataset_name}}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('{BigqueryTableType.VIEW}', '{BigqueryTableType.MATERIALIZED_VIEW}')
order by
  table_schema ASC,
  table_name ASC
"""

    views_for_dataset_without_data_read: str = f"""
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
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('{BigqueryTableType.VIEW}', '{BigqueryTableType.MATERIALIZED_VIEW}')
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

    optimized_columns_for_dataset: str = """
select * from
(select
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
  c.is_partitioning_column as is_partitioning_column,
  -- We count the columns to be able limit it later
  row_number() over (partition by c.table_catalog, c.table_schema, c.table_name order by c.ordinal_position asc, c.data_type DESC) as column_num,
  -- Getting the maximum shard for each table
  row_number() over (partition by c.table_catalog, c.table_schema, ifnull(REGEXP_EXTRACT(c.table_name, r'(.*)_\\d{{8}}$'), c.table_name), cfp.field_path order by c.table_catalog, c.table_schema asc, c.table_name desc) as shard_num
from
  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMNS c
  join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
  )
-- We filter column limit + 1 to make sure we warn about the limit being reached but not reading too much data
where column_num <= {column_limit} and shard_num = 1
ORDER BY
  table_catalog, table_schema, table_name, ordinal_position, column_num ASC, data_type DESC"""

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
