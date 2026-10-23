import textwrap
from typing import Dict, Optional

from datahub.ingestion.source.bigquery_v2.common import (
    BQ_NULL_PARTITION_ID,
    BQ_STREAMING_UNPARTITIONED_PARTITION_ID,
    BQ_UNPARTITIONED_PARTITION_ID,
)

# Collapses a date-sharded BigQuery table name (`events_20251121`, `events$20251121`,
# `events20251121`) to its logical wildcard base so per-shard stats can be summed back onto
# the single collapsed entity (see build_sharded_stats_clauses). The prefix group is
# optional, mirroring the Python shard matcher (_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX /
# extract_base_table_name in bigquery_audit.py):
#   - `events_20250101` / `events20250101` -> `events`
#   - date-only names (`20250101`, `20250102`) -> "" so they group together, matching the
#     Python "dataset as a sharded table" collapse (extract_base_table_name falls back to
#     the dataset name for these).
#   - `orders120250101` (a digit, not a separator, before the date) does NOT match, so it
#     stays a distinct table and is not merged into the `orders1` family's totals.
# ponytail: when the character before the separator is a digit (e.g. `orders1_20250101`),
# the captured base keeps the trailing `_`/`$` (-> `orders1_`) rather than Python's
# rstrip("_") form (`orders1`). This base is only ever used internally as the GROUP BY /
# join key and is applied identically to every shard of the family, so the rollup still
# groups and sums the exact same shards - it just differs cosmetically from the Python
# base string, which is never compared against it. Upgrade path: wrap in a second
# REGEXP_REPLACE(..., r"[_$]+$", "") if exact base-string parity is ever required.
SHARDED_TABLE_BASE_STRIP_REGEX = (
    r"^((?:.*?\D)?)[_$]?(\d\d\d\d(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))$"
)


def sharded_table_base_expr(column: str) -> str:
    """SQL expression that reduces a shard name to its wildcard base.

    Single source of truth for both the entity-grouping key and the rollup subqueries in
    build_sharded_stats_clauses, so they can never drift apart and aggregate a different
    set of shards than the collapse logic groups together.
    """
    return f'REGEXP_REPLACE({column}, r"{SHARDED_TABLE_BASE_STRIP_REGEX}", r"\\1")'


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

    # Sources row_count / size / last_altered from the legacy __TABLES__ construct.
    tables_for_dataset_with_legacy_stats = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  ts.last_modified_time as last_altered,
  tos.OPTION_VALUE as comment,
  t.is_insertable_into,
  t.ddl,
  {{row_count_select}} as row_count,
  {{bytes_select}} as bytes,
  p.num_partitions,
  p.max_partition_id,
  {{active_billable_select}} as active_billable_bytes,
  {{long_term_billable_select}} as long_term_billable_bytes,
  REGEXP_EXTRACT(t.table_name, r"(?:(?:.+\\D)[_$]?)(\\d\\d\\d\\d(?:0[1-9]|1[012])(?:0[1-9]|[12][0-9]|3[01]))$") as table_suffix,
  REGEXP_REPLACE(t.table_name, r"(?:[_$]?)(\\d\\d\\d\\d(?:0[1-9]|1[012])(?:0[1-9]|[12][0-9]|3[01]))$", "") as table_base

FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  join `{{project_id}}`.`{{dataset_name}}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
  left join (
    select
        table_name,
        sum(case when partition_id not in ('{BQ_NULL_PARTITION_ID}', '{BQ_UNPARTITIONED_PARTITION_ID}', '{BQ_STREAMING_UNPARTITIONED_PARTITION_ID}') then 1 else 0 END) as num_partitions,
        max(case when partition_id not in ('{BQ_NULL_PARTITION_ID}', '{BQ_UNPARTITIONED_PARTITION_ID}', '{BQ_STREAMING_UNPARTITIONED_PARTITION_ID}') then partition_id else NULL END) as max_partition_id,
        sum(total_rows) as total_rows,
        sum(case when storage_tier = 'LONG_TERM' then total_billable_bytes else 0 end) as long_term_billable_bytes,
        sum(case when storage_tier = 'ACTIVE' then total_billable_bytes else 0 end) as active_billable_bytes,
    from
        `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.PARTITIONS
    group by
        table_name) as p on
    t.table_name = p.table_name{{shard_stats_join}}
WHERE
  table_type in ('{BigqueryTableType.BASE_TABLE}', '{BigqueryTableType.EXTERNAL}', '{BigqueryTableType.CLONE}')
{{table_filter}}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
"""

    # Same as tables_for_dataset_with_legacy_stats, but sources row_count / size /
    # last_altered from INFORMATION_SCHEMA.PARTITIONS instead of the legacy __TABLES__
    # construct. PARTITIONS is dataset-scoped and already queried for partition metadata,
    # so this adds no extra scan. It covers base tables only; external tables (absent from
    # PARTITIONS) get null stats via the LEFT JOIN.
    tables_for_dataset_with_partition_stats = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  p.last_modified_time as last_altered,
  tos.OPTION_VALUE as comment,
  t.is_insertable_into,
  t.ddl,
  {{row_count_select}} as row_count,
  {{bytes_select}} as bytes,
  p.num_partitions,
  p.max_partition_id,
  {{active_billable_select}} as active_billable_bytes,
  {{long_term_billable_select}} as long_term_billable_bytes,
  REGEXP_EXTRACT(t.table_name, r"(?:(?:.+\\D)[_$]?)(\\d\\d\\d\\d(?:0[1-9]|1[012])(?:0[1-9]|[12][0-9]|3[01]))$") as table_suffix,
  REGEXP_REPLACE(t.table_name, r"(?:[_$]?)(\\d\\d\\d\\d(?:0[1-9]|1[012])(?:0[1-9]|[12][0-9]|3[01]))$", "") as table_base

FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
  left join (
    select
        table_name,
        sum(case when partition_id not in ('{BQ_NULL_PARTITION_ID}', '{BQ_UNPARTITIONED_PARTITION_ID}', '{BQ_STREAMING_UNPARTITIONED_PARTITION_ID}') then 1 else 0 END) as num_partitions,
        max(case when partition_id not in ('{BQ_NULL_PARTITION_ID}', '{BQ_UNPARTITIONED_PARTITION_ID}', '{BQ_STREAMING_UNPARTITIONED_PARTITION_ID}') then partition_id else NULL END) as max_partition_id,
        sum(total_rows) as total_rows,
        sum(total_logical_bytes) as total_logical_bytes,
        UNIX_MILLIS(max(last_modified_time)) as last_modified_time,
        sum(case when storage_tier = 'LONG_TERM' then total_billable_bytes else 0 end) as long_term_billable_bytes,
        sum(case when storage_tier = 'ACTIVE' then total_billable_bytes else 0 end) as active_billable_bytes,
    from
        `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.PARTITIONS
    group by
        table_name) as p on
    t.table_name = p.table_name{{shard_stats_join}}
WHERE
  table_type in ('{BigqueryTableType.BASE_TABLE}', '{BigqueryTableType.EXTERNAL}', '{BigqueryTableType.CLONE}')
{{table_filter}}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
"""

    # No row_count / size / last_altered stats at all (neither __TABLES__ nor PARTITIONS).
    tables_for_dataset_without_stats = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  tos.OPTION_VALUE as comment,
  t.is_insertable_into,
  t.ddl,
  REGEXP_EXTRACT(t.table_name, r"(?:(?:.+\\D)[_$]?)(\\d\\d\\d\\d(?:0[1-9]|1[012])(?:0[1-9]|[12][0-9]|3[01]))$") as table_suffix,
  REGEXP_REPLACE(t.table_name, r"(?:[_$]?)(\\d\\d\\d\\d(?:0[1-9]|1[012])(?:0[1-9]|[12][0-9]|3[01]))$", "") as table_base

FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type in ('{BigqueryTableType.BASE_TABLE}', '{BigqueryTableType.EXTERNAL}', '{BigqueryTableType.CLONE}')
{{table_filter}}
order by
  table_schema ASC,
  table_base ASC,
  table_suffix DESC
"""

    # Views keep last_altered / row_count / size_bytes from the legacy __TABLES__ construct.
    views_for_dataset_with_legacy_stats: str = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  ts.last_modified_time as last_altered,
  tos_description.OPTION_VALUE as comment,
  tos_labels.OPTION_VALUE as labels,
  t.is_insertable_into,
  t.ddl as view_definition,
  ts.row_count,
  ts.size_bytes
FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  join `{{project_id}}`.`{{dataset_name}}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos_description on t.table_schema = tos_description.table_schema
  and t.TABLE_NAME = tos_description.TABLE_NAME
  and tos_description.OPTION_NAME = "description"
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos_labels on t.table_schema = tos_labels.table_schema
  and t.TABLE_NAME = tos_labels.TABLE_NAME
  and tos_labels.OPTION_NAME = "labels"
WHERE
  table_type in ('{BigqueryTableType.VIEW}', '{BigqueryTableType.MATERIALIZED_VIEW}')
order by
  table_schema ASC,
  table_name ASC
"""

    # No __TABLES__ join, so no last_altered / row_count / size_bytes stats.
    views_for_dataset_without_stats: str = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  tos_description.OPTION_VALUE as comment,
  tos_labels.OPTION_VALUE as labels,
  t.is_insertable_into,
  t.ddl as view_definition
FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos_description on t.table_schema = tos_description.table_schema
  and t.TABLE_NAME = tos_description.TABLE_NAME
  and tos_description.OPTION_NAME = "description"
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos_labels on t.table_schema = tos_labels.table_schema
  and t.TABLE_NAME = tos_labels.TABLE_NAME
  and tos_labels.OPTION_NAME = "labels"
WHERE
  table_type in ('{BigqueryTableType.VIEW}', '{BigqueryTableType.MATERIALIZED_VIEW}')
order by
  table_schema ASC,
  table_name ASC
"""

    # Snapshots keep last_altered / rows_count / size_in_bytes from the legacy __TABLES__ construct.
    snapshots_for_dataset_with_legacy_stats: str = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  t.is_insertable_into,
  t.ddl,
  t.snapshot_time_ms as snapshot_time,
  t.base_table_catalog,
  t.base_table_schema,
  t.base_table_name,
  ts.last_modified_time as last_altered,
  tos.OPTION_VALUE as comment,
  ts.row_count,
  ts.size_bytes
FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  join `{{project_id}}`.`{{dataset_name}}`.__TABLES__ as ts on ts.table_id = t.TABLE_NAME
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type = '{BigqueryTableType.SNAPSHOT}'
order by
  table_schema ASC,
  table_name ASC
"""

    # No __TABLES__ join, so no last_altered / rows_count / size_in_bytes stats.
    snapshots_for_dataset_without_stats: str = f"""
SELECT
  t.table_catalog as table_catalog,
  t.table_schema as table_schema,
  t.table_name as table_name,
  t.table_type as table_type,
  t.creation_time as created,
  t.is_insertable_into,
  t.ddl,
  t.snapshot_time_ms as snapshot_time,
  t.base_table_catalog,
  t.base_table_schema,
  t.base_table_name,
  tos.OPTION_VALUE as comment,
FROM
  `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLES t
  left join `{{project_id}}`.`{{dataset_name}}`.INFORMATION_SCHEMA.TABLE_OPTIONS as tos on t.table_schema = tos.table_schema
  and t.TABLE_NAME = tos.TABLE_NAME
  and tos.OPTION_NAME = "description"
WHERE
  table_type = '{BigqueryTableType.SNAPSHOT}'
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
  cfp.policy_tags as policy_tags,
  c.is_nullable as is_nullable,
  CASE WHEN CONTAINS_SUBSTR(field_path, ".") THEN NULL ELSE c.data_type END as data_type,
  description as comment,
  c.is_hidden as is_hidden,
  c.is_partitioning_column as is_partitioning_column,
  c.clustering_ordinal_position as clustering_ordinal_position,
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
  cfp.policy_tags as policy_tags,
  c.is_nullable as is_nullable,
  CASE WHEN CONTAINS_SUBSTR(field_path, ".") THEN NULL ELSE c.data_type END as data_type,
  description as comment,
  c.is_hidden as is_hidden,
  c.is_partitioning_column as is_partitioning_column,
  c.clustering_ordinal_position as clustering_ordinal_position,
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
  cfp.policy_tags as policy_tags,
  c.is_nullable as is_nullable,
  CASE WHEN CONTAINS_SUBSTR(field_path, ".") THEN NULL ELSE c.data_type END as data_type,
  c.is_hidden as is_hidden,
  c.is_partitioning_column as is_partitioning_column,
  c.clustering_ordinal_position as clustering_ordinal_position,
  description as comment
from
  `{table_identifier.project_id}`.`{table_identifier.dataset}`.INFORMATION_SCHEMA.COLUMNS as c
  join `{table_identifier.project_id}`.`{table_identifier.dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as cfp on cfp.table_name = c.table_name
  and cfp.column_name = c.column_name
where
  c.table_name = '{table_identifier.table}'
ORDER BY
  table_catalog, table_schema, table_name, ordinal_position ASC, data_type DESC"""

    constraints_for_table: str = """
select
    kcu.constraint_name as constraint_name,
    kcu.table_catalog as table_catalog,
    kcu.table_schema as table_schema,
    kcu.table_name as table_name,
    kcu.column_name as column_name,
    tc.constraint_type,
    ccu.table_catalog as referenced_catalog,
    ccu.table_schema as referenced_schema,
    ccu.table_name as referenced_table,
    ccu.column_name as referenced_column
from
     `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
join  `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as ccu on
    kcu.constraint_catalog = ccu.constraint_catalog
    and kcu.constraint_schema = ccu.constraint_schema
    and kcu.constraint_name = ccu.constraint_name
join `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc on
    tc.constraint_catalog = ccu.constraint_catalog
    and tc.constraint_schema = ccu.constraint_schema
    and tc.constraint_name = ccu.constraint_name
"""

    @staticmethod
    def build_sharded_stats_clauses(
        project_id: str,
        dataset_name: str,
        use_legacy_table_stats: bool,
        aggregate_sharded_stats: bool,
    ) -> Dict[str, str]:
        """Build the row_count / bytes / billable-bytes selects and the optional
        shard-rollup join(s) for the stats query templates.

        A collapsed date-sharded table (the default, legacy support on) is represented by
        its latest shard alone, so its row_count / size / billable bytes would otherwise
        reflect only that one shard. When aggregate_sharded_stats is set we LEFT JOIN a
        per-base rollup that sums every shard's stats and COALESCE it over the latest
        shard's own values. The `t.table_name != <base>` join condition leaves un-sharded
        tables on their own per-table stats.

        Row count and size come from __TABLES__ under use_legacy_table_stats and from
        PARTITIONS otherwise, so the rollup `sh` mirrors that source. Billable bytes are
        always sourced from PARTITIONS in both templates, so under legacy stats they are
        summed by a second PARTITIONS rollup `shb`.
        """
        if use_legacy_table_stats:
            own_rows, own_bytes = "ts.row_count", "ts.size_bytes"
            source, name_col = "__TABLES__", "table_id"
            src_rows, src_bytes = "row_count", "size_bytes"
        else:
            own_rows, own_bytes = "p.total_rows", "p.total_logical_bytes"
            source, name_col = "INFORMATION_SCHEMA.PARTITIONS", "table_name"
            src_rows, src_bytes = "total_rows", "total_logical_bytes"

        own_active = "p.active_billable_bytes"
        own_long_term = "IFNULL(p.long_term_billable_bytes, 0)"

        if not aggregate_sharded_stats:
            return {
                "row_count_select": own_rows,
                "bytes_select": own_bytes,
                "active_billable_select": own_active,
                "long_term_billable_select": own_long_term,
                "shard_stats_join": "",
            }

        table_base = sharded_table_base_expr("t.table_name")
        billable_rollup = (
            "\n        sum(case when storage_tier = 'LONG_TERM' then total_billable_bytes"
            " else 0 end) as long_term_billable_bytes,"
            "\n        sum(case when storage_tier = 'ACTIVE' then total_billable_bytes"
            " else 0 end) as active_billable_bytes"
        )

        if use_legacy_table_stats:
            # Row count / size are summed from __TABLES__; billable bytes come from a
            # separate PARTITIONS rollup since __TABLES__ does not expose them.
            billable_alias = "shb"
            shard_stats_join = f"""
  left join (
    select
        {sharded_table_base_expr(name_col)} as table_base,
        sum({src_rows}) as total_rows,
        sum({src_bytes}) as total_bytes
    from
        `{project_id}`.`{dataset_name}`.{source}
    group by
        table_base) as sh on
    {table_base} = sh.table_base
    and t.table_name != {table_base}
  left join (
    select
        {sharded_table_base_expr("table_name")} as table_base,{billable_rollup}
    from
        `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.PARTITIONS
    group by
        table_base) as shb on
    {table_base} = shb.table_base
    and t.table_name != {table_base}"""
        else:
            # A single PARTITIONS rollup covers row count, size and billable bytes.
            billable_alias = "sh"
            shard_stats_join = f"""
  left join (
    select
        {sharded_table_base_expr(name_col)} as table_base,
        sum({src_rows}) as total_rows,
        sum({src_bytes}) as total_bytes,{billable_rollup}
    from
        `{project_id}`.`{dataset_name}`.{source}
    group by
        table_base) as sh on
    {table_base} = sh.table_base
    and t.table_name != {table_base}"""

        return {
            "row_count_select": f"COALESCE(sh.total_rows, {own_rows})",
            "bytes_select": f"COALESCE(sh.total_bytes, {own_bytes})",
            "active_billable_select": (
                f"COALESCE({billable_alias}.active_billable_bytes, {own_active})"
            ),
            "long_term_billable_select": (
                f"IFNULL(COALESCE({billable_alias}.long_term_billable_bytes,"
                " p.long_term_billable_bytes), 0)"
            ),
            "shard_stats_join": shard_stats_join,
        }


BQ_FILTER_RULE_TEMPLATE_V2_LINEAGE = """
resource.type=("bigquery_project")
AND
(
    protoPayload.methodName=
        (
            "google.cloud.bigquery.v2.JobService.Query"
            OR
            "google.cloud.bigquery.v2.JobService.InsertJob"
        )
    AND
    protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
    AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
    AND (
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
        OR
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedViews:*
    )
    AND (
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/_.*/tables/anon.*"
        AND
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/INFORMATION_SCHEMA.*"
        AND
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/__TABLES__"
        AND
        protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable !~ "projects/.*/datasets/_.*/tables/anon.*"
    )

)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip()
BQ_FILTER_RULE_TEMPLATE_V2_USAGE = """
resource.type=("bigquery_project" OR "bigquery_dataset")
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
AND protoPayload.serviceName="bigquery.googleapis.com"
AND
(
    (
        protoPayload.methodName=
            (
                "google.cloud.bigquery.v2.JobService.Query"
                OR
                "google.cloud.bigquery.v2.JobService.InsertJob"
            )
        AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
        AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
        AND protoPayload.metadata.jobChange.job.jobConfig.queryConfig:*
        AND
        (
            (
                protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
                AND NOT protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables =~ "projects/.*/datasets/.*/tables/__TABLES__|__TABLES_SUMMARY__|INFORMATION_SCHEMA.*"
            )
            OR
            (
                protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable:*
            )
        )
    )
    OR
    protoPayload.metadata.tableDataRead.reason = "JOB"
)
""".strip("\t \n")


def bigquery_audit_metadata_query_template_lineage(
    dataset: str, use_date_sharded_tables: bool, limit: Optional[int] = None
) -> str:
    """
    Receives a dataset (with project specified) and returns a query template that is used to query exported
    AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
    Include only those that:
    - have been completed (jobStatus.jobState = "DONE")
    - do not contain errors (jobStatus.errorResults is none)
    :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
    :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
           tables
    :param limit: set a limit for the maximum event to return. It is used for connection testing currently
    :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
    """
    limit_text = f"limit {limit}" if limit else ""

    shard_condition = ""
    if use_date_sharded_tables:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access_*`"
        shard_condition = (
            """ AND _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}" """
        )
    else:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access`"

    query = f"""
            SELECT
                timestamp,
                logName,
                insertId,
                protopayload_auditlog AS protoPayload,
                protopayload_auditlog.metadataJson AS metadata
            FROM
                {from_table}
            WHERE (
                timestamp >= "{{start_time}}"
                AND timestamp < "{{end_time}}"
            )
            {shard_condition}
            AND protopayload_auditlog.serviceName="bigquery.googleapis.com"
            AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
            AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.errorResults") IS NULL
            AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY insertId, timestamp, logName) = 1
            {limit_text};
        """

    return textwrap.dedent(query)


def bigquery_audit_metadata_query_template_usage(
    dataset: str,
    use_date_sharded_tables: bool,
    limit: Optional[int] = None,
) -> str:
    """
    Receives a dataset (with project specified) and returns a query template that is used to query exported
    v2 AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
    :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
    :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
           tables
    :param limit: maximum number of events to query for
    :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
    """

    limit_text = f"limit {limit}" if limit else ""

    shard_condition = ""
    if use_date_sharded_tables:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access_*`"
        shard_condition = (
            """ AND _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}" """
        )
    else:
        from_table = f"`{dataset}.cloudaudit_googleapis_com_data_access`"

    # Deduplicates insertId via QUALIFY, see:
    # https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry, insertId field
    query = f"""
        SELECT
            timestamp,
            logName,
            insertId,
            protopayload_auditlog AS protoPayload,
            protopayload_auditlog.metadataJson AS metadata
        FROM
            {from_table}
        WHERE (
            timestamp >= "{{start_time}}"
            AND timestamp < "{{end_time}}"
        )
        {shard_condition}
        AND protopayload_auditlog.serviceName="bigquery.googleapis.com"
        AND
        (
            (
                protopayload_auditlog.methodName IN
                    (
                        "google.cloud.bigquery.v2.JobService.Query",
                        "google.cloud.bigquery.v2.JobService.InsertJob"
                    )
                AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
                AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.errorResults") IS NULL
                AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL
                AND (
                        JSON_EXTRACT_ARRAY(protopayload_auditlog.metadataJson,
                                                            "$.jobChange.job.jobStats.queryStats.referencedTables") IS NOT NULL
                    OR
                        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig.destinationTable") IS NOT NULL
                    )
            )
            OR
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.tableDataRead.reason") = "JOB"
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY insertId, timestamp, logName) = 1
        {limit_text};
    """

    return textwrap.dedent(query)
