# Central home for every SQL string the profiler issues. Templates use str.format
# placeholders so they can live as module-level constants. Row values are bound as
# query parameters (@name) where BigQuery allows it. The interpolated placeholders are
# NOT parameter-bound and must be made safe by the caller first: identifiers
# ({table_ref}, {col_name}, {info_schema_ref}) are validated/backtick-escaped, filter
# fragments ({where}, {order_by}) come from create_safe_filter / validated builders, and
# {sample_percent} is a code-controlled float (the SAMPLING_PERCENT constant or a computed
# percentage, never user input) because TABLESAMPLE rejects a query parameter there.

# Profiling SELECT fragments, shared by the inline (internal) and deferred (external)
# custom_sql paths so they cannot drift apart.
SELECT_ALL = "SELECT * FROM {table_ref}"
TABLESAMPLE_SYSTEM = "TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)"
WHERE_CLAUSE = "WHERE {where}"
LIMIT_CLAUSE = "LIMIT {limit}"

# One query per dataset pre-fetches every table's partition columns (profiler cache).
PARTITION_METADATA_CACHE = """
SELECT table_name, column_name, data_type
FROM {info_schema_ref}
WHERE is_partitioning_column = '{flag}'
ORDER BY table_name, ordinal_position
"""

# INFORMATION_SCHEMA.COLUMNS lookups for a single table.
PARTITION_COLUMN_NAMES = """SELECT column_name
FROM {info_schema_ref}
WHERE table_name = @table_name AND is_partitioning_column = '{flag}'"""

PARTITION_COLUMN_TYPES = """SELECT column_name, data_type
FROM {info_schema_ref}
WHERE table_name = @table_name AND is_partitioning_column = '{flag}'"""

PARTITION_COLUMN_TYPES_FILTERED = """SELECT column_name, data_type
FROM {info_schema_ref}
WHERE table_name = @table_name
AND ({column_filter_clause})"""

# Most recently modified populated partitions (excludes the NULL/unpartitioned/streaming
# sentinels). Used to derive filters from INFORMATION_SCHEMA.PARTITIONS on internal tables.
PARTITIONS_BY_MODIFIED = """SELECT partition_id, last_modified_time, total_rows
FROM {info_schema_ref}
WHERE table_name = @table_name
AND partition_id NOT IN ('{null_id}', '{unpartitioned_id}', '{streaming_id}')
AND total_rows > 0
ORDER BY last_modified_time DESC
LIMIT @max_partitions"""

# Cheap probe: succeeds on an unpartitioned table, raises "requires filter over
# column(s) ..." on a partitioned one so callers can parse the required columns.
COUNT_STAR_PROBE = "SELECT COUNT(*) FROM {table_ref} LIMIT @limit_rows"

# Confirms a candidate filter set actually matches rows before it is used.
PARTITION_EXISTS_CHECK = """SELECT 1 as exists_check
FROM {table_ref}
WHERE {where}
LIMIT 1"""

# Sampling fallbacks: newest row by a date column, or a table sample when there is none.
LATEST_BY_DATE_SAMPLE = """SELECT *
FROM {table_ref}
WHERE `{date_col}` IS NOT NULL
ORDER BY `{date_col}` DESC
LIMIT @limit_rows"""

# BigQuery requires a literal (not a query parameter) in the TABLESAMPLE percentage;
# sample_percent is the code-controlled SAMPLING_PERCENT constant, not user input.
TABLESAMPLE_SAMPLE = """SELECT *
FROM {table_ref} TABLESAMPLE SYSTEM ({sample_percent:.8f} PERCENT)
LIMIT @limit_rows"""

# Most common values of a non-date column within an already-narrowed partition scope.
TOP_VALUES_BY_COUNT = """
SELECT DISTINCT `{col_name}` as col_value, COUNT(*) as row_count
FROM {table_ref}
WHERE {where} AND `{col_name}` IS NOT NULL
GROUP BY `{col_name}`
ORDER BY row_count DESC
LIMIT @max_values"""

# Top partition values for a column by row count. order_by/limit_clause vary per probe;
# extra_where is folded into {where} by the caller.
PARTITION_STATS_CTE = """WITH PartitionStats AS (
    SELECT `{col_name}` as val, COUNT(*) as record_count
    FROM {table_ref}
    WHERE {where}
    GROUP BY `{col_name}`
    HAVING record_count > 0
    ORDER BY {order_by}
    LIMIT {limit_clause}
)
SELECT val, record_count FROM PartitionStats"""
