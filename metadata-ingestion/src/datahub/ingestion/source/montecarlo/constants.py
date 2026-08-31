from typing import Dict, Set

# Monte Carlo connection/warehouse types -> DataHub platform names. There is no
# canonical DataHub platform-name enum/registry usable from Python ingestion
# code (the closest, common/data_platforms.py's KNOWN_VALID_PLATFORM_NAMES, is
# incomplete and documented as unsuitable for validation), so this maps plain
# strings, matching every other connector's platform-name fields.
CONNECTION_TYPE_TO_PLATFORM: Dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "databricks-metastore": "databricks",
    "databricks-sql": "databricks",
    "spark": "spark",
    "presto": "presto",
    "hive": "hive",
    "glue": "glue",
    "athena": "athena",
    "postgres": "postgres",
    "mysql": "mysql",
    "oracle": "oracle",
    "sql-server": "mssql",
    "synapse": "mssql",
    "teradata": "teradata",
    "transactional-db": "postgres",
}

# Warehouse platforms whose DataHub source emits lowercased dataset URNs by
# default (Snowflake sets convert_urns_to_lowercase=True; Redshift folds unquoted
# identifiers to lowercase). MC assertion URNs must match those exactly to attach
# to the right dataset, so we lowercase the table path for these platforms only.
# Case-preserving platforms (e.g. BigQuery) keep the original case. The
# convert_urns_to_lowercase config flag forces lowercase everywhere when set.
LOWERCASE_URN_PLATFORMS: Set[str] = {"snowflake", "redshift"}

# Monte Carlo comparison operators -> DataHub AssertionStdOperator. MC operators
# not in this map (AUTO, AUTO_HIGH, AUTO_LOW, NOOP, OUTSIDE_RANGE) have no clean
# DataHub equivalent and fall back to _NATIVE_ at the call site, matching dbt's
# unknown-test handling. INSIDE_RANGE maps to BETWEEN (min/max value parameters);
# OUTSIDE_RANGE is deliberately NOT mapped to NOT_IN — BETWEEN's negation isn't
# NOT_IN's semantics, so _NATIVE_ is the honest choice.
MC_OPERATOR_TO_STD_OPERATOR: Dict[str, str] = {
    "EQ": "EQUAL_TO",
    "GT": "GREATER_THAN",
    "GTE": "GREATER_THAN_OR_EQUAL_TO",
    "LT": "LESS_THAN",
    "LTE": "LESS_THAN_OR_EQUAL_TO",
    "NEQ": "NOT_EQUAL_TO",
    "INSIDE_RANGE": "BETWEEN",
    "IS_NULL": "NULL",
    "IS_NOT_NULL": "NOT_NULL",
}

# Monte Carlo metric -> DataHub AssertionStdAggregation. Intentionally sparse:
# only metrics with an unambiguous DataHub aggregation are mapped. Unmapped
# metrics (the long tail of MC metric strings, plus customMetric) fall back to
# _NATIVE_ at the call site, which still triggers the structured-rendering path
# via scope+operator/nativeType. Add entries here only when a metric's DataHub
# aggregation is certain; gaps are safe.
MC_METRIC_TO_STD_AGGREGATION: Dict[str, str] = {
    "row_count": "ROW_COUNT",
    "count": "ROW_COUNT",
    "distinct_count": "UNIQUE_COUNT",
    "null_count": "NULL_COUNT",
    "null_rate": "NULL_PROPORTION",
    "min": "MIN",
    "max": "MAX",
    "mean": "MEAN",
    "median": "MEDIAN",
    "stddev": "STDDEV",
    "sum": "SUM",
}
