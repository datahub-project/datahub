from typing import Dict, Set

# Monte Carlo warehouse connection types -> DataHub platform names.
#
# The keys are the lowercased values of Monte Carlo's GraphQL
# ``WarehouseModelConnectionType`` enum (returned by ``getTable.warehouse
# .connectionType``), which uses UPPER_UNDERSCORE names — e.g. ``SNOWFLAKE``,
# ``BIGQUERY``, ``TRANSACTIONAL_DB``. The resolver lowercases the value before
# lookup, so every key here is lowercase with underscores.
#
# There is no canonical DataHub platform-name enum/registry usable from Python
# ingestion code (the closest, common/data_platforms.py's
# KNOWN_VALID_PLATFORM_NAMES, is incomplete and documented as unsuitable for
# validation), so the values are plain strings matching the platform name each
# warehouse's DataHub source connector emits in its dataset URNs.
#
# ``TRANSACTIONAL_DB`` is deliberately NOT mapped: it is a category spanning
# postgres, sql-server, synapse, sap-hana, azure-sql and others, so mapping it
# to any single platform would silently mis-attach most assertions. Users with
# a transactional-db warehouse must set ``connection_to_platform_map`` for the
# specific warehouse resource id. Unmapped connection types warn and skip
# rather than guess.
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
    "mysql": "mysql",
    "oracle": "oracle",
    "teradata": "teradata",
    "clickhouse": "clickhouse",
    "dremio": "dremio",
    "db2": "db2",
    "starburst_enterprise": "trino",
    "starburst_galaxy": "trino",
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

# getMetricsV4 metricName -> AssertionResult typed slot. AssertionResult has
# exactly four typed slots: rowCount (long), missingCount (long),
# unexpectedCount (long), actualAggValue (float). Metrics not in either map
# degrade to nativeResults (map[string, string]) — the only catch-all on
# AssertionResult. This is the *measurement* path; MC_METRIC_TO_STD_AGGREGATION
# above is the *definition* path (monitor -> CustomAssertionInfo.aggregation).
MC_METRIC_TO_RESULT_SLOT: Dict[str, str] = {
    "total_row_count": "rowCount",
    "row_count": "rowCount",
    "null_count": "missingCount",
    "missing_count": "missingCount",
    "unexpected_count": "unexpectedCount",
}

# Metrics that carry a single scalar aggregate -> actualAggValue (float). No
# per-field typed slot exists on AssertionResult (DatasetFieldProfile is off
# the table per the no-external-entity-update constraint), so field-level
# metrics land here as the scalar aggregate, not as per-field profiles.
MC_METRIC_TO_AGG_VALUE: Set[str] = {
    "null_rate",
    "distinct_count",
    "unique_count",
    "min",
    "max",
    "mean",
    "median",
    "stddev",
    "sum",
}

# TABLE monitors declare four comparison metrics, but only total_row_count
# returns points from getMetricsV4; the other three are non-standard names
# that return zero points. Fetching only total_row_count for TABLE monitors
# avoids three wasted API calls per TABLE monitor per ingestion.
TABLE_METRICS_TO_FETCH: Set[str] = {"total_row_count"}

# Custom metric names (custom_value_based_metric_<uuid>) return zero points
# from getMetricsV4 — custom metrics use a separate surface. Skip fetching
# metrics for any comparison whose metric starts with this prefix.
CUSTOM_METRIC_PREFIX = "custom_value_based_metric_"
