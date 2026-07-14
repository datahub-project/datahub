import re
from typing import Set, Tuple

# BigQuery partition ID lengths for date/time formats: YYYYMMDD and YYYYMMDDHH.
PARTITION_ID_YYYYMMDD_LENGTH = 8
PARTITION_ID_YYYYMMDDHH_LENGTH = 10


# Column names that suggest date/time data, used as a fallback when type info is
# unavailable. 'day' is excluded: in partition contexts it's usually a day number (1-31).
DATE_LIKE_COLUMN_NAMES: Set[str] = {
    "date",
    "dt",
    "ts",
    "time",
    "timestamp",
    "datetime",
    "date_partition",
    "partition_time",
    "partition_timestamp",
    "partition_dt",
    "pt_date",
    "pdate",
    "event_date",
    "event_time",
    "event_timestamp",
    "event_dt",
    "created_date",
    "created_time",
    "created_at",
    "create_date",
    "creation_date",
    "creation_time",
    "updated_date",
    "updated_time",
    "updated_at",
    "modified_date",
    "modified_time",
    "modified_at",
    "last_modified",
    "last_updated",
    "trade_date",
    "trader_date",
    "trading_date",
    "business_date",
    "settlement_date",
    "value_date",
    "booking_date",
    "deal_date",
    "execution_date",
    "transaction_date",
    "transaction_time",
    "txn_date",
    "txn_time",
    "process_date",
    "process_time",
    "processed_date",
    "processed_time",
    "load_date",
    "load_time",
    "loaded_at",
    "insert_date",
    "insert_time",
    "inserted_at",
    "ingest_date",
    "ingest_time",
    "ingestion_date",
    "start_date",
    "start_time",
    "end_date",
    "end_time",
    "effective_date",
    "effective_time",
    "expiry_date",
    "expiration_date",
    "maturity_date",
    "record_date",
    "record_time",
    "snapshot_date",
    "snapshot_time",
    "run_date",
    "run_time",
    "batch_date",
    "batch_time",
}

DATE_COMPONENT_COLUMNS: Tuple[str, ...] = ("year", "month", "day")

DATE_TIME_TYPES: Set[str] = {
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIME",
}

# String types that must be quoted when used as literals in a filter.
BIGQUERY_STRING_TYPES: Set[str] = {
    "STRING",
    "BYTES",
    "GEOGRAPHY",
    "JSON",
}


VALID_COLUMN_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# BigQuery project ID: lowercase letters, numbers, hyphens; 6-30 chars.
PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]*[a-z0-9]$")

# Table name: hyphens allowed (unlike column/dataset identifiers).
TABLE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


# Collapses runs of whitespace when normalising a query before injection checks.
WHITESPACE_RE = re.compile(r"\s+")

# DDL, DML, admin, and script-injection patterns that must not appear in profiling queries
SQL_DANGEROUS_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|FUNCTION|PROCEDURE)",
        r"\bDROP\s+(?:TABLE|VIEW|FUNCTION|PROCEDURE|DATABASE|SCHEMA)",
        r"\bALTER\s+(?:TABLE|VIEW|DATABASE|SCHEMA)",
        r"\bTRUNCATE\s+TABLE",
        r"\bINSERT\s+INTO",
        r"\bUPDATE\s+.+\bSET\b",
        r"\bDELETE\s+FROM",
        r"\bMERGE\s+INTO",
        r"\bGRANT\s+",
        r"\bREVOKE\s+",
        r"\bEXEC(?:UTE)?\s+",
        r"\bCALL\s+",
        r";\s*(?:CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|GRANT|REVOKE)",
        r"<script[^>]*>",
        r"javascript:",
        r"vbscript:",
        r"data:",
        r"/\*.*(?:union|select|insert|update|delete|drop|create|alter).*\*/",
        r"--.*(?:union|select|insert|update|delete|drop|create|alter)",
    ]
]

# Patterns that a valid profiling query must start with.
SQL_ALLOWED_START_PATTERNS = [
    re.compile(p)
    for p in [
        r"^\s*SELECT\s+",
        r"^\s*WITH\s+",
        r"^\s*\(\s*SELECT\s+",
    ]
]

# Injection patterns that must not appear in WHERE-clause filter expressions
FILTER_DANGEROUS_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r";\s*(?:DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|TRUNCATE)\s+",
        r"UNION\s+(?:ALL\s+)?SELECT",
        r"--",
        r"/\*",
        r"xp_cmdshell",
        r"sp_executesql",
        r"<script",
        r"javascript:",
        r"eval\s*\(",
    ]
]

# A valid backtick-quoted column reference inside a filter expression.
FILTER_COLUMN_REF_RE = re.compile(r"`[a-zA-Z_][a-zA-Z0-9_]*`")

# Same, but captures the column name (without backticks).
BACKTICK_COLUMN_NAME_RE = re.compile(r"`([a-zA-Z_][a-zA-Z0-9_]*)`")

# Recognised SQL comparison / membership operators in filter expressions.
FILTER_OPERATOR_RE = re.compile(
    r"(?:=|!=|<>|<|>|<=|>=|IS\s+(?:NOT\s+)?NULL|LIKE|NOT\s+LIKE|IN\s*\()",
    re.IGNORECASE,
)

# Extracts required partition columns from BigQuery's "requires a filter over
# column(s) 'year', 'month', 'day'" error (up to four columns).
PARTITION_FILTER_PATTERN = re.compile(
    r"filter over column\(s\) '([^']+)'(?:, '([^']+)')?(?:, '([^']+)')?(?:, '([^']+)')?",
    re.IGNORECASE,
)

DATE_FORMAT_YYYYMMDD = "YYYYMMDD"
DATE_FORMAT_YYYY_MM_DD = "YYYY-MM-DD"
DATE_FORMAT_YYYYMMDDHH = "YYYYMMDDHH"

STRFTIME_FORMATS = {
    DATE_FORMAT_YYYYMMDD: "%Y%m%d",
    DATE_FORMAT_YYYY_MM_DD: "%Y-%m-%d",
    DATE_FORMAT_YYYYMMDDHH: "%Y%m%d%H",
}

# Matches an ISO 8601 date string, used to detect YYYY-MM-DD strings destined for
# integer partition columns so they can be converted.
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# BigQuery partition-id / date-literal shapes normalised by FilterBuilder.
DATETIME_SECONDS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
PARTITION_ID_YYYYMMDD_PATTERN = re.compile(r"^\d{8}$")
PARTITION_ID_YYYYMM_PATTERN = re.compile(r"^\d{6}$")
PARTITION_ID_YYYYMMDDHH_PATTERN = re.compile(r"^\d{10}$")

# Captures the column and right-hand literal of a `col` = <literal> partition
# predicate so date windowing can reproduce the literal's exact shape.
PARTITION_EQ_LITERAL_RE = re.compile(r"`([a-zA-Z_][a-zA-Z0-9_]*)`\s*=\s*(.+?)\s*$")

# Unwraps a DATE()/DATETIME()/TIMESTAMP() call around a date literal.
DATE_WRAPPER_RE = re.compile(r"(DATE|DATETIME|TIMESTAMP)\((.+)\)", re.IGNORECASE)

# Shape of a date literal's inner (unquoted) value -> format name. Used to render
# a windowing range bound with the same format the source equality filter used.
DATE_LITERAL_SHAPES = (
    (DATE_FORMAT_YYYYMMDDHH, PARTITION_ID_YYYYMMDDHH_PATTERN),
    (DATE_FORMAT_YYYY_MM_DD, ISO_DATE_PATTERN),
    (DATE_FORMAT_YYYYMMDD, PARTITION_ID_YYYYMMDD_PATTERN),
)


MAX_PARTITION_VALUES = 1000

SAMPLING_PERCENT = 0.001  # 0.1% sample rate for large tables
SAMPLING_LIMIT_ROWS = 5
TEST_QUERY_LIMIT_ROWS = 1

# Fallback row cap for unpartitioned tables when profiling_row_limit is 0 (unlimited).
# Applied only above BQ_SAFETY_ROW_LIMIT_THRESHOLD rows to keep a full-scan profile from OOMing.
BQ_SAFETY_ROW_LIMIT = 100_000
BQ_SAFETY_ROW_LIMIT_THRESHOLD = 1_000_000

DEFAULT_PARTITION_STATS_LIMIT = 10
DEFAULT_MAX_PARTITION_VALUES = 3  # distinct values to discover per partition column


# Numeric types requiring unquoted literals in SQL WHERE clauses.
BIGQUERY_NUMERIC_TYPES: Set[str] = {
    "INT64",
    "INTEGER",
    "INT",
    "SMALLINT",
    "BIGINT",
    "TINYINT",
    "BYTEINT",
    "NUMERIC",
    "DECIMAL",
    "BIGNUMERIC",
    "BIGDECIMAL",
    "FLOAT64",
    "FLOAT",
    "BOOL",
    "BOOLEAN",
}


# INFORMATION_SCHEMA.COLUMNS value flagging a column as part of the partition spec.
# SQL templates live in queries.py and take this as a format argument.
PARTITIONING_COLUMN_FLAG = "YES"

# batch_kwargs keys / values shared between the inline and deferred profiling paths
CUSTOM_SQL_KWARG = "custom_sql"
PARTITION_HANDLING_KWARG = "partition_handling"
PARTITION_HANDLING_ENABLED = "true"
