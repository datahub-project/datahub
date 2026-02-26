"""
Constants for BigQuery profiling operations.

This module centralizes all constants used across the profiling module including:
- Partition ID format specifications
- Date/time column name patterns
- Compiled regex patterns for performance
"""

import re
from typing import Set

# ============================================================================
# Partition ID Format Constants
# ============================================================================

# BigQuery partition ID lengths for different date/time formats
PARTITION_ID_YYYYMMDD_LENGTH = 8  # Date format: YYYYMMDD (e.g., 20250115)
PARTITION_ID_YYYYMMDDHH_LENGTH = 10  # Datetime with hour: YYYYMMDDHH (e.g., 2025011523)


# ============================================================================
# Date/Time Column Name Patterns
# ============================================================================

# Column names that suggest date/time data (used as fallback when type info is unavailable)
# Note: 'day' is excluded as it typically refers to day number (1-31) in partition contexts
DATE_LIKE_COLUMN_NAMES: Set[str] = {
    # Basic date/time column names
    "date",
    "dt",
    "ts",
    "time",
    "timestamp",
    "datetime",
    # Partition-specific patterns
    "partition_date",
    "date_partition",
    "partition_time",
    "partition_timestamp",
    "partition_dt",
    "pt_date",
    "pdate",
    # Event/action timestamps
    "event_date",
    "event_time",
    "event_timestamp",
    "event_dt",
    # Creation timestamps
    "created_date",
    "created_time",
    "created_at",
    "create_date",
    "creation_date",
    "creation_time",
    # Update/modification timestamps
    "updated_date",
    "updated_time",
    "updated_at",
    "modified_date",
    "modified_time",
    "modified_at",
    "last_modified",
    "last_updated",
    # Trading/Financial date columns
    "trade_date",
    "trader_date",
    "trading_date",
    "business_date",
    "settlement_date",
    "value_date",
    "booking_date",
    "deal_date",
    "execution_date",
    # Transaction timestamps
    "transaction_date",
    "transaction_time",
    "txn_date",
    "txn_time",
    # Data pipeline timestamps
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
    # Lifecycle dates
    "start_date",
    "start_time",
    "end_date",
    "end_time",
    "effective_date",
    "effective_time",
    "expiry_date",
    "expiration_date",
    "maturity_date",
    # Record timestamps
    "record_date",
    "record_time",
    "snapshot_date",
    "snapshot_time",
    "run_date",
    "run_time",
    "batch_date",
    "batch_time",
}

# BigQuery date/time data types
DATE_TIME_TYPES: Set[str] = {
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIME",
}

# BigQuery string types that should be quoted
BIGQUERY_STRING_TYPES: Set[str] = {
    "STRING",
    "BYTES",
    "GEOGRAPHY",
    "JSON",
}


# ============================================================================
# Compiled Regex Patterns
# ============================================================================

# Valid BigQuery column name / dataset identifier pattern (letters, numbers, underscores)
VALID_COLUMN_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# BigQuery project ID pattern (lowercase letters, numbers, hyphens; 6-30 chars)
PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]*[a-z0-9]$")

# BigQuery table name pattern (letters, numbers, underscores, hyphens allowed)
TABLE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


# ============================================================================
# Security / SQL Injection Validation Patterns
# ============================================================================

# Collapses runs of whitespace when normalising a query before injection checks
WHITESPACE_RE = re.compile(r"\s+")

# DDL, DML, admin, and script-injection patterns that must not appear in profiling queries
SQL_DANGEROUS_PATTERNS = [
    re.compile(p)
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

# Patterns that a valid profiling query must start with
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
    re.compile(p)
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

# A valid backtick-quoted column reference inside a filter expression
FILTER_COLUMN_REF_RE = re.compile(r"`[a-zA-Z_][a-zA-Z0-9_]*`")

# Recognised SQL comparison / membership operators in filter expressions
FILTER_OPERATOR_RE = re.compile(
    r"(?:=|!=|<>|<|>|<=|>=|IS\s+(?:NOT\s+)?NULL|LIKE|NOT\s+LIKE|IN\s*\()",
    re.IGNORECASE,
)

# Pattern to extract required partition columns from BigQuery error messages
# Example: "Cannot query over table without a filter over column(s) 'year', 'month', 'day'"
PARTITION_FILTER_PATTERN = re.compile(
    r"filter over column\(s\) '([^']+)'(?:, '([^']+)')?(?:, '([^']+)')?(?:, '([^']+)')?",
    re.IGNORECASE,
)

# Pattern to extract partition values from path-like error messages
# Example: "feed=pp_tse/year=2025/month=09/day=16" → [('feed', 'pp_tse'), ('year', '2025'), ...]
PARTITION_PATH_PATTERN = re.compile(r"([a-zA-Z_]+)=([^/\s]+)")


# ============================================================================
# Date Format Constants
# ============================================================================

# Date format identifiers used across BigQuery partitions
DATE_FORMAT_YYYYMMDD = "YYYYMMDD"  # Format: 20250115 (8 digits, no separators)
DATE_FORMAT_YYYY_MM_DD = "YYYY-MM-DD"  # Format: 2025-01-15 (with dashes)
DATE_FORMAT_YYYYMMDDHH = "YYYYMMDDHH"  # Format: 2025011523 (10 digits with hour)

# Python strftime format strings for each date format
STRFTIME_FORMATS = {
    DATE_FORMAT_YYYYMMDD: "%Y%m%d",  # 20250115
    DATE_FORMAT_YYYY_MM_DD: "%Y-%m-%d",  # 2025-01-15
    DATE_FORMAT_YYYYMMDDHH: "%Y%m%d%H",  # 2025011523
}

# Matches an ISO 8601 date string (e.g. '2026-02-25'). Used in filter construction
# to detect and convert YYYY-MM-DD strings destined for integer partition columns.
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Regex patterns to detect date formats in partition filter expressions
# These are used to maintain consistent formatting when adding date windowing
DATE_FORMAT_PATTERNS = {
    DATE_FORMAT_YYYYMMDD: re.compile(r"'(\d{8})'"),  # Matches '20250115'
    DATE_FORMAT_YYYY_MM_DD: re.compile(
        r"'(\d{4}-\d{2}-\d{2})'"
    ),  # Matches '2025-01-15'
    DATE_FORMAT_YYYYMMDDHH: re.compile(r"'(\d{10})'"),  # Matches '2025011523'
}


# ============================================================================
# Query Configuration Constants
# ============================================================================

# Maximum number of partition values to fetch in a single query
MAX_PARTITION_VALUES = 1000

# Default number of strategic candidate dates to check (today, yesterday)
DEFAULT_STRATEGIC_DATE_COUNT = 2

# Query sampling constants for partition discovery
SAMPLING_PERCENT = 0.001  # 0.1% sample rate for large tables
SAMPLING_LIMIT_ROWS = 5  # Maximum rows to return when sampling
TEST_QUERY_LIMIT_ROWS = 1  # Row limit for test queries (validation)

# Fallback row cap for unpartitioned tables when profiling_row_limit is explicitly set
# to 0 (unlimited). Applied only to tables exceeding BQ_SAFETY_ROW_LIMIT_THRESHOLD rows
# to prevent full-scan profiling of extremely large tables from causing OOM failures.
BQ_SAFETY_ROW_LIMIT = 100_000
BQ_SAFETY_ROW_LIMIT_THRESHOLD = 1_000_000

# Default limits for partition discovery operations
DEFAULT_POPULATED_PARTITIONS_LIMIT = (
    5  # Max partitions from get_most_populated_partitions
)
DEFAULT_INFO_SCHEMA_PARTITIONS_LIMIT = 100  # Max partitions from INFORMATION_SCHEMA
DEFAULT_PARTITION_STATS_LIMIT = 10  # Max results for partition statistics queries
DEFAULT_MAX_PARTITION_VALUES = 3  # Max distinct values to discover per partition column


# ============================================================================
# BigQuery Data Types
# ============================================================================

# Numeric types requiring unquoted literals in SQL WHERE clauses
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
