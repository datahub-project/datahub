"""
BigQuery profiler security utilities for SQL injection protection.

IMPORTANT SECURITY NOTE:
========================
This module provides TWO layers of SQL injection protection:

1. **Identifier Validation** (validate_bigquery_identifier):
   - Used for table/column/schema NAMES that cannot be parameterized
   - BigQuery does NOT support parameterized identifiers (e.g., table names)
   - These must be validated and escaped manually using backticks
   - Example: SELECT * FROM `project`.`dataset`.`table_name`

2. **Value Parameterization** (used throughout codebase):
   - Used for actual DATA VALUES in WHERE clauses, comparisons, etc.
   - BigQuery DOES support parameterized values via QueryJobConfig
   - Always use ScalarQueryParameter for values when possible
   - Example: WHERE partition_col = @partition_value

This file handles case #1. Case #2 is handled by QueryJobConfig with
ScalarQueryParameter throughout the codebase (see partition_discovery.py).
"""

import logging
from typing import List, Optional, Union

from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    FILTER_COLUMN_REF_RE,
    FILTER_DANGEROUS_PATTERNS,
    FILTER_OPERATOR_RE,
    PROJECT_ID_RE,
    SQL_ALLOWED_START_PATTERNS,
    SQL_DANGEROUS_PATTERNS,
    TABLE_IDENTIFIER_RE,
    VALID_COLUMN_NAME_PATTERN,
    WHITESPACE_RE,
)

logger = logging.getLogger(__name__)


def _validate_identifier_format(identifier_type: str, clean_identifier: str) -> None:
    """Validate identifier format according to BigQuery rules for the given type."""
    if identifier_type == "project":
        if not PROJECT_ID_RE.match(clean_identifier):
            raise ValueError(f"Invalid project ID format: {clean_identifier}")
        if len(clean_identifier) < 6 or len(clean_identifier) > 30:
            raise ValueError(f"Project ID must be 6-30 characters: {clean_identifier}")
        if "--" in clean_identifier:
            raise ValueError(
                f"Project ID cannot contain consecutive hyphens: {clean_identifier}"
            )
    elif identifier_type == "table":
        # BigQuery allows hyphens in table names when backtick-escaped.
        if not TABLE_IDENTIFIER_RE.match(clean_identifier):
            raise ValueError(
                f"Invalid {identifier_type} identifier format: {clean_identifier}"
            )
        if len(clean_identifier) > 1024:
            raise ValueError(
                f"{identifier_type} identifier too long: {len(clean_identifier)} chars"
            )
        if "--" in clean_identifier:
            raise ValueError(
                f"Table identifier cannot contain consecutive hyphens: {clean_identifier}"
            )
    else:
        # Datasets and columns: letters, numbers, underscores only (no hyphens).
        if not VALID_COLUMN_NAME_PATTERN.match(clean_identifier):
            raise ValueError(
                f"Invalid {identifier_type} identifier format: {clean_identifier}"
            )
        if len(clean_identifier) > 1024:
            raise ValueError(
                f"{identifier_type} identifier too long: {len(clean_identifier)} chars"
            )
        if clean_identifier.startswith("__"):
            raise ValueError(
                f"Invalid {identifier_type} identifier cannot start with double underscore: {clean_identifier}"
            )


def validate_bigquery_identifier(
    identifier: str, identifier_type: str = "general"
) -> str:
    """
    Validate and escape BigQuery identifiers (table/column/schema names) against SQL injection.

    IMPORTANT: This is for IDENTIFIERS only (table names, column names, etc.).
    For DATA VALUES, use parameterized queries with QueryJobConfig and ScalarQueryParameter.

    BigQuery does NOT support parameterized identifiers, so we must:
    1. Validate the identifier contains only safe characters
    2. Escape it with backticks for safe SQL construction

    Args:
        identifier: The identifier to validate (table name, column name, etc.)
        identifier_type: Type of identifier for error messages

    Returns:
        Backtick-escaped identifier safe for SQL construction

    Raises:
        ValueError: If identifier contains dangerous characters
    """
    if not identifier or not isinstance(identifier, str):
        raise ValueError(
            f"Invalid {identifier_type} identifier: must be non-empty string"
        )

    # Strip whitespace to prevent padding attacks
    identifier = identifier.strip()

    # Special handling for BigQuery system tables and schemas
    if identifier.startswith("INFORMATION_SCHEMA"):
        # INFORMATION_SCHEMA is a special system schema in BigQuery
        if identifier == "INFORMATION_SCHEMA" or identifier.startswith(
            "INFORMATION_SCHEMA."
        ):
            # Allow system tables like INFORMATION_SCHEMA.TABLES, INFORMATION_SCHEMA.COLUMNS, etc.
            # These are built-in BigQuery system views and are safe
            return f"`{identifier}`"

    # Check for actual SQL injection attack vectors in identifiers
    # Only check for patterns that would actually break SQL syntax or represent clear attacks
    # We don't block SQL keywords because BigQuery allows them as identifiers when backticked
    dangerous_patterns = [
        ";",  # Statement terminator - clear SQL injection
        "--",  # SQL comment - clear injection vector
        "/*",  # Block comment start - injection vector
        "*/",  # Block comment end - injection vector
        '"',  # Double quote - can break out of identifier context
        "'",  # Single quote - can break out of string context
        "\\",  # Backslash - escape character, potential injection
        "\n",  # Newline - can break SQL structure
        "\r",  # Carriage return - can break SQL structure
        "\t",  # Tab - generally not allowed in identifiers
        "`",  # Backtick - we add these ourselves, shouldn't be in input
    ]

    # Check for actual injection patterns, not legitimate words
    for pattern in dangerous_patterns:
        if pattern in identifier:
            raise ValueError(
                f"Invalid {identifier_type} identifier contains dangerous character '{pattern}': {identifier}"
            )

    # Check for script injection patterns (these are actual attack vectors)
    script_injection_patterns = [
        "javascript:",
        "vbscript:",
        "<script",
        "</script>",
        "eval(",
        "expression(",
        "onload=",
        "onerror=",
        "onclick=",
    ]

    identifier_lower = identifier.lower()
    for pattern in script_injection_patterns:
        if pattern in identifier_lower:
            raise ValueError(
                f"Invalid {identifier_type} identifier contains script injection pattern '{pattern}': {identifier}"
            )

    # Remove any existing backticks to prevent escape sequence injection
    clean_identifier = identifier.replace("`", "")

    # Additional check: ensure no control characters or non-printable characters
    if any(ord(c) < 32 or ord(c) > 126 for c in clean_identifier):
        raise ValueError(
            f"Invalid {identifier_type} identifier contains non-printable characters: {identifier}"
        )

    # BigQuery identifier rules validation (project / table / dataset+column)
    _validate_identifier_format(identifier_type, clean_identifier)

    # Check for truly problematic identifiers that could cause issues even when backticked
    truly_problematic = {
        "__null__",
        "__unpartitioned__",
        "__temp__",
        "null",
        "true",
        "false",
    }

    if clean_identifier.lower() in truly_problematic:
        logger.debug(
            f"Identifier '{clean_identifier}' may cause issues in BigQuery contexts but is allowed when backticked"
        )

    # Return with backticks for safe SQL usage
    return f"`{clean_identifier}`"


def build_safe_table_reference(project: str, dataset: str, table: str) -> str:
    """Build a safe fully-qualified BigQuery table reference."""
    # Special handling for INFORMATION_SCHEMA tables
    if table.startswith("INFORMATION_SCHEMA"):
        safe_project = validate_bigquery_identifier(project, "project")
        safe_dataset = validate_bigquery_identifier(dataset, "dataset")
        return f"{safe_project}.{safe_dataset}.`{table}`"

    safe_project = validate_bigquery_identifier(project, "project")
    safe_dataset = validate_bigquery_identifier(dataset, "dataset")
    safe_table = validate_bigquery_identifier(table, "table")

    return f"{safe_project}.{safe_dataset}.{safe_table}"


def validate_column_name(col_name: str, context: str = "") -> bool:
    """Validate a column name against BigQuery identifier rules."""
    if not col_name or not isinstance(col_name, str):
        logger.warning(
            f"Invalid column name{' in ' + context if context else ''}: {col_name}"
        )
        return False

    if not VALID_COLUMN_NAME_PATTERN.match(col_name):
        logger.warning(
            f"Column name fails validation{' in ' + context if context else ''}: {col_name}"
        )
        return False

    return True


def validate_column_names(col_names: List[str], context: str = "") -> List[str]:
    """Validate multiple column names and return only valid ones."""
    valid_columns = []
    for col in col_names:
        if validate_column_name(col, context):
            valid_columns.append(col)
    return valid_columns


def validate_sql_structure(query: str) -> bool:
    """Validate SQL query structure for security issues."""
    if not query or not isinstance(query, str):
        return False

    # Normalize query for analysis
    normalized_query = WHITESPACE_RE.sub(" ", query.upper().strip())

    # Check for dangerous SQL patterns that shouldn't appear in profiling queries
    for pattern in SQL_DANGEROUS_PATTERNS:
        if pattern.search(normalized_query):
            raise ValueError(f"Query contains dangerous pattern: {pattern.pattern}")

    # Validate query starts with expected operations for profiling
    if not any(p.match(normalized_query) for p in SQL_ALLOWED_START_PATTERNS):
        raise ValueError(f"Query must start with SELECT or WITH: {query[:100]}...")

    return True


def validate_filter_expression(filter_expr: str) -> bool:
    """
    Validate that a filter expression is safe.

    Args:
        filter_expr: The filter expression to validate

    Returns:
        True if the filter is considered safe
    """
    if not filter_expr or not isinstance(filter_expr, str):
        return False

    # Check for basic SQL injection patterns that would be dangerous
    for pattern in FILTER_DANGEROUS_PATTERNS:
        if pattern.search(filter_expr):
            logger.warning(
                f"Filter contains dangerous pattern {pattern.pattern}: {filter_expr}"
            )
            return False

    # Basic format check: ensure it looks like a reasonable WHERE clause condition
    # This is much simpler - just ensure it has column references and reasonable operators
    if not FILTER_COLUMN_REF_RE.search(filter_expr):
        logger.warning(f"Filter doesn't contain valid column reference: {filter_expr}")
        return False

    # Ensure it uses reasonable operators (=, !=, <, >, IS NULL, etc.)
    if not FILTER_OPERATOR_RE.search(filter_expr):
        logger.warning(f"Filter doesn't contain recognized operators: {filter_expr}")
        return False

    return True


def validate_and_filter_expressions(filters: List[str], context: str = "") -> List[str]:
    """
    Validate a list of filter expressions and return only safe ones.

    Args:
        filters: List of filter expressions to validate
        context: Context for logging (optional)

    Returns:
        List of validated filter expressions
    """
    validated_filters = []
    for filter_str in filters:
        if validate_filter_expression(filter_str):
            validated_filters.append(filter_str)
        else:
            logger.warning(
                f"Rejecting filter{' in ' + context if context else ''}: {filter_str}"
            )

    if not validated_filters and filters:
        logger.warning(
            f"No valid filters after validation{' in ' + context if context else ''}"
        )

    return validated_filters


def has_malicious_patterns(value: str) -> bool:
    """
    Check if a string contains potentially malicious SQL patterns.

    Args:
        value: String to check

    Returns:
        True if malicious patterns detected
    """
    malicious_patterns = [
        "UNION",
        "SELECT",
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "--",
        "/*",
        "xp_cmdshell",
        "sp_executesql",
    ]

    value_upper = str(value).upper()
    return any(pattern in value_upper for pattern in malicious_patterns)


def clamp_numeric_value(
    value: Union[str, int, float],
    min_val: int,
    max_val: int,
    default: Optional[int] = None,
) -> int:
    """
    Safely clamp a numeric value to a range.

    Args:
        value: Value to clamp
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        default: Default value if conversion fails

    Returns:
        Clamped integer value
    """
    try:
        int_val = int(value)
        return max(min_val, min(int_val, max_val))
    except (ValueError, TypeError):
        if default is not None:
            return default
        return min_val


def create_safe_parameter_name(base_name: str, value: Union[str, int, float]) -> str:
    """
    Create a safe parameter name for BigQuery parameterized queries.

    Args:
        base_name: Base name for the parameter
        value: Value being parameterized (used for uniqueness)

    Returns:
        Safe parameter name
    """
    # Create a hash-based suffix for uniqueness while keeping it deterministic
    hash_suffix = abs(hash(str(value))) % 10000
    return f"{base_name}_{hash_suffix}"


def build_column_list_for_query(columns: List[str], backtick_wrap: bool = True) -> str:
    """
    Build a comma-separated list of columns for use in SQL queries.

    Args:
        columns: List of column names (should be pre-validated)
        backtick_wrap: Whether to wrap column names in backticks

    Returns:
        Comma-separated column list
    """
    if backtick_wrap:
        return ", ".join([f"`{col}`" for col in columns])
    else:
        return ", ".join(columns)


def build_where_conditions(columns: List[str], condition: str = "IS NOT NULL") -> str:
    """
    Build WHERE conditions for multiple columns.

    Args:
        columns: List of column names (should be pre-validated)
        condition: Condition to apply to each column

    Returns:
        WHERE clause conditions joined with AND
    """
    return " AND ".join([f"`{col}` {condition}" for col in columns])
