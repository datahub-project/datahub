import logging
import re
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.types import AssertionStdParameterType

logger = logging.getLogger(__name__)


def validate_sql_is_select_only(
    sql: str,
    context: str = "SQL query",
    dialect: Optional[str] = None,
) -> None:
    """Validate that SQL contains only SELECT statements using sqlglot parsing.

    This provides robust SQL injection prevention by parsing the SQL syntax tree
    and ensuring only read-only SELECT operations are allowed. This prevents
    destructive operations like DROP, DELETE, INSERT, UPDATE, ALTER, etc.

    Args:
        sql: The SQL string to validate
        context: Description of where the SQL is being used (for error messages)
        dialect: SQL dialect to use for parsing (e.g., "databricks", "bigquery", "snowflake")
                If None, uses sqlglot's default dialect

    Raises:
        InvalidParametersException: If SQL is not a SELECT statement or cannot be parsed

    Example:
        >>> validate_sql_is_select_only("SELECT * FROM users", "filter")
        # No error - valid SELECT

        >>> validate_sql_is_select_only("DROP TABLE users", "filter")
        # Raises InvalidParametersException
    """
    if not sql or not sql.strip():
        raise InvalidParametersException(
            message=f"{context} cannot be empty",
            parameters={"sql": sql},
        )

    try:
        # Remove trailing and leading whitespace
        sql_clean = sql.strip()

        # Parse all statements in the SQL (handles semicolon-separated statements)
        # Using sqlglot.parse() instead of parse_one() to catch multiple statements
        parsed_statements = sqlglot.parse(sql_clean, dialect=dialect)

        if not parsed_statements:
            # If parsing produced no statements, try fallback validation
            _validate_sql_fallback(sql_clean, context)
            return

        # Check each statement - ALL must be SELECT statements
        for i, parsed in enumerate(parsed_statements):
            # Check if it's a SELECT statement
            if not isinstance(parsed, exp.Select):
                statement_type = type(parsed).__name__
                statement_count_msg = (
                    f" (statement {i + 1} of {len(parsed_statements)})"
                    if len(parsed_statements) > 1
                    else ""
                )
                raise InvalidParametersException(
                    message=f"{context} must be a SELECT statement, got {statement_type}{statement_count_msg}. "
                    f"Only read-only queries are allowed for security reasons.",
                    parameters={
                        "sql": sql[:200],  # Include preview
                        "statement_type": statement_type,
                        "allowed_types": ["Select"],
                    },
                )

            # Additional check: ensure no CTEs contain non-SELECT statements
            # (WITH clauses could potentially contain INSERT/UPDATE/DELETE)
            if parsed.ctes:
                for cte in parsed.ctes:
                    if not isinstance(cte.this, exp.Select):
                        raise InvalidParametersException(
                            message=f"{context} contains non-SELECT statement in CTE. "
                            f"Only SELECT statements are allowed.",
                            parameters={"sql": sql[:200]},
                        )

    except InvalidParametersException:
        # Re-raise our validation errors
        raise
    except Exception as e:
        # Parsing failed - could be due to unquoted keywords in identifiers or actual SQL issues
        # Use fallback validation to check for dangerous patterns
        try:
            _validate_sql_fallback(sql.strip(), context)
        except InvalidParametersException:
            # Fallback validation found dangerous keywords, re-raise
            raise
        # Fallback validation passed (no dangerous keywords found).
        # Check if this looks like a reasonable SELECT query despite parse errors
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("SELECT") and " FROM " in sql_upper:
            # Looks like a SELECT statement with formatting issues (e.g., unquoted keywords)
            # Allow it through since it's not dangerous
            return
        # Otherwise, it's likely invalid SQL - raise an error
        raise InvalidParametersException(
            message=f"{context} contains invalid SQL: {str(e)}",
            parameters={"sql": sql[:200], "parse_error": str(e)},
        )


def _validate_sql_fallback(sql: str, context: str) -> None:
    """Fallback validation when sqlglot parsing fails.

    This is used when SQL has formatting issues (like unquoted keywords in identifiers)
    that prevent parsing, but we still want to catch dangerous SQL commands.

    Checks for dangerous SQL keywords that would indicate non-SELECT operations.
    """
    # Convert to uppercase for keyword matching
    sql_upper = sql.upper()

    # List of dangerous SQL keywords that indicate destructive operations
    # These should NOT appear as statement keywords in read-only queries
    dangerous_patterns = [
        (r"\bINSERT\s+INTO\b", "INSERT"),
        (r"\bUPDATE\s+", "UPDATE"),
        (r"\bDELETE\s+FROM\b", "DELETE"),
        (r"\bDROP\s+", "DROP"),
        (r"\bCREATE\s+", "CREATE"),
        (r"\bALTER\s+", "ALTER"),
        (r"\bTRUNCATE\s+", "TRUNCATE"),
        (r"\bMERGE\s+", "MERGE"),
        (r"\bEXEC\s*\(", "EXEC"),
        (r"\bEXECUTE\s*\(", "EXECUTE"),
    ]

    for pattern, keyword in dangerous_patterns:
        if re.search(pattern, sql_upper):
            raise InvalidParametersException(
                message=f"{context} appears to contain {keyword} statement. "
                f"Only SELECT statements are allowed for security reasons.",
                parameters={
                    "sql": sql[:200],
                    "detected_keyword": keyword,
                },
            )


def setup_high_watermark_field_value_query(
    column_name: str,
    database_string: str,
    filter_sql: str,
    previous_value: Optional[str],
) -> str:
    filter_sql_part = ""
    if filter_sql:
        if previous_value:
            filter_sql_part = f"AND {filter_sql}"
        else:
            filter_sql_part = f"WHERE {filter_sql}"

    get_value_query = f"""
        SELECT {column_name}
        FROM {database_string}
        {f"WHERE {column_name} >= {previous_value}" if previous_value else ""}
        {filter_sql_part}
        ORDER by {column_name} DESC
        LIMIT 1;
    """
    logger.debug(get_value_query)

    return get_value_query


def setup_high_watermark_row_count_query(
    column_name: str,
    database_string: str,
    filter_sql: str,
    current_field_value: str,
) -> str:
    get_count_query = f"""
        SELECT COUNT(*)
        FROM {database_string}
        WHERE {column_name} = {current_field_value}
        {f"AND {filter_sql}" if filter_sql else ""}
    """
    logger.debug(get_count_query)
    return get_count_query


def setup_row_count_query(
    database_string: str,
    filter_sql: str,
) -> str:
    get_count_query = f"""
        SELECT COUNT(*)
        FROM {database_string}
        {f"WHERE {filter_sql}" if filter_sql else ""}
    """
    logger.debug(get_count_query)
    return get_count_query


def get_field_value(
    parameter_value: str,
    parameter_type: AssertionStdParameterType,
) -> str:
    if parameter_type == AssertionStdParameterType.STRING:
        return f"'{parameter_value}'"
    return parameter_value
