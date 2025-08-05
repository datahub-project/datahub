import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
)
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler

logger = logging.getLogger(__name__)


def validate_bigquery_identifier(
    identifier: str, identifier_type: str = "general"
) -> str:
    """
    Securely validate and escape BigQuery identifiers per official BigQuery rules.

    This function provides defense-in-depth against SQL injection attacks on identifiers
    that cannot be parameterized in BigQuery.

    BigQuery Rules:
    - Project IDs: letters, numbers, hyphens (but not at start/end)
    - Datasets: letters, numbers, underscores (NO hyphens)
    - Tables: letters, numbers, underscores (NO hyphens)
    - Columns: letters, numbers, underscores (NO hyphens)
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

    # Check for common SQL injection patterns in identifiers
    # Note: Removed 'script' from dangerous patterns as it interferes with legitimate table names
    dangerous_patterns = [
        ";",
        "--",
        "/*",
        "*/",
        "union",
        "select",
        "insert",
        "update",
        "delete",
        "drop",
        "create",
        "alter",
        "exec",
        "execute",
        "sp_",
        "xp_",
        "javascript:",
        "vbscript:",
        "<script",
        "</script>",
        "eval(",
        "expression(",
        "onload=",
        "onerror=",
        "onclick=",
        '"',
        "'",
        "\\",
        "\n",
        "\r",
        "\t",
    ]

    identifier_lower = identifier.lower()
    for pattern in dangerous_patterns:
        if pattern in identifier_lower:
            raise ValueError(
                f"Invalid {identifier_type} identifier contains dangerous pattern '{pattern}': {identifier}"
            )

    # Remove any existing backticks to prevent escape sequence injection
    clean_identifier = identifier.replace("`", "")

    # Additional check: ensure no control characters or non-printable characters
    if any(ord(c) < 32 or ord(c) > 126 for c in clean_identifier):
        raise ValueError(
            f"Invalid {identifier_type} identifier contains non-printable characters: {identifier}"
        )

    # BigQuery identifier rules - CORRECTED with additional security checks
    if identifier_type == "project":
        # Project IDs: letters, numbers, hyphens (6-30 chars, start with letter)
        if not re.match(r"^[a-z][a-z0-9-]*[a-z0-9]$", clean_identifier):
            raise ValueError(f"Invalid project ID format: {clean_identifier}")
        if len(clean_identifier) < 6 or len(clean_identifier) > 30:
            raise ValueError(f"Project ID must be 6-30 characters: {clean_identifier}")
        # Additional security: no consecutive hyphens
        if "--" in clean_identifier:
            raise ValueError(
                f"Project ID cannot contain consecutive hyphens: {clean_identifier}"
            )
    else:
        # Datasets, tables, columns: letters, numbers, underscores only
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", clean_identifier):
            raise ValueError(
                f"Invalid {identifier_type} identifier format: {clean_identifier}"
            )
        if len(clean_identifier) > 1024:
            raise ValueError(
                f"{identifier_type} identifier too long: {len(clean_identifier)} chars"
            )
        # Additional security: no consecutive underscores at the beginning (reserved patterns)
        if clean_identifier.startswith("__"):
            raise ValueError(
                f"Invalid {identifier_type} identifier cannot start with double underscore: {clean_identifier}"
            )

    # Note: We don't check for SQL reserved words here because:
    # 1. Project IDs follow Google Cloud naming rules, not SQL rules
    # 2. BigQuery allows reserved words as identifiers when escaped with backticks (which we do)
    # 3. The backticking ensures safe usage in SQL contexts

    # Only check for truly problematic identifiers that could cause issues even when backticked
    truly_problematic = {
        "__null__",
        "__unpartitioned__",
        "__temp__",
        "null",
        "true",
        "false",
    }

    if clean_identifier.lower() in truly_problematic:
        logger.warning(
            f"Identifier '{clean_identifier}' may cause issues in BigQuery contexts"
        )

    # Return with backticks for safe SQL usage
    return f"`{clean_identifier}`"


def execute_query(self, query: str) -> List[Any]:
    """
    Execute a BigQuery query with timeout from configuration and enhanced security validation.

    Args:
        query: SQL query to execute

    Returns:
        List of row results
    """
    # Enhanced security validation
    self._validate_sql_structure(query)

    # Additional basic pattern check for immediate threats
    dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
    for pattern in dangerous_patterns:
        if pattern in query:
            logger.error(
                f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
            )
            raise ValueError(f"Query contains dangerous pattern: {pattern}")

    try:
        logger.debug(
            f"Executing query with {self.config.profiling.partition_fetch_timeout}s timeout"
        )
        job_config = QueryJobConfig(
            job_timeout_ms=self.config.profiling.partition_fetch_timeout * 1000,
            # Additional security: disable query caching for sensitive operations
            use_query_cache=False,
        )
        query_job = self.config.get_bigquery_client().query(
            query, job_config=job_config
        )
        return list(query_job.result())
    except Exception as e:
        logger.warning(f"Query execution error: {e}")
        raise


def build_safe_table_reference(project: str, dataset: str, table: str) -> str:
    """
    Build a safe fully-qualified BigQuery table reference.

    Args:
        project: Project ID (can contain hyphens)
        dataset: Dataset name (letters, numbers, underscores only)
        table: Table name (letters, numbers, underscores only)

    Returns:
        Safe table reference like `project`.`dataset`.`table`
    """
    # Special handling for INFORMATION_SCHEMA tables
    if table.startswith("INFORMATION_SCHEMA"):
        safe_project = validate_bigquery_identifier(project, "project")
        safe_dataset = validate_bigquery_identifier(dataset, "dataset")
        return f"{safe_project}.{safe_dataset}.`{table}`"

    safe_project = validate_bigquery_identifier(project, "project")
    safe_dataset = validate_bigquery_identifier(dataset, "dataset")
    safe_table = validate_bigquery_identifier(table, "table")

    return f"{safe_project}.{safe_dataset}.{safe_table}"


class BigqueryProfiler(GenericProfiler):
    """
    BigQuery-specific profiler that handles partitioned tables intelligently.

    Features:
    - Smart date partition discovery (tries yesterday first, then recent dates)
    - SQL injection protection via parameterized queries
    - Timeout-aware query execution
    - Fallback mechanisms for when partition discovery fails
    - Support for various partition types (date, timestamp, year/month/day)
    """

    config: BigQueryV2Config
    report: BigQueryV2Report

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        super().__init__(config, report, "bigquery", state_handler)
        self.config = config
        self.report = report

    def _validate_and_escape_identifiers(
        self, project: str, schema: str, table_name: str
    ) -> Tuple[str, str, str]:
        """
        Validate and safely escape BigQuery identifiers.

        Args:
            project: BigQuery project ID
            schema: BigQuery dataset name
            table_name: BigQuery table name

        Returns:
            Tuple of (safe_project, safe_schema, safe_table_name) - already escaped with backticks

        Raises:
            ValueError: If any identifier is invalid
        """
        try:
            safe_project = validate_bigquery_identifier(project, "project")
            safe_schema = validate_bigquery_identifier(schema, "dataset")
            safe_table_name = validate_bigquery_identifier(table_name, "table")
            return safe_project, safe_schema, safe_table_name
        except ValueError as e:
            logger.error(f"Invalid BigQuery identifier: {e}")
            raise

    def _validate_column_name(self, col_name: str, context: str = "") -> bool:
        """
        Validate a column name against BigQuery identifier rules.

        Args:
            col_name: Column name to validate
            context: Context for logging (optional)

        Returns:
            True if valid, False otherwise
        """
        if not col_name or not isinstance(col_name, str):
            logger.warning(
                f"Invalid column name{' in ' + context if context else ''}: {col_name}"
            )
            return False

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
            logger.warning(
                f"Column name fails validation{' in ' + context if context else ''}: {col_name}"
            )
            return False

        return True

    def _validate_column_names(
        self, col_names: List[str], context: str = ""
    ) -> List[str]:
        """
        Validate multiple column names and return only valid ones.

        Args:
            col_names: List of column names to validate
            context: Context for logging (optional)

        Returns:
            List of valid column names
        """
        valid_columns = []
        for col in col_names:
            if self._validate_column_name(col, context):
                valid_columns.append(col)
        return valid_columns

    def _validate_and_filter_expressions(
        self, filters: List[str], context: str = ""
    ) -> List[str]:
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
            if self._validate_filter_expression(filter_str):
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

    def _is_date_like_column(self, col_name: str) -> bool:
        """
        Check if a column name suggests it contains date/time data.

        Args:
            col_name: Column name to check

        Returns:
            True if column appears to be date-related
        """
        return col_name.lower() in {
            "date",
            "day",
            "dt",
            "partition_date",
            "date_partition",
            "timestamp",
            "datetime",
            "time",
            "created_at",
            "modified_at",
            "event_time",
        }

    def _is_year_like_column(self, col_name: str) -> bool:
        """Check if column appears to contain year values."""
        col_lower = col_name.lower()
        return col_lower in {"year", "partition_year", "year_partition", "yr"}

    def _is_month_like_column(self, col_name: str) -> bool:
        """Check if column appears to contain month values."""
        col_lower = col_name.lower()
        return col_lower in {"month", "partition_month", "month_partition", "mo"}

    def _is_day_like_column(self, col_name: str) -> bool:
        """Check if column appears to contain day values."""
        col_lower = col_name.lower()
        return col_lower in {"day", "partition_day", "day_partition", "dy"}

    def _get_column_ordering_strategy(self, col_name: str) -> str:
        """
        Get the appropriate ORDER BY strategy for a column based on its type.

        Args:
            col_name: Column name

        Returns:
            ORDER BY clause fragment
        """
        if (
            self._is_date_like_column(col_name)
            or self._is_year_like_column(col_name)
            or self._is_month_like_column(col_name)
            or self._is_day_like_column(col_name)
        ):
            return f"`{col_name}` DESC"  # Most recent first for time-based columns
        else:
            return "record_count DESC"  # Most populated first for other columns

    def _execute_query_safely(
        self, query: str, job_config: Optional[QueryJobConfig] = None, context: str = ""
    ) -> List[Any]:
        """
        Execute a query with consistent error handling and logging.

        Args:
            query: SQL query to execute
            job_config: Optional query job configuration with parameters
            context: Context for logging (optional)

        Returns:
            Query results as list
        """
        try:
            logger.debug(
                f"Executing query{' for ' + context if context else ''}: {query}"
            )
            if job_config:
                return self.execute_query_with_config(query, job_config)
            else:
                return self.execute_query(query)
        except Exception as e:
            logger.warning(
                f"Query execution failed{' in ' + context if context else ''}: {e}"
            )
            raise

    def _build_column_list_for_query(
        self, columns: List[str], backtick_wrap: bool = True
    ) -> str:
        """
        Build a comma-separated list of columns for use in SQL queries.

        Args:
            columns: List of column names
            backtick_wrap: Whether to wrap column names in backticks

        Returns:
            Comma-separated column list
        """
        if backtick_wrap:
            return ", ".join([f"`{col}`" for col in columns])
        else:
            return ", ".join(columns)

    def _build_where_conditions(
        self, columns: List[str], condition: str = "IS NOT NULL"
    ) -> str:
        """
        Build WHERE conditions for multiple columns.

        Args:
            columns: List of column names
            condition: Condition to apply to each column

        Returns:
            WHERE clause conditions joined with AND
        """
        return " AND ".join([f"`{col}` {condition}" for col in columns])

    def _create_partition_stats_query(
        self, table_ref: str, col_name: str, max_results: int = 10
    ) -> Tuple[str, QueryJobConfig]:
        """
        Create a standardized partition statistics query with parameterized limit.

        Args:
            table_ref: Safe table reference
            col_name: Column name (must be pre-validated)
            max_results: Maximum number of results

        Returns:
            Tuple of (SQL query, QueryJobConfig with parameters)
        """
        order_by = self._get_column_ordering_strategy(col_name)
        safe_max_results = max(1, min(int(max_results), 1000))

        query = f"""WITH PartitionStats AS (
    SELECT `{col_name}` as val, COUNT(*) as record_count
    FROM {table_ref}
    WHERE `{col_name}` IS NOT NULL
    GROUP BY `{col_name}`
    HAVING record_count > 0
    ORDER BY {order_by}
    LIMIT @max_results
)
SELECT val, record_count FROM PartitionStats"""

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("max_results", "INT64", safe_max_results)
            ]
        )

        return query, job_config

    def _get_fallback_date_value(
        self, col_name: str, fallback_date: datetime
    ) -> Tuple[Any, str]:
        """
        Get an appropriate fallback value for a date-related column.

        Args:
            col_name: Column name
            fallback_date: Base date to derive value from

        Returns:
            Tuple of (value, format_string)
        """
        if self._is_year_like_column(col_name):
            return fallback_date.year, "numeric"
        elif self._is_month_like_column(col_name):
            return fallback_date.month, "zero_padded"
        elif self._is_day_like_column(col_name):
            return fallback_date.day, "zero_padded"
        else:
            return fallback_date.strftime("%Y-%m-%d"), "date_string"

    def _log_partition_attempt(
        self,
        method: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        success: Optional[bool] = None,
    ) -> None:
        """
        Standardized logging for partition discovery attempts.

        Args:
            method: Method being attempted
            table_name: Table name
            columns: Column names (optional)
            success: Whether attempt was successful (optional, for completion logging)
        """
        col_info = f" for columns {columns}" if columns else ""
        if success is None:
            logger.debug(f"Attempting {method} for table {table_name}{col_info}")
        elif success:
            logger.debug(f"{method} succeeded for table {table_name}{col_info}")
        else:
            logger.debug(f"{method} failed for table {table_name}{col_info}")

    def _has_malicious_patterns(self, value: str) -> bool:
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

    def _clamp_numeric_value(
        self, value: Any, min_val: int, max_val: int, default: Optional[int] = None
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

    def _validate_sql_structure(self, query: str) -> bool:
        """
        Validate SQL query structure for additional security beyond parameterization.

        Args:
            query: SQL query to validate

        Returns:
            True if query structure is safe

        Raises:
            ValueError: If query contains dangerous patterns
        """
        if not query or not isinstance(query, str):
            return False

        # Normalize query for analysis
        normalized_query = re.sub(r"\s+", " ", query.upper().strip())

        # Check for dangerous SQL patterns that shouldn't appear in profiling queries
        dangerous_patterns = [
            # DDL operations
            r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|FUNCTION|PROCEDURE)",
            r"\bDROP\s+(?:TABLE|VIEW|FUNCTION|PROCEDURE|DATABASE|SCHEMA)",
            r"\bALTER\s+(?:TABLE|VIEW|DATABASE|SCHEMA)",
            r"\bTRUNCATE\s+TABLE",
            # DML operations (beyond SELECT)
            r"\bINSERT\s+INTO",
            r"\bUPDATE\s+.+\bSET\b",
            r"\bDELETE\s+FROM",
            r"\bMERGE\s+INTO",
            # System/admin operations
            r"\bGRANT\s+",
            r"\bREVOKE\s+",
            r"\bEXEC(?:UTE)?\s+",
            r"\bCALL\s+",
            # Suspicious multi-statement patterns
            r";\s*(?:CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|GRANT|REVOKE)",
            # Script injection patterns
            r"<script[^>]*>",
            r"javascript:",
            r"vbscript:",
            r"data:",
            # Comment-based injections
            r"/\*.*(?:union|select|insert|update|delete|drop|create|alter).*\*/",
            r"--.*(?:union|select|insert|update|delete|drop|create|alter)",
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, normalized_query, re.IGNORECASE):
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

        # Validate query starts with expected operations for profiling
        allowed_start_patterns = [
            r"^\s*SELECT\s+",
            r"^\s*WITH\s+",
            r"^\s*\(\s*SELECT\s+",  # Subqueries
        ]

        if not any(
            re.match(pattern, normalized_query, re.IGNORECASE)
            for pattern in allowed_start_patterns
        ):
            raise ValueError(f"Query must start with SELECT or WITH: {query[:100]}...")

        return True

    def _create_whitelist_validator(self, project: str, schema: str) -> Callable:
        """
        Create a validator that checks identifiers against a whitelist.

        Args:
            project: BigQuery project ID
            schema: BigQuery dataset name

        Returns:
            Validator function that checks if table names are in allowed list
        """

        def validate_against_whitelist(table_name: str) -> bool:
            # In a production environment, you'd load this from configuration
            # or query INFORMATION_SCHEMA to get allowed tables
            try:
                # Special handling for INFORMATION_SCHEMA tables - always allow these system tables
                if table_name.startswith("INFORMATION_SCHEMA"):
                    return True

                # Query to get all tables in the dataset (this creates a dynamic whitelist)
                # Build the reference manually to avoid circular validation issues
                safe_project = validate_bigquery_identifier(project, "project")
                safe_schema = validate_bigquery_identifier(schema, "dataset")

                # For INFORMATION_SCHEMA.TABLES, we need to handle it specially
                info_schema_ref = (
                    f"{safe_project}.{safe_schema}.`INFORMATION_SCHEMA.TABLES`"
                )

                query = f"""SELECT table_name 
    FROM {info_schema_ref} 
    WHERE table_name = @table_name"""

                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter("table_name", "STRING", table_name)
                    ]
                )

                results = self._execute_query_safely(
                    query, job_config, "whitelist validation"
                )
                return len(results) > 0

            except Exception as e:
                logger.warning(f"Whitelist validation failed for {table_name}: {e}")
                # Fail securely - if we can't validate, assume it's not allowed
                return False

        return validate_against_whitelist

    def _build_safe_table_reference(
        self, project: str, schema: str, table_name: str
    ) -> str:
        """Build a safe table reference for use in SQL queries with enhanced validation."""

        # Special handling for INFORMATION_SCHEMA system tables
        if table_name.startswith("INFORMATION_SCHEMA"):
            # For system tables, use simpler validation
            safe_project = validate_bigquery_identifier(project, "project")
            safe_schema = validate_bigquery_identifier(schema, "dataset")
            # Don't validate INFORMATION_SCHEMA table names through the regular validator
            return f"{safe_project}.{safe_schema}.`{table_name}`"

        # Enhanced validation with whitelist checking for regular tables
        # Create whitelist validator
        whitelist_validator = self._create_whitelist_validator(project, schema)

        # Only validate against whitelist for non-system tables
        if not whitelist_validator(table_name):
            logger.warning(f"Table {table_name} not found in dataset {schema}")
            # Note: We log but don't fail here as the table might be external or newly created

        return build_safe_table_reference(project, schema, table_name)

    def _create_secure_partition_filter(
        self, col_name: str, val: Any, data_type: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """
        Create a secure filter string and parameters for a partition column with a specific value.

        Args:
            col_name: Column name (will be validated)
            val: Value for the filter
            data_type: Data type of the column

        Returns:
            Tuple of (filter string with parameter placeholders, list of parameters) or None if invalid
        """
        # Validate column name
        if not self._validate_column_name(col_name, "partition filter creation"):
            return None

        # Get escaped column name
        escaped_col = f"`{col_name}`"
        data_type_upper = data_type.upper() if data_type else ""

        try:
            # Delegate to type-specific handlers
            return self._create_filter_for_data_type(escaped_col, val, data_type_upper)
        except Exception as e:
            logger.warning(f"Error creating filter for {col_name}={val}: {e}")
            return None

    def _create_filter_for_data_type(
        self, escaped_col: str, val: Any, data_type_upper: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for BigQuery-specific data types with parameters."""
        param_name = f"filter_value_{hash(str(val)) % 10000}"

        # BigQuery standard types
        if data_type_upper in ("STRING",):  # BigQuery uses STRING, not VARCHAR
            return self._create_string_filter(escaped_col, val, param_name)
        elif data_type_upper == "DATE":
            return self._create_date_filter(escaped_col, val, param_name)
        elif data_type_upper in ("TIMESTAMP", "DATETIME"):
            return self._create_timestamp_filter(escaped_col, val, param_name)
        elif data_type_upper in ("INT64", "INTEGER"):  # BigQuery uses INT64
            return self._create_integer_filter(escaped_col, val, param_name)
        elif data_type_upper in ("FLOAT64", "NUMERIC", "BIGNUMERIC"):  # BigQuery types
            return self._create_float_filter(escaped_col, val, param_name)
        elif data_type_upper in ("BOOL", "BOOLEAN"):
            return self._create_boolean_filter(escaped_col, val, param_name)
        elif data_type_upper in ("BYTES",):  # BigQuery binary type
            return self._create_bytes_filter(escaped_col, val, param_name)
        elif data_type_upper in ("GEOGRAPHY", "JSON"):  # BigQuery special types
            return self._create_special_type_filter(
                escaped_col, val, data_type_upper, param_name
            )
        else:
            return self._create_default_filter(escaped_col, val, param_name)

    def _create_string_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for STRING/VARCHAR data types with parameters."""
        if not isinstance(val, str):
            val = str(val)

        # Additional validation - reject suspicious patterns
        if self._has_malicious_patterns(val):
            logger.warning(f"Rejecting potentially malicious string value: {val}")
            return None

        filter_str = f"{escaped_col} = @{param_name}"
        parameters = [ScalarQueryParameter(param_name, "STRING", val)]
        return filter_str, parameters

    def _create_date_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for DATE data type - BigQuery specific with parameters."""
        if isinstance(val, datetime):
            date_str = val.strftime("%Y-%m-%d")
        elif isinstance(val, str):
            # Validate BigQuery date format YYYY-MM-DD
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                # Try to parse common date formats for BigQuery
                try:
                    parsed_date = datetime.strptime(val, "%Y%m%d")  # YYYYMMDD format
                    date_str = parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    logger.warning(f"Invalid date format for BigQuery: {val}")
                    return None
            else:
                date_str = val
        else:
            logger.warning(f"Invalid date type for BigQuery: {type(val)}")
            return None

        filter_str = f"{escaped_col} = @{param_name}"
        parameters = [ScalarQueryParameter(param_name, "DATE", date_str)]
        return filter_str, parameters

    def _create_timestamp_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for TIMESTAMP data type - BigQuery specific with parameters."""
        if isinstance(val, datetime):
            # BigQuery TIMESTAMP format
            timestamp_str = val.strftime("%Y-%m-%d %H:%M:%S UTC")
        elif isinstance(val, str):
            # Validate BigQuery timestamp formats
            timestamp_patterns = [
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",  # YYYY-MM-DD HH:MM:SS
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$",  # ISO format
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$",  # With UTC
            ]

            if not any(re.match(pattern, val) for pattern in timestamp_patterns):
                logger.warning(f"Invalid timestamp format for BigQuery: {val}")
                return None
            timestamp_str = val
        else:
            logger.warning(f"Invalid timestamp type for BigQuery: {type(val)}")
            return None

        filter_str = f"{escaped_col} = @{param_name}"
        parameters = [ScalarQueryParameter(param_name, "TIMESTAMP", timestamp_str)]
        return filter_str, parameters

    def _create_integer_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for integer data types with parameters."""
        try:
            numeric_val = int(val)
            filter_str = f"{escaped_col} = @{param_name}"
            parameters = [ScalarQueryParameter(param_name, "INT64", numeric_val)]
            return filter_str, parameters
        except (ValueError, TypeError):
            logger.warning(f"Invalid integer value: {val}")
            return None

    def _create_float_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for float data types with parameters."""
        try:
            numeric_val = float(val)
            filter_str = f"{escaped_col} = @{param_name}"
            parameters = [ScalarQueryParameter(param_name, "FLOAT64", numeric_val)]
            return filter_str, parameters
        except (ValueError, TypeError):
            logger.warning(f"Invalid float value: {val}")
            return None

    def _create_boolean_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Tuple[str, List[ScalarQueryParameter]]:
        """Create filter for boolean data types with parameters."""
        if isinstance(val, bool):
            bool_val = val
        elif isinstance(val, str):
            bool_val = val.lower() in ("true", "1", "yes", "on")
        else:
            bool_val = bool(val)

        filter_str = f"{escaped_col} = @{param_name}"
        parameters = [ScalarQueryParameter(param_name, "BOOL", bool_val)]
        return filter_str, parameters

    def _create_bytes_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for BYTES data type with parameters."""
        # BigQuery BYTES literals use base64 encoding
        if isinstance(val, bytes):
            import base64

            bytes_str = base64.b64encode(val).decode("utf-8")
            filter_str = f"{escaped_col} = FROM_BASE64(@{param_name})"
            parameters = [ScalarQueryParameter(param_name, "STRING", bytes_str)]
            return filter_str, parameters
        elif isinstance(val, str):
            # Assume it's already base64 encoded
            filter_str = f"{escaped_col} = FROM_BASE64(@{param_name})"
            parameters = [ScalarQueryParameter(param_name, "STRING", val)]
            return filter_str, parameters
        else:
            logger.warning(f"Invalid bytes value for BigQuery: {val}")
            return None

    def _create_special_type_filter(
        self, escaped_col: str, val: Any, data_type: str, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filters for special BigQuery types like GEOGRAPHY, JSON with parameters."""
        if data_type == "GEOGRAPHY":
            # BigQuery GEOGRAPHY typically uses WKT format
            if isinstance(val, str):
                filter_str = f"{escaped_col} = ST_GEOGFROMTEXT(@{param_name})"
                parameters = [ScalarQueryParameter(param_name, "STRING", val)]
                return filter_str, parameters
        elif data_type == "JSON":
            # BigQuery JSON type
            if isinstance(val, (dict, list)):
                import json

                json_str = json.dumps(val)
                filter_str = f"{escaped_col} = PARSE_JSON(@{param_name})"
                parameters = [ScalarQueryParameter(param_name, "STRING", json_str)]
                return filter_str, parameters
            elif isinstance(val, str):
                filter_str = f"{escaped_col} = PARSE_JSON(@{param_name})"
                parameters = [ScalarQueryParameter(param_name, "STRING", val)]
                return filter_str, parameters

        logger.warning(f"Unsupported special type filter for {data_type}: {val}")
        return None

    def _create_default_filter(
        self, escaped_col: str, val: Any, param_name: str
    ) -> Optional[Tuple[str, List[ScalarQueryParameter]]]:
        """Create filter for unknown data types (treat as string with validation) using parameters."""
        str_val = str(val)

        if self._has_malicious_patterns(str_val):
            logger.warning(
                f"Rejecting potentially malicious value for unknown type: {val}"
            )
            return None

        filter_str = f"{escaped_col} = @{param_name}"
        parameters = [ScalarQueryParameter(param_name, "STRING", str_val)]
        return filter_str, parameters

    def _validate_filter_expression(self, filter_expr: str) -> bool:
        """
        Validate that a filter expression is safe.

        Args:
            filter_expr: The filter expression to validate

        Returns:
            True if the filter is considered safe
        """
        if not filter_expr or not isinstance(filter_expr, str):
            return False

        # Check for basic SQL injection patterns
        dangerous_patterns = [
            r";\s*DROP\s+",
            r";\s*DELETE\s+",
            r";\s*INSERT\s+",
            r";\s*UPDATE\s+",
            r"UNION\s+SELECT",
            r"--",
            r"/\*",
            r"xp_cmdshell",
            r"sp_executesql",
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, filter_expr, re.IGNORECASE):
                logger.warning(
                    f"Filter contains dangerous pattern {pattern}: {filter_expr}"
                )
                return False

        # Validate that filter follows expected format: `column` operator value or `column` operator @param
        expected_pattern = r"^`[a-zA-Z_][a-zA-Z0-9_]*`\s*(?:=|!=|<|>|<=|>=|IS\s+NOT\s+NULL|IS\s+NULL)\s*(?:\d+(?:\.\d+)?|\'[^\']*\'|@[a-zA-Z_][a-zA-Z0-9_]*|(?:DATE|TIMESTAMP)\s*(?:\'[^\']*\'|@[a-zA-Z_][a-zA-Z0-9_]*)|(?:TRUE|FALSE)|FROM_BASE64\(@[a-zA-Z_][a-zA-Z0-9_]*\)|ST_GEOGFROMTEXT\(@[a-zA-Z_][a-zA-Z0-9_]*\)|PARSE_JSON\(@[a-zA-Z_][a-zA-Z0-9_]*\))$"

        if not re.match(expected_pattern, filter_expr.strip(), re.IGNORECASE):
            logger.warning(f"Filter doesn't match expected pattern: {filter_expr}")
            return False

        return True

    def _create_partition_filter_from_value(
        self, col_name: str, val: Any, data_type: str
    ) -> Tuple[str, List[ScalarQueryParameter]]:
        """
        Create a secure filter string and parameters for a partition column with a specific value.
        """
        filter_result = self._create_secure_partition_filter(col_name, val, data_type)

        if filter_result is None:
            # Fallback to IS NOT NULL if we can't create a safe filter
            if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                return f"`{col_name}` IS NOT NULL", []
            else:
                raise ValueError(f"Cannot create safe filter for column: {col_name}")

        filter_expr, parameters = filter_result

        # Double-check the filter is safe
        if not self._validate_filter_expression(filter_expr):
            raise ValueError(
                f"Generated filter failed security validation: {filter_expr}"
            )

        return filter_expr, parameters

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        partition_range_map: Dict[int, Tuple[relativedelta, str]] = {
            4: (relativedelta(years=1), "%Y"),
            6: (relativedelta(months=1), "%Y%m"),
            8: (relativedelta(days=1), "%Y%m%d"),
            10: (relativedelta(hours=1), "%Y%m%d%H"),
        }

        duration: relativedelta
        if partition_range_map.get(len(partition_id)):
            (delta, format) = partition_range_map[len(partition_id)]
            duration = delta
            if not partition_datetime:
                partition_datetime = datetime.strptime(partition_id, format)
            else:
                partition_datetime = datetime.strptime(
                    partition_datetime.strftime(format), format
                )
        else:
            raise ValueError(
                f"check your partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
            )
        upper_bound_partition_datetime = partition_datetime + duration
        return partition_datetime, upper_bound_partition_datetime

    def _get_most_populated_partitions(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        max_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Find the most populated partitions for a table.

        Uses INFORMATION_SCHEMA.PARTITIONS for regular tables.
        Falls back to table queries for external tables.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_columns: List of partition column names
            max_results: Maximum number of top partitions to return

        Returns:
            Dictionary mapping partition column names to their values
        """
        if not partition_columns:
            return {}

        # For external tables, INFORMATION_SCHEMA.PARTITIONS will be empty
        # so we need to query the actual table
        if table.external:
            logger.debug(f"Using table query approach for external table {table.name}")
            return self._get_partition_info_from_table_query(
                table, project, schema, partition_columns, max_results
            )
        else:
            logger.debug(
                f"Using INFORMATION_SCHEMA approach for regular table {table.name}"
            )
            # Try INFORMATION_SCHEMA first (more efficient)
            result = self._get_partition_info_from_information_schema(
                table,
                project,
                schema,
                partition_columns,
                max_results * 10,  # Query more partitions from info schema
            )

            # If INFORMATION_SCHEMA didn't work, fall back to table query
            if not result:
                logger.debug(
                    f"INFORMATION_SCHEMA approach failed, falling back to table query for {table.name}"
                )
                result = self._get_partition_info_from_table_query(
                    table, project, schema, partition_columns, max_results
                )

            return result

    def _get_partition_column_types(self, table, project, schema, partition_columns):
        """Get data types for partition columns using parameterized queries."""
        if not partition_columns:
            return {}

        try:
            # Use utility for validation
            safe_columns = self._validate_column_names(
                partition_columns, "column type lookup"
            )

            if not safe_columns:
                logger.warning(f"No valid column names provided for table {table.name}")
                return {}

            # Build safe table reference
            safe_info_schema_ref = self._build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            # Use parameterized query building
            column_conditions = []
            parameters = [ScalarQueryParameter("table_name", "STRING", table.name)]

            for i, col_name in enumerate(safe_columns):
                param_name = f"col_{i}"
                column_conditions.append(f"column_name = @{param_name}")
                parameters.append(ScalarQueryParameter(param_name, "STRING", col_name))

            column_filter_clause = " OR ".join(column_conditions)

            query = f"""SELECT column_name, data_type
FROM {safe_info_schema_ref}
WHERE table_name = @table_name 
AND ({column_filter_clause})"""

            job_config = QueryJobConfig(query_parameters=parameters)

            # Use utility for execution
            query_results = self._execute_query_safely(
                query, job_config, "partition column types"
            )
            return {row.column_name: row.data_type for row in query_results}
        except Exception as e:
            logger.warning(f"Error getting partition column types: {e}")
            return {}

    def _get_partition_info_from_information_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        max_results: int = 100,
    ) -> Dict[str, Any]:
        """
        Get partition information from INFORMATION_SCHEMA.PARTITIONS for regular tables.
        This is more efficient and works with partition filter requirements.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_columns: List of partition column names
            max_results: Maximum number of partitions to query

        Returns:
            Dictionary mapping partition column names to their values
        """
        if not partition_columns:
            return {}

        try:
            # Query INFORMATION_SCHEMA.PARTITIONS to get partition information
            # This is cheaper and works with partition filter requirements
            safe_max_results = max(
                1, min(int(max_results), 1000)
            )  # Clamp between 1 and 1000

            safe_info_schema_ref = self._build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.PARTITIONS"
            )

            query = f"""SELECT partition_id, total_rows
FROM {safe_info_schema_ref}
WHERE table_name = @table_name 
AND partition_id != '__NULL__'
AND partition_id != '__UNPARTITIONED__'
AND total_rows > 0
ORDER BY total_rows DESC
LIMIT @max_results"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name),
                    ScalarQueryParameter("max_results", "INT64", safe_max_results),
                ]
            )

            partition_info_results = self._execute_query_safely(
                query, job_config, "partition info from information schema"
            )

            if not partition_info_results:
                logger.warning(
                    f"No partitions found in INFORMATION_SCHEMA for table {table.name}"
                )
                return {}

            # Take the partition with the most rows
            best_partition = partition_info_results[0]
            partition_id = best_partition.partition_id

            logger.debug(
                f"Found best partition {partition_id} with {best_partition.total_rows} rows"
            )

            # Parse partition_id to extract values for each partition column
            # partition_id format can be like: 20231201, 2023120100, feed=value$year=2023$month=12$day=01
            result_values = {}

            if "$" in partition_id:
                # Multi-column partitioning with format: col1=val1$col2=val2$col3=val3
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in partition_columns:
                            # Try to convert to appropriate type
                            if val.isdigit():
                                result_values[col] = int(val)
                            else:
                                result_values[col] = val
            else:
                # Single column partitioning - assume it's a date/time based partition
                if len(partition_columns) == 1:
                    col_name = partition_columns[0]
                    if partition_id.isdigit():
                        # Handle date partitions like 20231201, 2023120100
                        if (
                            len(partition_id) == 8 or len(partition_id) == 10
                        ):  # YYYYMMDD
                            result_values[col_name] = partition_id
                        else:
                            result_values[col_name] = partition_id
                    else:
                        result_values[col_name] = partition_id
                else:
                    # Multiple columns but single partition_id - try to parse based on common patterns
                    self._parse_single_partition_id_for_multiple_columns(
                        partition_id, partition_columns, result_values
                    )

            return result_values

        except Exception as e:
            logger.warning(f"Error getting partition info from INFORMATION_SCHEMA: {e}")
            return {}

    def _parse_single_partition_id_for_multiple_columns(
        self,
        partition_id: str,
        partition_columns: List[str],
        result_values: Dict[str, Any],
    ) -> None:
        """
        Parse a single partition_id when there are multiple partition columns.
        Common patterns: YYYYMMDD for year/month/day, YYYYMMDDHH for year/month/day/hour
        """
        if partition_id.isdigit():
            if len(partition_id) == 8:  # YYYYMMDD
                year = partition_id[:4]
                month = partition_id[4:6]
                day = partition_id[6:8]

                for col in partition_columns:
                    col_lower = col.lower()
                    if col_lower in ["year", "yr"]:
                        result_values[col] = year
                    elif col_lower in ["month", "mo"]:
                        result_values[col] = month
                    elif col_lower in ["day", "dy"]:
                        result_values[col] = day

            elif len(partition_id) == 10:  # YYYYMMDDHH
                year = partition_id[:4]
                month = partition_id[4:6]
                day = partition_id[6:8]
                hour = partition_id[8:10]

                for col in partition_columns:
                    col_lower = col.lower()
                    if col_lower in ["year", "yr"]:
                        result_values[col] = year
                    elif col_lower in ["month", "mo"]:
                        result_values[col] = month
                    elif col_lower in ["day", "dy"]:
                        result_values[col] = day
                    elif col_lower in ["hour", "hr"]:
                        result_values[col] = hour

    def _get_partition_info_from_table_query(
        self, table, project, schema, partition_columns, max_results=5
    ):
        """Get partition information by querying the actual table using parameterized queries."""
        if not partition_columns:
            return {}

        result_values = {}
        safe_table_ref = self._build_safe_table_reference(project, schema, table.name)

        for col_name in partition_columns:
            try:
                # Use utility for validation
                if not self._validate_column_name(col_name, "table query partition"):
                    continue

                # Use utility for query building with parameters
                query, job_config = self._create_partition_stats_query(
                    safe_table_ref, col_name, max_results
                )

                # Use utility for execution with context
                self._log_partition_attempt("table query", table.name, [col_name])
                partition_values_results = self._execute_query_safely(
                    query, job_config, f"partition column {col_name}"
                )

                if (
                    not partition_values_results
                    or partition_values_results[0].val is None
                ):
                    logger.warning(
                        f"No non-empty partition values found for column {col_name}"
                    )
                    self._log_partition_attempt(
                        "table query", table.name, [col_name], success=False
                    )
                    continue

                # Use utility to determine how to choose the result
                if (
                    self._is_date_like_column(col_name)
                    or self._is_year_like_column(col_name)
                    or self._is_month_like_column(col_name)
                    or self._is_day_like_column(col_name)
                ):
                    chosen_result = partition_values_results[
                        0
                    ]  # Already sorted by date DESC
                else:
                    chosen_result = max(
                        partition_values_results, key=lambda r: r.record_count
                    )

                result_values[col_name] = chosen_result.val
                logger.debug(
                    f"Selected partition {col_name}={chosen_result.val} with {chosen_result.record_count} records"
                )
                self._log_partition_attempt(
                    "table query", table.name, [col_name], success=True
                )

            except Exception as e:
                logger.error(f"Error getting partition value for {col_name}: {e}")
                self._log_partition_attempt(
                    "table query", table.name, [col_name], success=False
                )
                continue

        return result_values

    def _get_partition_columns_from_info_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Dict[str, str]:
        """
        Get partition columns from INFORMATION_SCHEMA using parameterized queries.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name

        Returns:
            Dictionary mapping column names to data types
        """
        try:
            safe_info_schema_ref = self._build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""SELECT column_name, data_type
FROM {safe_info_schema_ref}
WHERE table_name = @table_name AND is_partitioning_column = 'YES'"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            partition_column_rows = self._execute_query_safely(
                query, job_config, "partition columns from info schema"
            )

            partition_columns = [row.column_name for row in partition_column_rows]

            # Use the new method to get column types
            if partition_columns:
                return self._get_partition_column_types(
                    table, project, schema, partition_columns
                )
            else:
                return {}
        except Exception as e:
            logger.warning(
                f"Error getting partition columns from INFORMATION_SCHEMA: {e}"
            )
            return {}

    def _get_partition_columns_from_ddl(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Dict[str, str]:
        """
        Extract partition columns from table DDL using robust regex patterns.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name

        Returns:
            Dictionary mapping column names to data types
        """
        partition_cols_with_types: Dict[str, str] = {}

        if not table.ddl:
            return partition_cols_with_types

        try:
            # Normalize DDL by removing extra whitespace and newlines
            normalized_ddl = re.sub(r"\s+", " ", table.ddl.upper().strip())

            # Use regex to find PARTITION BY clause
            # This handles various formats:
            # - PARTITION BY DATE(column)
            # - PARTITION BY RANGE_BUCKET(column, ...)
            # - PARTITION BY column1, column2
            # - PARTITION BY DATE(column) OPTIONS(...)
            partition_pattern = (
                r"PARTITION\s+BY\s+([^)]+(?:\([^)]*\))?[^)]*)(?:\s+OPTIONS|$|;)"
            )

            match = re.search(partition_pattern, normalized_ddl)
            if not match:
                logger.debug(
                    f"No PARTITION BY clause found in DDL for table {table.name}"
                )
                return partition_cols_with_types

            partition_clause = match.group(1).strip()
            logger.debug(f"Found partition clause: {partition_clause}")

            # Extract column names from various partition patterns
            column_names = self._extract_column_names_from_partition_clause(
                partition_clause
            )

            if not column_names:
                logger.warning(
                    f"Could not extract column names from partition clause: {partition_clause}"
                )
                return partition_cols_with_types

            # Get data types for the extracted columns
            if column_names:
                return self._get_partition_column_types(
                    table, project, schema, column_names
                )

        except Exception as e:
            logger.warning(f"Error parsing DDL for partition columns: {e}")

        return partition_cols_with_types

    def _get_function_patterns(self) -> List[str]:
        """
        Get regex patterns for extracting column names from function-based partitioning.

        Returns:
            List of regex patterns for common partitioning functions
        """
        return [
            # DATE(column), DATETIME(column), TIMESTAMP(column)
            r"(?:DATE|DATETIME|TIMESTAMP)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)",
            # DATE_TRUNC(column, unit), DATETIME_TRUNC(column, unit)
            r"(?:DATE_TRUNC|DATETIME_TRUNC)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,",
            # EXTRACT(DATE FROM column)
            r"EXTRACT\s*\(\s*DATE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\)",
            # RANGE_BUCKET(column, ...)
            r"RANGE_BUCKET\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,",
            # Generic function(column, ...)
            r"[a-zA-Z_][a-zA-Z0-9_]*\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*[,)]",
        ]

    def _extract_simple_column_names(self, partition_clause: str) -> List[str]:
        """
        Extract simple comma-separated column names from partition clause.

        Args:
            partition_clause: The PARTITION BY clause content

        Returns:
            List of column names if simple pattern matches, empty list otherwise
        """
        # Pattern for simple column names (col1, col2, col3)
        simple_pattern = r"^([a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*)$"
        simple_match = re.match(simple_pattern, partition_clause.strip())

        if simple_match:
            # Direct column names separated by commas
            columns = [col.strip() for col in simple_match.group(1).split(",")]
            return [col for col in columns if col]

        return []

    def _extract_function_based_column_names(self, partition_clause: str) -> List[str]:
        """
        Extract column names from function-based partitioning patterns.

        Args:
            partition_clause: The PARTITION BY clause content

        Returns:
            List of column names extracted from function patterns
        """
        column_names = []
        function_patterns = self._get_function_patterns()
        seen = set()

        for pattern in function_patterns:
            matches = re.findall(pattern, partition_clause, re.IGNORECASE)
            for match in matches:
                if match not in seen:
                    seen.add(match)
                    column_names.append(match)

        return column_names

    def _split_partition_clause_respecting_parentheses(
        self, partition_clause: str
    ) -> List[str]:
        """
        Split partition clause by commas while respecting parentheses.

        This ensures that function calls like DATETIME_TRUNC(col, DAY) are not
        split in the middle.

        Args:
            partition_clause: The partition clause to split

        Returns:
            List of clause parts split by top-level commas only
        """
        parts = []
        current_part = ""
        paren_depth = 0

        for char in partition_clause:
            if char == "(":
                paren_depth += 1
                current_part += char
            elif char == ")":
                paren_depth -= 1
                current_part += char
            elif char == "," and paren_depth == 0:
                # Only split on commas that are not inside parentheses
                if current_part.strip():
                    parts.append(current_part.strip())
                current_part = ""
            else:
                current_part += char

        # Add the last part
        if current_part.strip():
            parts.append(current_part.strip())

        return parts

    def _extract_mixed_column_names(self, partition_clause: str) -> List[str]:
        """
        Extract column names from mixed scenarios (comma-separated parts with functions).

        Args:
            partition_clause: The PARTITION BY clause content

        Returns:
            List of column names extracted from mixed patterns
        """
        column_names = []
        function_patterns = self._get_function_patterns()

        # Use smarter splitting that respects parentheses
        parts = self._split_partition_clause_respecting_parentheses(partition_clause)

        for part in parts:
            part = part.strip()
            found_match = False

            # Try to extract column name from each part using function patterns
            for pattern in function_patterns:
                matches = re.findall(pattern, part, re.IGNORECASE)
                if matches:
                    column_names.extend(matches)
                    found_match = True
                    break  # Stop after first successful pattern match to avoid duplicates

            # If no function found, check if it's a simple column name
            if not found_match:
                simple_col_match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)$", part)
                if simple_col_match:
                    column_names.append(simple_col_match.group(1))

        return column_names

    def _remove_duplicate_columns(self, column_names: List[str]) -> List[str]:
        """
        Remove duplicate column names while preserving order.

        Args:
            column_names: List of column names that may contain duplicates

        Returns:
            List of unique column names in uppercase, preserving order
        """
        seen = set()
        unique_columns = []

        for col in column_names:
            col_upper = col.upper()
            if col_upper not in seen:
                seen.add(col_upper)
                unique_columns.append(col_upper)

        return unique_columns

    def _extract_column_names_from_partition_clause(
        self, partition_clause: str
    ) -> List[str]:
        """
        Extract column names from a PARTITION BY clause using various patterns.

        Handles:
        - DATE(column_name)
        - DATETIME_TRUNC(column_name, DAY)
        - RANGE_BUCKET(column_name, GENERATE_ARRAY(...))
        - column1, column2, column3
        - Complex expressions with nested functions
        """
        try:
            # If there are commas, it's likely a mixed scenario
            if "," in partition_clause:
                # Try mixed scenarios first for comma-separated clauses
                mixed_columns = self._extract_mixed_column_names(partition_clause)
                if mixed_columns:
                    return self._remove_duplicate_columns(mixed_columns)

            # Pattern 1: Try simple column names first (col1, col2, col3)
            simple_columns = self._extract_simple_column_names(partition_clause)
            if simple_columns:
                return self._remove_duplicate_columns(simple_columns)

            # Pattern 2: Try function-based partitioning
            function_columns = self._extract_function_based_column_names(
                partition_clause
            )
            if function_columns:
                return self._remove_duplicate_columns(function_columns)

            # If no columns found, return empty list
            logger.debug(
                f"No column names extracted from partition clause: {partition_clause}"
            )
            return []

        except Exception as e:
            logger.warning(
                f"Error extracting column names from partition clause '{partition_clause}': {e}"
            )
            return []

    def _get_external_table_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        current_time: datetime,
    ) -> Optional[List[str]]:
        """Get partition filters specifically for external tables.

        Args:
            table: BigqueryTable instance containing table metadata
            project: The BigQuery project ID
            schema: The dataset/schema name
            current_time: Current UTC datetime

        Returns:
            List of partition filter strings if partition columns found and filters could be constructed
            Empty list if no partitions found
            None if partition filters could not be determined
        """
        try:
            # Try sampling approach first - most efficient
            sample_filters = self._get_partitions_with_sampling(table, project, schema)
            if sample_filters:
                return sample_filters

            # Step 1: Get partition columns from INFORMATION_SCHEMA
            partition_cols_with_types = self._get_partition_columns_from_info_schema(
                table, project, schema
            )

            # Step 2: If no columns found, try extracting from DDL
            if not partition_cols_with_types:
                partition_cols_with_types = self._get_partition_columns_from_ddl(
                    table, project, schema
                )

            # Step 3: If still no columns found, return empty list
            if not partition_cols_with_types:
                logger.debug(
                    f"No partition columns found for external table {table.name}"
                )
                return []

            logger.debug(
                f"Found {len(partition_cols_with_types)} partition columns: {list(partition_cols_with_types.keys())}"
            )

            # Step 4: Find a valid combination of partition filters that returns data
            return self._find_valid_partition_combination(
                table, project, schema, partition_cols_with_types
            )

        except Exception as e:
            logger.error(f"Error checking external table partitioning: {e}")
            return None

    def _convert_partition_value_to_type(self, raw_val: Any, data_type: str) -> Any:
        """
        Convert a raw partition value to the appropriate type based on column data type.

        Args:
            raw_val: Raw value from error message or other source
            data_type: BigQuery column data type

        Returns:
            Converted value appropriate for the data type
        """
        if raw_val is None:
            return None

        data_type_upper = data_type.upper() if data_type else ""

        # For string types, keep as string
        if data_type_upper in ("STRING", "VARCHAR"):
            return str(raw_val)

        # For date types, handle various input formats
        elif data_type_upper == "DATE":
            if isinstance(raw_val, str):
                # If it looks like a date string, keep as is
                if re.match(r"\d{4}-\d{2}-\d{2}", raw_val) or raw_val.isdigit():
                    return raw_val
            return str(raw_val)

        # For timestamp/datetime types
        elif data_type_upper in ("TIMESTAMP", "DATETIME"):
            return str(raw_val)

        # For numeric types, try to convert to appropriate type
        elif data_type_upper in ("INT64", "INTEGER", "BIGINT"):
            try:
                return int(raw_val)
            except (ValueError, TypeError):
                return raw_val

        elif data_type_upper in ("FLOAT64", "FLOAT", "NUMERIC", "DECIMAL"):
            try:
                return float(raw_val)
            except (ValueError, TypeError):
                return raw_val

        # For boolean types
        elif data_type_upper in ("BOOL", "BOOLEAN"):
            if isinstance(raw_val, str):
                return raw_val.lower() in ("true", "1", "yes", "on")
            return bool(raw_val)

        # Default: return as-is
        else:
            return raw_val

    def _try_fallback_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """
        Attempt fallback approaches to find valid partition values.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_cols_with_types: Dictionary of column names to data types

        Returns:
            List of filter strings if found, None otherwise
        """
        # 1. Try date columns first
        date_columns = [
            col
            for col in partition_cols_with_types
            if col.lower() in {"date", "day", "dt", "created_date", "partition_date"}
        ]

        if date_columns:
            logger.debug(f"Found date columns to try first: {date_columns}")
            date_filters = self._try_date_column_fallback(
                table, project, schema, date_columns, partition_cols_with_types
            )
            if date_filters:
                return date_filters

        # 2. Try generic approach for all columns
        fallback_filters = self._try_generic_column_fallback(
            table, project, schema, partition_cols_with_types
        )

        # 3. If we found filters, verify them
        if fallback_filters:
            if self._verify_partition_has_data(
                table, project, schema, fallback_filters
            ):
                logger.debug(f"Found valid fallback filters: {fallback_filters}")
                return fallback_filters
            else:
                logger.warning("Fallback filters validation failed")

        # 4. Last resort - try simplified approach
        return self._try_simplified_fallback(
            table, project, schema, partition_cols_with_types
        )

    def _try_date_column_fallback(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        date_columns: List[str],
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """Try fallback approach specifically for date-type columns, prioritizing most recent dates."""
        logger.debug(
            f"Trying date column fallback for {len(date_columns)} date columns"
        )

        # Sort date columns by priority - look for common date column patterns first
        priority_patterns = [
            "date",
            "partition_date",
            "dt",
            "day",
            "created_date",
            "event_date",
        ]
        sorted_date_columns = sorted(
            date_columns,
            key=lambda col: next(
                (
                    i
                    for i, pattern in enumerate(priority_patterns)
                    if pattern in col.lower()
                ),
                len(priority_patterns),
            ),
        )

        for col_name in sorted_date_columns:
            # Validate column name for safety
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                logger.warning(f"Invalid date column name detected: {col_name}")
                continue

            data_type = partition_cols_with_types.get(col_name, "DATE")
            logger.debug(
                f"Trying fallback approach for date column {col_name} with type {data_type}"
            )

            try:
                # OPTIMIZATION: First try common recent dates (yesterday, today, etc.)
                # before doing expensive MAX() query. Most tables have recent data.
                current_date = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

                logger.debug(f"Trying quick recent date checks for {col_name}")
                # Try most likely recent dates first (yesterday is most common, then today, then 2-3 days ago)
                quick_test_days = [1, 0, 2, 3]  # Yesterday, today, 2-3 days ago

                for days_ago in quick_test_days:
                    test_date = current_date - timedelta(days=days_ago)
                    filter_str, parameters = self._create_partition_filter_from_value(
                        col_name, test_date, data_type
                    )

                    logger.debug(
                        f"Quick test: {days_ago} days ago ({test_date.strftime('%Y-%m-%d')}): {filter_str}"
                    )
                    if self._verify_partition_has_data_with_params(
                        table, project, schema, [(filter_str, parameters)]
                    ):
                        logger.debug(
                            f"Found working date filter using quick recent check: {filter_str}"
                        )
                        return [filter_str]

                # If quick checks failed, try finding the actual most recent date in the table
                logger.debug(
                    f"Quick checks failed, querying for most recent date in {col_name}"
                )

                # Build safe table reference
                safe_table_ref = self._build_safe_table_reference(
                    project, schema, table.name
                )

                recent_date_query = f"""SELECT MAX(`{col_name}`) as max_date
FROM {safe_table_ref}
WHERE `{col_name}` IS NOT NULL"""

                try:
                    recent_results = self._execute_query_safely(
                        recent_date_query, context=f"recent date for {col_name}"
                    )
                    if recent_results and recent_results[0].max_date:
                        # Start from the most recent date found in the table
                        recent_date = recent_results[0].max_date
                        if isinstance(recent_date, str):
                            recent_date = datetime.fromisoformat(
                                recent_date.replace("Z", "+00:00")
                            )
                        elif hasattr(recent_date, "date"):
                            recent_date = datetime.combine(
                                recent_date.date(), datetime.min.time()
                            ).replace(tzinfo=timezone.utc)

                        logger.debug(f"Found most recent date in table: {recent_date}")

                        # Try dates starting from the most recent, going back gradually
                        # Skip days we already tested in the quick check
                        for days_ago in [0, 1, 2, 3, 7, 14, 30, 90]:
                            test_date = recent_date - timedelta(days=days_ago)

                            # Skip if we already tested this date in quick check
                            days_from_current = (current_date - test_date).days
                            if days_from_current in [0, 1, 2, 3]:
                                continue

                            filter_str, parameters = (
                                self._create_partition_filter_from_value(
                                    col_name, test_date, data_type
                                )
                            )

                            logger.debug(
                                f"Testing table max date - {days_ago} days: {filter_str}"
                            )
                            if self._verify_partition_has_data_with_params(
                                table, project, schema, [(filter_str, parameters)]
                            ):
                                logger.debug(
                                    f"Found working date filter using table max date: {filter_str}"
                                )
                                return [filter_str]
                except Exception as e:
                    logger.debug(
                        f"Could not find recent date for {col_name}, falling back to extended range: {e}"
                    )

                # Final fallback: try extended date range from current date
                logger.debug(f"Trying extended date range fallback for {col_name}")
                # Try wider date range, skipping days already tested
                for days_ago in [7, 14, 30, 60, 90, 180, 365]:
                    test_date = current_date - timedelta(days=days_ago)
                    filter_str, parameters = self._create_partition_filter_from_value(
                        col_name, test_date, data_type
                    )

                    logger.debug(
                        f"Extended range test - {days_ago} days ago: {filter_str}"
                    )
                    if self._verify_partition_has_data_with_params(
                        table, project, schema, [(filter_str, parameters)]
                    ):
                        logger.debug(
                            f"Found working date filter using extended range: {filter_str}"
                        )
                        return [filter_str]

            except Exception as e:
                logger.error(
                    f"Error in date fallback for column {col_name}: {e}",
                    exc_info=True,
                )

        logger.warning("No working date filters found for any date columns")
        return None

    def _try_generic_column_fallback(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> List[str]:
        """Try generic fallback approach for any column type."""
        fallback_filters = []
        # Skip date columns that were already tried
        date_columns = [
            col
            for col in partition_cols_with_types
            if col.lower() in {"date", "day", "dt", "created_date", "partition_date"}
        ]

        for col_name, data_type in partition_cols_with_types.items():
            if col_name in date_columns:
                continue  # Already tried this as a date column

            try:
                logger.debug(
                    f"Trying generic fallback for column {col_name} with type {data_type}"
                )
                filter_str = self._try_column_with_different_orderings(
                    table, project, schema, col_name, data_type
                )
                if filter_str:
                    fallback_filters.append(filter_str)
            except Exception as e:
                logger.error(
                    f"Error in fallback for column {col_name}: {e}", exc_info=True
                )

        return fallback_filters

    def _try_column_with_different_orderings(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        col_name: str,
        data_type: str,
    ) -> Optional[str]:
        """Try different ordering strategies to find a valid value for a column."""
        # Validate column name
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
            logger.warning(f"Invalid column name: {col_name}")
            return None

        safe_table_ref = self._build_safe_table_reference(project, schema, table.name)

        for order_by in ["DESC", "ASC", "record_count DESC"]:
            query = f"""WITH PartitionStats AS (
    SELECT `{col_name}` as val, COUNT(*) as record_count
    FROM {safe_table_ref}
    WHERE `{col_name}` IS NOT NULL
    GROUP BY `{col_name}`
    HAVING record_count > 0
    ORDER BY `{col_name}` {order_by}
    LIMIT @max_results
)
SELECT val, record_count FROM PartitionStats"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("max_results", "INT64", 10)]
            )

            logger.debug(f"Executing fallback query with {order_by} ordering")
            try:
                partition_stats_rows = self._execute_query_safely(
                    query, job_config, f"fallback query {order_by}"
                )

                if partition_stats_rows:
                    logger.debug(
                        f"Query returned {len(partition_stats_rows)} potential values"
                    )

                    for result in partition_stats_rows:
                        val = result.val
                        if val is not None:
                            filter_str, parameters = (
                                self._create_partition_filter_from_value(
                                    col_name, val, data_type
                                )
                            )

                            logger.debug(f"Testing filter: {filter_str}")
                            # Test each filter individually
                            if self._verify_partition_has_data_with_params(
                                table, project, schema, [(filter_str, parameters)]
                            ):
                                logger.debug(f"Found working filter: {filter_str}")
                                return filter_str
                else:
                    logger.debug(f"No results for {order_by} ordering")
            except Exception as e:
                logger.warning(f"Error executing query with {order_by} ordering: {e}")
                continue

        return None

    def _try_simplified_fallback(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """Last resort approach when other methods fail."""
        logger.warning(
            "All approaches failed, trying with a single most populated partition"
        )

        # Try with simplified approach - just get most recent partition
        recent_filters = self._try_get_most_recent_partition(
            table, project, schema, partition_cols_with_types
        )
        if recent_filters:
            return recent_filters

        # Try to find the most populated partition for any column
        return self._try_find_most_populated_partition(
            table, project, schema, partition_cols_with_types
        )

    def _try_get_most_recent_partition(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """Try to get the most recent partition."""
        logger.debug("Trying simplified approach to find most recent partition")
        try:
            fallback_filters = []

            # Get the first partition column and validate it
            if not partition_cols_with_types:
                logger.warning(
                    "No partition columns provided for most recent partition lookup"
                )
                return None

            first_col = list(partition_cols_with_types.keys())[0]
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", first_col):
                logger.warning(f"Invalid partition column name: {first_col}")
                return None

            # Build safe table reference
            safe_table_ref = self._build_safe_table_reference(
                project, schema, table.name
            )

            # This query just gets the most recent data for any partition column
            query = f"""SELECT *
FROM {safe_table_ref}
ORDER BY `{first_col}` DESC
LIMIT @limit_rows"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
            )

            latest_partition_row = self._execute_query_safely(
                query, job_config, "most recent partition"
            )

            if latest_partition_row and len(latest_partition_row) > 0:
                for col_name in partition_cols_with_types:
                    val = getattr(latest_partition_row[0], col_name, None)
                    if val is not None:
                        data_type = partition_cols_with_types.get(col_name, "STRING")
                        filter_str, parameters = (
                            self._create_partition_filter_from_value(
                                col_name, val, data_type
                            )
                        )
                        fallback_filters.append(filter_str)
                        logger.debug(
                            f"Found filter from simplified approach: {filter_str}"
                        )

                if fallback_filters and self._verify_partition_has_data(
                    table, project, schema, fallback_filters
                ):
                    return fallback_filters
        except Exception as e:
            logger.error(f"Error in simplified approach: {e}", exc_info=True)

        return None

    def _try_find_most_populated_partition(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """Find the single most populated partition across all columns."""
        try:
            best_col = None
            best_val = None
            best_count = 0

            for col_name in partition_cols_with_types:
                # Validate column name to prevent injection
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                    logger.warning(f"Invalid column name detected: {col_name}")
                    continue

                # Build safe table reference
                safe_table_ref = self._build_safe_table_reference(
                    project, schema, table.name
                )

                query = f"""SELECT `{col_name}` as val, COUNT(*) as cnt
FROM {safe_table_ref}
WHERE `{col_name}` IS NOT NULL
GROUP BY `{col_name}`
ORDER BY cnt DESC
LIMIT @limit_rows"""

                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )

                try:
                    most_populated_results = self._execute_query_safely(
                        query, job_config, f"most populated for {col_name}"
                    )

                    if (
                        most_populated_results
                        and most_populated_results[0].val is not None
                        and most_populated_results[0].cnt > best_count
                    ):
                        best_col = col_name
                        best_val = most_populated_results[0].val
                        best_count = most_populated_results[0].cnt
                except Exception:
                    continue

            if best_col and best_val:
                data_type = partition_cols_with_types.get(best_col, "STRING")
                filter_str, parameters = self._create_partition_filter_from_value(
                    best_col, best_val, data_type
                )
                logger.debug(f"Last resort filter: {filter_str} with {best_count} rows")
                return [filter_str]
        except Exception as e:
            logger.error(f"Error in last resort approach: {e}", exc_info=True)

        logger.warning("All fallback approaches failed to find valid partition values")
        return None

    def _verify_partition_has_data(self, table, project, schema, filters):
        """Verify that the partition filters actually return data using parameterized queries."""
        if not filters:
            return False

        # Use utility for filter validation
        validated_filters = self._validate_and_filter_expressions(
            filters, "partition verification"
        )
        if not validated_filters:
            return False

        safe_table_ref = self._build_safe_table_reference(project, schema, table.name)
        where_clause = " AND ".join(validated_filters)

        try:
            query = f"""SELECT COUNT(*) as cnt
FROM {safe_table_ref}
WHERE {where_clause}
LIMIT @limit_rows"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1000)]
            )

            # Use utility for execution
            count_verification_results = self._execute_query_safely(
                query, job_config, "partition verification"
            )

            if count_verification_results and count_verification_results[0].cnt > 0:
                logger.debug(
                    f"Verified partition filters return {count_verification_results[0].cnt} rows: {where_clause}"
                )
                return True
            else:
                logger.warning(f"Partition verification found no data: {where_clause}")
                return False
        except Exception as e:
            logger.warning(f"Error verifying partition data: {e}", exc_info=True)

            # Use utility for fallback attempt
            try:
                simpler_query = f"""SELECT 1 FROM {safe_table_ref} WHERE {where_clause} LIMIT @limit_rows"""

                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )

                sample_verification_results = self._execute_query_safely(
                    simpler_query, job_config, "partition verification fallback"
                )
                return len(sample_verification_results) > 0
            except Exception as e:
                logger.warning(f"Simple verification also failed: {e}")
                return False

    def _verify_partition_has_data_with_params(
        self, table, project, schema, filter_param_pairs
    ):
        """Verify that partition filters with parameters return data."""
        if not filter_param_pairs:
            return False

        safe_table_ref = self._build_safe_table_reference(project, schema, table.name)

        # Combine all filters and parameters
        where_clauses = []
        all_parameters = []

        for filter_str, parameters in filter_param_pairs:
            if self._validate_filter_expression(filter_str):
                where_clauses.append(filter_str)
                all_parameters.extend(parameters)
            else:
                logger.warning(
                    f"Rejecting invalid filter in verification: {filter_str}"
                )

        if not where_clauses:
            return False

        where_clause = " AND ".join(where_clauses)

        try:
            query = f"""SELECT COUNT(*) as cnt
FROM {safe_table_ref}
WHERE {where_clause}
LIMIT @limit_rows"""

            # Add limit parameter
            all_parameters.append(ScalarQueryParameter("limit_rows", "INT64", 1000))

            job_config = QueryJobConfig(query_parameters=all_parameters)

            count_verification_results = self._execute_query_safely(
                query, job_config, "partition verification with params"
            )

            if count_verification_results and count_verification_results[0].cnt > 0:
                logger.debug(
                    f"Verified partition filters return {count_verification_results[0].cnt} rows: {where_clause}"
                )
                return True
            else:
                logger.warning(f"Partition verification found no data: {where_clause}")
                return False
        except Exception as e:
            logger.warning(
                f"Error verifying partition data with params: {e}", exc_info=True
            )
            return False

    def _get_partitions_with_sampling(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """
        Get partition filters using sampling to avoid full table scans.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name

        Returns:
            List of partition filter strings, or None if unable to build filters
        """
        try:
            # First get partition columns
            partition_cols_with_types = self._get_partition_columns_from_info_schema(
                table, project, schema
            )

            if not partition_cols_with_types:
                partition_cols_with_types = self._get_partition_columns_from_ddl(
                    table, project, schema
                )

            if not partition_cols_with_types:
                return None

            logger.debug(
                f"Using sampling to find partition values for {len(partition_cols_with_types)} columns"
            )

            # Build safe table reference
            safe_table_ref = self._build_safe_table_reference(
                project, schema, table.name
            )

            # Use TABLESAMPLE to get a small sample of data
            sample_query = f"""SELECT *
FROM {safe_table_ref} TABLESAMPLE SYSTEM (@sample_percent PERCENT)
LIMIT @limit_rows"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("sample_percent", "FLOAT64", 1.0),
                    ScalarQueryParameter("limit_rows", "INT64", 100),
                ]
            )

            partition_sample_rows = self._execute_query_safely(
                sample_query, job_config, "partition sampling"
            )

            if not partition_sample_rows:
                logger.debug("Sample query returned no results")
                return None

            # Extract values for partition columns
            filters = []
            for col_name, data_type in partition_cols_with_types.items():
                for row in partition_sample_rows:
                    if hasattr(row, col_name) and getattr(row, col_name) is not None:
                        val = getattr(row, col_name)
                        filter_str, parameters = (
                            self._create_partition_filter_from_value(
                                col_name, val, data_type
                            )
                        )
                        filters.append(filter_str)
                        logger.debug(
                            f"Found partition value from sample: {col_name}={val}"
                        )
                        break

            # Verify the filters return data
            if filters and self._verify_partition_has_data(
                table, project, schema, filters
            ):
                logger.debug(
                    f"Successfully created partition filters from sample: {filters}"
                )
                return filters

            return None

        except Exception as e:
            logger.warning(f"Error getting partition filters with sampling: {e}")
            return None

    def _find_valid_partition_combination(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """
        Find a valid combination of partition filters that returns data.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_cols_with_types: Dictionary of partition column names to data types

        Returns:
            List of partition filter strings that return data, or None if no valid combination found
        """
        logger.debug(
            f"Searching for valid partition combination for {table.name} with columns: {list(partition_cols_with_types.keys())}"
        )

        partition_cols = list(partition_cols_with_types.keys())

        try:
            # Approach 1: Try to get combinations that work together
            if len(partition_cols) > 1:
                logger.debug(
                    f"Trying combined partition approach for {len(partition_cols)} columns"
                )
                partition_values = self._get_multi_column_partition_values(
                    table, project, schema, partition_cols, use_info_schema=True
                )

                if partition_values:
                    # Convert to filter strings
                    combined_filters = []
                    for col_name, value in partition_values.items():
                        data_type = partition_cols_with_types.get(col_name, "STRING")
                        filter_str, parameters = (
                            self._create_partition_filter_from_value(
                                col_name, value, data_type
                            )
                        )
                        combined_filters.append(filter_str)

                    # Verify the filters return data
                    if self._verify_partition_has_data(
                        table, project, schema, combined_filters
                    ):
                        return combined_filters

                logger.debug(
                    "Combined partition approach didn't yield results, trying individual columns"
                )

            # Approach 2: Try each partition column individually
            individual_filters = []
            for col_name, data_type in partition_cols_with_types.items():
                logger.debug(
                    f"Trying individual column approach for {col_name} with type {data_type}"
                )
                try:
                    # Use the unified method to get partition value
                    partition_value = self._get_single_column_partition_value(
                        table,
                        project,
                        schema,
                        col_name,
                        data_type,
                        use_info_schema=True,
                    )

                    if partition_value is not None:
                        filter_str, parameters = (
                            self._create_partition_filter_from_value(
                                col_name, partition_value, data_type
                            )
                        )
                        logger.debug(
                            f"Found filter for column {col_name}: {filter_str}"
                        )
                        individual_filters.append(filter_str)
                except Exception as e:
                    logger.error(
                        f"Error processing column {col_name}: {e}", exc_info=True
                    )
                    continue

            # Verify the individual filters
            if individual_filters:
                logger.debug(f"Verifying {len(individual_filters)} individual filters")
                if self._verify_partition_has_data(
                    table, project, schema, individual_filters
                ):
                    logger.debug(
                        f"Found valid individual filters: {individual_filters}"
                    )
                    return individual_filters
                logger.debug("Individual filters verification failed")

        except Exception as e:
            logger.error(
                f"Error finding valid partition combination: {e}", exc_info=True
            )

        # Approach 3: Last resort, try fallback approaches
        logger.warning(f"Trying fallback approach for {table.name}")
        return self._try_fallback_partition_values(
            table, project, schema, partition_cols_with_types
        )

    def _handle_external_table_partitioning(
        self, table: BigqueryTable, project: str, schema: str, current_time: datetime
    ) -> Optional[List[str]]:
        """
        Handle partitioning for external tables.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            current_time: Current datetime

        Returns:
            List of partition filter strings or None
        """
        if table.external:
            logger.debug(f"Processing external table partitioning for {table.name}")
            return self._get_external_table_partition_filters(
                table, project, schema, current_time
            )
        return None

    def _get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """Get partition filters for all required partition columns.

        Args:
            table: BigqueryTable instance containing table metadata
            project: The BigQuery project ID
            schema: The dataset/schema name

        Returns:
            List of partition filter strings if partition columns found and filters could be constructed
            Empty list if no partitions found
            None if partition filters could not be determined and profiling should be skipped
        """
        current_time = datetime.now(timezone.utc)

        # First try sampling approach as it's most efficient
        sample_filters = self._get_partitions_with_sampling(table, project, schema)
        if sample_filters:
            return sample_filters

        # Get required partition columns from table info
        required_partition_columns = self._get_partition_columns_from_table_info(table)

        # If no partition columns found from partition_info, query INFORMATION_SCHEMA
        if not required_partition_columns:
            required_partition_columns = self._get_partition_columns_from_schema(
                table, project, schema
            )

        # If we still don't have partition columns, try to trigger a partition error to detect them
        if not required_partition_columns:
            try:
                # Build safe table reference
                safe_table_ref = self._build_safe_table_reference(
                    project, schema, table.name
                )

                # Run a simple query to trigger partition error
                test_query = (
                    f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                )
                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )
                self._execute_query_safely(
                    test_query, job_config, "partition detection"
                )

                # If the query succeeds, table is not partitioned
                logger.debug(f"Table {table.name} is not partitioned")
                return []

            except Exception as e:
                # Extract partition requirements from error message
                error_info = self._extract_partition_info_from_error(str(e))
                required_partition_columns = set(error_info.get("required_columns", []))

                if required_partition_columns:
                    logger.debug(
                        f"Detected required partition columns from error: {required_partition_columns}"
                    )
                else:
                    logger.debug(f"No partition columns found for table {table.name}")
                    return []

        # If still no partition columns found, check for external table partitioning
        if not required_partition_columns:
            logger.debug(f"No partition columns found for table {table.name}")
            return self._handle_external_table_partitioning(
                table, project, schema, current_time
            )

        logger.debug(f"Required partition columns: {required_partition_columns}")

        # Try to find REAL partition values that exist in the table
        partition_filters = self._find_real_partition_values(
            table, project, schema, list(required_partition_columns)
        )

        if partition_filters:
            logger.debug(f"Found valid partition filters: {partition_filters}")
            return partition_filters
        else:
            # If we can't find real partition values, skip profiling instead of using fallbacks
            logger.warning(
                f"Could not find valid partition values for table {table.name} "
                f"with required columns {required_partition_columns}. "
                f"Skipping profiling to avoid inaccurate results."
            )
            return None

    def _find_real_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
    ) -> Optional[List[str]]:
        """
        Find real partition values that exist in the table.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            required_columns: List of required partition columns

        Returns:
            List of partition filter strings if valid values found, None otherwise
        """
        if not required_columns:
            return []

        logger.debug(f"Determining partition values for columns: {required_columns}")

        # Get column data types first
        column_data_types = self._get_partition_column_types(
            table, project, schema, required_columns
        )

        # Try to find values using the unified methods
        try:
            # For multiple columns, try to find a combination that works
            if len(required_columns) > 1:
                partition_values = self._get_multi_column_partition_values(
                    table, project, schema, required_columns, use_info_schema=True
                )

                if partition_values:
                    # Convert to filter strings
                    filters = []
                    for col_name, value in partition_values.items():
                        data_type = column_data_types.get(col_name, "STRING")
                        filter_str, parameters = (
                            self._create_partition_filter_from_value(
                                col_name, value, data_type
                            )
                        )
                        filters.append(filter_str)

                    # Verify the filters return data
                    if self._verify_partition_has_data(table, project, schema, filters):
                        return filters

            # For single column or if multi-column failed, try individual columns
            individual_filters = []
            for col_name in required_columns:
                data_type = column_data_types.get(col_name, "STRING")

                # Try to find the most recent/populated value for this column
                partition_value = self._get_single_column_partition_value(
                    table, project, schema, col_name, data_type, use_info_schema=True
                )

                if partition_value is not None:
                    filter_str, parameters = self._create_partition_filter_from_value(
                        col_name, partition_value, data_type
                    )
                    individual_filters.append(filter_str)
                else:
                    # Log the missing column but continue to try other columns and fallbacks
                    logger.warning(
                        f"Could not find value for required partition column: {col_name}"
                    )

            # Verify all individual filters together return data
            if individual_filters and self._verify_partition_has_data(
                table, project, schema, individual_filters
            ):
                return individual_filters
            else:
                if individual_filters:
                    logger.warning(
                        "Individual filters generated but verification failed"
                    )
                else:
                    logger.warning("No individual filters could be generated")

        except Exception as e:
            logger.warning(f"Error finding real partition values: {e}")

        # If unified methods failed, try fallback approach before giving up
        logger.debug(
            f"Unified methods failed for {table.name}, trying fallback approach"
        )
        try:
            fallback_filters = self._get_fallback_partition_filters(
                table, project, schema, required_columns
            )

            if fallback_filters:
                # Verify fallback filters return data
                if self._verify_partition_has_data(
                    table, project, schema, fallback_filters
                ):
                    logger.debug(f"Fallback filters successful: {fallback_filters}")
                    return fallback_filters
                else:
                    logger.warning("Fallback filters generated but verification failed")

        except Exception as e:
            logger.warning(f"Fallback approach also failed: {e}")

        return None

    def _get_partition_columns_from_table_info(self, table: BigqueryTable) -> set:
        """
        Extract required partition columns from table partition_info.

        Args:
            table: BigqueryTable instance containing table metadata

        Returns:
            Set of partition column names
        """
        required_partition_columns = set()

        if table.partition_info:
            if isinstance(table.partition_info.fields, list):
                required_partition_columns.update(table.partition_info.fields)

            if (
                hasattr(table.partition_info, "columns")
                and table.partition_info.columns is not None
            ):
                # Safe iteration over columns when not None
                cols = table.partition_info.columns
                required_partition_columns.update(
                    col.name for col in cols if col is not None
                )

        return required_partition_columns

    def _get_partition_columns_from_schema(
        self, table: BigqueryTable, project: str, schema: str
    ) -> set:
        """
        Extract partition column names from INFORMATION_SCHEMA using parameterized queries.

        Args:
            table: BigqueryTable instance
            project: The BigQuery project ID
            schema: The dataset/schema name

        Returns:
            Set of partition column names, empty set if error occurs
        """
        required_partition_columns = set()

        try:
            safe_info_schema_ref = self._build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""SELECT column_name
FROM {safe_info_schema_ref}
WHERE table_name = @table_name AND is_partitioning_column = 'YES'"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            # Use the configured timeout for partition operations
            query_results = self._execute_query_safely(
                query, job_config, "partition columns from schema"
            )
            required_partition_columns = {row.column_name for row in query_results}
            logger.debug(
                f"Found partition columns from schema: {required_partition_columns}"
            )
        except Exception as e:
            logger.warning(f"Error querying partition columns: {e}")
            # If we can't determine the partition columns due to timeout, try to extract from an error
            try:
                # Build safe table reference
                safe_table_ref = self._build_safe_table_reference(
                    project, schema, table.name
                )

                test_query = (
                    f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                )
                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )
                self._execute_query_safely(
                    test_query, job_config, "partition error detection"
                )
            except Exception as e:
                error_info = self._extract_partition_info_from_error(str(e))
                required_partition_columns = set(error_info.get("required_columns", []))

        return required_partition_columns

    def _process_time_based_columns(
        self,
        time_based_columns: set,
        current_time: datetime,
        column_data_types: Dict[str, str],
    ) -> List[str]:
        """
        Process time-based partition columns (year, month, day, hour) with parameterized queries.

        Args:
            time_based_columns: Set of time-based column names
            current_time: Current datetime
            column_data_types: Dictionary mapping column names to data types

        Returns:
            List of partition filter strings
        """
        partition_filters = []

        for col_name in time_based_columns:
            col_name_lower = col_name.lower()
            col_data_type = column_data_types.get(col_name, "STRING")

            if col_name_lower == "year":
                pass
            elif col_name_lower == "month":
                # Format month with leading zero if used as string
                if col_data_type.upper() in {"STRING"}:
                    partition_filters.append(
                        f"`{col_name}` = @month_val_{hash(col_name) % 1000}"
                    )
                    continue
            elif col_name_lower == "day":
                # Format day with leading zero if used as string
                if col_data_type.upper() in {"STRING"}:
                    partition_filters.append(
                        f"`{col_name}` = @day_val_{hash(col_name) % 1000}"
                    )
                    continue
            elif col_name_lower == "hour":
                # Format hour with leading zero if used as string
                if col_data_type.upper() in {"STRING"}:
                    partition_filters.append(
                        f"`{col_name}` = @hour_val_{hash(col_name) % 1000}"
                    )
                    continue
            else:
                continue

            # Handle casting based on column type
            if col_data_type.upper() in {"STRING"}:
                partition_filters.append(
                    f"`{col_name}` = @str_val_{hash(col_name) % 1000}"
                )
            else:
                partition_filters.append(
                    f"`{col_name}` = @num_val_{hash(col_name) % 1000}"
                )

        return partition_filters

    def _get_fallback_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
    ) -> List[str]:
        """
        Generate fallback partition filters based on configuration when regular methods time out.

        Args:
            table: BigqueryTable instance containing table metadata
            project: The BigQuery project ID
            schema: The dataset/schema name
            required_columns: List of required partition columns

        Returns:
            List of partition filter strings generated from fallback values
        """
        logger.debug(f"Using fallback partition values for {table.name}")

        # Get column data types and fallback date
        column_data_types = self._get_column_data_types_for_fallback(
            table, project, schema
        )
        fallback_date = self._get_fallback_date()

        logger.debug(
            f"Configured fallback values: {self.config.profiling.fallback_partition_values}"
        )

        # Generate filters for each required column
        fallback_filters = []
        for col_name in required_columns:
            filter_str = self._create_fallback_filter_for_column(
                col_name, column_data_types, fallback_date
            )
            if filter_str:
                fallback_filters.append(filter_str)

        logger.debug(f"Generated fallback partition filters: {fallback_filters}")
        return fallback_filters

    def _get_column_data_types_for_fallback(
        self, table: BigqueryTable, project: str, schema: str
    ) -> Dict[str, str]:
        """Get column data types for fallback filter generation using parameterized queries."""
        column_data_types = {}
        try:
            safe_info_schema_ref = self._build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""SELECT column_name, data_type
FROM {safe_info_schema_ref}
WHERE table_name = @table_name"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            query_results = self._execute_query_safely(
                query, job_config, "column data types for fallback"
            )
            column_data_types = {
                row.column_name: row.data_type for row in query_results
            }
            logger.debug(
                f"Retrieved column data types for {len(column_data_types)} columns"
            )
        except Exception as e:
            logger.warning(f"Error fetching column data types for fallback: {e}")
            # Continue without type information

        return column_data_types

    def _get_fallback_date(self) -> datetime:
        """Get the fallback date (yesterday) for date-based partition filters."""
        today = datetime.now(timezone.utc)
        fallback_date = today - timedelta(days=1)
        logger.debug(
            f"Using fallback date {fallback_date.strftime('%Y-%m-%d')} (yesterday as default)"
        )
        return fallback_date

    def _create_fallback_filter_for_column(
        self, col_name, column_data_types, fallback_date
    ):
        """Create a fallback filter for a specific column using parameterized approach."""
        # Note: Since we're returning simple filter strings for fallback (not parameterized),
        # we maintain the existing logic but could enhance this further if needed
        data_type = (
            column_data_types.get(col_name, "").upper() if column_data_types else ""
        )

        # Check for explicit fallback value in config
        if col_name in self.config.profiling.fallback_partition_values:
            return self._create_explicit_fallback_filter(col_name)

        # Use utilities to determine column type and get appropriate value
        if self._is_day_like_column(col_name):
            value, format_type = self._get_fallback_date_value(col_name, fallback_date)
            formatted_value = self._format_filter_value(value, format_type)
            return f"`{col_name}` = {formatted_value}"
        elif data_type == "DATE":
            return f"`{col_name}` = DATE '{fallback_date.strftime('%Y-%m-%d')}'"
        elif data_type in ["TIMESTAMP", "DATETIME"]:
            return f"`{col_name}` = TIMESTAMP '{fallback_date.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif self._is_year_like_column(col_name) or self._is_month_like_column(
            col_name
        ):
            value, format_type = self._get_fallback_date_value(col_name, fallback_date)
            formatted_value = self._format_filter_value(value, format_type)
            return f"`{col_name}` = {formatted_value}"
        elif self._is_date_like_column(col_name) and data_type in [
            "STRING",
            "VARCHAR",
            "",
        ]:
            formatted_date = fallback_date.strftime("%Y-%m-%d")
            return f"`{col_name}` = '{formatted_date}'"
        else:
            # Last resort - use IS NOT NULL
            logger.warning(
                f"No fallback value for partition column {col_name}, using IS NOT NULL"
            )
            return f"`{col_name}` IS NOT NULL"

    def _format_filter_value(self, value: Any, format_type: str) -> str:
        """
        Format a value for use in a filter based on the format type.

        Args:
            value: Value to format
            format_type: How to format ("numeric", "zero_padded", "date_string", "string")

        Returns:
            Formatted value string for SQL
        """
        if format_type == "numeric":
            return str(value)
        elif format_type == "zero_padded":
            return f"'{value:02d}'"
        elif format_type in ("date_string", "string"):
            # Note: For fallback filters, we're keeping the simple approach
            # but could enhance to use parameters if needed
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"
        else:
            # Default to escaped string
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"

    def _create_explicit_fallback_filter(self, col_name: str) -> str:
        """Create filter using explicit fallback value from config."""
        fallback_value = self.config.profiling.fallback_partition_values[col_name]

        if isinstance(fallback_value, str):
            # Escape string values
            escaped_value = fallback_value.replace("'", "''")
            filter_str = f"`{col_name}` = '{escaped_value}'"
        else:
            filter_str = f"`{col_name}` = {fallback_value}"

        logger.debug(f"Using explicit fallback value for {col_name}: {fallback_value}")
        return filter_str

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """Handle partition-aware querying for all operations including COUNT."""
        bq_table = cast(BigqueryTable, table)

        # Validate identifiers
        try:
            safe_project = validate_bigquery_identifier(db_name, "project")
            safe_schema = validate_bigquery_identifier(schema_name, "dataset")
            safe_table = validate_bigquery_identifier(bq_table.name, "table")
        except ValueError as e:
            logger.error(f"Invalid identifier in batch kwargs: {e}")
            raise

        base_kwargs = {
            "schema": db_name,  # <project>
            "table": f"{schema_name}.{table.name}",  # <dataset>.<table>
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        # For external tables, add specific handling
        if bq_table.external:
            base_kwargs["is_external"] = "true"
            # Add any specific external table options needed

        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        if partition_filters is None:
            logger.warning(
                f"Could not construct partition filters for {bq_table.name}. "
                "This may cause partition elimination errors."
            )
            return base_kwargs

        # If no partition filters needed (e.g. some external tables), return base kwargs
        if not partition_filters:
            return base_kwargs

        # Validate all partition filters before using them
        validated_filters = []
        for filter_expr in partition_filters:
            if self._validate_filter_expression(filter_expr):
                validated_filters.append(filter_expr)
            else:
                logger.warning(f"Rejecting invalid partition filter: {filter_expr}")

        if not validated_filters:
            logger.warning("No valid partition filters after validation")
            return base_kwargs

        partition_where = " AND ".join(validated_filters)
        logger.debug(f"Using partition filters: {partition_where}")

        # Build safe table reference
        safe_table_ref = f"{safe_project}.{safe_schema}.{safe_table}"

        if self.config.profiling.profiling_row_limit > 0:
            # Validate row limit is a positive integer
            row_limit = max(1, int(self.config.profiling.profiling_row_limit))
            custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}
LIMIT {row_limit}"""
        else:
            custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}"""

        base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})

        return base_kwargs

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        """Get profile request with appropriate partition handling."""
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)

        # For partitioned tables, if it has a row count but not a valid partition, that means something went wrong with the partition detection.
        if (
            hasattr(bq_table, "partition_info")
            and bq_table.partition_info
            and bq_table.rows_count
        ):
            partition = getattr(
                bq_table.partition_info, "partition_id", None
            ) or getattr(bq_table.partition_info, "partition_type", None)
            if partition is None:
                self.report.report_warning(
                    title="Profile skipped for partitioned table",
                    message="profile skipped as partition id or type was invalid",
                    context=profile_request.pretty_name,
                )
                return None

        # Skip external tables if configured to do so
        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.report_warning(
                title="Profiling skipped for external table",
                message="profiling.profile_external_tables is disabled",
                context=profile_request.pretty_name,
            )
            return None

        # Get partition filters
        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        # If we got None back, that means there was an error getting partition filters
        if partition_filters is None:
            self.report.report_warning(
                title="Profile skipped for partitioned table",
                message="Could not construct partition filters - required for partition elimination",
                context=profile_request.pretty_name,
            )
            return None

        if not self.config.profiling.partition_profiling_enabled:
            logger.debug(
                f"{profile_request.pretty_name} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # Only add partition handling if we actually have partition filters
        if partition_filters:
            # Validate filters before using
            validated_filters = []
            for filter_expr in partition_filters:
                if self._validate_filter_expression(filter_expr):
                    validated_filters.append(filter_expr)
                else:
                    logger.warning(
                        f"Rejecting filter in profile request: {filter_expr}"
                    )

            if validated_filters:
                partition_where = " AND ".join(validated_filters)
                safe_table_ref = self._build_safe_table_reference(
                    db_name, schema_name, bq_table.name
                )

                custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {partition_where}"""

                logger.debug(f"Using partition filters: {partition_where}")
                profile_request.batch_kwargs.update(
                    dict(custom_sql=custom_sql, partition_handling="true")
                )

        return profile_request

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """Get profile workunits handling both internal and external tables."""
        profile_requests: List[TableProfilerRequest] = []

        for dataset in tables:
            for table in tables[dataset]:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()

                if table.external and not self.config.profiling.profile_external_tables:
                    self.report.profiling_skipped_other[f"{project_id}.{dataset}"] += 1
                    logger.debug(
                        f"Skipping profiling of external table {project_id}.{dataset}.{table.name}"
                    )
                    continue

                # Emit profile work unit
                logger.debug(
                    f"Creating profile request for table {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)
                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                else:
                    logger.debug(
                        f"Table {normalized_table_name} was not eligible for profiling."
                    )

        if len(profile_requests) == 0:
            return

        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """Get dataset name in BigQuery format."""
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()

    def _extract_partition_info_from_error(self, error_message: str) -> Dict[str, Any]:
        """
        Extract partition information from error messages.

        Args:
            error_message: The error message string

        Returns:
            Dictionary containing partition information
        """
        result: Dict[str, Any] = {"required_columns": [], "partition_values": {}}

        # Look for "filter over column(s)" pattern which lists required partition columns
        column_match = re.search(
            r"filter over column\(s\) '([^']+)'(?:, '([^']+)')?(?:, '([^']+)')?(?:, '([^']+)')?",
            error_message,
        )

        if column_match:
            required_columns = []
            for i in range(1, 5):  # Check up to 4 matched groups
                if column_match.group(i):
                    required_columns.append(column_match.group(i))

            if required_columns:
                result["required_columns"] = required_columns
                logger.debug(
                    f"Extracted required partition columns: {required_columns}"
                )

        # Look for partition path patterns like feed=value/year=value/month=value/day=value
        path_matches = re.findall(r"([a-zA-Z_]+)=([^/\s]+)", error_message)

        if path_matches:
            partition_values = {}
            for key, value in path_matches:
                # Clean up the value (remove any trailing punctuation)
                clean_value = value.rstrip(".,;'\"")
                # Handle numeric values
                if clean_value.isdigit():
                    clean_value = int(clean_value)
                partition_values[key] = clean_value

            if partition_values:
                result["partition_values"] = partition_values
                logger.debug(
                    f"Extracted partition values from path: {partition_values}"
                )

        return result

    def _handle_partition_error(
        self, e: Exception, table: BigqueryTable, project: str, schema: str
    ) -> List[str]:
        """
        Handle partition-related errors by extracting filter information.

        Args:
            e: The exception that was raised
            table: The BigQuery table
            project: The project ID
            schema: The dataset name

        Returns:
            List of filter expressions to use
        """
        error_msg = str(e)

        # Check if this is a partition-related error
        is_partition_error = any(
            keyword in error_msg
            for keyword in [
                "partition elimination",
                "without a filter over column",
                "exceeds the maximum allowed size",
                "specifying a constant filter expression",
            ]
        )

        if not is_partition_error:
            return []

        # Extract partition information from the error
        partition_info = self._extract_partition_info_from_error(error_msg)

        filters = []

        # If we have partition values from the error message, use them
        if partition_info.get("partition_values"):
            partition_values = partition_info["partition_values"]
            for col, value in partition_values.items():
                # Validate column name
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                    logger.warning(f"Invalid column name in error: {col}")
                    continue

                # Format the value based on its type
                if isinstance(value, (int, float)):
                    filters.append(f"`{col}` = {value}")
                else:
                    # Escape string values
                    escaped_value = str(value).replace("'", "''")
                    filters.append(f"`{col}` = '{escaped_value}'")

            logger.debug(f"Created filters from error message: {filters}")
            return filters

        # If we have required columns but no values, try to get values
        required_columns = partition_info.get("required_columns", [])
        if required_columns:
            # Try to use current date values for date-related columns
            now = datetime.now()
            date_values = {"year": now.year, "month": now.month, "day": now.day}

            for col in required_columns:
                # Validate column name
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                    logger.warning(f"Invalid column name in error: {col}")
                    continue

                # For date-related columns, use current date
                if col.lower() in date_values:
                    filters.append(f"`{col}` = {date_values[col.lower()]}")
                # For other columns, we need to sample values
                else:
                    try:
                        # Build safe table reference
                        safe_table_ref = self._build_safe_table_reference(
                            project, schema, table.name
                        )

                        # Try to get a single value from the column
                        sample_query = f"""SELECT DISTINCT `{col}` as value
FROM {safe_table_ref}
WHERE `{col}` IS NOT NULL
LIMIT @limit_rows"""

                        # Add date filters if we have them, to reduce query scope
                        where_clauses = []
                        parameters = [ScalarQueryParameter("limit_rows", "INT64", 1)]

                        for date_col in ["year", "month", "day"]:
                            if date_col in date_values and date_col in required_columns:
                                where_clauses.append(f"`{date_col}` = @{date_col}_val")
                                parameters.append(
                                    ScalarQueryParameter(
                                        f"{date_col}_val",
                                        "INT64",
                                        date_values[date_col],
                                    )
                                )

                        if where_clauses:
                            sample_query = sample_query.replace(
                                f"WHERE `{col}` IS NOT NULL",
                                f"WHERE {' AND '.join(where_clauses)} AND `{col}` IS NOT NULL",
                            )

                        job_config = QueryJobConfig(query_parameters=parameters)
                        result = self._execute_query_safely(
                            sample_query, job_config, f"sample value for {col}"
                        )

                        if result and len(result) > 0 and hasattr(result[0], "value"):
                            value = result[0].value
                            if isinstance(value, (int, float)):
                                filters.append(f"`{col}` = {value}")
                            else:
                                escaped_value = str(value).replace("'", "''")
                                filters.append(f"`{col}` = '{escaped_value}'")
                        else:
                            # If no value found, use IS NOT NULL
                            filters.append(f"`{col}` IS NOT NULL")
                    except Exception as e:
                        logger.warning(f"Error getting sample value for {col}: {e}")
                        # Fallback to IS NOT NULL
                        filters.append(f"`{col}` IS NOT NULL")

        return filters

    def _check_partition_has_data(
        self,
        project: str,
        schema: str,
        table_name: str,
        filters: List[str],
    ) -> Tuple[bool, int]:
        """
        Check if a partition has data by running a COUNT query with parameters.

        Args:
            project: Project ID
            schema: Dataset name
            table_name: Table name
            filters: List of filter conditions

        Returns:
            Tuple of (has_data, row_count)
        """
        if not filters:
            return False, 0

        try:
            # Validate all filters
            validated_filters = []
            for filter_str in filters:
                if self._validate_filter_expression(filter_str):
                    validated_filters.append(filter_str)
                else:
                    logger.warning(f"Rejecting filter in partition check: {filter_str}")

            if not validated_filters:
                return False, 0

            safe_table_ref = self._build_safe_table_reference(
                project, schema, table_name
            )

            query = f"""SELECT COUNT(*) as row_count
FROM {safe_table_ref}
WHERE {" AND ".join(validated_filters)}"""

            results = self._execute_query_safely(query, context="partition data check")

            if results and len(results) > 0:
                row_count = getattr(results[0], "row_count", 0)
                logger.debug(
                    f"Partition row count with filters {validated_filters}: {row_count}"
                )
                return row_count > 0, row_count
        except Exception as e:
            logger.warning(f"Error checking if partition has data: {e}")

        return False, 0

    def _find_partition_with_data(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        required_columns: List[str],
        fallback_days: int = 7,
    ) -> List[str]:
        """
        Find a partition with data by trying different date combinations.

        Args:
            project: Project ID
            schema: Dataset name
            table: BigQuery table
            required_columns: List of required partition columns
            fallback_days: Number of days to look back

        Returns:
            List of filter conditions for a partition with data
        """
        # Start with today and go back fallback_days days
        now = datetime.now()

        # Start from yesterday (1 day back) as the most common case
        start_offset = 1

        for days_back in range(start_offset, start_offset + fallback_days):
            check_date = now - timedelta(days=days_back)

            # Create date filters
            date_filters = []
            if "year" in required_columns:
                date_filters.append(f"`year` = {check_date.year}")
            if "month" in required_columns:
                date_filters.append(f"`month` = {check_date.month}")
            if "day" in required_columns:
                date_filters.append(f"`day` = {check_date.day}")

            # Skip if we don't have any date filters
            if not date_filters:
                continue

            logger.debug(f"Checking date partition {check_date.strftime('%Y-%m-%d')}")

            # Check if this date partition has data
            has_data, row_count = self._check_partition_has_data(
                project, schema, table.name, date_filters
            )

            if has_data:
                logger.debug(
                    f"Found partition with data: {check_date.strftime('%Y-%m-%d')} ({row_count} rows)"
                )

                # If we need other columns like 'feed', try to find values for them
                other_columns = [
                    col
                    for col in required_columns
                    if col.lower() not in ["year", "month", "day"]
                ]

                if other_columns:
                    for col in other_columns:
                        # Validate column name
                        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                            logger.warning(f"Invalid column name: {col}")
                            continue

                        try:
                            # Build safe table reference
                            safe_table_ref = self._build_safe_table_reference(
                                project, schema, table.name
                            )

                            # Sample query with the date filters to find a valid value
                            sample_query = f"""SELECT DISTINCT `{col}` as value, COUNT(*) as count
FROM {safe_table_ref}
WHERE {" AND ".join(date_filters)} AND `{col}` IS NOT NULL
GROUP BY `{col}`
ORDER BY count DESC
LIMIT @limit_rows"""

                            job_config = QueryJobConfig(
                                query_parameters=[
                                    ScalarQueryParameter("limit_rows", "INT64", 1)
                                ]
                            )

                            result = self._execute_query_safely(
                                sample_query, job_config, f"sample for {col}"
                            )

                            if (
                                result
                                and len(result) > 0
                                and hasattr(result[0], "value")
                            ):
                                value = result[0].value
                                if isinstance(value, (int, float)):
                                    date_filters.append(f"`{col}` = {value}")
                                else:
                                    escaped_value = str(value).replace("'", "''")
                                    date_filters.append(f"`{col}` = '{escaped_value}'")

                                # Verify we still have data with this additional filter
                                has_data, _ = self._check_partition_has_data(
                                    project, schema, table.name, date_filters
                                )

                                if not has_data:
                                    # Remove the filter we just added
                                    date_filters.pop()
                                    # Fall back to IS NOT NULL
                                    date_filters.append(f"`{col}` IS NOT NULL")
                            else:
                                # Check if we have a fallback value in config
                                if (
                                    col
                                    in self.config.profiling.fallback_partition_values
                                ):
                                    fallback_value = (
                                        self.config.profiling.fallback_partition_values[
                                            col
                                        ]
                                    )
                                    if isinstance(fallback_value, str):
                                        escaped_fallback = fallback_value.replace(
                                            "'", "''"
                                        )
                                        date_filters.append(
                                            f"`{col}` = '{escaped_fallback}'"
                                        )
                                    else:
                                        date_filters.append(
                                            f"`{col}` = {fallback_value}"
                                        )
                                else:
                                    date_filters.append(f"`{col}` IS NOT NULL")
                        except Exception as e:
                            logger.warning(f"Error finding value for column {col}: {e}")
                            # Check for fallback value in config
                            if col in self.config.profiling.fallback_partition_values:
                                fallback_value = (
                                    self.config.profiling.fallback_partition_values[col]
                                )
                                if isinstance(fallback_value, str):
                                    escaped_fallback = fallback_value.replace("'", "''")
                                    date_filters.append(
                                        f"`{col}` = '{escaped_fallback}'"
                                    )
                                else:
                                    date_filters.append(f"`{col}` = {fallback_value}")
                            else:
                                date_filters.append(f"`{col}` IS NOT NULL")

                # Final check to ensure we have data with all filters
                has_data, row_count = self._check_partition_has_data(
                    project, schema, table.name, date_filters
                )

                if has_data:
                    logger.debug(
                        f"Final partition has {row_count} rows with filters: {date_filters}"
                    )
                    return date_filters

        # If no data found with all approaches, use the fallback values
        logger.warning(
            f"Could not find any partition with data after checking {fallback_days} days, using fallbacks"
        )
        return self._get_fallback_partition_filters(
            table, project, schema, required_columns
        )

    def _try_date_filters(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
    ) -> Tuple[List[str], Dict[str, Any], bool]:
        """
        Try to use simple date filters (year, month, day) and check if they work.

        Args:
            project: Project ID
            schema: Dataset name
            table: BigQuery table

        Returns:
            Tuple of (filters, partition_values, has_data)
        """
        # Use yesterday as the default for date partitions (most common case)
        now = datetime.now() - timedelta(days=1)

        date_filters = [
            f"`year` = {now.year}",
            f"`month` = '{now.month:02d}'",
            f"`day` = '{now.day:02d}'",
        ]

        logger.debug(f"Trying simple date filters: {date_filters}")

        # Check if these filters work AND the partition has data
        has_data, row_count = self._check_partition_has_data(
            project, schema, table.name, date_filters
        )

        if has_data:
            # Date filters work and have data
            logger.debug(
                f"Simple date filters worked with {row_count} rows: {date_filters}"
            )
            partition_values = self._extract_partition_values_from_filters(date_filters)
            return date_filters, partition_values, True

        # Either the filters don't work or the partition is empty
        try:
            # Try running a simple query to check if date filters work at all
            safe_table_ref = self._build_safe_table_reference(
                project, schema, table.name
            )

            test_query = f"""SELECT COUNT(*) as count
FROM {safe_table_ref}
WHERE {" AND ".join(date_filters)}
LIMIT @limit_rows"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
            )

            self._execute_query_safely(test_query, job_config, "date filter test")
            # If we get here, filters work but partition is empty
            logger.debug(
                "Date filters work but partition is empty, trying to find a partition with data"
            )
            return [], {}, False
        except Exception as e:
            # If date filters don't work, try to extract partition info from error
            logger.debug(f"Simple date filters failed: {e}")
            return [], {}, False

    def _try_error_based_filters(
        self,
        e: Exception,
        project: str,
        schema: str,
        table: BigqueryTable,
    ) -> Tuple[List[str], Dict[str, Any], bool]:
        """
        Try to extract partition filters from an error message.

        Args:
            e: Exception that was raised
            project: Project ID
            schema: Dataset name
            table: BigQuery table

        Returns:
            Tuple of (filters, partition_values, has_data)
        """
        error_filters = self._handle_partition_error(e, table, project, schema)

        if error_filters:
            # Check if these error-based filters have data
            has_data, row_count = self._check_partition_has_data(
                project, schema, table.name, error_filters
            )

            if has_data:
                logger.debug(
                    f"Error-based filters have {row_count} rows: {error_filters}"
                )
                partition_values = self._extract_partition_values_from_filters(
                    error_filters
                )
                return error_filters, partition_values, True

        return [], {}, False

    def _get_required_columns(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> List[str]:
        """
        Get required partition columns from table info or by running a test query.

        Args:
            table: BigQuery table
            project: Project ID
            schema: Dataset name

        Returns:
            List of required partition column names
        """
        required_columns = []

        # Try to get required columns from table info
        if hasattr(table, "partition_info") and table.partition_info:
            if hasattr(table.partition_info, "columns"):
                if table.partition_info.columns is not None:
                    required_columns = [
                        col.name
                        for col in table.partition_info.columns
                        if col is not None
                    ]
            elif (
                hasattr(table.partition_info, "column") and table.partition_info.column
            ):
                required_columns = [table.partition_info.column.name]

        # If we couldn't get columns from table info, try to extract from error
        if not required_columns:
            try:
                safe_table_ref = self._build_safe_table_reference(
                    project, schema, table.name
                )
                test_query = (
                    f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                )
                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )
                self._execute_query_safely(
                    test_query, job_config, "required columns detection"
                )
            except Exception as e:
                error_info = self._extract_partition_info_from_error(str(e))
                if "required_columns" in error_info and error_info["required_columns"]:
                    required_columns = error_info["required_columns"]

        return required_columns

    def _process_date_column(self, col_name: str, partition_filters: List[str]) -> None:
        """
        Process a date-related column for sampling approach.

        Args:
            col_name: Name of the column
            partition_filters: List to append the filter to
        """
        now = datetime.now() - timedelta(days=1)  # Use yesterday as default
        if col_name.lower() == "year":
            partition_filters.append(f"`{col_name}` = '{now.year}'")
        elif col_name.lower() == "month":
            partition_filters.append(f"`{col_name}` = '{now.month}'")
        elif col_name.lower() == "day":
            partition_filters.append(f"`{col_name}` = '{now.day}'")

    def _process_fallback_column(
        self, col_name: str, partition_filters: List[str]
    ) -> bool:
        """
        Apply fallback value from config if available.

        Args:
            col_name: Name of the column
            partition_filters: List to append the filter to

        Returns:
            True if fallback was applied, False otherwise
        """
        if col_name in self.config.profiling.fallback_partition_values:
            fallback_value = self.config.profiling.fallback_partition_values[col_name]
            if isinstance(fallback_value, str):
                escaped_value = fallback_value.replace("'", "''")
                partition_filters.append(f"`{col_name}` = '{escaped_value}'")
            else:
                partition_filters.append(f"`{col_name}` = {fallback_value}")
            return True
        return False

    def _get_date_filters_for_query(self, required_columns: List[str]) -> List[str]:
        """
        Create date filters for a sampling query.

        Args:
            required_columns: List of required column names

        Returns:
            List of date filter strings
        """
        date_filters = []
        now = datetime.now() - timedelta(days=1)  # Use yesterday as default
        for date_col, date_val, format_with_zero in [
            ("year", now.year, False),
            ("month", now.month, True),
            ("day", now.day, True),
        ]:
            if date_col in required_columns:
                if format_with_zero:
                    date_filters.append(f"`{date_col}` = '{date_val:02d}'")
                else:
                    date_filters.append(f"`{date_col}` = '{date_val}'")
        return date_filters

    def _sample_column_value(
        self,
        project: str,
        schema: str,
        table_name: str,
        col_name: str,
        date_filters: List[str],
    ) -> Tuple[Any, bool]:
        """
        Sample a value for a column using a parameterized query.

        Args:
            project: BigQuery project ID
            schema: Dataset name
            table_name: Table name
            col_name: Column to sample
            date_filters: Date filters to narrow the search

        Returns:
            Tuple of (sampled value, success flag)
        """
        try:
            # Validate column name
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                logger.warning(f"Invalid column name for sampling: {col_name}")
                return None, False

            # Build safe table reference
            safe_table_ref = self._build_safe_table_reference(
                project, schema, table_name
            )

            # Create the sample query
            sample_query = f"""SELECT DISTINCT `{col_name}` as value, COUNT(*) as count
FROM {safe_table_ref}
WHERE `{col_name}` IS NOT NULL
GROUP BY `{col_name}`
ORDER BY count DESC
LIMIT @limit_rows"""

            parameters = [ScalarQueryParameter("limit_rows", "INT64", 1)]

            # Apply date filters if available
            if date_filters:
                # Validate date filters
                validated_date_filters = []
                for filter_str in date_filters:
                    if self._validate_filter_expression(filter_str):
                        validated_date_filters.append(filter_str)

                if validated_date_filters:
                    sample_query = sample_query.replace(
                        f"WHERE `{col_name}` IS NOT NULL",
                        f"WHERE {' AND '.join(validated_date_filters)} AND `{col_name}` IS NOT NULL",
                    )

            job_config = QueryJobConfig(query_parameters=parameters)

            # Execute the query
            result = self._execute_query_safely(
                sample_query, job_config, f"sample column {col_name}"
            )

            if result and len(result) > 0:
                value = getattr(result[0], "value", None)
                if value is not None:
                    return value, True

            return None, False

        except Exception as e:
            logger.warning(f"Error getting sample for column {col_name}: {e}")
            return None, False

    def _process_column_for_sampling(
        self,
        col: str,
        required_columns: List[str],
        project: str,
        schema: str,
        table: BigqueryTable,
        partition_filters: List[str],
    ) -> None:
        """
        Process a single column for the sampling approach.

        Args:
            col: Column name to process
            required_columns: List of all required columns
            project: BigQuery project ID
            schema: Dataset name
            table: BigqueryTable instance
            partition_filters: List to append filters to
        """
        try:
            # Validate column name
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*", col):
                logger.warning(f"Invalid column name: {col}")
                return

            # Special handling for date-related columns
            if col.lower() in ["year", "month", "day"]:
                self._process_date_column(col, partition_filters)
            else:
                # Check for fallback value in config
                if self._process_fallback_column(col, partition_filters):
                    return

                # Get date filters to narrow the search
                date_filters = self._get_date_filters_for_query(required_columns)

                # Try to sample a value
                value, success = self._sample_column_value(
                    project, schema, table.name, col, date_filters
                )

                if success:
                    # Add the value to filters
                    if isinstance(value, (int, float)):
                        partition_filters.append(f"`{col}` = {value}")
                    else:
                        escaped_value = str(value).replace("'", "''")
                        partition_filters.append(f"`{col}` = '{escaped_value}'")
                else:
                    # Try to extract partition info from error or use IS NOT NULL
                    self._handle_failed_sample(
                        col, table, project, schema, partition_filters
                    )
        except Exception as e:
            logger.warning(f"Error processing column {col}: {e}")
            # Try to use fallback value if available
            if not self._process_fallback_column(col, partition_filters):
                partition_filters.append(f"`{col}` IS NOT NULL")

    def _try_sampling_approach(
        self,
        project: str,
        schema: str,
        table: BigqueryTable,
        required_columns: List[str],
    ) -> Tuple[List[str], Dict[str, Any], bool]:
        """
        Try to use sampling to find partition values.

        Args:
            project: Project ID
            schema: Dataset name
            table: BigQuery table
            required_columns: List of required partition columns

        Returns:
            Tuple of (filters, partition_values, has_data)
        """
        if not required_columns:
            return [], {}, False

        logger.debug(
            f"Using sampling to find partition values for {len(required_columns)} columns"
        )
        partition_filters: List[str] = []

        try:
            # Process each column
            for col in required_columns:
                self._process_column_for_sampling(
                    col, required_columns, project, schema, table, partition_filters
                )

            # If we have filters, check if they return data
            if partition_filters:
                return self._verify_sampling_filters(
                    partition_filters, project, schema, table.name
                )

        except Exception as e:
            logger.warning(f"Error in sampling approach: {e}")

        return [], {}, False

    def _handle_failed_sample(
        self,
        col_name: str,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: List[str],
    ) -> None:
        """
        Handle the case when sampling fails for a column.

        Args:
            col_name: Column name
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            partition_filters: List to append filters to
        """
        try:
            # Try to extract partition info from error
            error_filters = self._handle_partition_error(
                Exception(f"No values found for {col_name}"), table, project, schema
            )

            if error_filters:
                # Find filters for this column
                col_filters = [f for f in error_filters if col_name in f]
                if col_filters:
                    partition_filters.extend(col_filters)
                else:
                    partition_filters.append(f"`{col_name}` IS NOT NULL")
            else:
                partition_filters.append(f"`{col_name}` IS NOT NULL")
        except Exception:
            partition_filters.append(f"`{col_name}` IS NOT NULL")

    def _verify_sampling_filters(
        self, partition_filters: List[str], project: str, schema: str, table_name: str
    ) -> Tuple[List[str], Dict[str, Any], bool]:
        """
        Verify that sampling filters return data.

        Args:
            partition_filters: List of filter strings
            project: BigQuery project ID
            schema: Dataset name
            table_name: Table name

        Returns:
            Tuple of (filters, partition_values, has_data)
        """
        logger.debug(f"Using partition filters: {' AND '.join(partition_filters)}")

        # Verify we have data with these filters
        has_data, row_count = self._check_partition_has_data(
            project, schema, table_name, partition_filters
        )

        if has_data:
            logger.debug(f"Sampling found partition with {row_count} rows")
            partition_values = self._extract_partition_values_from_filters(
                partition_filters
            )
            return partition_filters, partition_values, True
        else:
            logger.warning("Sampling found filters but partition has no data")
            return [], {}, False

    def _apply_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        batch_kwargs: Dict[str, Any],
    ) -> None:
        """
        Apply partition filters to batch_kwargs.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            batch_kwargs: Dictionary to update with partition information
        """
        logger.debug(
            f"Table {table.name} has partition information, determining optimal filters"
        )

        # Special handling for external tables
        is_external = getattr(table, "external", False)
        if is_external:
            logger.debug(
                f"Table {table.name} is an external table, using special handling"
            )

        # Try different approaches to find partition filters with data
        partition_filters = None
        partition_values = None
        has_data = False

        # Approach 1: Try simple date filters
        try:
            date_filters, date_values, date_has_data = self._try_date_filters(
                project, schema, table
            )

            if date_has_data:
                partition_filters = date_filters
                partition_values = date_values
                has_data = True
            else:
                # Approach 2: If date filters don't work, try error-based filters
                try:
                    # Build safe table reference
                    safe_table_ref = self._build_safe_table_reference(
                        project, schema, table.name
                    )
                    # Run a simple query to trigger partition error
                    test_query = (
                        f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                    )
                    job_config = QueryJobConfig(
                        query_parameters=[
                            ScalarQueryParameter("limit_rows", "INT64", 1)
                        ]
                    )
                    self._execute_query_safely(
                        test_query, job_config, "partition trigger"
                    )
                except Exception as e:
                    error_filters, error_values, error_has_data = (
                        self._try_error_based_filters(e, project, schema, table)
                    )

                    if error_has_data:
                        partition_filters = error_filters
                        partition_values = error_values
                        has_data = True

                # Approach 3: If we still don't have filters with data, try to find a partition with data
                if not has_data:
                    required_columns = self._get_required_columns(
                        table, project, schema
                    )

                    if required_columns:
                        logger.debug(
                            f"Searching for a partition with data using columns: {required_columns}"
                        )
                        data_filters = self._find_partition_with_data(
                            project, schema, table, required_columns
                        )

                        if data_filters:
                            partition_filters = data_filters
                            partition_values = (
                                self._extract_partition_values_from_filters(
                                    partition_filters
                                )
                            )
                            has_data = True
        except Exception as e:
            logger.warning(f"Error in primary partition approaches: {str(e)}")

        # Approach 4: If we still don't have filters, try sampling approach
        if not has_data and hasattr(table, "partition_info") and table.partition_info:
            try:
                required_columns = self._get_required_columns(table, project, schema)

                if required_columns:
                    sampling_filters, sampling_values, sampling_has_data = (
                        self._try_sampling_approach(
                            project, schema, table, required_columns
                        )
                    )

                    if sampling_has_data:
                        partition_filters = sampling_filters
                        partition_values = sampling_values
                        has_data = True
            except Exception as e:
                logger.warning(f"Error getting partition filters with sampling: {e}")
                # Last resort - try to extract from error
                try:
                    error_filters, error_values, error_has_data = (
                        self._try_error_based_filters(e, project, schema, table)
                    )

                    if error_has_data:
                        partition_filters = error_filters
                        partition_values = error_values
                        has_data = True
                except Exception:
                    pass

        # If we still don't have filters with data, try custom approach
        if not partition_filters or not has_data:
            logger.debug(f"Trying custom partition approach for {table.name}")
            self._apply_custom_partition_approach(table, project, schema, batch_kwargs)
            return

        # If we have partition filters with data, create a temporary table or view
        logger.debug(f"Creating temp table with filters for {table.name}")

        # Add partition values to batch_kwargs if available
        if partition_values:
            batch_kwargs["partition_values"] = json.dumps(partition_values)

        self._create_temp_table_with_filters(
            table, project, schema, partition_filters, batch_kwargs
        )

    def execute_query(self, query: str) -> List[Any]:
        """
        Execute a BigQuery query with timeout from configuration and enhanced security validation.

        Args:
            query: SQL query to execute

        Returns:
            List of row results
        """
        # Enhanced security validation
        self._validate_sql_structure(query)

        # Additional basic pattern check for immediate threats
        dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
        for pattern in dangerous_patterns:
            if pattern in query:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            logger.debug(f"Executing query with {timeout}s timeout")
            job_config = QueryJobConfig(
                timeout_ms=timeout * 1000,
                # Additional security: disable query caching for sensitive operations
                use_query_cache=False,
                # Enable dry run for initial validation (commented out as it would prevent actual execution)
                # dry_run=True
            )
            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            return list(query_job.result())
        except Exception as e:
            logger.warning(f"Query execution error: {e}")
            raise

    def execute_query_with_config(
        self, query: str, job_config: QueryJobConfig
    ) -> List[Any]:
        """
        Execute a BigQuery query with custom job configuration including timeout and enhanced security validation.

        Args:
            query: SQL query to execute
            job_config: QueryJobConfig with custom parameters

        Returns:
            List of row results
        """
        # Enhanced security validation
        self._validate_sql_structure(query)

        # Additional basic pattern check
        dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
        for pattern in dangerous_patterns:
            if pattern in query:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

        try:
            logger.debug(
                f"Executing query with {self.config.profiling.partition_fetch_timeout}s timeout and custom config"
            )
            # Ensure timeout is set even with custom config
            job_config.job_timeout_ms = (
                self.config.profiling.partition_fetch_timeout * 1000
            )
            # Additional security settings
            job_config.use_query_cache = False

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            # Convert to list to ensure consistent return type
            return list(query_job.result())
        except Exception as e:
            logger.warning(f"Query execution error: {e}")
            raise

    def _extract_partition_values_from_filters(
        self, filters: List[str]
    ) -> Dict[str, Any]:
        """
        Extract partition column values from filter strings.

        Args:
            filters: List of filter strings in the format `column` = value

        Returns:
            Dictionary mapping column names to their values
        """
        partition_values: Dict[str, Any] = {}

        for filter_str in filters:
            if "=" not in filter_str:
                continue

            # Extract column name and value
            parts = filter_str.split("=", 1)
            if len(parts) != 2:
                continue

            col_part = parts[0].strip()
            value_part = parts[1].strip()

            # Extract column name without backticks
            col_name = col_part.strip("` \t")

            # Extract value without quotes
            value = value_part.strip("'\" \t")

            # Convert to appropriate type if possible
            if value.isdigit():
                # Store as int for integer values
                partition_values[col_name] = int(value)
            elif value.replace(".", "", 1).isdigit() and "." in value:
                # Store as float for decimal values
                partition_values[col_name] = float(value)
            else:
                # Keep as string for non-numeric values
                partition_values[col_name] = value

        return partition_values

    def _create_temp_table_with_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_filters: List[str],
        batch_kwargs: Dict[str, Any],
    ) -> None:
        """
        Create a filtered query in batch_kwargs with secure construction.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_filters: List of partition filter strings
            batch_kwargs: Dictionary to update with partition information
        """
        if not partition_filters:
            return

        # Validate all filters before using
        validated_filters = []
        for filter_str in partition_filters:
            if self._validate_filter_expression(filter_str):
                validated_filters.append(filter_str)
            else:
                logger.warning(f"Rejecting filter in temp table creation: {filter_str}")

        if not validated_filters:
            logger.warning("No valid filters for temp table creation")
            return

        # Build safe table reference
        safe_table_ref = self._build_safe_table_reference(project, schema, table.name)
        where_clause = " AND ".join(validated_filters)

        custom_sql = f"""SELECT * 
FROM {safe_table_ref}
WHERE {where_clause}"""

        batch_kwargs.update(
            {
                "custom_sql": custom_sql,
                "partition_handling": "true",
                "partition_filters": where_clause,
            }
        )

        logger.debug(f"Applied secure partition filters to {table.name}")

    def _apply_custom_partition_approach(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        batch_kwargs: Dict[str, Any],
    ) -> None:
        """
        Apply custom partition approach when standard methods fail.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            batch_kwargs: Dictionary to update with partition information
        """
        logger.debug(f"Applying custom partition approach for {table.name}")

        # First check if the table is actually partitioned
        try:
            # Query INFORMATION_SCHEMA to check for partitioning
            safe_info_schema_ref = self._build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = f"""SELECT count(*) as is_partitioned
FROM {safe_info_schema_ref}
WHERE table_name = @table_name AND is_partitioning_column = 'YES'"""

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table.name)
                ]
            )

            partitioning_check_result = self._execute_query_safely(
                query, job_config, "custom partition check"
            )
            is_partitioned = 0
            if partitioning_check_result and len(partitioning_check_result) > 0:
                is_partitioned = getattr(
                    partitioning_check_result[0], "is_partitioned", 0
                )

            if is_partitioned == 0:
                logger.debug(
                    f"Table {table.name} is not partitioned, no filters needed"
                )
                return

            # Try to find ANY data in the table
            safe_table_ref = self._build_safe_table_reference(
                project, schema, table.name
            )
            sample_query = f"""SELECT * FROM {safe_table_ref} LIMIT @limit_rows"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 10)]
            )

            try:
                # This might fail if partition elimination is required
                sample_results = self._execute_query_safely(
                    sample_query, job_config, "custom partition sample"
                )
                if sample_results and len(sample_results) > 0:
                    logger.debug(
                        f"Found data in table {table.name} without partition filters"
                    )
                    return
            except Exception as e:
                logger.warning(f"Error sampling table without filters: {e}")

            # Last resort - use the LIMIT approach which bypasses partition elimination
            logger.warning(f"Using LIMIT approach to profile {table.name}")

            # Note: For the final custom SQL in batch kwargs, we need to use literal values
            # since the execution context may not support parameters
            custom_sql_literal = f"""SELECT * FROM {safe_table_ref} LIMIT 1000000"""

            batch_kwargs.update(
                {
                    "custom_sql": custom_sql_literal,
                    "partition_handling": "true",
                    "fallback_approach": "true",
                }
            )

            logger.debug(f"Applied fallback LIMIT approach to {table.name}")
        except Exception as e:
            logger.error(f"Error in custom partition approach: {e}")
            # Don't modify batch_kwargs if we can't determine the right approach

    def _get_single_column_partition_value(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        col_name: str,
        data_type: str,
        use_info_schema: bool = True,
    ) -> Optional[Any]:
        """
        Unified method to find a partition value for a single column using parameterized queries.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            col_name: Column name
            data_type: Column data type
            use_info_schema: Whether to try INFORMATION_SCHEMA first

        Returns:
            The partition value if found, None otherwise
        """
        # Validate column name
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*", col_name):
            logger.warning(f"Invalid column name: {col_name}")
            return None

        # First try INFORMATION_SCHEMA.PARTITIONS for regular tables
        if use_info_schema and not table.external:
            info_schema_result = self._get_partition_info_from_information_schema(
                table, project, schema, [col_name], max_results=5
            )

            if info_schema_result and col_name in info_schema_result:
                val = info_schema_result[col_name]
                logger.debug(
                    f"Found partition value from INFORMATION_SCHEMA for {col_name}: {val}"
                )
                return val

        # Try querying the table directly
        try:
            # For date columns, get the most recent value; for others, get most populated
            is_date_column = col_name.lower() in {
                "date",
                "day",
                "dt",
                "partition_date",
                "date_partition",
            }

            order_by = f"`{col_name}` DESC" if is_date_column else "record_count DESC"

            safe_table_ref = self._build_safe_table_reference(
                project, schema, table.name
            )

            query = f"""WITH PartitionStats AS (
    SELECT `{col_name}` as val, COUNT(*) as record_count
    FROM {safe_table_ref}
    WHERE `{col_name}` IS NOT NULL
    GROUP BY `{col_name}`
    HAVING record_count > 0
    ORDER BY {order_by}
    LIMIT @limit_rows
)
SELECT val, record_count FROM PartitionStats"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
            )

            logger.debug(f"Finding partition value for {col_name}: {query}")

            column_partition_results = self._execute_query_safely(
                query, job_config, f"single column partition {col_name}"
            )

            if not column_partition_results or column_partition_results[0].val is None:
                logger.warning(f"No valid partition value found for column {col_name}")
                return None

            val = column_partition_results[0].val
            record_count = column_partition_results[0].record_count

            logger.debug(
                f"Found partition value for {col_name}: {val} ({record_count} records)"
            )
            return val

        except Exception as e:
            logger.warning(f"Error finding partition value for {col_name}: {e}")

            # Try to extract partition information from the error message
            try:
                error_info = self._extract_partition_info_from_error(str(e))
                if error_info.get("partition_values"):
                    partition_values = error_info["partition_values"]
                    if col_name in partition_values:
                        raw_val = partition_values[col_name]
                        # Convert value to appropriate type based on column data type
                        val = self._convert_partition_value_to_type(raw_val, data_type)
                        logger.debug(
                            f"Extracted partition value for {col_name} from error: {val} (type: {data_type})"
                        )
                        return val
            except Exception as e:
                logger.debug(f"Could not extract partition info from error: {e}")

            return None

    def _get_multi_column_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        use_info_schema: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Unified method to find partition values for multiple columns using parameterized queries.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            required_columns: List of required partition columns
            use_info_schema: Whether to try INFORMATION_SCHEMA first

        Returns:
            Dictionary mapping column names to values if found, None otherwise
        """
        # Validate column names
        valid_columns = []
        for col in required_columns:
            if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*", col):
                valid_columns.append(col)
            else:
                logger.warning(f"Invalid column name: {col}")

        if not valid_columns:
            return None

        # First try INFORMATION_SCHEMA.PARTITIONS for regular tables
        if use_info_schema and not table.external:
            info_schema_result = self._get_partition_info_from_information_schema(
                table, project, schema, valid_columns, max_results=10
            )

            if info_schema_result:
                logger.debug(
                    f"Found partition values from INFORMATION_SCHEMA: {info_schema_result}"
                )
                return info_schema_result

        # Try querying the table directly
        try:
            columns_select = ", ".join([f"`{col}`" for col in valid_columns])
            columns_group = ", ".join([f"`{col}`" for col in valid_columns])
            where_conditions = " AND ".join(
                [f"`{col}` IS NOT NULL" for col in valid_columns]
            )

            safe_table_ref = self._build_safe_table_reference(
                project, schema, table.name
            )

            query = f"""SELECT {columns_select}, COUNT(*) as record_count
FROM {safe_table_ref}
WHERE {where_conditions}
GROUP BY {columns_group}
HAVING record_count > 0
ORDER BY record_count DESC
LIMIT @limit_rows"""

            job_config = QueryJobConfig(
                query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 10)]
            )

            logger.debug(f"Finding multi-column partition values: {query}")

            query_results = self._execute_query_safely(
                query, job_config, "multi-column partition values"
            )

            if not query_results:
                logger.warning("No valid partition combinations found")
                return None

            # Return the combination with the most records
            best_result = query_results[0]
            result_values = {}

            for col_name in valid_columns:
                val = getattr(best_result, col_name)
                if val is not None:
                    result_values[col_name] = val

            logger.debug(f"Found multi-column partition combination: {result_values}")
            return result_values

        except Exception as e:
            logger.warning(f"Error finding multi-column partition values: {e}")

            # Try to extract partition information from the error message
            try:
                error_info = self._extract_partition_info_from_error(str(e))
                if error_info.get("partition_values"):
                    partition_values = error_info["partition_values"]

                    # Get data types for proper conversion
                    column_data_types = self._get_partition_column_types(
                        table, project, schema, valid_columns
                    )

                    # Check if we have values for all required columns
                    if all(col in partition_values for col in valid_columns):
                        # Convert values to appropriate types
                        converted_values = {}
                        for col in valid_columns:
                            raw_val = partition_values[col]
                            data_type = column_data_types.get(col, "STRING")
                            converted_values[col] = (
                                self._convert_partition_value_to_type(
                                    raw_val, data_type
                                )
                            )

                        logger.debug(
                            f"Extracted multi-column partition values from error: {converted_values}"
                        )
                        return converted_values
                    # Or return partial values if we have some
                    elif any(col in partition_values for col in valid_columns):
                        # Convert partial values to appropriate types
                        partial_values = {}
                        for col in valid_columns:
                            if col in partition_values:
                                raw_val = partition_values[col]
                                data_type = column_data_types.get(col, "STRING")
                                partial_values[col] = (
                                    self._convert_partition_value_to_type(
                                        raw_val, data_type
                                    )
                                )

                        logger.debug(
                            f"Extracted partial partition values from error: {partial_values}"
                        )
                        return partial_values
            except Exception as e:
                logger.debug(f"Could not extract partition info from error: {e}")

            return None
