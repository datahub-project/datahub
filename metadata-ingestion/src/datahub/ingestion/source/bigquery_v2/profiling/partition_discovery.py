"""BigQuery partition discovery and filter generation with enhanced security."""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple, TypedDict, Union

from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_column_names,
    validate_filter_expression,
)

logger = logging.getLogger(__name__)


class PartitionInfo(TypedDict):
    """Type definition for partition information."""

    required_columns: List[str]
    partition_values: Dict[str, Union[str, int]]


class PartitionResult(TypedDict):
    """Type definition for partition discovery results."""

    partition_values: Dict[str, Union[str, int, float]]
    row_count: Optional[int]


class PartitionDiscovery:
    """
    Handles partition discovery and filter generation for BigQuery tables.

    This class encapsulates all the logic needed to:
    1. Discover partition columns from table metadata
    2. Find valid partition values that contain data
    3. Generate secure partition filters for profiling queries
    """

    def __init__(self, config: BigQueryV2Config):
        """
        Initialize the partition discovery helper.

        Args:
            config: BigQuery configuration containing profiling settings
        """
        self.config = config

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        """
        Get partition date range from partition ID.

        This method handles the standard BigQuery partition ID formats:
        - YYYY (yearly)
        - YYYYMM (monthly)
        - YYYYMMDD (daily)
        - YYYYMMDDHH (hourly)

        Args:
            partition_id: Partition identifier string
            partition_datetime: Optional datetime to use as base

        Returns:
            Tuple of (start_datetime, end_datetime) for the partition

        Raises:
            ValueError: If partition_id format is not recognized
        """
        partition_range_map: Dict[int, Tuple[relativedelta, str]] = {
            4: (relativedelta(years=1), "%Y"),
            6: (relativedelta(months=1), "%Y%m"),
            8: (relativedelta(days=1), "%Y%m%d"),
            10: (relativedelta(hours=1), "%Y%m%d%H"),
        }

        duration: relativedelta
        if partition_range_map.get(len(partition_id)):
            (delta, format_str) = partition_range_map[len(partition_id)]
            duration = delta
            if not partition_datetime:
                partition_datetime = datetime.strptime(partition_id, format_str)
            else:
                partition_datetime = datetime.strptime(
                    partition_datetime.strftime(format_str), format_str
                )
        else:
            raise ValueError(
                f"Invalid partition_id {partition_id}. It must be yearly/monthly/daily/hourly."
            )
        upper_bound_partition_datetime = partition_datetime + duration
        return partition_datetime, upper_bound_partition_datetime

    def get_partition_columns_from_info_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        """
        Get partition columns from INFORMATION_SCHEMA using parameterized queries.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            execute_query_func: Function to execute queries safely

        Returns:
            Dictionary mapping column names to data types
        """
        try:
            safe_info_schema_ref = build_safe_table_reference(
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

            partition_column_rows = execute_query_func(
                query, job_config, "partition columns from info schema"
            )

            partition_columns = [row.column_name for row in partition_column_rows]

            # Get data types for the partition columns
            if partition_columns:
                return self._get_partition_column_types(
                    table, project, schema, partition_columns, execute_query_func
                )
            else:
                return {}
        except Exception as e:
            logger.warning(
                f"Error getting partition columns from INFORMATION_SCHEMA: {e}"
            )
            return {}

    def get_partition_columns_from_ddl(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        """
        Extract partition columns from table DDL using robust regex patterns.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            execute_query_func: Function to execute queries safely

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
                    table, project, schema, column_names, execute_query_func
                )

        except Exception as e:
            logger.warning(f"Error parsing DDL for partition columns: {e}")

        return partition_cols_with_types

    def get_most_populated_partitions(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = 5,
    ) -> PartitionResult:
        """Find the most populated partitions for a table."""
        if not partition_columns:
            return {"partition_values": {}, "row_count": None}

        # External tables require direct table queries
        if table.external:
            logger.debug(f"Using table query approach for external table {table.name}")
            return self._get_partition_info_from_table_query(
                table,
                project,
                schema,
                partition_columns,
                execute_query_func,
                max_results,
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
                execute_query_func,
                max_results * 10,  # Query more partitions from info schema
            )

            # If INFORMATION_SCHEMA didn't work, fall back to table query
            if not result:
                logger.debug(
                    f"INFORMATION_SCHEMA approach failed, falling back to table query for {table.name}"
                )
                result = self._get_partition_info_from_table_query(
                    table,
                    project,
                    schema,
                    partition_columns,
                    execute_query_func,
                    max_results,
                )

            return result

    def get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """
        Get partition filters for all required partition columns.

        Args:
            table: BigqueryTable instance containing table metadata
            project: The BigQuery project ID
            schema: The dataset/schema name
            execute_query_func: Function to execute queries safely

        Returns:
            List of partition filter strings if partition columns found and filters could be constructed
            Empty list if no partitions found
            None if partition filters could not be determined and profiling should be skipped
        """
        current_time = datetime.now(timezone.utc)

        # First try sampling approach, but only as a fallback for very large tables
        # We want to discover actual partitions, not just sample a few values
        logger.info(
            f"Starting partition discovery for table {table.name} (may try multiple date formats to find data)"
        )
        logger.debug(
            "Attempting comprehensive partition discovery before falling back to sampling"
        )

        # Get required partition columns from table info
        required_partition_columns = self._get_partition_columns_from_table_info(table)

        # If no partition columns found from partition_info, query INFORMATION_SCHEMA
        if not required_partition_columns:
            required_partition_columns = self._get_partition_columns_from_schema(
                table, project, schema, execute_query_func
            )

        # If we still don't have partition columns, try to trigger a partition error to detect them
        if not required_partition_columns:
            try:
                safe_table_ref = build_safe_table_reference(project, schema, table.name)

                # Run a simple query to trigger partition error
                test_query = (
                    f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                )
                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )
                execute_query_func(test_query, job_config, "partition detection")

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
                table, project, schema, current_time, execute_query_func
            )

        logger.debug(f"Required partition columns: {required_partition_columns}")

        # For internal tables, try INFORMATION_SCHEMA approach first (much more efficient)
        if not table.external:
            partition_filters = self._get_partition_filters_from_information_schema(
                table,
                project,
                schema,
                list(required_partition_columns),
                execute_query_func,
            )
            if partition_filters:
                logger.info(
                    f"Successfully obtained partition filters from INFORMATION_SCHEMA for internal table {table.name}: {len(partition_filters)} filters"
                )
                return partition_filters
            else:
                logger.debug(
                    f"INFORMATION_SCHEMA approach failed for internal table {table.name}, falling back to strategic dates"
                )

        # Try to find REAL partition values that exist in the table (strategic date approach)
        partition_filters = self._find_real_partition_values(
            table, project, schema, list(required_partition_columns), execute_query_func
        )

        if partition_filters:
            logger.debug(f"Found valid partition filters: {partition_filters}")
            return partition_filters
        else:
            # If comprehensive partition discovery failed, try sampling as fallback
            logger.info(
                f"Comprehensive partition discovery failed for table {table.name}. "
                f"Attempting sampling-based approach as fallback."
            )
            sample_filters = self._get_partitions_with_sampling(
                table, project, schema, execute_query_func
            )
            if sample_filters:
                logger.info(
                    f"Found partition values via sampling for {table.name}: {len(sample_filters)} filters"
                )
                return sample_filters
            else:
                logger.warning(
                    f"Could not find valid partition values for table {table.name} "
                    f"with required columns {required_partition_columns}. "
                    f"Skipping profiling to avoid inaccurate results."
                )
                return None

    def _get_partition_column_types(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        """Get data types for partition columns using parameterized queries."""
        if not partition_columns:
            return {}

        try:
            # Use utility for validation
            safe_columns = validate_column_names(
                partition_columns, "column type lookup"
            )

            if not safe_columns:
                logger.warning(f"No valid column names provided for table {table.name}")
                return {}

            # Build safe table reference
            safe_info_schema_ref = build_safe_table_reference(
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
            query_results = execute_query_func(
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
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = 100,
    ) -> PartitionResult:
        """Get partition information from INFORMATION_SCHEMA.PARTITIONS."""
        if not partition_columns:
            return {"partition_values": {}, "row_count": None}

        try:
            safe_max_results = max(1, min(int(max_results), 1000))

            safe_info_schema_ref = build_safe_table_reference(
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

            partition_info_results = execute_query_func(
                query, job_config, "partition info from information schema"
            )

            if not partition_info_results:
                logger.warning(
                    f"No partitions found in INFORMATION_SCHEMA for table {table.name}"
                )
                return {"partition_values": {}, "row_count": None}

            # Take the partition with the most rows
            best_partition = partition_info_results[0]
            partition_id = best_partition.partition_id

            logger.debug(
                f"Found best partition {partition_id} with {best_partition.total_rows} rows"
            )

            partition_values: Dict[str, Union[str, int, float]] = {}
            row_count = (
                best_partition.total_rows
                if hasattr(best_partition, "total_rows")
                else None
            )

            if "$" in partition_id:
                # Multi-column partitioning with format: col1=val1$col2=val2$col3=val3
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in partition_columns:
                            if val.isdigit():
                                partition_values[col] = int(val)
                            else:
                                partition_values[col] = val
            else:
                # Single column partitioning
                if len(partition_columns) == 1:
                    col_name = partition_columns[0]
                    if partition_id.isdigit():
                        if len(partition_id) == 8 or len(partition_id) == 10:
                            partition_values[col_name] = partition_id
                        else:
                            partition_values[col_name] = partition_id
                    else:
                        partition_values[col_name] = partition_id
                else:
                    self._parse_single_partition_id_for_multiple_columns(
                        partition_id, partition_columns, partition_values
                    )

            if partition_values:
                logger.info(
                    f"Successfully obtained partition values from INFORMATION_SCHEMA for table {table.name}: {dict(partition_values)}"
                )

            return {"partition_values": partition_values, "row_count": row_count}

        except Exception as e:
            logger.warning(f"Error getting partition info from INFORMATION_SCHEMA: {e}")
            return {"partition_values": {}, "row_count": None}

    def _get_partition_info_from_table_query(
        self,
        table,
        project,
        schema,
        partition_columns,
        execute_query_func,
        max_results=5,
    ):
        """Get partition information by querying the actual table using parameterized queries."""
        if not partition_columns:
            return {}

        result_values = {}
        safe_table_ref = build_safe_table_reference(project, schema, table.name)

        # Get column data types first for better ordering strategy
        column_types = self._get_partition_column_types(
            table, project, schema, partition_columns, execute_query_func
        )

        for col_name in partition_columns:
            try:
                # Use utility for validation
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                    logger.warning(f"Invalid column name: {col_name}")
                    continue

                # Use utility for query building with parameters
                col_data_type = column_types.get(col_name, "")
                query, job_config = self._create_partition_stats_query(
                    safe_table_ref, col_name, max_results, col_data_type
                )

                # Use utility for execution with context
                self._log_partition_attempt("table query", table.name, [col_name])
                partition_values_results = execute_query_func(
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
                if self._is_date_like_column(col_name):
                    chosen_result = partition_values_results[
                        0
                    ]  # Already sorted by date DESC
                    logger.info(
                        f"Found latest date for {col_name}: {chosen_result.val} with {chosen_result.record_count} records (queried table directly for maximum date)"
                    )
                else:
                    chosen_result = max(
                        partition_values_results, key=lambda r: r.record_count
                    )
                    logger.info(
                        f"Found most populated partition for {col_name}: {chosen_result.val} with {chosen_result.record_count} records (queried table directly for partition statistics)"
                    )

                result_values[col_name] = chosen_result.val
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

    def _create_partition_stats_query(
        self, table_ref: str, col_name: str, max_results: int = 10, data_type: str = ""
    ) -> Tuple[str, QueryJobConfig]:
        """
        Create a standardized partition statistics query with parameterized limit.
        """
        order_by = self._get_column_ordering_strategy(col_name, data_type)
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

    def _get_column_ordering_strategy(self, col_name: str, data_type: str = "") -> str:
        """
        Get the appropriate ORDER BY strategy for a column based on its name and data type.
        """
        if self._is_date_like_column(col_name) or self._is_date_type_column(data_type):
            return f"`{col_name}` DESC"  # Most recent first for time-based columns
        else:
            return "record_count DESC"  # Most populated first for other columns

    def _is_date_like_column(self, col_name: str) -> bool:
        """
        Check if a column name suggests it contains date/time data.
        Note: 'day' is excluded as it typically refers to day number (1-31) in partition contexts.
        """
        return col_name.lower() in {
            "date",
            "dt",
            "partition_date",
            "date_partition",
            "event_date",
            "created_date",
            "updated_date",
            "timestamp",
            "datetime",
            "time",
            "created_at",
            "modified_at",
            "updated_at",
            "event_time",
        }

    def _is_date_type_column(self, data_type: str) -> bool:
        """
        Check if a column's data type is a date/time type in BigQuery.
        """
        if not data_type:
            return False

        data_type_upper = data_type.upper()
        return data_type_upper in {
            "DATE",
            "DATETIME",
            "TIMESTAMP",
            "TIME",
        }

    def _log_partition_attempt(
        self,
        method: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        success: Optional[bool] = None,
    ) -> None:
        """
        Standardized logging for partition discovery attempts.
        """
        col_info = f" for columns {columns}" if columns else ""
        if success is None:
            logger.debug(f"Attempting {method} for table {table_name}{col_info}")
        elif success:
            logger.debug(f"{method} succeeded for table {table_name}{col_info}")
        else:
            logger.debug(f"{method} failed for table {table_name}{col_info}")

    def _parse_single_partition_id_for_multiple_columns(
        self,
        partition_id: str,
        partition_columns: List[str],
        result_values: Dict[str, Union[str, int, float]],
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

    def _extract_simple_column_names(self, partition_clause: str) -> List[str]:
        """Extract simple comma-separated column names from partition clause."""
        # Pattern for simple column names (col1, col2, col3)
        simple_pattern = r"^([a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*)$"
        simple_match = re.match(simple_pattern, partition_clause.strip())

        if simple_match:
            # Direct column names separated by commas
            columns = [col.strip() for col in simple_match.group(1).split(",")]
            return [col for col in columns if col]

        return []

    def _extract_function_based_column_names(self, partition_clause: str) -> List[str]:
        """Extract column names from function-based partitioning patterns."""
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

    def _extract_mixed_column_names(self, partition_clause: str) -> List[str]:
        """Extract column names from mixed scenarios (comma-separated parts with functions)."""
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

    def _get_function_patterns(self) -> List[str]:
        """Get regex patterns for extracting column names from function-based partitioning."""
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

    def _split_partition_clause_respecting_parentheses(
        self, partition_clause: str
    ) -> List[str]:
        """Split partition clause by commas while respecting parentheses."""
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

    def _remove_duplicate_columns(self, column_names: List[str]) -> List[str]:
        """Remove duplicate column names while preserving order."""
        seen = set()
        unique_columns = []

        for col in column_names:
            col_upper = col.upper()
            if col_upper not in seen:
                seen.add(col_upper)
                unique_columns.append(col_upper)

        return unique_columns

    def _get_partition_columns_from_table_info(self, table: BigqueryTable) -> set:
        """Extract required partition columns from table partition_info."""
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
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> set:
        """Extract partition column names from INFORMATION_SCHEMA using parameterized queries."""
        required_partition_columns = set()

        try:
            safe_info_schema_ref = build_safe_table_reference(
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
            query_results = execute_query_func(
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
                safe_table_ref = build_safe_table_reference(project, schema, table.name)

                test_query = (
                    f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                )
                job_config = QueryJobConfig(
                    query_parameters=[ScalarQueryParameter("limit_rows", "INT64", 1)]
                )
                execute_query_func(test_query, job_config, "partition error detection")
            except Exception as e:
                error_info = self._extract_partition_info_from_error(str(e))
                required_partition_columns = set(error_info.get("required_columns", []))

        return required_partition_columns

    def _get_partitions_with_sampling(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """
        Get partition filters using sampling to avoid full table scans.
        """
        try:
            # First get partition columns
            partition_cols_with_types = self.get_partition_columns_from_info_schema(
                table, project, schema, execute_query_func
            )

            if not partition_cols_with_types:
                partition_cols_with_types = self.get_partition_columns_from_ddl(
                    table, project, schema, execute_query_func
                )

            if not partition_cols_with_types:
                return None

            # For date-type columns, prioritize recent data instead of random sampling
            date_columns = [
                col
                for col, data_type in partition_cols_with_types.items()
                if self._is_date_like_column(col)
                or self._is_date_type_column(data_type)
            ]

            if date_columns:
                logger.debug(
                    f"Using recent-data sampling (ORDER BY {date_columns[0]} DESC) to find partition values for {len(partition_cols_with_types)} columns"
                )
            else:
                logger.debug(
                    f"Using random sampling (TABLESAMPLE) to find partition values for {len(partition_cols_with_types)} columns"
                )

            # Build safe table reference
            safe_table_ref = build_safe_table_reference(project, schema, table.name)

            if date_columns:
                # Use ORDER BY to get the most recent date only (1 row for efficiency)
                primary_date_col = date_columns[0]  # Use first date column for ordering
                sample_query = f"""SELECT *
FROM {safe_table_ref}
WHERE `{primary_date_col}` IS NOT NULL
ORDER BY `{primary_date_col}` DESC
LIMIT @limit_rows"""

                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter("limit_rows", "INT64", 1),
                    ]
                )
            else:
                # Fall back to random sampling for non-date partitioned tables
                sample_query = f"""SELECT *
FROM {safe_table_ref} TABLESAMPLE SYSTEM (@sample_percent PERCENT)
LIMIT @limit_rows"""

                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter("sample_percent", "FLOAT64", 0.001),
                        ScalarQueryParameter("limit_rows", "INT64", 5),
                    ]
                )

            partition_sample_rows = execute_query_func(
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
                        filter_str = self._create_partition_filter_from_value(
                            col_name, val, data_type
                        )
                        filters.append(filter_str)
                        logger.debug(
                            f"Found partition value from sample: {col_name}={val}"
                        )
                        break

            # Verify the filters return data
            if filters and self._verify_partition_has_data(
                table, project, schema, filters, execute_query_func
            ):
                # Extract partition values for logging
                partition_values_for_log = {}
                for col_name, _data_type in partition_cols_with_types.items():
                    for row in partition_sample_rows:
                        if (
                            hasattr(row, col_name)
                            and getattr(row, col_name) is not None
                        ):
                            partition_values_for_log[col_name] = getattr(row, col_name)
                            break

                logger.info(
                    f"Successfully obtained partition values from sampling for table {table.name}: {partition_values_for_log}"
                )
                logger.debug(f"Generated partition filters from sample: {filters}")
                return filters

            return None

        except Exception as e:
            logger.warning(f"Error getting partition filters with sampling: {e}")
            return None

    def _create_partition_filter_from_value(
        self, col_name: str, val: Union[str, int, float], data_type: str
    ) -> str:
        """
        Create a safe partition filter string for a column value with upstream validation.
        """
        return self._create_safe_filter(col_name, val)

    def _create_safe_filter(
        self, col_name: str, val: Union[str, int, float], col_type: Optional[str] = None
    ) -> str:
        """
        Create a safe partition filter with upstream validation of inputs.

        This ensures we only create filters with safe, validated inputs before
        they reach the downstream validation. Always uses string quoting for all values
        to rely on BigQuery's implicit type casting, which handles most type
        conversions automatically and avoids type mismatch errors between
        schema-declared types and actual stored values.
        """
        # Validate column name
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
            raise ValueError(f"Invalid column name for filter: {col_name}")

        # Convert value to string for consistent handling
        str_val = str(val)

        # Check for SQL injection patterns
        if any(pattern in str_val for pattern in [";", "--", "/*", "\\"]):
            raise ValueError(f"Invalid value for filter: {val}")

        # Always quote values to avoid type mismatch issues
        # BigQuery's implicit casting handles STRING -> INT64, STRING -> DATE, etc.
        # This is safer than trying to guess the correct format based on schema types
        if "'" in str_val:
            escaped_val = str_val.replace("'", "''")
            return f"`{col_name}` = '{escaped_val}'"
        else:
            return f"`{col_name}` = '{str_val}'"

    def _verify_partition_has_data(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> bool:
        """Verify that the partition filters actually return data."""
        if not filters:
            return False

        # Use utility for filter validation
        validated_filters = []
        for filter_str in filters:
            if validate_filter_expression(filter_str):
                validated_filters.append(filter_str)

        if not validated_filters:
            return False

        safe_table_ref = build_safe_table_reference(project, schema, table.name)
        where_clause = " AND ".join(validated_filters)

        try:
            # Use existence check for maximum efficiency - just check if ANY row exists
            query = f"""SELECT 1 as exists_check
FROM {safe_table_ref}
WHERE {where_clause}
LIMIT 1"""

            job_config = QueryJobConfig()

            count_verification_results = execute_query_func(
                query, job_config, "partition verification"
            )

            if count_verification_results and len(count_verification_results) > 0:
                logger.debug(
                    f"Partition verification successful for table {project}.{schema}.{table.name}: {where_clause}"
                )
                return True
            else:
                logger.debug(
                    f"Partition verification found no data for table {project}.{schema}.{table.name}: {where_clause} (trying other partition values...)"
                )
                return False
        except Exception as e:
            logger.warning(
                f"Error verifying partition data for table {project}.{schema}.{table.name}: {e}"
            )
            return False

    def _handle_external_table_partitioning(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        current_time: datetime,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """Handle partitioning for external tables."""
        if table.external:
            logger.debug(f"Processing external table partitioning for {table.name}")
            return self._get_external_table_partition_filters(
                table, project, schema, current_time, execute_query_func
            )
        return None

    def _get_external_table_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        current_time: datetime,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """Get partition filters specifically for external tables."""
        try:
            # Try sampling approach first - most efficient
            sample_filters = self._get_partitions_with_sampling(
                table, project, schema, execute_query_func
            )
            if sample_filters:
                return sample_filters

            # Step 1: Get partition columns from INFORMATION_SCHEMA
            partition_cols_with_types = self.get_partition_columns_from_info_schema(
                table, project, schema, execute_query_func
            )

            # Step 2: If no columns found, try extracting from DDL
            if not partition_cols_with_types:
                partition_cols_with_types = self.get_partition_columns_from_ddl(
                    table, project, schema, execute_query_func
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
                table, project, schema, partition_cols_with_types, execute_query_func
            )

        except Exception as e:
            logger.error(f"Error checking external table partitioning: {e}")
            return None

    def _find_valid_partition_combination(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """Find a valid combination of partition filters that returns data."""
        logger.debug(
            f"Searching for valid partition combination for {table.name} with columns: {list(partition_cols_with_types.keys())}"
        )

        # For now, return a simple fallback - in practice you'd implement the full logic
        # This would include trying different date combinations, sampling, etc.
        return self._get_fallback_partition_filters(
            table, project, schema, list(partition_cols_with_types.keys())
        )

    def _find_real_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """Find real partition values that exist in the table."""
        if not required_columns:
            return []

        logger.debug(f"Determining partition values for columns: {required_columns}")

        # Get column data types to better identify date columns
        column_types = self._get_partition_column_types(
            table, project, schema, required_columns, execute_query_func
        )

        # Strategy 1: For tables with date-like columns, date types, or date component columns,
        # use the same approach as external tables - query directly for actual max dates
        has_date_columns = any(
            self._is_date_like_column(col) for col in required_columns
        )
        has_date_types = any(
            self._is_date_type_column(column_types.get(col, ""))
            for col in required_columns
        )
        has_date_components = any(
            col.lower() in ["year", "month", "day"] for col in required_columns
        )

        if has_date_columns or has_date_types or has_date_components:
            # For date/timestamp partitioned tables, use direct table query like external tables
            # This finds the actual latest dates instead of just checking today/yesterday
            logger.info(
                f"Table {table.name} has date/timestamp partition columns. Using direct table query to find actual latest dates (same as external tables)"
            )
            actual_partition_values = self._get_partition_info_from_table_query(
                table, project, schema, required_columns, execute_query_func
            )

            if actual_partition_values:
                # Convert the result values to filter strings using safe filter creation
                actual_filters = []
                for col, val in actual_partition_values.items():
                    try:
                        filter_expr = self._create_safe_filter(col, val)
                        actual_filters.append(filter_expr)
                    except ValueError as e:
                        logger.warning(f"Skipping invalid filter for {col}={val}: {e}")
                        continue

                logger.info(
                    f"Successfully found actual partition values for table {table.name}: {dict(actual_partition_values)}"
                )
                logger.info(
                    f"Generated partition filters from direct table analysis: {actual_filters}"
                )
                return actual_filters
            else:
                logger.debug(
                    f"Direct table query failed for {table.name}, falling back to strategic dates"
                )

        # Fallback: Try strategic dates for non-date columns or when direct query fails
        # This is mainly for backward compatibility with tables that don't have date/timestamp columns
        logger.debug(
            "Falling back to strategic date candidates for non-date columns or when direct query failed"
        )
        candidate_dates = self._get_strategic_candidate_dates()

        for test_date, description in candidate_dates:
            filters = []
            for col in required_columns:
                try:
                    col_data_type = column_types.get(col, "")

                    if self._is_date_like_column(col) or self._is_date_type_column(
                        col_data_type
                    ):
                        date_str = test_date.strftime("%Y-%m-%d")
                        filters.append(self._create_safe_filter(col, date_str))
                    elif col.lower() == "year":
                        filters.append(
                            self._create_safe_filter(col, str(test_date.year))
                        )
                    elif col.lower() == "month":
                        filters.append(
                            self._create_safe_filter(col, f"{test_date.month:02d}")
                        )
                    elif col.lower() == "day":
                        filters.append(
                            self._create_safe_filter(col, f"{test_date.day:02d}")
                        )
                    else:
                        # For other non-date columns, use fallback values from config
                        if col in self.config.profiling.fallback_partition_values:
                            fallback_val = (
                                self.config.profiling.fallback_partition_values[col]
                            )
                            filters.append(self._create_safe_filter(col, fallback_val))
                        else:
                            # IS NOT NULL is safe and doesn't need the helper
                            filters.append(f"`{col}` IS NOT NULL")
                except ValueError as e:
                    logger.warning(f"Skipping invalid filter for column {col}: {e}")
                    # Use IS NOT NULL as fallback for problematic columns
                    filters.append(f"`{col}` IS NOT NULL")

            # Verify these filters work
            if self._verify_partition_has_data(
                table, project, schema, filters, execute_query_func
            ):
                logger.debug(
                    f"Found valid date partition for {description}: {test_date.strftime('%Y-%m-%d')}"
                )

                # Now discover actual values for other partition columns using the valid date
                enhanced_filters = self._enhance_partition_filters_with_actual_values(
                    table,
                    project,
                    schema,
                    required_columns,
                    filters,
                    execute_query_func,
                )

                if enhanced_filters:
                    logger.info(
                        f"Enhanced partition filters with actual values for table {table.name}: {len(enhanced_filters)} filters"
                    )
                    return enhanced_filters
                else:
                    logger.debug(
                        "Failed to enhance filters, using original date-based filters"
                    )
                    return filters
            else:
                logger.debug(
                    f"No data found for {description} ({test_date.strftime('%Y-%m-%d')}), trying next candidate..."
                )

        logger.debug(
            f"No data found in any strategic date candidates for table {table.name}"
        )

        # Final fallback: Query the table directly to find actual partition values
        # This is for tables that don't have date/timestamp columns or when all other methods failed
        logger.info(
            f"All strategic approaches failed for table {table.name}. Querying table directly as final fallback"
        )
        actual_partition_values = self._get_partition_info_from_table_query(
            table, project, schema, required_columns, execute_query_func
        )

        if actual_partition_values:
            # Convert the result values to filter strings using safe filter creation
            actual_filters = []
            for col, val in actual_partition_values.items():
                try:
                    filter_expr = self._create_safe_filter(col, val)
                    actual_filters.append(filter_expr)
                except ValueError as e:
                    logger.warning(f"Skipping invalid filter for {col}={val}: {e}")
                    continue

            logger.info(
                f"Successfully found actual partition values for table {table.name}: {dict(actual_partition_values)}"
            )
            logger.info(
                f"Generated partition filters from direct table analysis: {actual_filters}"
            )
            return actual_filters

        # Last resort: Use configured fallback values if direct table query also failed
        return self._get_fallback_partition_filters(
            table, project, schema, required_columns
        )

    def _get_fallback_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
    ) -> List[str]:
        """
        Generate fallback partition filters based on configuration when regular methods time out.
        """
        logger.debug(f"Using fallback partition values for {table.name}")

        fallback_date = datetime.now(timezone.utc) - timedelta(days=1)
        logger.debug(
            f"Configured fallback values: {self.config.profiling.fallback_partition_values}"
        )

        # Generate filters for each required column
        fallback_filters = []
        for col_name in required_columns:
            filter_str = self._create_fallback_filter_for_column(
                col_name, fallback_date
            )
            if filter_str:
                fallback_filters.append(filter_str)

        logger.debug(f"Generated fallback partition filters: {fallback_filters}")
        return fallback_filters

    def _create_fallback_filter_for_column(
        self, col_name: str, fallback_date: datetime
    ) -> str:
        """Create a fallback filter for a specific column using safe filter creation."""
        # Check for explicit fallback value in config
        if col_name in self.config.profiling.fallback_partition_values:
            fallback_value = self.config.profiling.fallback_partition_values[col_name]
            try:
                return self._create_safe_filter(col_name, fallback_value)
            except ValueError as e:
                logger.warning(f"Invalid fallback value for {col_name}: {e}")
                return f"`{col_name}` IS NOT NULL"

        # Use date-based fallbacks for date-like columns
        try:
            if self._is_date_like_column(col_name):
                # For date columns, if specific dates don't work, use IS NOT NULL as last resort
                logger.warning(
                    f"Specific date values failed for column {col_name}, using IS NOT NULL for broader partition coverage"
                )
                return f"`{col_name}` IS NOT NULL"
            elif col_name.lower() == "year":
                return self._create_safe_filter(col_name, str(fallback_date.year))
            elif col_name.lower() == "month":
                return self._create_safe_filter(col_name, f"{fallback_date.month:02d}")
            elif col_name.lower() == "day":
                return self._create_safe_filter(col_name, f"{fallback_date.day:02d}")
            else:
                # Last resort - use IS NOT NULL
                logger.warning(
                    f"No fallback value for partition column {col_name}, using IS NOT NULL"
                )
                return f"`{col_name}` IS NOT NULL"
        except ValueError as e:
            logger.warning(f"Error creating fallback filter for {col_name}: {e}")
            return f"`{col_name}` IS NOT NULL"

    def _get_partition_filters_from_information_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """
        Get comprehensive partition filters using INFORMATION_SCHEMA.PARTITIONS.

        This is the optimal approach for internal BigQuery tables as it:
        - Uses pure metadata queries (no data scanning)
        - Gets comprehensive partition coverage
        - Applies date windowing efficiently
        - Returns multiple recent partitions for better profiling coverage

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            required_columns: Partition columns that need values
            execute_query_func: Function to execute queries safely

        Returns:
            List of partition filter strings, or None if method fails
        """
        if not required_columns:
            return []

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.PARTITIONS"
            )

            # Build comprehensive query with date windowing
            query_parts = [
                "SELECT partition_id, last_modified_time, total_rows",
                f"FROM {safe_info_schema_ref}",
                "WHERE table_name = @table_name",
                "AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__', '__STREAMING_UNPARTITIONED__')",
                "AND total_rows > 0",
            ]

            # For partition discovery, we want to find the actual latest partitions
            # Don't apply restrictive windowing here - the profiling stage will apply partition_datetime_window_days
            # This ensures we find tables with latest data even if it's outside the configured window
            logger.debug(
                f"Discovering partitions for {table.name} without restrictive windowing to find actual latest data. "
                f"Date windowing (partition_datetime_window_days) will be applied during profiling."
            )

            # Order by recency and limit results
            query_parts.extend(
                ["ORDER BY last_modified_time DESC", "LIMIT @max_partitions"]
            )

            query = " ".join(query_parts)

            # Build query parameters
            parameters = [
                ScalarQueryParameter("table_name", "STRING", table.name),
                ScalarQueryParameter(
                    "max_partitions", "INT64", 10
                ),  # Get multiple partitions
            ]

            # No windowing parameters needed - we want to discover actual latest partitions

            job_config = QueryJobConfig(query_parameters=parameters)

            logger.debug(
                "Querying INFORMATION_SCHEMA.PARTITIONS for comprehensive partition discovery"
            )
            partition_rows = execute_query_func(
                query,
                job_config,
                "comprehensive partition discovery from information schema",
            )

            if not partition_rows:
                logger.debug(
                    f"No partitions found in INFORMATION_SCHEMA for table {table.name}"
                )
                return None

            # Convert partition IDs to filters
            partition_filters = []

            for partition_row in partition_rows:
                partition_id = partition_row.partition_id

                try:
                    filters_for_partition = self._convert_partition_id_to_filters(
                        partition_id, required_columns
                    )

                    if filters_for_partition:
                        # Verify this partition has data (quick check)
                        if self._verify_partition_has_data(
                            table,
                            project,
                            schema,
                            filters_for_partition,
                            execute_query_func,
                        ):
                            partition_filters.extend(filters_for_partition)
                            logger.debug(
                                f"Added partition {partition_id} with {partition_row.total_rows} rows"
                            )
                            break  # Found one working partition, that's enough
                        else:
                            logger.debug(
                                f"Partition {partition_id} verification failed, trying next"
                            )

                except Exception as e:
                    logger.warning(f"Error processing partition {partition_id}: {e}")
                    continue

            if partition_filters:
                logger.info(
                    f"Successfully discovered {len(partition_filters)} partition filters from INFORMATION_SCHEMA for {table.name}"
                )
                return partition_filters
            else:
                logger.debug(
                    "No valid partition filters could be generated from INFORMATION_SCHEMA"
                )
                return None

        except Exception as e:
            logger.warning(f"Error in INFORMATION_SCHEMA partition discovery: {e}")
            return None

    def _convert_partition_id_to_filters(
        self, partition_id: str, required_columns: List[str]
    ) -> Optional[List[str]]:
        """
        Convert a partition_id from INFORMATION_SCHEMA.PARTITIONS to filter expressions.

        Handles various BigQuery partition formats:
        - Single column: "20231225" -> date_col = "2023-12-25"
        - Multi-column: "col1=val1$col2=val2" -> col1 = "val1" AND col2 = "val2"
        - Date formats: YYYYMMDD, YYYYMMDDHH, etc.
        """
        try:
            filters = []

            if "$" in partition_id:
                # Multi-column partitioning: col1=val1$col2=val2$col3=val3
                parts = partition_id.split("$")
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in required_columns:
                            filters.append(self._create_safe_filter(col, val))

            else:
                # Single column partitioning - need to map to the right column
                if len(required_columns) == 1:
                    col_name = required_columns[0]

                    # Handle date partition formats
                    if len(partition_id) == 8 and partition_id.isdigit():
                        # YYYYMMDD format
                        date_str = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                        filters.append(self._create_safe_filter(col_name, date_str))
                    elif len(partition_id) == 10 and partition_id.isdigit():
                        # YYYYMMDDHH format
                        date_str = f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:8]}"
                        filters.append(self._create_safe_filter(col_name, date_str))
                    else:
                        # Use partition_id as-is
                        filters.append(self._create_safe_filter(col_name, partition_id))
                else:
                    # Multiple columns but single partition_id - this is complex
                    # Try to parse based on common patterns or fall back
                    logger.debug(
                        f"Complex partition mapping for {partition_id} with {len(required_columns)} columns"
                    )
                    return None

            return filters if filters else None

        except Exception as e:
            logger.warning(
                f"Error converting partition_id {partition_id} to filters: {e}"
            )
            return None

    def _enhance_partition_filters_with_actual_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        initial_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """
        Enhance partition filters by discovering actual values for non-date columns.

        After finding a valid date partition, query the table to find actual values
        for other partition columns instead of using fallback values.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            required_columns: All partition columns that need values
            initial_filters: Working filters with date values and fallbacks
            execute_query_func: Function to execute queries safely

        Returns:
            Enhanced filters with actual partition values, or None if enhancement fails
        """
        try:
            safe_table_ref = build_safe_table_reference(project, schema, table.name)

            # Extract date filters from initial filters to use as WHERE conditions
            where_conditions = []
            date_filters = []

            for filter_expr in initial_filters:
                # Look for date-like filters (not IS NOT NULL fallbacks)
                if "IS NOT NULL" not in filter_expr and (
                    "=" in filter_expr or "BETWEEN" in filter_expr
                ):
                    date_filters.append(filter_expr)
                    where_conditions.append(filter_expr)

            if not where_conditions:
                logger.debug(
                    "No date conditions found to enhance other partition values"
                )
                return None

            where_clause = " AND ".join(where_conditions)
            enhanced_filters = list(date_filters)  # Start with working date filters

            # Get column types to identify non-date columns that need actual values
            column_types = self._get_partition_column_types(
                table, project, schema, required_columns, execute_query_func
            )

            # For each non-date column, discover actual values
            for col_name in required_columns:
                col_data_type = column_types.get(col_name, "")

                # Skip if this is a date column (already handled)
                if self._is_date_like_column(col_name) or self._is_date_type_column(
                    col_data_type
                ):
                    continue

                # Skip if we already have a good filter for this column
                if any(f"`{col_name}` =" in f for f in date_filters):
                    continue

                try:
                    # Query for actual values of this column using the date filters
                    discover_query = f"""
SELECT DISTINCT `{col_name}` as col_value, COUNT(*) as row_count
FROM {safe_table_ref}
WHERE {where_clause} AND `{col_name}` IS NOT NULL
GROUP BY `{col_name}`
ORDER BY row_count DESC
LIMIT @max_values"""

                    job_config = QueryJobConfig(
                        query_parameters=[
                            ScalarQueryParameter("max_values", "INT64", 3),
                        ]
                    )

                    results = execute_query_func(
                        discover_query, job_config, f"discover values for {col_name}"
                    )

                    if results and results[0].col_value is not None:
                        # Use the most common value
                        actual_value = results[0].col_value
                        enhanced_filter = self._create_safe_filter(
                            col_name, actual_value, col_data_type
                        )
                        enhanced_filters.append(enhanced_filter)

                        logger.debug(
                            f"Found actual value for {col_name}: {actual_value} ({results[0].row_count} rows)"
                        )
                    else:
                        # Fall back to IS NOT NULL for this column
                        enhanced_filters.append(f"`{col_name}` IS NOT NULL")
                        logger.debug(
                            f"No actual values found for {col_name}, using IS NOT NULL"
                        )

                except Exception as e:
                    logger.warning(
                        f"Error discovering values for column {col_name}: {e}"
                    )
                    # Fall back to IS NOT NULL
                    enhanced_filters.append(f"`{col_name}` IS NOT NULL")

            return enhanced_filters

        except Exception as e:
            logger.warning(f"Error enhancing partition filters: {e}")
            return None

    def _get_strategic_candidate_dates(self) -> List[Tuple[datetime, str]]:
        """
        Get strategic candidate dates that are most likely to contain data in real-world BigQuery tables.
        Optimized to only check today and yesterday for cost efficiency.

        Returns:
            List of (datetime, description) tuples in priority order
        """
        now = datetime.now(timezone.utc)
        candidates = []

        # 1. Today (current date - most likely to have data)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        candidates.append((today, "today (current date)"))

        # 2. Yesterday (very common case)
        yesterday = now - timedelta(days=1)
        candidates.append((yesterday, "yesterday"))

        logger.debug(
            f"Generated {len(candidates)} strategic date candidates for partition discovery (optimized for cost)"
        )
        return candidates

    def _extract_partition_info_from_error(self, error_message: str) -> PartitionInfo:
        """Extract partition information from error messages."""
        result: PartitionInfo = {"required_columns": [], "partition_values": {}}

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
