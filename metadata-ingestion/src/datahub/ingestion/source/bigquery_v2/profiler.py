import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import QueryJobConfig

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


class BigqueryProfiler(GenericProfiler):
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
            logger.info(f"Using table query approach for external table {table.name}")
            return self._get_partition_info_from_table_query(
                table, project, schema, partition_columns, max_results
            )
        else:
            logger.info(
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
                logger.info(
                    f"INFORMATION_SCHEMA approach failed, falling back to table query for {table.name}"
                )
                result = self._get_partition_info_from_table_query(
                    table, project, schema, partition_columns, max_results
                )

            return result

    def _get_partition_column_types(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
    ) -> Dict[str, str]:
        """
        Get data types for partition columns.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_columns: List of partition column names

        Returns:
            Dictionary mapping column names to their data types
        """
        if not partition_columns:
            return {}

        try:
            columns_filter = "', '".join(partition_columns)
            query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' 
AND column_name IN ('{columns_filter}')"""

            query_job = self.config.get_bigquery_client().query(query)
            query_results = list(query_job.result())

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
            query = f"""SELECT partition_id, total_rows
FROM `{project}.{schema}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = '{table.name}' 
AND partition_id != '__NULL__'
AND partition_id != '__UNPARTITIONED__'
AND total_rows > 0
ORDER BY total_rows DESC
LIMIT {max_results}"""

            query_job = self.config.get_bigquery_client().query(query)
            query_results = list(query_job.result())

            if not query_results:
                logger.warning(
                    f"No partitions found in INFORMATION_SCHEMA for table {table.name}"
                )
                return {}

            # Take the partition with the most rows
            best_partition = query_results[0]
            partition_id = best_partition.partition_id

            logger.info(
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
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        max_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Get partition information by querying the actual table.
        This is used for external tables where INFORMATION_SCHEMA.PARTITIONS is empty.

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

        result_values = {}

        # For external tables, we need to query the actual table data
        # Try each column individually to avoid partition filter issues
        for col_name in partition_columns:
            try:
                # Special handling for date-named columns
                is_date_column = col_name.lower() in {
                    "date",
                    "day",
                    "dt",
                    "partition_date",
                    "date_partition",
                }
                is_time_column = col_name.lower() in {
                    "timestamp",
                    "datetime",
                    "time",
                    "created_at",
                    "modified_at",
                    "event_time",
                }
                is_month_column = col_name.lower() in {
                    "month",
                    "partition_month",
                    "month_partition",
                }
                is_year_column = col_name.lower() in {
                    "year",
                    "partition_year",
                    "year_partition",
                }

                # For date-related columns, order by the column itself to get most recent
                # For other columns, order by record count to get most populated
                if (
                    is_date_column
                    or is_time_column
                    or is_month_column
                    or is_year_column
                ):
                    order_by = f"{col_name} DESC"  # Most recent date first
                else:
                    order_by = "record_count DESC"  # Most records first

                # Use a limited query to avoid scanning too much data
                query = f"""WITH PartitionStats AS (
    SELECT {col_name} as val, COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {order_by}
    LIMIT {max_results}
)
SELECT val, record_count FROM PartitionStats"""

                logger.debug(
                    f"Executing table query for partition column {col_name}: {query}"
                )
                query_job = self.config.get_bigquery_client().query(query)
                query_results = list(query_job.result())

                if not query_results or query_results[0].val is None:
                    logger.warning(
                        f"No non-empty partition values found for column {col_name}"
                    )
                    continue

                # For time-based columns, prefer the most recent with data
                # For other columns, use the one with the most data
                if (
                    is_date_column
                    or is_time_column
                    or is_month_column
                    or is_year_column
                ):
                    chosen_result = query_results[0]  # Already sorted by date DESC
                else:
                    # Find the result with the most records
                    chosen_result = max(query_results, key=lambda r: r.record_count)

                result_values[col_name] = chosen_result.val
                logger.info(
                    f"Selected partition {col_name}={chosen_result.val} with {chosen_result.record_count} records"
                )

            except Exception as e:
                logger.error(f"Error getting partition value for {col_name}: {e}")
                continue

        return result_values

    def _get_partition_columns_from_info_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Dict[str, str]:
        """
        Get partition columns from INFORMATION_SCHEMA.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name

        Returns:
            Dictionary mapping column names to data types
        """
        try:
            query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""
            query_job = self.config.get_bigquery_client().query(query)
            query_results_3 = list(query_job.result())

            partition_columns = [row.column_name for row in query_results_3]

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
                logger.info(
                    f"No partition columns found for external table {table.name}"
                )
                return []

            logger.info(
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

    def _create_partition_filter_from_value(
        self, col_name: str, val: Any, data_type: str
    ) -> str:
        """
        Create a filter string for a partition column with a specific value.

        Args:
            col_name: Column name
            val: Value for the filter
            data_type: Data type of the column

        Returns:
            Filter string in the format `column` = value
        """
        data_type_upper = data_type.upper() if data_type else ""

        if data_type_upper in ("STRING", "VARCHAR"):
            return f"`{col_name}` = '{val}'"
        elif data_type_upper == "DATE":
            if isinstance(val, datetime):
                return f"`{col_name}` = DATE '{val.strftime('%Y-%m-%d')}'"
            return f"`{col_name}` = DATE '{val}'"
        elif data_type_upper in ("TIMESTAMP", "DATETIME"):
            if isinstance(val, datetime):
                return f"`{col_name}` = TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            return f"`{col_name}` = TIMESTAMP '{val}'"
        else:
            # Default to numeric or other type
            return f"`{col_name}` = {val}"

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
            logger.info(f"Found date columns to try first: {date_columns}")
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
                logger.info(f"Found valid fallback filters: {fallback_filters}")
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
        """Try fallback approach specifically for date-type columns."""
        for col_name in date_columns:
            data_type = partition_cols_with_types.get(col_name, "DATE")
            logger.info(
                f"Trying fallback approach for date column {col_name} with type {data_type}"
            )

            try:
                # Simple approach - try the current date, then 1 day ago, then 2 days ago, etc.
                current_date = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

                for days_ago in [0, 1, 7, 30, 90, 365]:
                    test_date = current_date - timedelta(days=days_ago)
                    filter_str = self._create_partition_filter_from_value(
                        col_name, test_date, data_type
                    )

                    logger.info(
                        f"Testing date filter for {days_ago} days ago: {filter_str}"
                    )
                    # Test this filter
                    if self._verify_partition_has_data(
                        table, project, schema, [filter_str]
                    ):
                        logger.info(f"Found working date filter: {filter_str}")
                        return [filter_str]
            except Exception as e:
                logger.error(
                    f"Error in date fallback for column {col_name}: {e}", exc_info=True
                )

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
                logger.info(
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
        for order_by in ["DESC", "ASC", "record_count DESC"]:
            query = f"""WITH PartitionStats AS (
    SELECT {col_name} as val, COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {col_name} {order_by}
    LIMIT 10
)
SELECT val, record_count FROM PartitionStats"""

            logger.info(f"Executing fallback query with {order_by} ordering")
            try:
                query_job = self.config.get_bigquery_client().query(query)
                query_results_7: List[Any] = list(query_job.result())

                if query_results_7:
                    logger.info(
                        f"Query returned {len(query_results_7)} potential values"
                    )

                    for result in query_results_7:
                        val = result.val
                        if val is not None:
                            filter_str = self._create_partition_filter_from_value(
                                col_name, val, data_type
                            )

                            logger.info(f"Testing filter: {filter_str}")
                            # Test each filter individually
                            if self._verify_partition_has_data(
                                table, project, schema, [filter_str]
                            ):
                                logger.info(f"Found working filter: {filter_str}")
                                return filter_str
                else:
                    logger.info(f"No results for {order_by} ordering")
            except Exception as query_e:
                logger.warning(
                    f"Error executing query with {order_by} ordering: {query_e}"
                )
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
        if self._try_get_most_recent_partition(
            table, project, schema, partition_cols_with_types
        ):
            return self._try_get_most_recent_partition(
                table, project, schema, partition_cols_with_types
            )

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
        logger.info("Trying simplified approach to find most recent partition")
        try:
            fallback_filters = []
            # This query just gets the most recent data for any partition column
            query = f"""SELECT *
FROM `{project}.{schema}.{table.name}`
ORDER BY {list(partition_cols_with_types.keys())[0]} DESC
LIMIT 1"""

            query_job = self.config.get_bigquery_client().query(query)
            query_results_8: List[Any] = list(query_job.result())

            if query_results_8 and len(query_results_8) > 0:
                for col_name in partition_cols_with_types:
                    val = getattr(query_results_8[0], col_name, None)
                    if val is not None:
                        data_type = partition_cols_with_types.get(col_name, "STRING")
                        filter_str = self._create_partition_filter_from_value(
                            col_name, val, data_type
                        )
                        fallback_filters.append(filter_str)
                        logger.info(
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
                query = f"""SELECT {col_name} as val, COUNT(*) as cnt
FROM `{project}.{schema}.{table.name}`
WHERE {col_name} IS NOT NULL
GROUP BY {col_name}
ORDER BY cnt DESC
LIMIT 1"""

                try:
                    query_job = self.config.get_bigquery_client().query(query)
                    query_results_9: List[Any] = list(query_job.result())

                    if (
                        query_results_9
                        and query_results_9[0].val is not None
                        and query_results_9[0].cnt > best_count
                    ):
                        best_col = col_name
                        best_val = query_results_9[0].val
                        best_count = query_results_9[0].cnt
                except Exception:
                    continue

            if best_col and best_val:
                data_type = partition_cols_with_types.get(best_col, "STRING")
                filter_str = self._create_partition_filter_from_value(
                    best_col, best_val, data_type
                )
                logger.info(f"Last resort filter: {filter_str} with {best_count} rows")
                return [filter_str]
        except Exception as e:
            logger.error(f"Error in last resort approach: {e}", exc_info=True)

        logger.warning("All fallback approaches failed to find valid partition values")
        return None

    def _verify_partition_has_data(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
    ) -> bool:
        """
        Verify that the partition filters actually return data.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            filters: List of partition filter strings

        Returns:
            True if data exists, False otherwise
        """
        if not filters:
            return False

        # Build WHERE clause from filters
        where_clause = " AND ".join(filters)

        # Run a simple count query to check if data exists
        query = f"""SELECT COUNT(*) as cnt
FROM `{project}.{schema}.{table.name}`
WHERE {where_clause}
LIMIT 1000"""  # Limit to avoid expensive full table scans

        try:
            logger.debug(f"Verifying partition data with query: {query}")
            query_job = self.config.get_bigquery_client().query(query)
            query_results_10: List[Any] = list(query_job.result())

            if query_results_10 and query_results_10[0].cnt > 0:
                logger.info(
                    f"Verified partition filters return {query_results_10[0].cnt} rows: {where_clause}"
                )
                return True
            else:
                logger.warning(f"Partition verification found no data: {where_clause}")
                return False
        except Exception as e:
            logger.warning(f"Error verifying partition data: {e}", exc_info=True)

            # Try with a simpler query as fallback
            try:
                simpler_query = f"""SELECT 1 
FROM `{project}.{schema}.{table.name}`
WHERE {where_clause}
LIMIT 1"""
                query_job = self.config.get_bigquery_client().query(simpler_query)
                query_results_11: List[Any] = list(query_job.result())

                return len(query_results_11) > 0
            except Exception as simple_e:
                logger.warning(f"Simple verification also failed: {simple_e}")
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

            logger.info(
                f"Using sampling to find partition values for {len(partition_cols_with_types)} columns"
            )

            # Use TABLESAMPLE to get a small sample of data
            sample_query = f"""SELECT *
FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (1 PERCENT)
LIMIT 100"""

            query_job = self.config.get_bigquery_client().query(sample_query)
            query_results_12: List[Any] = list(query_job.result())

            if not query_results_12:
                logger.info("Sample query returned no results")
                return None

            # Extract values for partition columns
            filters = []
            for col_name, data_type in partition_cols_with_types.items():
                for row in query_results_12:
                    if hasattr(row, col_name) and getattr(row, col_name) is not None:
                        val = getattr(row, col_name)
                        filter_str = self._create_partition_filter_from_value(
                            col_name, val, data_type
                        )
                        filters.append(filter_str)
                        logger.info(
                            f"Found partition value from sample: {col_name}={val}"
                        )
                        break

            # Verify the filters return data
            if filters and self._verify_partition_has_data(
                table, project, schema, filters
            ):
                logger.info(
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
        logger.info(
            f"Searching for valid partition combination for {table.name} with columns: {list(partition_cols_with_types.keys())}"
        )

        partition_cols = list(partition_cols_with_types.keys())

        try:
            # Approach 1: Try to get combinations that work together
            if len(partition_cols) > 1:
                logger.info(
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
                        filter_str = self._create_partition_filter_from_value(
                            col_name, value, data_type
                        )
                        combined_filters.append(filter_str)

                    # Verify the filters return data
                    if self._verify_partition_has_data(
                        table, project, schema, combined_filters
                    ):
                        return combined_filters

                logger.info(
                    "Combined partition approach didn't yield results, trying individual columns"
                )

            # Approach 2: Try each partition column individually
            individual_filters = []
            for col_name, data_type in partition_cols_with_types.items():
                logger.info(
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
                        filter_str = self._create_partition_filter_from_value(
                            col_name, partition_value, data_type
                        )
                        logger.info(f"Found filter for column {col_name}: {filter_str}")
                        individual_filters.append(filter_str)
                except Exception as col_e:
                    logger.error(
                        f"Error processing column {col_name}: {col_e}", exc_info=True
                    )
                    continue

            # Verify the individual filters
            if individual_filters:
                logger.info(f"Verifying {len(individual_filters)} individual filters")
                if self._verify_partition_has_data(
                    table, project, schema, individual_filters
                ):
                    logger.info(f"Found valid individual filters: {individual_filters}")
                    return individual_filters
                logger.info("Individual filters verification failed")

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
                # Run a simple query to trigger partition error
                test_query = f"""SELECT COUNT(*) FROM `{project}.{schema}.{table.name}` LIMIT 1"""
                self.execute_query(test_query)

                # If the query succeeds, table is not partitioned
                logger.debug(f"Table {table.name} is not partitioned")
                return []

            except Exception as e:
                # Extract partition requirements from error message
                error_info = self._extract_partition_info_from_error(str(e))
                required_partition_columns = set(error_info.get("required_columns", []))

                if required_partition_columns:
                    logger.info(
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
            logger.info(f"Found valid partition filters: {partition_filters}")
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

        logger.info(f"Determining partition values for columns: {required_columns}")

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
                        filter_str = self._create_partition_filter_from_value(
                            col_name, value, data_type
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
                    filter_str = self._create_partition_filter_from_value(
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
        logger.info(
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
                    logger.info(f"Fallback filters successful: {fallback_filters}")
                    return fallback_filters
                else:
                    logger.warning("Fallback filters generated but verification failed")

        except Exception as fallback_e:
            logger.warning(f"Fallback approach also failed: {fallback_e}")

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
        Extract partition column names from INFORMATION_SCHEMA.

        Args:
            table: BigqueryTable instance
            project: The BigQuery project ID
            schema: The dataset/schema name

        Returns:
            Set of partition column names, empty set if error occurs
        """
        required_partition_columns = set()

        try:
            query = f"""SELECT column_name
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""

            # Use the configured timeout for partition operations
            query_job = self.config.get_bigquery_client().query(query)
            query_results = list(
                query_job.result(timeout=self.config.profiling.partition_fetch_timeout)
            )
            required_partition_columns = {row.column_name for row in query_results}
            logger.debug(
                f"Found partition columns from schema: {required_partition_columns}"
            )
        except Exception as e:
            logger.warning(f"Error querying partition columns: {e}")
            # If we can't determine the partition columns due to timeout, try to extract from an error
            try:
                test_query = f"""SELECT COUNT(*) FROM `{project}.{schema}.{table.name}` LIMIT 1"""
                self.config.get_bigquery_client().query(test_query).result(
                    timeout=self.config.profiling.partition_fetch_timeout
                )
            except Exception as test_e:
                error_info = self._extract_partition_info_from_error(str(test_e))
                required_partition_columns = set(error_info.get("required_columns", []))

        return required_partition_columns

    def _process_time_based_columns(
        self,
        time_based_columns: set,
        current_time: datetime,
        column_data_types: Dict[str, str],
    ) -> List[str]:
        """
        Process time-based partition columns (year, month, day, hour).

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
                value = current_time.year
            elif col_name_lower == "month":
                value = current_time.month
                # Format month with leading zero if used as string
                if col_data_type.upper() in {"STRING"}:
                    partition_filters.append(f"`{col_name}` = '{value:02d}'")
                    continue
            elif col_name_lower == "day":
                value = current_time.day
                # Format day with leading zero if used as string
                if col_data_type.upper() in {"STRING"}:
                    partition_filters.append(f"`{col_name}` = '{value:02d}'")
                    continue
            elif col_name_lower == "hour":
                value = current_time.hour
                # Format hour with leading zero if used as string
                if col_data_type.upper() in {"STRING"}:
                    partition_filters.append(f"`{col_name}` = '{value:02d}'")
                    continue
            else:
                continue

            # Handle casting based on column type
            if col_data_type.upper() in {"STRING"}:
                partition_filters.append(f"`{col_name}` = '{value}'")
            else:
                partition_filters.append(f"`{col_name}` = {value}")

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
        logger.info(f"Using fallback partition values for {table.name}")
        fallback_filters = []

        # Get column data types
        column_data_types = {}
        try:
            query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}'"""

            # Use a short timeout for this operation
            query_job = self.config.get_bigquery_client().query(query)
            query_job.result(timeout=self.config.profiling.partition_fetch_timeout)
            column_data_types = {row.column_name: row.data_type for row in query_job}
            logger.debug(
                f"Retrieved column data types for {len(column_data_types)} columns"
            )
        except Exception as e:
            logger.warning(f"Error fetching column data types for fallback: {e}")
            # Continue without type information

        # Calculate the date for date-based fallbacks
        today = datetime.now(timezone.utc)
        fallback_date = today - timedelta(
            days=self.config.profiling.date_partition_offset
        )
        logger.info(
            f"Using fallback date {fallback_date.strftime('%Y-%m-%d')} (offset: {self.config.profiling.date_partition_offset} days)"
        )

        logger.info(
            f"Configured fallback values: {self.config.profiling.fallback_partition_values}"
        )

        for col_name in required_columns:
            col_lower = col_name.lower()
            data_type = (
                column_data_types.get(col_name, "").upper() if column_data_types else ""
            )

            # Check if the column has a direct fallback value in config
            if col_name in self.config.profiling.fallback_partition_values:
                fallback_value = self.config.profiling.fallback_partition_values[
                    col_name
                ]

                # Format the filter based on the value type
                if isinstance(fallback_value, str):
                    fallback_filters.append(f"`{col_name}` = '{fallback_value}'")
                else:
                    fallback_filters.append(f"`{col_name}` = {fallback_value}")

                logger.info(
                    f"Using explicit fallback value for {col_name}: {fallback_value}"
                )

            # Special handling for "day" column - use the day of month as a STRING
            elif col_lower == "day":
                # Use the day as a string with leading zero, not an integer
                day_of_month = fallback_date.day
                fallback_filters.append(f"`{col_name}` = '{day_of_month:02d}'")
                logger.info(
                    f"Using day of month as string for 'day' column: '{day_of_month:02d}'"
                )

            # Handle date/time partition columns using date_partition_offset
            # Check actual data_type first, then column name patterns
            elif data_type == "DATE":
                formatted_date = fallback_date.strftime("%Y-%m-%d")
                fallback_filters.append(f"`{col_name}` = DATE '{formatted_date}'")
                logger.info(
                    f"Using DATE literal for {col_name} (DATE type): {formatted_date}"
                )

            elif data_type in ["TIMESTAMP", "DATETIME"]:
                formatted_datetime = fallback_date.strftime("%Y-%m-%d %H:%M:%S")
                fallback_filters.append(
                    f"`{col_name}` = TIMESTAMP '{formatted_datetime}'"
                )
                logger.info(
                    f"Using TIMESTAMP literal for {col_name} ({data_type} type): {formatted_datetime}"
                )

            # For STRING columns that look like date columns, use string format
            elif col_lower in [
                "date",
                "dt",
                "partition_date",
                "created_date",
            ] and data_type in ["STRING", "VARCHAR", ""]:
                formatted_date = fallback_date.strftime("%Y-%m-%d")
                fallback_filters.append(f"`{col_name}` = '{formatted_date}'")
                logger.info(
                    f"Using string literal for {col_name} (STRING type): {formatted_date}"
                )

            elif col_lower in [
                "timestamp",
                "datetime",
                "time",
                "created_at",
                "event_time",
            ] and data_type in ["STRING", "VARCHAR", ""]:
                # For string columns that look like timestamp columns
                formatted_date = fallback_date.strftime("%Y-%m-%d")
                fallback_filters.append(f"`{col_name}` = '{formatted_date}'")
                logger.info(
                    f"Using string literal for timestamp-like {col_name} (STRING type): {formatted_date}"
                )

            # Treat year column as a string
            elif col_lower == "year" or (
                col_lower.endswith("year") and len(col_lower) < 10
            ):
                year_value = fallback_date.year
                fallback_filters.append(f"`{col_name}` = '{year_value}'")
                logger.info(
                    f"Using year fallback as string for {col_name}: '{year_value}'"
                )

            # Treat month column as a string
            elif col_lower == "month" or (
                col_lower.endswith("month") and len(col_lower) < 10
            ):
                month_value = fallback_date.month
                fallback_filters.append(f"`{col_name}` = '{month_value:02d}'")
                logger.info(
                    f"Using month fallback as string for {col_name}: '{month_value:02d}'"
                )

            # Treat other day-related columns as strings too
            elif col_lower.endswith("day") and len(col_lower) < 8:
                day_value = fallback_date.day
                fallback_filters.append(f"`{col_name}` = '{day_value:02d}'")
                logger.info(
                    f"Using day fallback as string for {col_name}: '{day_value:02d}'"
                )

            # For any other column with no fallback, use IS NOT NULL as a last resort
            else:
                logger.warning(
                    f"No fallback value for partition column {col_name}, using IS NOT NULL"
                )
                fallback_filters.append(f"`{col_name}` IS NOT NULL")

        logger.info(f"Generated fallback partition filters: {fallback_filters}")
        return fallback_filters

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """Handle partition-aware querying for all operations including COUNT."""
        bq_table = cast(BigqueryTable, table)
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

        # Construct query with partition filters
        partition_where = " AND ".join(partition_filters)
        logger.debug(f"Using partition filters: {partition_where}")

        if self.config.profiling.profiling_row_limit > 0:
            custom_sql = f"""SELECT * 
FROM `{db_name}.{schema_name}.{table.name}`
WHERE {partition_where}
LIMIT {self.config.profiling.profiling_row_limit}"""
        else:
            custom_sql = f"""SELECT * 
FROM `{db_name}.{schema_name}.{table.name}`
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
            partition_where = " AND ".join(partition_filters)
            custom_sql = f"""SELECT * 
FROM `{db_name}.{schema_name}.{bq_table.name}`
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
                    logger.info(
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
                # Format the value based on its type
                if isinstance(value, (int, float)):
                    filters.append(f"`{col}` = {value}")
                else:
                    filters.append(f"`{col}` = '{value}'")

            logger.info(f"Created filters from error message: {filters}")
            return filters

        # If we have required columns but no values, try to get values
        required_columns = partition_info.get("required_columns", [])
        if required_columns:
            # Try to use current date values for date-related columns
            now = datetime.now()
            date_values = {"year": now.year, "month": now.month, "day": now.day}

            for col in required_columns:
                # For date-related columns, use current date
                if col.lower() in date_values:
                    filters.append(f"`{col}` = {date_values[col.lower()]}")
                # For other columns, we need to sample values
                else:
                    try:
                        # Try to get a single value from the column
                        sample_query = f"""SELECT DISTINCT `{col}` as value
FROM `{project}.{schema}.{table.name}`
WHERE `{col}` IS NOT NULL
LIMIT 1"""

                        # Add date filters if we have them, to reduce query scope
                        where_clauses = []
                        for date_col in ["year", "month", "day"]:
                            if date_col in date_values and date_col in required_columns:
                                where_clauses.append(
                                    f"`{date_col}` = {date_values[date_col]}"
                                )

                        if where_clauses:
                            sample_query = sample_query.replace(
                                "WHERE `{col}` IS NOT NULL",
                                f"WHERE {' AND '.join(where_clauses)} AND `{col}` IS NOT NULL",
                            )

                        result = self.execute_query(sample_query)
                        if result and len(result) > 0 and hasattr(result[0], "value"):
                            value = result[0].value
                            if isinstance(value, (int, float)):
                                filters.append(f"`{col}` = {value}")
                            else:
                                filters.append(f"`{col}` = '{value}'")
                        else:
                            # If no value found, use IS NOT NULL
                            filters.append(f"`{col}` IS NOT NULL")
                    except Exception as inner_e:
                        logger.warning(
                            f"Error getting sample value for {col}: {inner_e}"
                        )
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
        Check if a partition has data by running a COUNT query.

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
            query = f"""SELECT COUNT(*) as row_count
FROM `{project}.{schema}.{table_name}`
WHERE {" AND ".join(filters)}"""

            results = self.execute_query(query)

            if results and len(results) > 0:
                row_count = getattr(results[0], "row_count", 0)
                logger.debug(f"Partition row count with filters {filters}: {row_count}")
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

        # Use the configured date_partition_offset as the starting point
        if self.config.profiling.date_partition_offset > 0:
            start_offset = self.config.profiling.date_partition_offset
        else:
            start_offset = 0

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
                logger.info(
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
                        try:
                            # Sample query with the date filters to find a valid value
                            sample_query = f"""SELECT DISTINCT `{col}` as value, COUNT(*) as count
FROM `{project}.{schema}.{table.name}`
WHERE {" AND ".join(date_filters)} AND `{col}` IS NOT NULL
GROUP BY `{col}`
ORDER BY count DESC
LIMIT 1"""

                            result = self.execute_query(sample_query)

                            if (
                                result
                                and len(result) > 0
                                and hasattr(result[0], "value")
                            ):
                                value = result[0].value
                                if isinstance(value, (int, float)):
                                    date_filters.append(f"`{col}` = {value}")
                                else:
                                    date_filters.append(f"`{col}` = '{value}'")

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
                                        date_filters.append(
                                            f"`{col}` = '{fallback_value}'"
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
                                    date_filters.append(f"`{col}` = '{fallback_value}'")
                                else:
                                    date_filters.append(f"`{col}` = {fallback_value}")
                            else:
                                date_filters.append(f"`{col}` IS NOT NULL")

                # Final check to ensure we have data with all filters
                has_data, row_count = self._check_partition_has_data(
                    project, schema, table.name, date_filters
                )

                if has_data:
                    logger.info(
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
        now = datetime.now()

        # If date_partition_offset is set, adjust the date
        if self.config.profiling.date_partition_offset > 0:
            now = now - timedelta(days=self.config.profiling.date_partition_offset)

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
            test_query = f"""SELECT COUNT(*) as count
FROM `{project}.{schema}.{table.name}`
WHERE {" AND ".join(date_filters)}
LIMIT 1"""

            self.execute_query(test_query)
            # If we get here, filters work but partition is empty
            logger.info(
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
                test_query = f"""SELECT COUNT(*) FROM `{project}.{schema}.{table.name}` LIMIT 1"""
                self.execute_query(test_query)
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
        now = datetime.now() - timedelta(
            days=self.config.profiling.date_partition_offset
        )
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
                partition_filters.append(f"`{col_name}` = '{fallback_value}'")
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
        now = datetime.now() - timedelta(
            days=self.config.profiling.date_partition_offset
        )
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
        Sample a value for a column using a query.

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
            # Create the sample query
            sample_query = f"""SELECT DISTINCT `{col_name}` as value, COUNT(*) as count
FROM `{project}.{schema}.{table_name}`
WHERE `{col_name}` IS NOT NULL
GROUP BY `{col_name}`
ORDER BY count DESC
LIMIT 1"""

            # Apply date filters if available
            if date_filters:
                sample_query = sample_query.replace(
                    "WHERE `{col_name}` IS NOT NULL",
                    f"WHERE {' AND '.join(date_filters)} AND `{col_name}` IS NOT NULL",
                )

            # Execute the query
            result = self.execute_query(
                sample_query,
                timeout=self.config.profiling.partition_fetch_timeout,
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
                        partition_filters.append(f"`{col}` = '{value}'")
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

        logger.info(
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
            logger.info(f"Sampling found partition with {row_count} rows")
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
        logger.info(
            f"Table {table.name} has partition information, determining optimal filters"
        )

        # Special handling for external tables
        is_external = getattr(table, "external", False)
        if is_external:
            logger.info(
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
                    # Run a simple query to trigger partition error
                    test_query = f"""SELECT COUNT(*) FROM `{project}.{schema}.{table.name}` LIMIT 1"""
                    self.execute_query(test_query)
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
                        logger.info(
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
            logger.info(f"Trying custom partition approach for {table.name}")
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
        Execute a BigQuery query with timeout from configuration.

        Args:
            query: SQL query to execute

        Returns:
            List of row results
        """
        try:
            timeout = self.config.profiling.partition_fetch_timeout
            logger.debug(f"Executing query with {timeout}s timeout: {query}")
            job_config = QueryJobConfig(timeout_ms=timeout * 1000)
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
        Create a filtered query in batch_kwargs.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_filters: List of partition filter strings
            batch_kwargs: Dictionary to update with partition information
        """
        if not partition_filters:
            return

        # Construct WHERE clause from filters
        where_clause = " AND ".join(partition_filters)

        # Create a SQL query with filters
        custom_sql = f"""SELECT * 
FROM `{project}.{schema}.{table.name}`
WHERE {where_clause}"""

        # Update batch_kwargs with the custom SQL
        batch_kwargs.update(
            {
                "custom_sql": custom_sql,
                "partition_handling": "true",
                "partition_filters": where_clause,
            }
        )

        logger.info(f"Applied partition filters to {table.name}: {where_clause}")

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
        logger.info(f"Applying custom partition approach for {table.name}")

        # First check if the table is actually partitioned
        try:
            # Query INFORMATION_SCHEMA to check for partitioning
            query = f"""SELECT count(*) as is_partitioned
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""

            query_result = self.execute_query(query)
            is_partitioned = 0
            if query_result and len(query_result) > 0:
                is_partitioned = getattr(query_result[0], "is_partitioned", 0)

            if is_partitioned == 0:
                logger.info(f"Table {table.name} is not partitioned, no filters needed")
                return

            # Try to find ANY data in the table
            sample_query = f"""SELECT * FROM `{project}.{schema}.{table.name}`
LIMIT 10"""

            try:
                # This might fail if partition elimination is required
                sample_results = self.execute_query(sample_query)
                if sample_results and len(sample_results) > 0:
                    logger.info(
                        f"Found data in table {table.name} without partition filters"
                    )
                    return
            except Exception as e:
                logger.warning(f"Error sampling table without filters: {e}")

            # Last resort - use the LIMIT approach which bypasses partition elimination
            logger.warning(f"Using LIMIT approach to profile {table.name}")
            custom_sql = f"""SELECT * FROM `{project}.{schema}.{table.name}`
LIMIT 1000000"""

            batch_kwargs.update(
                {
                    "custom_sql": custom_sql,
                    "partition_handling": "true",
                    "fallback_approach": "true",
                }
            )

            logger.info(f"Applied fallback LIMIT approach to {table.name}")
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
        Unified method to find a partition value for a single column.

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
        # First try INFORMATION_SCHEMA.PARTITIONS for regular tables
        if use_info_schema and not table.external:
            info_schema_result = self._get_partition_info_from_information_schema(
                table, project, schema, [col_name], max_results=5
            )

            if info_schema_result and col_name in info_schema_result:
                val = info_schema_result[col_name]
                logger.info(
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

            order_by = f"{col_name} DESC" if is_date_column else "record_count DESC"

            query = f"""WITH PartitionStats AS (
    SELECT {col_name} as val, COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {order_by}
    LIMIT 1
)
SELECT val, record_count FROM PartitionStats"""

            logger.debug(f"Finding partition value for {col_name}: {query}")

            query_job = self.config.get_bigquery_client().query(query)
            query_results = list(query_job.result())

            if not query_results or query_results[0].val is None:
                logger.warning(f"No valid partition value found for column {col_name}")
                return None

            val = query_results[0].val
            record_count = query_results[0].record_count

            logger.info(
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
                        logger.info(
                            f"Extracted partition value for {col_name} from error: {val} (type: {data_type})"
                        )
                        return val
            except Exception as extract_e:
                logger.debug(
                    f"Could not extract partition info from error: {extract_e}"
                )

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
        Unified method to find partition values for multiple columns.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            required_columns: List of required partition columns
            use_info_schema: Whether to try INFORMATION_SCHEMA first

        Returns:
            Dictionary mapping column names to values if found, None otherwise
        """
        # First try INFORMATION_SCHEMA.PARTITIONS for regular tables
        if use_info_schema and not table.external:
            info_schema_result = self._get_partition_info_from_information_schema(
                table, project, schema, required_columns, max_results=10
            )

            if info_schema_result:
                logger.info(
                    f"Found partition values from INFORMATION_SCHEMA: {info_schema_result}"
                )
                return info_schema_result

        # Try querying the table directly
        try:
            columns_select = ", ".join(required_columns)
            columns_group = ", ".join(required_columns)

            query = f"""SELECT {columns_select}, COUNT(*) as record_count
FROM `{project}.{schema}.{table.name}`
WHERE {" AND ".join([f"{col} IS NOT NULL" for col in required_columns])}
GROUP BY {columns_group}
HAVING record_count > 0
ORDER BY record_count DESC
LIMIT 10"""

            logger.debug(f"Finding multi-column partition values: {query}")

            query_job = self.config.get_bigquery_client().query(query)
            query_results = list(query_job.result())

            if not query_results:
                logger.warning("No valid partition combinations found")
                return None

            # Return the combination with the most records
            best_result = query_results[0]
            result_values = {}

            for col_name in required_columns:
                val = getattr(best_result, col_name)
                if val is not None:
                    result_values[col_name] = val

            logger.info(f"Found multi-column partition combination: {result_values}")
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
                        table, project, schema, required_columns
                    )

                    # Check if we have values for all required columns
                    if all(col in partition_values for col in required_columns):
                        # Convert values to appropriate types
                        converted_values = {}
                        for col in required_columns:
                            raw_val = partition_values[col]
                            data_type = column_data_types.get(col, "STRING")
                            converted_values[col] = (
                                self._convert_partition_value_to_type(
                                    raw_val, data_type
                                )
                            )

                        logger.info(
                            f"Extracted multi-column partition values from error: {converted_values}"
                        )
                        return converted_values
                    # Or return partial values if we have some
                    elif any(col in partition_values for col in required_columns):
                        # Convert partial values to appropriate types
                        partial_values = {}
                        for col in required_columns:
                            if col in partition_values:
                                raw_val = partition_values[col]
                                data_type = column_data_types.get(col, "STRING")
                                partial_values[col] = (
                                    self._convert_partition_value_to_type(
                                        raw_val, data_type
                                    )
                                )

                        logger.info(
                            f"Extracted partial partition values from error: {partial_values}"
                        )
                        return partial_values
            except Exception as extract_e:
                logger.debug(
                    f"Could not extract partition info from error: {extract_e}"
                )

            return None
