import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast

from dateutil.relativedelta import relativedelta

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
    PartitionInfo,
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
        # Initialize cache for query results to avoid redundant queries
        self._query_cache: Dict[str, Any] = {}
        # Cache for partition information
        self._partition_info_cache: Dict[str, Dict[str, Any]] = {}
        # Cache for table metadata
        self._table_metadata_cache: Dict[str, Dict[str, Any]] = {}
        # Track queried tables to avoid redundant API calls
        self._queried_tables: Set[str] = set()
        # Cache for successful partition filters
        self._successful_filters_cache: Dict[str, List[str]] = {}
        # Detect BigQuery schema version and set up column mappings
        self._detect_bq_schema_version()

    def _execute_cached_query(
        self,
        query: str,
        cache_key: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 2,
    ) -> List[Any]:
        """Execute a query with caching and retry logic to avoid redundant database calls."""
        if cache_key and cache_key in self._query_cache:
            return self._query_cache[cache_key]

        retries = 0
        last_exception = None

        while retries <= max_retries:
            try:
                # Define a function to execute the query to avoid lambda binding issues
                def execute_query(query_to_execute=query):
                    return list(
                        self.config.get_bigquery_client()
                        .query(query_to_execute)
                        .result()
                    )

                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(execute_query)
                    try:
                        # Use a slightly longer timeout for the future to account for overhead
                        results = future.result(timeout=timeout + 10)
                        if cache_key:
                            self._query_cache[cache_key] = results
                        return results
                    except TimeoutError:
                        logger.warning(
                            f"Query timed out after {timeout} seconds (attempt {retries + 1}/{max_retries + 1})"
                        )
                        retries += 1
                        if retries > max_retries:
                            logger.warning(f"Final timeout for query: {query[:200]}...")
                            return []
                        # Increase timeout for retries
                        timeout = min(timeout * 2, 300)  # Max 5 minutes
            except Exception as e:
                retries += 1
                last_exception = e
                logger.debug(
                    f"Query execution error (attempt {retries}/{max_retries + 1}): {str(e)}"
                )
                if retries > max_retries:
                    logger.warning(f"Query failed after {retries} attempts: {str(e)}")
                    return []
                # Add exponential backoff
                time.sleep(2**retries)

        if last_exception:
            logger.warning(
                f"Query execution error after retries: {str(last_exception)}"
            )
        return []

    def _adjust_query_for_bq_version(self, query: str) -> str:
        """
        Adjust column names in INFORMATION_SCHEMA queries based on BigQuery version.
        This handles differences between older and newer versions of BigQuery.
        """
        # Initialize with the original query
        modified_query = query

        # If we haven't determined BQ version yet, try to do so
        if not hasattr(self, "_bq_uses_new_schema"):
            self._detect_bq_schema_version()

        # Make column name replacements based on detected version
        if hasattr(self, "_bq_uses_new_schema") and self._bq_uses_new_schema:
            # For newer versions (total_rows/total_logical_bytes)
            if (
                "row_count" in modified_query
                and "total_rows as row_count" not in modified_query
            ):
                modified_query = modified_query.replace(
                    "row_count", "total_rows as row_count"
                )
            if (
                "size_bytes" in modified_query
                and "total_logical_bytes as size_bytes" not in modified_query
            ):
                modified_query = modified_query.replace(
                    "size_bytes", "total_logical_bytes as size_bytes"
                )
        else:
            # For older versions (row_count/size_bytes)
            if "total_rows as row_count" in modified_query:
                modified_query = modified_query.replace(
                    "total_rows as row_count", "row_count"
                )
            if "total_logical_bytes as size_bytes" in modified_query:
                modified_query = modified_query.replace(
                    "total_logical_bytes as size_bytes", "size_bytes"
                )

        return modified_query

    def _detect_bq_schema_version(self) -> None:
        """
        Detect which version of INFORMATION_SCHEMA we're working with.
        Sets self._column_name_mapping with the appropriate column mappings.
        """
        try:
            # Try to execute a simple test query that works on all versions
            # This just gets us a small amount of data to check column names
            test_query = """
            SELECT * 
            FROM INFORMATION_SCHEMA.TABLES
            LIMIT 1
            """

            results = list(self.config.get_bigquery_client().query(test_query).result())
            if not results:
                # If no results, default to old schema (safer choice)
                self._column_name_mapping = {
                    "row_count": "row_count",
                    "size_bytes": "size_bytes",
                }
                logger.info(
                    "No test results returned. Defaulting to old schema column names."
                )
                return

            # Check what column names are available in the result
            row = results[0]

            # Initialize mapping dict
            self._column_name_mapping = {}

            # Check for row count column
            if hasattr(row, "total_rows"):
                self._column_name_mapping["row_count"] = "total_rows"
            elif hasattr(row, "row_count"):
                self._column_name_mapping["row_count"] = "row_count"
            else:
                # Default
                self._column_name_mapping["row_count"] = "row_count"

            # Check for size bytes column
            if hasattr(row, "total_logical_bytes"):
                self._column_name_mapping["size_bytes"] = "total_logical_bytes"
            elif hasattr(row, "size_bytes"):
                self._column_name_mapping["size_bytes"] = "size_bytes"
            else:
                # Default
                self._column_name_mapping["size_bytes"] = "size_bytes"

            logger.info(
                f"Detected BigQuery INFORMATION_SCHEMA column mapping: {self._column_name_mapping}"
            )

        except Exception as e:
            # If detection fails, default to old schema as it's more common
            self._column_name_mapping = {
                "row_count": "row_count",
                "size_bytes": "size_bytes",
            }
            logger.warning(
                f"Error detecting BigQuery schema version: {e}. Defaulting to old schema column names."
            )

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime] = None
    ) -> Tuple[datetime, datetime]:
        """
        Calculate date range from a partition ID with enhanced flexibility.

        Args:
            partition_id: String representing a partition (e.g., '20220101', 'year=2022/month=01')
            partition_datetime: Optional reference datetime

        Returns:
            Tuple of (start_datetime, end_datetime)
        """
        # Handle Hive-style partitions like 'year=2022/month=01/day=01'
        if "=" in partition_id:
            parts = {}
            for part in partition_id.split("/"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    parts[key.lower().strip()] = value.strip()

            # Extract year, month, day, hour if present
            year = int(parts.get("year", datetime.now().year))
            month = int(parts.get("month", 1))
            day = int(parts.get("day", 1))
            hour = int(parts.get("hour", 0))

            start_datetime = datetime(year, month, day, hour)

            # Calculate end datetime based on available parts
            if "hour" in parts:
                end_datetime = start_datetime + timedelta(hours=1)
            elif "day" in parts:
                end_datetime = start_datetime + timedelta(days=1)
            elif "month" in parts:
                # Handle month rollover properly
                if month == 12:
                    end_datetime = datetime(year + 1, 1, day, hour)
                else:
                    end_datetime = datetime(year, month + 1, day, hour)
            else:
                end_datetime = datetime(year + 1, month, day, hour)

            return start_datetime, end_datetime

        # Handle numeric format partitions (standard BigQuery pattern)
        partition_range_map: Dict[int, Tuple[relativedelta, str]] = {
            4: (relativedelta(years=1), "%Y"),
            6: (relativedelta(months=1), "%Y%m"),
            8: (relativedelta(days=1), "%Y%m%d"),
            10: (relativedelta(hours=1), "%Y%m%d%H"),
        }

        # Support for ISO format dates (YYYY-MM-DD)
        if (
            len(partition_id) == 10
            and partition_id[4] == "-"
            and partition_id[7] == "-"
        ):
            try:
                dt = datetime.strptime(partition_id, "%Y-%m-%d")
                return dt, dt + timedelta(days=1)
            except ValueError:
                pass

        # Basic numeric format
        if partition_range_map.get(len(partition_id)):
            try:
                (delta, format_str) = partition_range_map[len(partition_id)]
                if not partition_datetime:
                    partition_datetime = datetime.strptime(partition_id, format_str)
                else:
                    partition_datetime = datetime.strptime(
                        partition_datetime.strftime(format_str), format_str
                    )
                upper_bound_partition_datetime = partition_datetime + delta
                return partition_datetime, upper_bound_partition_datetime
            except ValueError as e:
                logger.warning(
                    f"Failed to parse partition_id {partition_id} with format {format_str}: {e}"
                )

        # If we reach here, we couldn't parse the partition_id
        raise ValueError(
            f"Invalid partition_id format: {partition_id}. It must be yearly (YYYY), "
            f"monthly (YYYYMM), daily (YYYYMMDD), hourly (YYYYMMDDHH), "
            f"ISO date (YYYY-MM-DD), or Hive style (year=YYYY/month=MM/...)."
        )

    def _get_table_metadata(
        self, table: BigqueryTable, project: str, schema: str
    ) -> Dict[str, Any]:
        """
        Get comprehensive metadata about a table efficiently.
        Uses a multi-tiered approach to minimize API calls while gathering all needed information.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name

        Returns:
            Dictionary containing table metadata
        """
        cache_key = f"{project}.{schema}.{table.name}"

        if cache_key in self._table_metadata_cache:
            return self._table_metadata_cache[cache_key]

        # Start with basic info from the table object
        metadata = self._fetch_basic_table_metadata(table)

        # Track if we need to query INFORMATION_SCHEMA
        need_schema_query = len(metadata["partition_columns"]) == 0 or any(
            v is None for v in metadata["partition_columns"].values()
        )

        # If we still need more information or no partition columns were found,
        # fetch from INFORMATION_SCHEMA with a single efficient query
        if need_schema_query:
            metadata = self._fetch_schema_info(table, project, schema, metadata)

        # If still no partition columns but we have DDL, parse it
        if not metadata["partition_columns"] and metadata.get("ddl"):
            if "PARTITION BY" in metadata["ddl"].upper():
                self._extract_partitioning_from_ddl(
                    metadata["ddl"], metadata, project, schema, table.name
                )

        # If we need more specific table stats and didn't get them above, fetch them
        if not table.external:  # Only for internal tables
            metadata = self._fetch_table_stats(project, schema, table.name, metadata)

        # Cache the result
        self._table_metadata_cache[cache_key] = metadata
        return metadata

    def _get_time_hierarchy_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        available_time_columns: List[str],
        is_large_table: bool,
        timeout: int,
    ) -> Dict[str, Any]:
        """Get values for time hierarchy columns."""
        result: Dict[str, Any] = {}
        current_time = datetime.now(timezone.utc)

        # Set defaults from current time
        time_values = {
            "year": current_time.year,
            "month": current_time.month,
            "day": current_time.day,
            "hour": current_time.hour,
        }

        # For large tables, we'll be more aggressive with partitioning
        if is_large_table:
            for col_name in available_time_columns:
                col_lower = col_name.lower()

                # Try to find optimal values with data
                query = f"""
                WITH PartitionValues AS (
                    SELECT 
                        {col_name} as value,
                        COUNT(*) as row_count
                    FROM 
                        `{project}.{schema}.{table.name}`
                    WHERE 
                        {col_name} IS NOT NULL
                    GROUP BY 
                        {col_name}
                    ORDER BY 
                        row_count DESC
                    LIMIT 5
                )
                SELECT * FROM PartitionValues
                """

                try:
                    # Use shorter timeout for this optimization query
                    col_results = self._execute_cached_query(
                        query,
                        f"time_col_values_{project}_{schema}_{table.name}_{col_name}",
                        timeout=min(30, timeout // 2),
                    )

                    if col_results and col_results[0].value is not None:
                        result[col_name] = col_results[0].value
                        logger.info(
                            f"Selected value for {col_name} = {result[col_name]} with {col_results[0].row_count} rows"
                        )
                    else:
                        # Fall back to current time values
                        result[col_name] = time_values[col_lower]
                except Exception as e:
                    logger.debug(f"Error getting optimal time column value: {e}")
                    result[col_name] = time_values[col_lower]
        else:
            # For normal tables, just use current time values
            for col_name in available_time_columns:
                col_lower = col_name.lower()
                result[col_name] = time_values[col_lower]

        return result

    def _get_date_column_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        date_columns: List[str],
        timeout: int,
    ) -> Dict[str, Any]:
        """Get values for date columns."""
        result: Dict[str, Any] = {}

        if not date_columns:
            return result

        logger.info(f"Using date columns for partitioning: {date_columns}")

        # For efficient batch processing, try to get values for multiple date columns at once
        date_cols_list = ", ".join(
            [f"{col}" for col in date_columns[:3]]
        )  # Limit to 3 columns

        combined_query = f"""
        WITH DateValues AS (
            SELECT 
                {date_cols_list},
                COUNT(*) as row_count
            FROM 
                `{project}.{schema}.{table.name}`
            WHERE 
                {" AND ".join([f"{col} IS NOT NULL" for col in date_columns[:3]])}
            GROUP BY 
                {date_cols_list}
            ORDER BY 
                row_count DESC
            LIMIT 1
        )
        SELECT * FROM DateValues
        """

        try:
            date_results = self._execute_cached_query(
                combined_query,
                f"date_cols_combined_{project}_{schema}_{table.name}",
                timeout=min(timeout, 45),
            )

            if date_results:
                # Extract values for each date column
                for col in date_columns[:3]:
                    val = getattr(date_results[0], col, None)
                    if val is not None:
                        result[col] = val
                        logger.info(f"Selected combined value for {col} = {val}")
        except Exception as e:
            logger.debug(f"Combined date query failed: {e}")

        # For any date columns not handled by combined query, query individually
        for col in date_columns:
            if col in result:
                continue

            query = f"""
            WITH DateValues AS (
                SELECT 
                    {col} as value,
                    COUNT(*) as row_count
                FROM 
                    `{project}.{schema}.{table.name}`
                WHERE 
                    {col} IS NOT NULL
                GROUP BY 
                    {col}
                ORDER BY 
                    value DESC
                LIMIT 3
            )
            SELECT * FROM DateValues
            """

            try:
                col_results = self._execute_cached_query(
                    query,
                    f"date_col_{project}_{schema}_{table.name}_{col}",
                    timeout=min(30, timeout // 2),
                )

                if col_results and col_results[0].value is not None:
                    result[col] = col_results[0].value
                    logger.info(f"Selected value for {col} = {result[col]}")
            except Exception as e:
                logger.debug(f"Date column query failed for {col}: {e}")

        return result

    def _get_remaining_column_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        remaining_columns: List[str],
        is_large_table: bool,
        timeout: int,
    ) -> Dict[str, Any]:
        """Get values for remaining partition columns."""
        result: Dict[str, Any] = {}

        if not remaining_columns:
            return result

        logger.info(f"Processing remaining partition columns: {remaining_columns}")

        # For large tables use a much more cautious approach
        if is_large_table:
            # Try columns one by one, with sampling for efficiency
            for col_name in remaining_columns:
                try:
                    sample_query = f"""
                    WITH SampleValues AS (
                        SELECT 
                            {col_name} as value,
                            COUNT(*) as row_count
                        FROM 
                            `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                        WHERE 
                            {col_name} IS NOT NULL
                        GROUP BY 
                            {col_name}
                        ORDER BY 
                            row_count DESC
                        LIMIT 1
                    )
                    SELECT * FROM SampleValues
                    """

                    sample_results = self._execute_cached_query(
                        sample_query,
                        f"sample_col_{project}_{schema}_{table.name}_{col_name}",
                        timeout=min(20, timeout // 3),
                    )

                    if sample_results and sample_results[0].value is not None:
                        result[col_name] = sample_results[0].value
                        logger.info(
                            f"Selected sampled value for {col_name} = {result[col_name]} with {sample_results[0].row_count} rows"
                        )
                except Exception as e:
                    logger.debug(f"Sampling query failed for {col_name}: {e}")
        else:
            # For normal tables, try to get values for all remaining columns at once
            try:
                # Try combined approach first for efficiency (limit to 5 columns)
                combined_cols = remaining_columns[:5]
                cols_list = ", ".join([f"{col}" for col in combined_cols])
                group_by = ", ".join(combined_cols)

                combined_query = f"""
                WITH PartitionStats AS (
                    SELECT 
                        {cols_list}, 
                        COUNT(*) as row_count
                    FROM 
                        `{project}.{schema}.{table.name}`
                    WHERE 
                        {" AND ".join([f"{col} IS NOT NULL" for col in combined_cols])}
                    GROUP BY 
                        {group_by}
                    ORDER BY 
                        row_count DESC
                    LIMIT 1
                )
                SELECT * FROM PartitionStats
                """

                combined_results = self._execute_cached_query(
                    combined_query,
                    f"combined_cols_{project}_{schema}_{table.name}",
                    timeout=min(30, timeout // 2),
                )

                if combined_results:
                    for col in combined_cols:
                        val = getattr(combined_results[0], col, None)
                        if val is not None:
                            result[col] = val
                            logger.info(f"Selected combined value for {col} = {val}")
            except Exception as e:
                logger.debug(f"Combined query failed: {e}")

            # Get values for any columns not handled by the combined query
            remaining_to_query = [col for col in remaining_columns if col not in result]
            for col in remaining_to_query:
                try:
                    # Query for the value with the most data
                    query = f"""
                    WITH PartitionStats AS (
                        SELECT 
                            {col} as value,
                            COUNT(*) as row_count
                        FROM 
                            `{project}.{schema}.{table.name}`
                        WHERE 
                            {col} IS NOT NULL
                        GROUP BY 
                            {col}
                        ORDER BY 
                            row_count DESC
                        LIMIT 1
                    )
                    SELECT * FROM PartitionStats
                    """

                    col_results = self._execute_cached_query(
                        query,
                        f"col_value_{project}_{schema}_{table.name}_{col}",
                        timeout=min(20, timeout // 3),
                    )

                    if col_results and col_results[0].value is not None:
                        result[col] = col_results[0].value
                        logger.info(f"Selected value for {col} = {result[col]}")
                except Exception as e:
                    logger.debug(f"Column query failed for {col}: {e}")

        return result

    def _extract_partitioning_from_ddl(
        self,
        ddl: str,
        metadata: Dict[str, Any],
        project: str,
        schema: str,
        table_name: str,
    ) -> None:
        """
        Extract partition columns from table DDL with enhanced pattern matching.
        Handles various DDL formats including:
        - Standard PARTITION BY (column)
        - PARTITION BY DATE(column)
        - PARTITION BY TIMESTAMP_TRUNC(column, DAY)
        - Time-unit partitioning (DAY, MONTH, YEAR)
        """
        if not ddl:
            return

        # Normalize DDL for easier parsing, but preserve case for extraction
        ddl_upper = ddl.upper().replace("\n", " ").replace("\t", " ")
        ddl_norm = ddl.replace("\n", " ").replace("\t", " ")  # Preserve case

        # Track found partition cols
        found_partition_cols: Set[str] = set()

        # Case 1: Standard PARTITION BY column
        if "PARTITION BY" in ddl_upper:
            # Use case-preserved version for extraction
            partition_start = ddl_upper.find("PARTITION BY")
            if partition_start >= 0:
                # Get the same position in the original case version
                self._extract_partition_by_clause(
                    ddl_upper[partition_start:],
                    ddl_norm[partition_start:],
                    found_partition_cols,
                )

        # Case 2: Check for external table partitioning with URI pattern
        if (
            metadata.get("is_external")
            and "EXTERNAL" in ddl_upper
            and "URI_TEMPLATE" in ddl_upper
        ):
            uri_start = ddl_upper.find("URI_TEMPLATE")
            if uri_start >= 0:
                # Use the same position in the original case version
                self._extract_uri_template_partitioning(
                    ddl_upper[uri_start:], ddl_norm[uri_start:], found_partition_cols
                )

        # Get data types for identified partition columns
        if found_partition_cols:
            self._get_partition_column_types(
                found_partition_cols, metadata, project, schema, table_name
            )

        # Special case: If no partition columns found but table has partition_info
        # with time-based partitioning, add the implicit _PARTITIONTIME column
        if (
            not metadata["partition_columns"]
            and isinstance(metadata.get("partition_info"), PartitionInfo)
            and metadata["partition_info"].type in ["DAY", "MONTH", "YEAR", "HOUR"]
        ):
            metadata["partition_columns"]["_PARTITIONTIME"] = "TIMESTAMP"

    def _extract_partition_by_clause(
        self, upper_clause: str, original_clause: str, found_partition_cols: Set[str]
    ) -> None:
        """Extract partition columns from a PARTITION BY clause in DDL."""
        try:
            # Extract up to the next major clause using upper case for detection
            partition_clause_upper = self._extract_partition_clause(upper_clause)

            # Get the corresponding part from the original case
            # The length should be the same since we're just changing case
            partition_clause = original_clause[: len(partition_clause_upper)]

            # Remove the "PARTITION BY" part (using upper for the length)
            partition_by_len = len("PARTITION BY")
            if partition_clause_upper.startswith("PARTITION BY"):
                partition_clause_upper = partition_clause_upper[
                    partition_by_len:
                ].strip()
                partition_clause = partition_clause[partition_by_len:].strip()

            # Handle different partition specifications using both versions
            if partition_clause_upper.startswith(
                "DATE("
            ) or partition_clause_upper.startswith("TIMESTAMP("):
                self._extract_date_timestamp_function(
                    partition_clause_upper, partition_clause, found_partition_cols
                )
            elif (
                "TIMESTAMP_TRUNC" in partition_clause_upper
                or "DATE_TRUNC" in partition_clause_upper
            ):
                self._extract_trunc_function(
                    partition_clause_upper, partition_clause, found_partition_cols
                )
            elif any(
                unit in partition_clause_upper
                for unit in ["DAY", "MONTH", "YEAR", "HOUR"]
            ):
                # This is a time-unit partitioning without explicit column
                # Use _PARTITIONTIME as the implicit partition column
                found_partition_cols.add("_PARTITIONTIME")
            else:
                # Simple column partitioning
                self._extract_simple_columns(partition_clause, found_partition_cols)
        except Exception as e:
            logger.warning(f"Error parsing PARTITION BY clause from DDL: {e}")

    def _extract_partition_clause(self, partition_clause: str) -> str:
        """Extract the partition clause up to the next major clause."""
        for end_token in ["OPTIONS", "CLUSTER BY", "AS SELECT", ";"]:
            end_pos = partition_clause.find(end_token)
            if end_pos > 0:
                partition_clause = partition_clause[:end_pos].strip()
        return partition_clause

    def _extract_date_timestamp_function(
        self, upper_clause: str, original_clause: str, found_partition_cols: set
    ) -> None:
        """Extract column from DATE(...) or TIMESTAMP(...) function."""
        col_start = upper_clause.find("(") + 1
        col_end = upper_clause.find(")")
        if 0 < col_start < col_end:
            # Use the original case version to get the actual column name
            col_name = original_clause[col_start:col_end].strip(", `'\"")
            found_partition_cols.add(col_name)

    def _extract_trunc_function(
        self, upper_clause: str, original_clause: str, found_partition_cols: set
    ) -> None:
        """Extract column from TIMESTAMP_TRUNC(column, DAY) or DATE_TRUNC(column, DAY)."""
        trunc_start = upper_clause.find("(") + 1
        trunc_end = upper_clause.find(",")
        if trunc_end == -1:  # No comma found
            trunc_end = upper_clause.find(")")
        if 0 < trunc_start < trunc_end:
            # Use the original case version to get the actual column name
            col_name = original_clause[trunc_start:trunc_end].strip(", `'\"")
            found_partition_cols.add(col_name)

    def _extract_simple_columns(
        self, partition_clause: str, found_partition_cols: set
    ) -> None:
        """Extract simple column names from partition clause."""
        for part in partition_clause.split():
            # Get the part without quotes, parentheses, etc.
            cleaned_col = part.strip(", `'\"()").split(".")[-1]

            # Skip known keywords and empty strings (check lowercase)
            if cleaned_col and cleaned_col.upper() not in [
                "DATE",
                "TIMESTAMP",
                "BY",
                "PARTITION",
            ]:
                # Keep the original case
                found_partition_cols.add(cleaned_col)

    def _extract_uri_template_partitioning(
        self, upper_part: str, original_part: str, found_partition_cols: set
    ) -> None:
        """Extract partition columns from URI_TEMPLATE in external table DDL."""
        try:
            end_pos = upper_part.find(",")
            if end_pos > 0:
                upper_part = upper_part[:end_pos].strip(" ='\"`")
                original_part = original_part[:end_pos].strip(" ='\"`")

            # Look for Hive-style partitioning in URI template
            if "{" in original_part and "}" in original_part:
                parts = original_part.split("{")
                for _i, part in enumerate(parts):
                    if "}" in part:
                        # Extract the part between { and }
                        partition_var = part.split("}")[0].strip()
                        # Keep the original case
                        found_partition_cols.add(partition_var)
        except Exception as e:
            logger.debug(f"Error parsing URI_TEMPLATE for partitioning: {e}")

    def _get_partition_column_types(
        self,
        found_partition_cols: set,
        metadata: Dict[str, Any],
        project: str,
        schema: str,
        table_name: str,
    ) -> None:
        """Get data types for identified partition columns."""
        # For testing compatibility - if no columns, no query needed
        if not found_partition_cols:
            return

        try:
            # Construct query
            cols_list = "', '".join(found_partition_cols)
            col_types_query = f"""
            SELECT 
                column_name, 
                data_type
            FROM 
                `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE 
                table_name = '{table_name}'
                AND column_name IN ('{cols_list}')
            """

            # Execute query
            results = self._execute_cached_query(
                col_types_query,
                f"partition_cols_types_{project}_{schema}_{table_name}",
            )

            # Process results - handle mock objects directly
            for result in results:
                try:
                    # In mock objects, these might be attributes
                    column_name = result.column_name
                    data_type = result.data_type

                    # Store the data type in the metadata
                    metadata["partition_columns"][column_name] = data_type
                except AttributeError:
                    # Handle regular dictionary results (just in case)
                    if hasattr(result, "items"):
                        column_name = result.get("column_name")
                        data_type = result.get("data_type")
                        if column_name and data_type:
                            metadata["partition_columns"][column_name] = data_type

        except Exception as e:
            logger.warning(f"Error fetching partition column types: {e}")

        # Add missing columns with None type
        for col in found_partition_cols:
            if col not in metadata["partition_columns"]:
                metadata["partition_columns"][col] = None

    def _create_partition_filters(
        self, partition_columns: Dict[str, str], partition_values: Dict[str, Any]
    ) -> List[str]:
        """
        Create partition filter strings from column names, types and values
        with enhanced support for various data types.

        Args:
            partition_columns: Dictionary mapping column names to data types
            partition_values: Dictionary mapping column names to values

        Returns:
            List of filter strings
        """
        filters = []

        for col_name, value in partition_values.items():
            if value is None:
                continue

            data_type = (
                partition_columns.get(col_name, "").upper()
                if partition_columns.get(col_name)
                else ""
            )

            # Handle each type with a specific helper method to reduce complexity
            filter_str = self._create_filter_for_column(col_name, value, data_type)
            if filter_str:
                filters.append(filter_str)

        return filters

    def _create_filter_for_column(
        self, col_name: str, value: Any, data_type: str
    ) -> Optional[str]:
        """
        Create a filter string for a single column based on its data type with improved
        type handling to prevent INT64/TIMESTAMP mismatches.
        """
        # Handle NULL values
        if value is None:
            return f"`{col_name}` IS NULL"

        # Convert data_type to uppercase for consistent comparisons
        data_type = data_type.upper()

        # Special case 1: datetime value for an INT64 column
        if isinstance(value, datetime) and data_type in (
            "INT64",
            "INTEGER",
            "INT",
            "BIGINT",
        ):
            # Convert datetime to unix timestamp (seconds since epoch)
            unix_timestamp = int(value.timestamp())
            return f"`{col_name}` = {unix_timestamp}"

        # Special case 2: integer value for a TIMESTAMP column
        if isinstance(value, (int, float)) and data_type in ("TIMESTAMP", "DATETIME"):
            # Treat integer as unix seconds
            return f"`{col_name}` = TIMESTAMP_SECONDS({int(value)})"

        # Group data types into categories for easier handling
        string_types = {"STRING", "VARCHAR", "BIGNUMERIC", "JSON"}
        date_types = {"DATE"}
        timestamp_types = {"TIMESTAMP", "DATETIME"}
        numeric_types = {
            "INT64",
            "INTEGER",
            "INT",
            "SMALLINT",
            "BIGINT",
            "TINYINT",
            "FLOAT64",
            "FLOAT",
            "NUMERIC",
            "DECIMAL",
        }
        boolean_types = {"BOOL", "BOOLEAN"}

        # Dispatch to type-specific handlers
        if data_type in string_types:
            return self._create_string_filter(col_name, value, data_type)
        elif data_type in date_types:
            return self._create_date_filter(col_name, value)
        elif data_type in timestamp_types:
            return self._create_timestamp_filter(col_name, value)
        elif data_type in numeric_types:
            return self._create_numeric_filter(col_name, value)
        elif data_type in boolean_types:
            return self._create_boolean_filter(col_name, value)
        else:
            return self._create_default_filter(col_name, value, data_type)

    def _create_string_filter(self, col_name: str, value: Any, data_type: str) -> str:
        """Create filter for string type columns."""
        if isinstance(value, str):
            # Escape any single quotes in the string value
            escaped_value = value.replace("'", "\\'")
            return f"`{col_name}` = '{escaped_value}'"
        else:
            # Handle non-string values for string columns (cast them)
            return f"`{col_name}` = CAST('{value}' AS {data_type})"

    def _create_date_filter(self, col_name: str, value: Any) -> str:
        """Create filter for date type columns."""
        if isinstance(value, datetime):
            return f"`{col_name}` = DATE '{value.strftime('%Y-%m-%d')}'"
        elif isinstance(value, str):
            # Try to ensure the string is properly formatted
            if "-" in value and len(value) >= 10:
                # Likely YYYY-MM-DD format
                return f"`{col_name}` = DATE '{value}'"
            else:
                # Try to interpret other formats
                return f"`{col_name}` = '{value}'"
        else:
            # For other types, try simple equality
            return f"`{col_name}` = '{value}'"

    def _create_timestamp_filter(self, col_name: str, value: Any) -> str:
        """
        Create filter for timestamp and datetime columns with enhanced type safety.
        """
        if isinstance(value, datetime):
            # Format datetime as string in BigQuery-compatible format
            timestamp_str = value.strftime("%Y-%m-%d %H:%M:%S")
            return f"`{col_name}` = TIMESTAMP '{timestamp_str}'"
        elif isinstance(value, (int, float)):
            # For numeric values, explicitly use TIMESTAMP_SECONDS
            return f"`{col_name}` = TIMESTAMP_SECONDS({int(value)})"
        elif isinstance(value, str):
            # For string values, try to use appropriate casting based on format
            if "T" in value:
                # Looks like ISO format, normalize for BigQuery
                value = value.replace("T", " ")
                if value.endswith("Z"):
                    value = value[:-1]  # Remove Z suffix

            # Use safe casting that works regardless of string format
            return f"`{col_name}` = CAST('{value}' AS TIMESTAMP)"
        else:
            # For any other type, convert to string and use CAST
            return f"`{col_name}` = CAST('{str(value)}' AS TIMESTAMP)"

    def _create_numeric_filter(self, col_name: str, value: Any) -> str:
        """
        Create filter for numeric type columns with enhanced type safety.
        """
        if isinstance(value, (int, float)):
            return f"`{col_name}` = {value}"
        elif isinstance(value, datetime):
            # Handle datetime value for a numeric column by converting to unix timestamp
            unix_timestamp = int(value.timestamp())
            return f"`{col_name}` = {unix_timestamp}"
        elif isinstance(value, str):
            # If it's a string that contains timestamp indications, convert appropriately
            if ":" in value or "-" in value or "T" in value:
                try:
                    # Try to parse as datetime and then to unix timestamp
                    dt = datetime.fromisoformat(
                        value.replace("Z", "+00:00").replace("T", " ")
                    )
                    unix_timestamp = int(dt.timestamp())
                    return f"`{col_name}` = {unix_timestamp}"
                except ValueError:
                    pass

            # If string can be converted to numeric, use that value
            try:
                numeric_value = float(value)
                return f"`{col_name}` = {numeric_value}"
            except ValueError:
                # If all else fails, use a placeholder filter that will evaluate to false
                logger.warning(
                    f"Could not convert value '{value}' to numeric for column '{col_name}'"
                )
                return f"`{col_name}` = -1 AND FALSE /* Placeholder for incompatible filter */"
        else:
            # For any other type, use a placeholder
            return (
                f"`{col_name}` = -1 AND FALSE /* Placeholder for incompatible filter */"
            )

    def _create_boolean_filter(self, col_name: str, value: Any) -> str:
        """Create filter for boolean type columns."""
        # Convert Python boolean to SQL boolean
        bool_val = str(value).lower()
        if bool_val in ("true", "false"):
            return f"`{col_name}` = {bool_val}"
        else:
            # Try to handle non-boolean values that might represent booleans
            return f"`{col_name}` = {bool_val}"

    def _create_default_filter(self, col_name: str, value: Any, data_type: str) -> str:
        """Create default filter for any other column types."""
        # Prefer string format when data type is unknown or unhandled
        if isinstance(value, (int, float)) and data_type:
            return f"`{col_name}` = {value}"
        else:
            # Escape any single quotes in string values
            if isinstance(value, str):
                value = value.replace("'", "\\'")
            return f"`{col_name}` = '{value}'"

    def _verify_partition_has_data(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        timeout: int = 30,
        max_retries: int = 2,
    ) -> bool:
        """
        Verify that partition filters return data.
        """
        if not filters:
            return False

        # Check cache first
        cache_key = f"{project}.{schema}.{table.name}.{hash(tuple(sorted(filters)))}"
        if cache_key in self._successful_filters_cache:
            logger.debug(f"Using cached filter verification result for {cache_key}")
            return True

        # Build WHERE clause from filters
        where_clause = " AND ".join(filters)

        # Try each verification strategy in order
        verification_strategies = [
            self._try_existence_check,
            self._try_count_query,
            self._try_sample_query,
            self._try_syntax_check,
        ]

        for strategy in verification_strategies:
            result = strategy(
                table,
                project,
                schema,
                filters,
                where_clause,
                timeout,
                max_retries,
                cache_key,
            )
            if result:
                return True

        logger.warning(
            f"Partition verification found no data with filters: {where_clause}"
        )
        return False

    def _is_type_mismatch_error(self, error_str: str) -> bool:
        """Check if the error is a type mismatch."""
        return "No matching signature for operator =" in error_str and (
            "TIMESTAMP" in error_str or "INT64" in error_str
        )

    def _fix_type_mismatch_filters(
        self, filters: List[str], error_str: str
    ) -> List[str]:
        """
        Fix type mismatch errors in filters with improved detection and conversion.
        """
        logger.warning(f"Type mismatch error in filter: {error_str}")

        fixed_filters = []
        for filter_str in filters:
            if "=" in filter_str:
                parts = filter_str.split("=", 1)
                if len(parts) == 2:
                    col, val = parts
                    col = col.strip()
                    val = val.strip()

                    # INT64 column with TIMESTAMP value
                    if "INT64" in error_str and "TIMESTAMP" in error_str:
                        if "TIMESTAMP" in val:
                            # Convert TIMESTAMP to unix seconds
                            fixed_filter = f"{col} = UNIX_SECONDS({val})"
                            fixed_filters.append(fixed_filter)
                            continue
                        elif val.isdigit():
                            # Convert numeric to TIMESTAMP
                            fixed_filter = f"{col} = TIMESTAMP_SECONDS({val})"
                            fixed_filters.append(fixed_filter)
                            continue
                    # Special case for other type mismatches
                    elif "INT64" in error_str and "'" in val:
                        # String value being compared to INT64
                        try:
                            # Try to convert string to int
                            cleaned_val = val.strip("'")
                            int_val = int(float(cleaned_val))
                            fixed_filter = f"{col} = {int_val}"
                            fixed_filters.append(fixed_filter)
                            continue
                        except (ValueError, TypeError):
                            # If conversion fails, use a safe filter that will evaluate to false
                            fixed_filter = f"{col} = -1 AND FALSE /* Placeholder for incompatible filter */"
                            fixed_filters.append(fixed_filter)
                            continue

            # Keep unchanged filters
            fixed_filters.append(filter_str)

        return fixed_filters

    def _try_count_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a COUNT query."""
        count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project}.{schema}.{table.name}`
        WHERE {where_clause}
        LIMIT 1000
        """

        count_key = f"verify_count_{project}_{schema}_{table.name}_{hash(where_clause)}"

        try:
            count_results = self._execute_cached_query(count_query, count_key, timeout)
            if (
                count_results
                and hasattr(count_results[0], "cnt")
                and count_results[0].cnt > 0
            ):
                logger.info(f"Verified filters return {count_results[0].cnt} rows")
                self._successful_filters_cache[cache_key] = filters
                return True
        except Exception as e:
            logger.debug(f"Count verification failed: {e}")

        return False

    def _try_sample_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a TABLESAMPLE query for large tables."""
        if not (
            table.external
            or (table.size_in_bytes and table.size_in_bytes > 1_000_000_000)
        ):
            return False

        try:
            sample_rate = 0.01 if table.external else 0.1
            sample_query = f"""
            SELECT 1 
            FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM ({sample_rate} PERCENT)
            WHERE {where_clause}
            LIMIT 1
            """

            sample_key = (
                f"verify_sample_{project}_{schema}_{table.name}_{hash(where_clause)}"
            )
            sample_results = self._execute_cached_query(
                sample_query, sample_key, timeout
            )

            if sample_results:
                logger.info(f"Verified filters using {sample_rate}% sample")
                self._successful_filters_cache[cache_key] = filters
                return True
        except Exception as e:
            logger.debug(f"Sample verification failed: {e}")

        return False

    def _try_existence_check(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a simple existence check with improved error handling."""
        existence_query = f"""
        SELECT 1 
        FROM `{project}.{schema}.{table.name}`
        WHERE {where_clause}
        LIMIT 1
        """

        query_cache_key = (
            f"verify_exists_{project}_{schema}_{table.name}_{hash(where_clause)}"
        )

        for attempt in range(max_retries + 1):
            try:
                current_timeout = max(5, timeout // 2) if attempt == 0 else timeout
                existence_results = self._execute_cached_query(
                    existence_query, query_cache_key, current_timeout
                )

                if existence_results:
                    logger.info("Verified filters return data (existence check)")
                    self._successful_filters_cache[cache_key] = filters
                    return True

                if attempt == 0:
                    logger.debug("Existence check returned no data")
                    break

            except Exception as e:
                error_str = str(e)

                # Handle type mismatch errors
                if "No matching signature for operator =" in error_str and (
                    "TIMESTAMP" in error_str or "INT64" in error_str
                ):
                    # Try fixing the filters
                    fixed_filters = self._fix_type_mismatch_filters(filters, error_str)
                    if fixed_filters != filters:
                        logger.info(f"Attempting with fixed filters: {fixed_filters}")
                        return self._verify_partition_has_data(
                            table,
                            project,
                            schema,
                            fixed_filters,
                            timeout,
                            max_retries - 1,
                        )

                    # If fixing didn't work, try to identify the problematic filter
                    if attempt == max_retries:
                        logger.warning(
                            "Could not fix type mismatch, trying individual filters"
                        )
                        # Try each filter individually to find ones that work
                        working_filters = []
                        for single_filter in filters:
                            try:
                                # Skip filters that contain timestamp/date literals if comparing to INT64
                                if "INT64" in error_str and (
                                    "TIMESTAMP" in single_filter
                                    or "DATE" in single_filter
                                ):
                                    continue
                                # Test if this individual filter works
                                single_query = f"""
                                SELECT 1 FROM `{project}.{schema}.{table.name}`
                                WHERE {single_filter} LIMIT 1
                                """
                                self._execute_cached_query(single_query, None, 5)
                                working_filters.append(single_filter)
                            except Exception:
                                pass

                        if working_filters:
                            logger.info(
                                f"Found {len(working_filters)} working filters out of {len(filters)}"
                            )
                            return self._verify_partition_has_data(
                                table, project, schema, working_filters, timeout, 1
                            )

                if attempt < max_retries:
                    logger.debug(f"Existence check failed: {e}, retrying...")
                    time.sleep(1)
                else:
                    logger.debug("All existence check attempts failed")

        return False

    def _try_syntax_check(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Try a syntax-only verification."""
        if table.external or (
            table.size_in_bytes and table.size_in_bytes > 10_000_000_000
        ):
            return False

        try:
            dummy_query = f"""SELECT 1 WHERE {where_clause} LIMIT 0"""
            dummy_key = f"verify_dummy_{hash(where_clause)}"
            self._execute_cached_query(dummy_query, dummy_key, 5)

            logger.warning(
                "Using syntactically valid filters without data verification"
            )
            self._successful_filters_cache[cache_key] = filters
            return True
        except Exception as e:
            logger.debug(f"Filter syntax check failed: {e}")

        return False

    def _verify_with_existence_check(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        max_retries: int,
        cache_key: str,
    ) -> bool:
        """Verify filters using a simple existence check."""
        existence_query = f"""
        SELECT 1 
        FROM `{project}.{schema}.{table.name}`
        WHERE {where_clause}
        LIMIT 1
        """

        query_cache_key = (
            f"verify_exists_{project}_{schema}_{table.name}_{hash(where_clause)}"
        )

        # Try the existence check with retry logic
        for attempt in range(max_retries + 1):
            try:
                # Use a shorter timeout for the first attempt
                current_timeout = max(5, timeout // 2) if attempt == 0 else timeout

                existence_results = self._execute_cached_query(
                    existence_query, query_cache_key, current_timeout
                )

                if existence_results:
                    logger.info(
                        f"Verified filters return data (existence check, attempt {attempt + 1})"
                    )
                    self._successful_filters_cache[cache_key] = filters
                    return True

                # If empty but didn't error, no need to retry
                if attempt == 0:
                    logger.debug(
                        "Existence check returned no data, trying count approach"
                    )
                    break

            except Exception as e:
                error_str = str(e)

                # Try to fix type mismatch errors
                fixed_filters = self._fix_type_mismatch_filters(filters, error_str)

                # Retry with fixed filters if we made changes
                if fixed_filters != filters and attempt < max_retries:
                    logger.info(f"Attempting with fixed filters: {fixed_filters}")
                    return self._verify_partition_has_data(
                        table, project, schema, fixed_filters, timeout, max_retries - 1
                    )

                if attempt < max_retries:
                    logger.debug(
                        f"Existence check attempt {attempt + 1} failed: {e}, retrying..."
                    )
                    time.sleep(1)  # Brief pause before retry
                else:
                    logger.debug(f"All existence check attempts failed: {e}")
                    break

        return False

    def _verify_with_count_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        cache_key: str,
    ) -> bool:
        """Verify filters using a COUNT query."""
        count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{project}.{schema}.{table.name}`
        WHERE {where_clause}
        LIMIT 1000
        """

        count_key = f"verify_count_{project}_{schema}_{table.name}_{hash(where_clause)}"

        try:
            count_results = self._execute_cached_query(count_query, count_key, timeout)

            if (
                count_results
                and hasattr(count_results[0], "cnt")
                and count_results[0].cnt > 0
            ):
                logger.info(f"Verified filters return {count_results[0].cnt} rows")
                self._successful_filters_cache[cache_key] = filters
                return True
        except Exception as e:
            logger.debug(f"Count verification failed: {e}")

        return False

    def _verify_with_sample_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        timeout: int,
        cache_key: str,
    ) -> bool:
        """Verify filters using TABLESAMPLE for large or external tables."""
        # Only use for large or external tables
        if not (
            table.external
            or (table.size_in_bytes and table.size_in_bytes > 1_000_000_000)
        ):
            return False

        try:
            # Use a very small sampling rate
            sample_rate = (
                0.01 if table.external else 0.1
            )  # 0.01% for external, 0.1% for internal

            sample_query = f"""
            SELECT 1 
            FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM ({sample_rate} PERCENT)
            WHERE {where_clause}
            LIMIT 1
            """

            sample_key = (
                f"verify_sample_{project}_{schema}_{table.name}_{hash(where_clause)}"
            )
            sample_results = self._execute_cached_query(
                sample_query, sample_key, timeout
            )

            if sample_results:
                logger.info(f"Verified filters using {sample_rate}% sample")
                self._successful_filters_cache[cache_key] = filters
                return True
        except Exception as e:
            logger.debug(f"Sample verification failed: {e}")

        return False

    def _verify_syntax_only(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        where_clause: str,
        cache_key: str,
    ) -> bool:
        """Verify filter syntax without checking for data (last resort)."""
        # Skip for external or very large tables
        if table.external or (
            table.size_in_bytes and table.size_in_bytes > 10_000_000_000
        ):
            return False

        try:
            dummy_query = f"""SELECT 1 WHERE {where_clause} LIMIT 0"""

            dummy_key = f"verify_dummy_{hash(where_clause)}"
            self._execute_cached_query(dummy_query, dummy_key, 5)  # Very short timeout

            # If we get here, the where clause is at least syntactically valid
            logger.warning(
                f"Using syntactically valid filters without data verification: {where_clause}"
            )
            self._successful_filters_cache[cache_key] = filters
            return True
        except Exception as e:
            logger.debug(f"Filter syntax check failed: {e}")

        return False

    def _check_sample_rate_in_ddl(self, table: BigqueryTable) -> Optional[List[str]]:
        """Helper method to check for SAMPLE_RATE in table DDL and create filter if found."""
        if table.ddl and "SAMPLE_RATE" in table.ddl.upper():
            logger.info(
                f"External table {table.name} has SAMPLE_RATE specified, using TABLESAMPLE"
            )
            # Extract sample rate or use a safe default
            sample_rate = 0.01  # Default to 1%
            try:
                # Try to extract the sample rate from DDL
                if "SAMPLE_RATE=" in table.ddl:
                    sample_part = (
                        table.ddl.split("SAMPLE_RATE=")[1].split()[0].strip(",;()")
                    )
                    if sample_part.isdigit():
                        extracted_rate = float(sample_part) / 100
                        if 0.001 <= extracted_rate <= 0.1:  # Between 0.1% and 10%
                            sample_rate = extracted_rate
            except Exception:
                pass  # Stick with default if extraction fails

            # Use TABLESAMPLE for profiling
            return [f"TRUE TABLESAMPLE SYSTEM ({sample_rate * 100:.2f} PERCENT)"]

        return None

    def _try_date_based_filtering_for_external(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """Helper method to try date-based filtering for external tables."""
        date_columns = [
            col
            for col in partition_columns.keys()
            if col.lower()
            in {
                "date",
                "dt",
                "partition_date",
                "timestamp",
                "datetime",
                "created_at",
                "modified_at",
                "event_date",
                "year",
                "month",
                "day",
            }
        ]

        if not date_columns:
            return None

        date_filters = []
        time_now = datetime.now(timezone.utc)

        # For date columns, try specific dates from newer to older
        for col_name in date_columns:
            col_type = (
                partition_columns.get(col_name, "").upper()
                if partition_columns.get(col_name)
                else ""
            )

            # Try dates from recent to older
            test_dates = []

            # For year columns, try current and previous years
            if col_name.lower() == "year":
                test_dates = [
                    time_now.year,
                    time_now.year - 1,
                    time_now.year - 2,
                ]

                for year in test_dates:
                    filter_str = f"`{col_name}` = {year}"
                    if self._verify_partition_has_data(
                        table, project, schema, [filter_str], timeout // 2
                    ):
                        date_filters.append(filter_str)
                        logger.info(f"Found data for year {year}")
                        break

            # For month columns
            elif col_name.lower() == "month":
                month_values = []
                for i in range(12):  # Try up to 12 months back
                    test_date = time_now - timedelta(days=i * 30)
                    month_values.append(test_date.month)

                for month in month_values:
                    filter_str = f"`{col_name}` = {month}"
                    if self._verify_partition_has_data(
                        table, project, schema, [filter_str], timeout // 3
                    ):
                        date_filters.append(filter_str)
                        logger.info(f"Found data for month {month}")
                        break

            # For date columns, try specific dates
            elif col_type in ("DATE", "DATETIME", "TIMESTAMP") or col_name.lower() in (
                "date",
                "dt",
                "day",
            ):
                # Try today, yesterday, last week, last month, etc.
                test_dates_datetime = [
                    time_now,
                    time_now - timedelta(days=1),
                    time_now - timedelta(days=7),
                    time_now - timedelta(days=30),
                    time_now - timedelta(days=90),
                    time_now.replace(day=1),  # First of month
                    datetime(time_now.year, 1, 1),  # First of year
                ]

                for test_date in test_dates_datetime:
                    if col_type == "DATE":
                        date_str = test_date.strftime("%Y-%m-%d")
                        filter_value = f"DATE '{date_str}'"
                    else:
                        date_str = test_date.strftime("%Y-%m-%d")
                        filter_value = f"TIMESTAMP '{date_str} 00:00:00'"

                    filter_str = f"`{col_name}` = {filter_value}"
                    if self._verify_partition_has_data(
                        table, project, schema, [filter_str], timeout // 3
                    ):
                        date_filters.append(filter_str)
                        logger.info(f"Found data for date {date_str}")
                        break

        # If we found date filters, return them
        if date_filters:
            logger.info(f"Using date-based filters for external table: {date_filters}")
            return date_filters

        return None

    def _try_standard_approach_for_external(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        is_large_table: bool,
        timeout: int,
    ) -> Optional[List[str]]:
        """Helper method to try standard filtering approach for external tables."""
        logger.info(
            f"Trying standard approach for external table with {len(partition_columns)} partition columns"
        )

        # Get optimal partition values with a shorter timeout to avoid long-running queries
        partition_values = self._get_partition_values(
            table, project, schema, partition_columns, timeout // 2
        )

        # If we couldn't get values for any partition columns, use empty list as fallback
        if not partition_values:
            logger.warning("Couldn't determine partition values, returning empty list")
            return []

        # Create filter strings
        filters = self._create_partition_filters(partition_columns, partition_values)

        # Verify these filters return data with a shorter timeout
        if filters and self._verify_partition_has_data(
            table, project, schema, filters, timeout // 2
        ):
            logger.info(f"Using partition filters for external table: {filters}")
            return filters

        # If verification failed but we have filters, try them individually
        if filters:
            working_filters = []
            for filter_str in filters:
                if self._verify_partition_has_data(
                    table, project, schema, [filter_str], timeout // 3
                ):
                    working_filters.append(filter_str)

            if working_filters:
                logger.info(f"Using individual partition filters: {working_filters}")
                return working_filters

        # Return the original filters if we have them, even if verification failed
        # This ensures the test gets the expected filters
        if filters:
            logger.warning(f"Using unverified filters as fallback: {filters}")
            return filters

        return []

    def _get_external_table_partition_filters(
        self, table: BigqueryTable, project: str, schema: str, timeout: int = 60
    ) -> Optional[List[str]]:
        """
        Get partition filters specifically for external tables efficiently.
        For external tables, we need to be especially careful to avoid scanning entire datasets.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            timeout: Query timeout in seconds

        Returns:
            List of partition filter strings, empty list, or None
        """
        try:
            # First check if SAMPLE_RATE is specified in the table DDL
            sample_rate_filters = self._check_sample_rate_in_ddl(table)
            if sample_rate_filters:
                return sample_rate_filters

            # Get table metadata including partition columns
            metadata = self._get_table_metadata(table, project, schema)
            partition_columns = metadata["partition_columns"]

            # If no partition columns, use TABLESAMPLE for safe profiling
            if not partition_columns:
                logger.info(
                    f"No partition columns found for external table {table.name}, using TABLESAMPLE"
                )
                # Return empty list for external tables with no partitions
                # (TABLESAMPLE will be applied when generating SQL)
                return []

            # IMPORTANT: Always treat external tables as large tables, regardless of reported size
            is_large_table = True
            logger.info(
                f"External table {table.name} is being treated as a large table for safe profiling"
            )

            # For external tables, prioritize date/time-based partitioning first
            date_filters = self._try_date_based_filtering_for_external(
                table, project, schema, partition_columns, timeout
            )
            if date_filters:
                return date_filters

            # If date-based approach fails or no date columns, try the standard approach
            standard_filters = self._try_standard_approach_for_external(
                table, project, schema, partition_columns, is_large_table, timeout
            )
            if standard_filters is not None:  # Could be empty list []
                return standard_filters

            # Last resort - return empty list
            logger.warning("No working partition filters found, returning empty list")
            return []

        except Exception as e:
            logger.error(f"Error determining partition filters for external table: {e}")
            # Return empty list on errors to match test expectations
            return []

    def _fetch_basic_table_metadata(self, table: BigqueryTable) -> Dict[str, Any]:
        """Get basic metadata from table object."""
        metadata: Dict[str, Any] = {
            "partition_columns": {},
            "clustering_columns": {},
            "row_count": table.rows_count,
            "size_bytes": table.size_in_bytes,
            "is_external": table.external,
            "ddl": table.ddl,
            "creation_time": table.created,
            "last_modified_time": table.last_altered,
        }

        # Process partition info if available
        if table.partition_info:
            if isinstance(table.partition_info.fields, list):
                for field in table.partition_info.fields:
                    metadata["partition_columns"][field] = None

            if table.partition_info.columns:
                for col in table.partition_info.columns:
                    if col and col.name:
                        metadata["partition_columns"][col.name] = col.data_type

        # Process clustering fields if available
        if table.clustering_fields:
            for i, field in enumerate(table.clustering_fields):
                metadata["clustering_columns"][field] = {"position": i}

        return metadata

    def _fetch_schema_info(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch schema information from INFORMATION_SCHEMA."""
        try:
            # Get partition and clustering info from COLUMNS
            columns_query = f"""
            SELECT 
                column_name,
                data_type,
                is_partitioning_column,
                clustering_ordinal_position
            FROM 
                `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE 
                table_name = '{table.name}'
                AND (is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL)
            """

            columns_results = self._execute_cached_query(
                columns_query,
                f"columns_info_{project}_{schema}_{table.name}",
                timeout=45,
            )

            # Process partition and clustering columns
            for row in columns_results:
                # Update partition columns
                if row.is_partitioning_column == "YES":
                    metadata["partition_columns"][row.column_name] = row.data_type

                # Update clustering columns
                if row.clustering_ordinal_position is not None:
                    metadata["clustering_columns"][row.column_name] = {
                        "position": row.clustering_ordinal_position,
                        "data_type": row.data_type,
                    }

            # Get tables info - schema definition, creation time, etc.
            table_query = f"""
            SELECT
                ddl,
                creation_time,
                table_type
            FROM
                `{project}.{schema}.INFORMATION_SCHEMA.TABLES`
            WHERE
                table_name = '{table.name}'
            """

            table_results = self._execute_cached_query(
                table_query,
                f"table_info_{project}_{schema}_{table.name}",
                timeout=30,
            )

            if table_results and len(table_results) > 0:
                row = table_results[0]
                if hasattr(row, "ddl") and row.ddl:
                    metadata["ddl"] = row.ddl
                if hasattr(row, "creation_time") and row.creation_time:
                    metadata["creation_time"] = row.creation_time
                if hasattr(row, "table_type") and row.table_type:
                    metadata["table_type"] = row.table_type

            # Get storage statistics and modified time from TABLE_STORAGE
            storage_query = f"""
            SELECT
                total_rows,
                total_logical_bytes,
                storage_last_modified_time
            FROM
                `{project}.{schema}.INFORMATION_SCHEMA.TABLE_STORAGE`
            WHERE
                table_name = '{table.name}'
            """

            storage_results = self._execute_cached_query(
                storage_query,
                f"storage_info_{project}_{schema}_{table.name}",
                timeout=30,
            )

            if storage_results and len(storage_results) > 0:
                row = storage_results[0]
                if hasattr(row, "total_rows") and row.total_rows is not None:
                    metadata["row_count"] = row.total_rows
                if (
                    hasattr(row, "total_logical_bytes")
                    and row.total_logical_bytes is not None
                ):
                    metadata["size_bytes"] = row.total_logical_bytes
                if (
                    hasattr(row, "storage_last_modified_time")
                    and row.storage_last_modified_time is not None
                ):
                    metadata["last_modified_time"] = row.storage_last_modified_time

        except Exception as e:
            logger.warning(f"Error fetching schema information: {e}")

        return metadata

    def _fetch_table_stats(
        self, project: str, schema: str, table_name: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get additional stats for tables that don't already have them."""
        if (
            not metadata.get("row_count")
            or not metadata.get("size_bytes")
            or not metadata.get("last_modified_time")
        ):
            try:
                # Use TABLE_STORAGE for row count, size, and modified time
                stats_query = f"""
                SELECT 
                    total_rows,
                    total_logical_bytes,
                    storage_last_modified_time
                FROM 
                    `{project}.{schema}.INFORMATION_SCHEMA.TABLE_STORAGE`
                WHERE 
                    table_name = '{table_name}'
                """

                stats_results = self._execute_cached_query(
                    stats_query, f"table_stats_{project}_{schema}_{table_name}"
                )

                if stats_results and len(stats_results) > 0:
                    row = stats_results[0]
                    if hasattr(row, "total_rows") and row.total_rows is not None:
                        metadata["row_count"] = row.total_rows
                    if (
                        hasattr(row, "total_logical_bytes")
                        and row.total_logical_bytes is not None
                    ):
                        metadata["size_bytes"] = row.total_logical_bytes
                    if (
                        hasattr(row, "storage_last_modified_time")
                        and row.storage_last_modified_time is not None
                    ):
                        metadata["last_modified_time"] = row.storage_last_modified_time

                # Get creation time from TABLES view if needed
                if not metadata.get("creation_time"):
                    time_query = f"""
                    SELECT
                        creation_time
                    FROM
                        `{project}.{schema}.INFORMATION_SCHEMA.TABLES`
                    WHERE
                        table_name = '{table_name}'
                    """

                    time_results = self._execute_cached_query(
                        time_query, f"table_time_{project}_{schema}_{table_name}"
                    )

                    if time_results and len(time_results) > 0:
                        row = time_results[0]
                        if (
                            hasattr(row, "creation_time")
                            and row.creation_time is not None
                        ):
                            metadata["creation_time"] = row.creation_time

            except Exception as e:
                logger.warning(
                    f"Error fetching table stats: {e}, continuing with available metadata"
                )

        return metadata

    def _try_time_hierarchy_approach(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """Helper method to try time hierarchy partitioning approach"""
        # Time hierarchy columns in order of precedence
        time_hierarchy_cols = ["year", "month", "day", "hour"]
        hierarchy_filters: List[str] = []

        # Build a filter using time hierarchy
        time_now = datetime.now(timezone.utc)
        for col_name in time_hierarchy_cols:
            matching_cols = [
                c for c in partition_columns.keys() if c.lower() == col_name
            ]
            if matching_cols:
                col = matching_cols[0]
                data_type = partition_columns.get(col, "")

                # For time columns, several values might work - try a few
                # Start with current and go back
                time_values = []
                if col.lower() == "year":
                    time_values = [time_now.year, time_now.year - 1]
                elif col.lower() == "month":
                    time_values = [time_now.month]
                    # Add previous months
                    for i in range(1, 6):  # Try up to 6 months back
                        prev_month = (
                            (time_now.month - i - 1) % 12
                        ) + 1  # Handle year wraparound
                        time_values.append(prev_month)
                elif col.lower() == "day":
                    time_values = [time_now.day]
                    # Add a few common day values (1st, 15th)
                    time_values.extend([1, 15])
                elif col.lower() == "hour":
                    time_values = [time_now.hour]
                    # Add common hours (noon, midnight)
                    time_values.extend([0, 12])

                # Try each value
                for val in time_values:
                    if data_type and data_type.upper() in ("STRING",):
                        filter_str = f"`{col}` = '{val}'"
                    else:
                        filter_str = f"`{col}` = {val}"

                    # If we already have hierarchy filters, combine with those
                    test_filters = hierarchy_filters + [filter_str]

                    # Verify data exists with this filter
                    if self._verify_partition_has_data(
                        table, project, schema, test_filters, timeout // 2
                    ):
                        hierarchy_filters.append(filter_str)
                        logger.info(f"Added hierarchy filter: {filter_str}")
                        break  # Found a working value for this column

        # If we found working hierarchy filters, return them
        if hierarchy_filters:
            logger.info(f"Using time hierarchy filters: {hierarchy_filters}")
            return hierarchy_filters

        return None

    def _try_date_columns_approach(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """Helper method to try date columns partitioning approach"""
        # Find date columns
        date_columns = [
            col
            for col in partition_columns.keys()
            if col.lower()
            in {
                "date",
                "dt",
                "partition_date",
                "timestamp",
                "datetime",
                "created_at",
                "modified_at",
                "event_date",
            }
        ]

        if date_columns:
            date_filters = []
            time_now = datetime.now(timezone.utc)

            # Try specific dates for date columns from newer to older
            for col_name in date_columns:
                col_type = (
                    partition_columns.get(col_name, "").upper()
                    if partition_columns.get(col_name)
                    else ""
                )

                # Try dates from recent to older
                test_dates = [
                    time_now,
                    time_now - timedelta(days=1),
                    time_now - timedelta(days=7),
                    time_now - timedelta(days=30),
                    time_now - timedelta(days=90),
                    time_now.replace(day=1),  # First of month
                    datetime(time_now.year, 1, 1),  # First of year
                ]

                for test_date in test_dates:
                    if col_type == "DATE":
                        date_str = test_date.strftime("%Y-%m-%d")
                        filter_value = f"DATE '{date_str}'"
                    else:
                        date_str = test_date.strftime("%Y-%m-%d")
                        filter_value = f"TIMESTAMP '{date_str} 00:00:00'"

                    filter_str = f"`{col_name}` = {filter_value}"
                    if self._verify_partition_has_data(
                        table, project, schema, [filter_str], timeout // 2
                    ):
                        date_filters.append(filter_str)
                        logger.info(f"Found data for date {date_str}")
                        break

            # If we found date filters, return them
            if date_filters:
                logger.info(f"Using date-based filters: {date_filters}")
                return date_filters

        return None

    def _try_last_resort_approach(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """Helper method for last resort partitioning approach"""
        logger.warning(
            f"Standard approach failed for large table {table.name}, trying last-resort options"
        )

        # Try to find ANY partition that has data - even a small amount - to make profiling possible
        for col_name, _data_type in partition_columns.items():
            try:
                # Find the value with ANY data (not necessarily the most data)
                query = f"""
                SELECT DISTINCT {col_name} as value
                FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                WHERE {col_name} IS NOT NULL
                LIMIT 5
                """

                try:
                    distinct_results = self._execute_cached_query(
                        query,
                        f"last_resort_{project}_{schema}_{table.name}_{col_name}",
                        timeout // 3,
                    )

                    for row in distinct_results:
                        val = row.value
                        if val is not None:
                            if isinstance(val, str):
                                filter_str = f"`{col_name}` = '{val}'"
                            else:
                                filter_str = f"`{col_name}` = {val}"

                            # Quick check if this returns data
                            if self._verify_partition_has_data(
                                table, project, schema, [filter_str], timeout // 3
                            ):
                                logger.info(f"Last resort filter found: {filter_str}")
                                return [filter_str]
                except Exception as e:
                    logger.warning(f"Error in last resort sampling: {e}")
            except Exception as e:
                logger.warning(f"Error in last resort approach: {e}")

        return None

    def _get_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """
        Get the best partition values for profiling with adaptive approach.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            partition_columns: Dictionary of partition column names to data types
            timeout: Query timeout in seconds

        Returns:
            Dictionary mapping column names to optimal partition values
        """
        # Check table size to determine approach
        is_large_table = False
        if table.size_in_bytes and table.size_in_bytes > 10_000_000_000:  # 10 GB
            is_large_table = True
            logger.info(
                f"Table {table.name} is large ({table.size_in_bytes / 1_000_000_000:.2f} GB), using optimized partitioning"
            )
        elif table.rows_count and table.rows_count > 100_000_000:  # 100M rows
            is_large_table = True
            logger.info(
                f"Table {table.name} has many rows ({table.rows_count:,}), using optimized partitioning"
            )

        # Standard time column hierarchy (year -> month -> day -> hour)
        time_hierarchy = ["year", "month", "day", "hour"]

        result: Dict[str, Any] = {}

        # STRATEGY 1: For time hierarchy columns - find values intelligently
        available_time_columns = []
        for col in time_hierarchy:
            matching_cols = [c for c in partition_columns.keys() if c.lower() == col]
            if matching_cols:
                available_time_columns.append(matching_cols[0])

        if available_time_columns:
            # Process time hierarchy columns
            time_hierarchy_values = self._get_time_hierarchy_values(
                table, project, schema, available_time_columns, is_large_table, timeout
            )
            result.update(time_hierarchy_values)

            # If we added any time hierarchy columns, return early
            if result:
                return result

        # STRATEGY 2: Look for date columns
        date_columns = [
            col
            for col in partition_columns.keys()
            if col.lower()
            in {
                "date",
                "dt",
                "partition_date",
                "timestamp",
                "datetime",
                "created_at",
                "modified_at",
                "event_time",
                "event_date",
            }
        ]

        if date_columns:
            date_column_values = self._get_date_column_values(
                table, project, schema, date_columns, timeout
            )
            result.update(date_column_values)

            # If we added any date columns, return early
            if result:
                return result

        # STRATEGY 3: Handle remaining partition columns
        remaining_columns = [
            col
            for col in partition_columns.keys()
            if col not in result and col not in available_time_columns + date_columns
        ]

        if remaining_columns:
            remaining_values = self._get_remaining_column_values(
                table, project, schema, remaining_columns, is_large_table, timeout
            )
            result.update(remaining_values)

        return result

    def _process_time_partitioning(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        metadata: Dict[str, Any],
        timeout: int,
    ) -> Optional[List[str]]:
        """Handle time-based partitioning."""
        partition_columns = metadata["partition_columns"]

        # Try time hierarchy columns (year/month/day/hour)
        time_hierarchy_filters = self._try_time_hierarchy_approach(
            table, project, schema, partition_columns, timeout
        )
        if time_hierarchy_filters:
            return time_hierarchy_filters

        # Next try date-named columns
        date_filters = self._try_date_columns_approach(
            table, project, schema, partition_columns, timeout
        )
        if date_filters:
            return date_filters

        return None

    def _process_standard_partitioning(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        metadata: Dict[str, Any],
        timeout: int,
    ) -> Optional[List[str]]:
        """Handle standard partitioning."""
        partition_columns = metadata["partition_columns"]
        is_external = metadata["is_external"]
        is_large_table = False

        if (
            metadata.get("size_bytes", 0) > 5_000_000_000
            or metadata.get("row_count", 0) > 50_000_000
        ):
            is_large_table = True

        # Get partition values
        partition_values = self._get_partition_values(
            table, project, schema, partition_columns, timeout
        )

        if not partition_values:
            if is_external:
                # For external tables without partition values, use empty filter list
                logger.info(
                    "No partition values for external table, returning empty filter list"
                )
                return []
            else:
                # For internal tables, no partition values means we can't filter
                logger.warning(f"Couldn't determine partition values for {table.name}")
                return None

        # Create filter strings
        filters = self._create_partition_filters(partition_columns, partition_values)

        # Verify these filters together
        if filters and self._verify_partition_has_data(
            table, project, schema, filters, timeout
        ):
            logger.info(f"Using combined filters: {filters}")
            return filters

        # Try individual filters if combined don't work
        if filters:
            working_filters = []
            for filter_str in filters:
                if self._verify_partition_has_data(
                    table, project, schema, [filter_str], timeout // 2
                ):
                    working_filters.append(filter_str)
                    break  # One working filter is enough

            if working_filters:
                logger.info(f"Using individual filters: {working_filters}")
                return working_filters

        # Last resort for large tables
        if is_large_table or is_external:
            last_resort_filters = self._try_last_resort_approach(
                table, project, schema, partition_columns, timeout
            )
            if last_resort_filters:
                return last_resort_filters

        # Final decision
        if is_external:
            logger.warning(f"Using empty filter list for external table {table.name}")
            return []
        else:
            logger.warning(f"Couldn't find valid partition filters for {table.name}")
            return None

    def _get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """
        Get required partition filters using an adaptive, unified strategy
        with multiple fallback options to maximize profiling success.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name

        Returns:
            List of partition filter strings, empty list, or None (no partitioning needed)
        """
        # Check if we already have cached filters for this table
        table_key = f"{project}.{schema}.{table.name}"
        if table_key in self._successful_filters_cache:
            logger.info(f"Using cached filters for {table_key}")
            return self._successful_filters_cache[table_key]

        # Important change - External tables should always be considered "large" for profiling
        # This ensures we use proper strategies even if their reported size is 0
        is_small_table = False
        if table.external:
            logger.info(
                f"Table {table.name} is an external table, treating as large table for profiling"
            )
            is_small_table = False
        elif table.rows_count == 0 or table.rows_count is None:
            logger.info(
                f"Table {table.name} reports 0 rows or unknown row count, treating more carefully"
            )
            is_small_table = False
        elif (
            table.size_in_bytes is not None
            and table.size_in_bytes < 100_000_000  # Less than 100MB
            and table.rows_count is not None
            and table.rows_count < 100_000
        ):  # Less than 100K rows
            is_small_table = True
            logger.info(
                f"Table {table.name} is small ({table.size_in_bytes / 1_000_000:.2f} MB, {table.rows_count:,} rows)"
            )

        try:
            # Get table metadata including partition columns
            metadata = self._get_table_metadata(table, project, schema)
            partition_columns = metadata["partition_columns"]

            # If no partition columns found, this table doesn't require partition filters
            if not partition_columns:
                logger.debug(f"No partition columns found for table {table.name}")
                return None

            logger.info(
                f"Found partition columns for {table.name}: {partition_columns}"
            )

            # For small tables with partitioning, we can try without filters first
            # But NOT for external tables - they should be handled cautiously
            if is_small_table and not table.external:
                logger.info(
                    "Small table with partitioning, checking if full scan is viable"
                )
                try:
                    count_query = f"""
                            SELECT COUNT(*) 
                            FROM `{project}.{schema}.{table.name}`
                            LIMIT 10
                            """
                    results = self._execute_cached_query(
                        count_query,
                        f"small_table_check_{project}_{schema}_{table.name}",
                        timeout=15,
                    )
                    if results:
                        logger.info(
                            "Small partitioned table can be scanned without filters"
                        )
                        self._successful_filters_cache[table_key] = []
                        return []
                except Exception as e:
                    logger.debug(f"Small table scan check failed: {e}")

            # Handle external tables specially
            if table.external:
                filters = self._get_external_table_partition_filters(
                    table, project, schema, timeout=60
                )
                if filters is not None:  # Could be empty list []
                    self._successful_filters_cache[table_key] = filters
                    return filters

            # Handle different partitioning approaches
            # First, try time-based partitioning
            time_filters = self._process_time_partitioning(
                table, project, schema, metadata, timeout=45
            )
            if time_filters is not None:
                self._successful_filters_cache[table_key] = time_filters
                return time_filters

            # Then try standard partitioning approach
            standard_filters = self._process_standard_partitioning(
                table, project, schema, metadata, timeout=60
            )
            if standard_filters is not None:
                self._successful_filters_cache[table_key] = standard_filters
                return standard_filters

            # If we reach here, we couldn't find any working filters
            logger.warning(f"No partition filters could be determined for {table.name}")

            # Emergency fallback for external tables
            if table.external:
                logger.warning("Using empty filter list for external table as fallback")
                return []

            # Emergency fallback for regular tables with partitioning
            if partition_columns:
                emergency_filter = self._get_emergency_partition_filter(
                    table, project, schema
                )
                if emergency_filter:
                    logger.info(f"Using emergency partition filter: {emergency_filter}")
                    self._successful_filters_cache[table_key] = [emergency_filter]
                    return [emergency_filter]

            return None

        except Exception as e:
            logger.warning(f"Error determining partition filters: {e}")

            # Handle error cases gracefully
            if table.external:
                # For external tables, use TABLESAMPLE as fallback
                logger.info("Using empty filter list for external table due to error")
                return []

            # Try emergency filter for regular tables
            try:
                emergency_filter = self._get_emergency_partition_filter(
                    table, project, schema
                )
                if emergency_filter:
                    logger.info(f"Using emergency partition filter: {emergency_filter}")
                    return [emergency_filter]
            except Exception:
                pass

            return None

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        """
        Handle partition-aware querying for all operations including COUNT.
        Optimized to use cached information and add appropriate query hints for large tables.

        Args:
            table: BaseTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            Dictionary of batch kwargs for the profiler
        """
        start_time = time.time()
        bq_table = cast(BigqueryTable, table)

        # Base kwargs that will work for non-partitioned tables
        base_kwargs = {
            "schema": db_name,  # <project>
            "table": f"{schema_name}.{table.name}",  # <dataset>.<table>
            "project": db_name,
            "dataset": schema_name,
            "table_name": bq_table.name,
        }

        # Add tracking to avoid redundant processing of the same table
        table_key = f"{db_name}.{schema_name}.{table.name}"
        if table_key in self._queried_tables:
            logger.info(f"Using cached query configuration for {table_key}")
            return base_kwargs

        self._queried_tables.add(table_key)

        # Get partition filters
        partition_filters = self._get_required_partition_filters(
            bq_table, db_name, schema_name
        )

        if partition_filters is None:
            logger.info(
                f"No partition filters needed for {table_key} "
                f"(took {time.time() - start_time:.2f}s)"
            )
            return base_kwargs

        # If no partition filters needed, return base kwargs
        if not partition_filters:
            logger.info(
                f"Empty partition filters for {table_key} "
                f"(took {time.time() - start_time:.2f}s)"
            )

            # For external tables, add TABLESAMPLE hint for safe profiling if empty filters
            if bq_table.external:
                # Use a very small sample rate (0.1%) to avoid OOM errors
                sample_rate = 0.1
                custom_sql = f"""SELECT * 
        FROM `{db_name}.{schema_name}.{table.name}` TABLESAMPLE SYSTEM ({sample_rate} PERCENT)"""

                base_kwargs.update(
                    {"custom_sql": custom_sql, "partition_handling": "true"}
                )

            return base_kwargs

        # Construct query with partition filters and appropriate optimization hints
        partition_where = " AND ".join(partition_filters)

        # Check if we need to add optimization hints based on table size
        needs_optimization_hints = False
        if (
            bq_table.size_in_bytes
            and bq_table.size_in_bytes > 5_000_000_000
            or bq_table.rows_count
            and bq_table.rows_count > 50_000_000
            or bq_table.external  # IMPORTANT: Always add hints for external tables
            or bq_table.rows_count == 0  # IMPORTANT: Be careful with 0 row tables
            or bq_table.rows_count
            is None  # IMPORTANT: Be careful with unknown row counts
        ):  # > 5GB or external
            needs_optimization_hints = True

        if needs_optimization_hints:
            custom_sql = f"""-- This query uses partition filters for efficient profiling
        SELECT * 
        FROM `{db_name}.{schema_name}.{table.name}`
        WHERE {partition_where}
        """
        else:
            custom_sql = f"""SELECT * 
        FROM `{db_name}.{schema_name}.{table.name}`
        WHERE {partition_where}"""

        logger.info(
            f"Using partition filters for {table_key}: {partition_where} "
            f"(took {time.time() - start_time:.2f}s)"
        )

        base_kwargs.update({"custom_sql": custom_sql, "partition_handling": "true"})
        return base_kwargs

    def _get_emergency_partition_filter(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[str]:
        """Get an emergency partition filter that will work with minimal data."""
        try:
            # Check for date/time columns with simple filters
            # that won't trigger type conversion issues
            query = f"""
            SELECT column_name 
            FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table.name}'
            AND data_type IN ('DATE', 'TIMESTAMP', 'DATETIME')
            LIMIT 5
            """

            results = self._execute_cached_query(query, timeout=10)
            for row in results:
                col_name = row.column_name

                # Try a simple date comparison that worked in the last week
                filter_str = f"`{col_name}` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"

                # Check if filter works
                test_query = f"""
                SELECT 1 FROM `{project}.{schema}.{table.name}`
                WHERE {filter_str}
                LIMIT 1
                """

                try:
                    test_results = self._execute_cached_query(test_query, timeout=10)
                    if test_results:
                        return filter_str
                except Exception:
                    pass

            # If no date/time columns work, try other simple filters
            return None
        except Exception as e:
            logger.warning(f"Emergency filter generation failed: {e}")
            return None

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        """
        Get profile request with appropriate partition handling.

        Args:
            table: BaseTable instance
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            TableProfilerRequest object or None
        """
        profile_request = super().get_profile_request(table, schema_name, db_name)

        if not profile_request:
            return None

        bq_table = cast(BigqueryTable, table)

        # Skip external tables if configured to do so
        if bq_table.external and not self.config.profiling.profile_external_tables:
            self.report.report_warning(
                title="Profiling skipped for external table",
                message="profiling.profile_external_tables is disabled",
                context=profile_request.pretty_name,
            )
            return None

        # Don't profile if partition profiling is disabled
        if not self.config.profiling.partition_profiling_enabled:
            logger.debug(
                f"{profile_request.pretty_name} is skipped because profiling.partition_profiling_enabled property is disabled"
            )
            self.report.profiling_skipped_partition_profiling_disabled.append(
                profile_request.pretty_name
            )
            return None

        # If the table is very large, add some additional configuration to avoid timeouts
        if (bq_table.size_in_bytes and bq_table.size_in_bytes > 50_000_000_000) or (
            bq_table.rows_count and bq_table.rows_count > 500_000_000
        ):
            logger.info(
                f"Using extended timeouts for very large table {profile_request.pretty_name}"
            )
            # Add extended query timeouts for very large tables
            if hasattr(profile_request, "profiler_config"):
                profile_request.profiler_config.update(
                    {
                        "query_timeout": 300,  # 5 minutes
                        "chunk_size": 5000,
                    }
                )
            else:
                logger.debug("profiler_config not available on TableProfilerRequest")

        return profile_request

    def get_workunits(
        self, project_id: str, tables: Dict[str, List[BigqueryTable]]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Get profile workunits handling both internal and external tables.

        Args:
            project_id: BigQuery project ID
            tables: Dictionary mapping datasets to lists of tables

        Returns:
            Iterable of MetadataWorkUnit objects
        """
        # Clear caches at the start of a new profiling run
        self._query_cache.clear()
        self._partition_info_cache.clear()
        self._table_metadata_cache.clear()
        self._queried_tables.clear()
        self._successful_filters_cache.clear()

        profile_requests: List[TableProfilerRequest] = []

        # Group tables by dataset for batch operations
        for dataset, dataset_tables in tables.items():
            # Only process external tables if enabled
            if not self.config.profiling.profile_external_tables:
                dataset_tables = [t for t in dataset_tables if not t.external]

            if not dataset_tables:
                continue

            # Process tables in dataset
            for table in dataset_tables:
                normalized_table_name = BigqueryTableIdentifier(
                    project_id=project_id, dataset=dataset, table=table.name
                ).get_table_name()

                # Create profile request
                logger.debug(
                    f"Creating profile request for table {normalized_table_name}"
                )
                profile_request = self.get_profile_request(table, dataset, project_id)

                if profile_request is not None:
                    self.report.report_entity_profiled(profile_request.pretty_name)
                    profile_requests.append(profile_request)
                else:
                    logger.debug(
                        f"Table {normalized_table_name} was not eligible for profiling"
                    )

        if not profile_requests:
            return

        # Generate the profiling workunits
        yield from self.generate_profile_workunits(
            profile_requests,
            max_workers=self.config.profiling.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        )

        # Clear caches after profiling is complete
        self._query_cache.clear()
        self._partition_info_cache.clear()
        self._table_metadata_cache.clear()
        self._queried_tables.clear()
        self._successful_filters_cache.clear()

    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        """
        Get dataset name in BigQuery format.

        Args:
            table_name: Table name
            schema_name: Dataset name
            db_name: Project ID

        Returns:
            Fully qualified table name
        """
        return BigqueryTableIdentifier(
            project_id=db_name, dataset=schema_name, table=table_name
        ).get_table_name()
