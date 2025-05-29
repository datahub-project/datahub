import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta

from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable

logger = logging.getLogger(__name__)


class BigQueryPartitionManager:
    """
    Manager for BigQuery partition operations.
    Handles extracting partition values and creating optimal partition filtering.
    """

    def __init__(self, execute_query_callback, filter_builder):
        """
        Initialize the partition manager.

        Args:
            execute_query_callback: Callback function to execute BigQuery queries
            filter_builder: Instance of BigQueryFilterBuilder for creating filters
        """
        self._execute_query = execute_query_callback
        self._filter_builder = filter_builder
        self._successful_filters_cache: Dict[str, List[str]] = {}
        self._last_error_message: Optional[str] = None

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

    def get_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """
        Get the best partition values for profiling with an optimized batch approach.
        Improved to handle multiple partition keys more efficiently.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            partition_columns: Dictionary of partition column names to data types
            timeout: Query timeout in seconds

        Returns:
            Dictionary mapping column names to optimal partition values
        """
        if not partition_columns:
            return {}

        # Check table size to determine approach
        is_large_table = False
        is_external_table = table.external

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

        # Always consider external tables as large for safety
        if is_external_table:
            is_large_table = True
            logger.info(f"Table {table.name} is external, treating as large table")

        # Group partition columns by type for specialized handling
        time_hierarchy_cols = []  # year, month, day, hour
        date_cols = []  # date, timestamp, datetime, etc.
        remaining_cols = []  # everything else

        # Standard time column hierarchy
        time_hierarchy = ["year", "month", "day", "hour"]

        for col in partition_columns:
            col_lower = col.lower()
            if col_lower in time_hierarchy:
                time_hierarchy_cols.append(col)
            elif col_lower in [
                "date",
                "dt",
                "partition_date",
                "timestamp",
                "datetime",
                "created_at",
                "modified_at",
                "event_time",
                "event_date",
            ]:
                date_cols.append(col)
            else:
                remaining_cols.append(col)

        # New optimized approach: batch fetch partition values when possible
        result = {}

        # 1. First attempt to get values for multiple columns at once (most efficient)
        if not is_large_table and len(partition_columns) <= 5:
            batch_result = self._batch_fetch_partition_values(
                table, project, schema, list(partition_columns.keys()), timeout
            )
            if batch_result:
                logger.info(
                    f"Successfully fetched {len(batch_result)} partition values in batch"
                )
                result.update(batch_result)
                # If we got values for all columns, return early
                if len(result) == len(partition_columns):
                    return result

        # 2. Handle time hierarchy columns - important for time-partitioned tables
        if time_hierarchy_cols and not all(
            col in result for col in time_hierarchy_cols
        ):
            time_hierarchy_values = self._get_time_hierarchy_values(
                table,
                project,
                schema,
                [col for col in time_hierarchy_cols if col not in result],
                is_large_table,
                timeout,
            )
            result.update(time_hierarchy_values)

        # 3. Process date columns - often critical for partitioning
        if date_cols and not all(col in result for col in date_cols):
            date_column_values = self._get_date_column_values(
                table,
                project,
                schema,
                [col for col in date_cols if col not in result],
                timeout,
            )
            result.update(date_column_values)

        # 4. Process any remaining columns
        remaining_to_process = [col for col in remaining_cols if col not in result]
        if remaining_to_process:
            remaining_values = self._get_remaining_column_values(
                table, project, schema, remaining_to_process, is_large_table, timeout
            )
            result.update(remaining_values)

        return result

    def _batch_fetch_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        timeout: int,
    ) -> Dict[str, Any]:
        """
        Efficiently fetch values for multiple partition columns at once.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: Dataset name
            partition_columns: List of partition column names
            timeout: Query timeout in seconds

        Returns:
            Dictionary mapping column names to optimal partition values
        """
        if not partition_columns:
            return {}

        result = {}

        # External tables need special handling - use sampling
        sampling_clause = ""
        if table.external:
            sampling_clause = " TABLESAMPLE SYSTEM (0.1 PERCENT)"

        # Build a query that gets the most frequent values for all partition columns at once
        columns_select = ", ".join([f"{col}" for col in partition_columns])
        columns_where = " AND ".join(
            [f"{col} IS NOT NULL" for col in partition_columns]
        )
        count_groupby = ", ".join(partition_columns)

        batch_query = f"""
        WITH PartitionStats AS (
            SELECT 
                {columns_select},
                COUNT(*) as row_count
            FROM 
                `{project}.{schema}.{table.name}`{sampling_clause}
            WHERE 
                {columns_where}
            GROUP BY 
                {count_groupby}
            ORDER BY 
                row_count DESC
            LIMIT 1
        )
        SELECT * FROM PartitionStats
        """

        try:
            cache_key = f"batch_partitions_{project}_{schema}_{table.name}_{hash(tuple(sorted(partition_columns)))}"
            batch_results = self._execute_query(batch_query, cache_key, timeout=timeout)

            if batch_results and len(batch_results) > 0:
                for col in partition_columns:
                    val = getattr(batch_results[0], col, None)
                    if val is not None:
                        result[col] = val
                        logger.info(f"Batch query: found value for {col} = {val}")

        except Exception as e:
            logger.debug(
                f"Batch partition query failed: {e}, will fall back to individual queries"
            )

        return result

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
                    col_results = self._execute_query(
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
            date_results = self._execute_query(
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
                col_results = self._execute_query(
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

                    sample_results = self._execute_query(
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

                combined_results = self._execute_query(
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

                    col_results = self._execute_query(
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

    def _extract_partition_info_from_error(
        self, error_message: str
    ) -> Optional[Dict[str, str]]:
        """
        Extract partition information from BigQuery error messages.
        Enhanced to handle multiple partition column requirements.

        Args:
            error_message: The error message string to parse

        Returns:
            Dictionary with partition info or None
        """
        if not error_message:
            return None

        logger.debug(
            f"Attempting to extract partition info from error: {error_message}"
        )

        # First handle the most common error format with single quotes around column names
        # Example: "Cannot query over table 'project.dataset.table' without a filter over column(s) 'day', 'feedhandler', 'month', 'year' that can be used for partition elimination"
        # The regex below handles both single columns and multiple columns with comma separation
        multi_column_pattern = r"Cannot query over table '[^']+' without a filter over column\(s\) '([^']+)'(?:, '([^']+)')?(?:, '([^']+)')?(?:, '([^']+)')?(?:, '([^']+)')?(?: that can be used for partition elimination)?"
        multi_match = re.search(multi_column_pattern, error_message)
        if multi_match:
            columns = []
            for i in range(1, 6):  # Check up to 5 captured groups
                if multi_match.group(i):
                    columns.append(multi_match.group(i))

            if columns:
                return {
                    "type": "multi_column_partition",
                    "required_columns": ", ".join(columns),
                }

        # Handle comma-separated column list format with single quotes
        # Alternative pattern that captures all columns at once
        quoted_cols_pattern = r"Cannot query over table '[^']+' without a filter over column\(s\) '([^']+(?:',\s*'[^']+)*)' that can be used for partition elimination"
        quoted_cols_match = re.search(quoted_cols_pattern, error_message)
        if quoted_cols_match:
            quoted_cols_str = quoted_cols_match.group(1)
            # Split by "', '" pattern to get individual column names
            columns = [col.strip("' ") for col in re.split(r"',\s*'", quoted_cols_str)]
            if columns:
                return {
                    "type": "multi_column_partition",
                    "required_columns": ", ".join(columns),
                }

        # Handle comma-separated column list format without quotes around each column
        # Example: "Cannot query over table 'project.dataset.table' without a filter over column(s) day, feedhandler, month, year that can be used for partition elimination"
        comma_pattern = r"Cannot query over table '[^']+' without a filter over column\(s\) ([a-zA-Z0-9_,\s]+) that can be used for partition elimination"
        comma_match = re.search(comma_pattern, error_message)
        if comma_match and comma_match.group(1):
            columns = [col.strip() for col in comma_match.group(1).split(",")]
            if columns:
                return {
                    "type": "multi_column_partition",
                    "required_columns": ", ".join(columns),
                }

        # Original patterns for single column requirements
        single_column_pattern = r"requires that the query filter by range on `([^`]+)`"
        match = re.search(single_column_pattern, error_message)
        if match:
            return {"type": "range_partition", "column": match.group(1)}

        single_column_alternative = r"filter by partition column `([^`]+)`"
        alt_match = re.search(single_column_alternative, error_message)
        if alt_match:
            return {"type": "partition", "column": alt_match.group(1)}

        return None

    def get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[List[str]]:
        """
        Detect required partition columns for a table and generate appropriate filters.
        Enhanced to better handle multi-column partition requirements.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name

        Returns:
            List of filter strings or None if no partitioning is required
        """
        table_id = f"{project}.{schema}.{table.name}"
        logger.debug(f"Getting required partition filters for {table_id}")

        # Check cache first
        cached_filters = self._check_filters_cache(table_id)
        if cached_filters is not None:
            return cached_filters

        # Get table metadata including partition info
        metadata = self._get_table_metadata(table, project, schema)
        if not metadata:
            return None

        # Try different approaches to find partition filters
        filters = self._try_all_partition_approaches(table, project, schema, metadata)

        # If we found filters, cache them for future use
        if filters:
            self._successful_filters_cache[table_id] = filters

        return filters

    def _check_filters_cache(self, table_id: str) -> Optional[List[str]]:
        """Check if we have cached filters for this table."""
        if table_id in self._successful_filters_cache:
            cached_filters = self._successful_filters_cache[table_id]
            if cached_filters:
                logger.debug(f"Using cached partition filters for {table_id}")
                return cached_filters
        return None

    def _try_all_partition_approaches(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Optional[List[str]]:
        """Try all partition approaches in order of preference."""
        table_id = f"{project}.{schema}.{table.name}"

        # Try each approach in order
        approaches = [
            # 1. External table approach
            self._try_external_table_approach,
            # 2. Time-based partitioning
            self._try_time_based_partitioning,
            # 3. Standard partitioning
            self._try_standard_partitioning,
            # 4. Multi-column from error messages
            self._try_error_based_partitioning,
            # 5. Last resort approach
            self._try_last_resort_approach,
        ]

        for approach_func in approaches:
            filters = approach_func(table, project, schema, metadata)
            if filters is not None:
                logger.debug(
                    f"Found filters using {approach_func.__name__} for {table_id}"
                )
                return filters

        # If we reach here, we couldn't find any filters
        logger.warning(f"Could not determine partition filters for {table_id}")
        return None

    def _try_external_table_approach(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Optional[List[str]]:
        """Try external table approach."""
        if not table.external:
            return None

        filters = self._get_external_table_partition_filters(
            table, project, schema, metadata, timeout=30
        )

        if filters:
            logger.debug(
                f"Found external table partition filters for {project}.{schema}.{table.name}"
            )
            return filters

        return None

    def _try_time_based_partitioning(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Optional[List[str]]:
        """Try time-based partitioning approach."""
        filters = self._process_time_partitioning(
            table, project, schema, metadata, timeout=30
        )

        if filters:
            logger.debug(
                f"Found time-based partition filters for {project}.{schema}.{table.name}"
            )
            return filters

        return None

    def _try_standard_partitioning(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Optional[List[str]]:
        """Try standard partitioning approach."""
        filters = self._process_standard_partitioning(
            table, project, schema, metadata, timeout=30
        )

        if filters:
            logger.debug(
                f"Found standard partition filters for {project}.{schema}.{table.name}"
            )
            return filters

        return None

    def _try_error_based_partitioning(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Optional[List[str]]:
        """
        Try partitioning based on error message analysis.
        This method is particularly important for external tables that require
        specific partition filters but don't expose that information in metadata.
        """
        # Get error message from available sources
        error_message = self._get_error_message_from_sources()
        if not error_message:
            return None

        logger.info(f"Analyzing error message for partition info: {error_message}")
        partition_info = self._extract_partition_info_from_error(error_message)

        if not partition_info:
            logger.warning("Failed to extract partition info from error message")
            return None

        # Handle multi-column partition requirement
        if partition_info.get("type") == "multi_column_partition":
            return self._handle_multi_column_partition(
                table, project, schema, partition_info
            )
        # Handle single column partitioning
        elif partition_info.get("type") in ["range_partition", "partition"]:
            return self._handle_single_column_partition(
                table, project, schema, partition_info
            )

        return None

    def _get_error_message_from_sources(self) -> Optional[str]:
        """Get error message from available sources."""
        error_message = getattr(self, "_last_error_message", None)
        if not error_message:
            # Try to get the error from the profiler if available
            from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler

            if hasattr(self, "_execute_query") and hasattr(
                self._execute_query, "__self__"
            ):
                profiler = self._execute_query.__self__
                if isinstance(profiler, BigqueryProfiler) and hasattr(
                    profiler, "_last_partition_error"
                ):
                    error_message = profiler._last_partition_error

        return error_message

    def _handle_multi_column_partition(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_info: Dict[str, str],
    ) -> Optional[List[str]]:
        """Handle multi-column partition requirements."""
        required_cols = partition_info.get("required_columns", "").split(",")
        required_cols = [col.strip() for col in required_cols if col.strip()]

        if not required_cols:
            return None

        logger.info(f"Found multi-column partition requirement: {required_cols}")

        # Generate appropriate filters for required columns
        filters = self._generate_filters_for_required_columns(
            table, project, schema, required_cols
        )

        if not filters:
            logger.warning("Failed to generate filters for required columns")
            return None

        logger.info(f"Generated filters: {filters}")

        # For external tables, we should be more permissive in accepting filters
        # even without verification, since verification might fail for various reasons
        if table.external:
            logger.info(
                f"Using filters for external table without verification: {filters}"
            )
            return filters

        # For regular tables, verify the filters work
        return self._verify_and_return_filters(table, project, schema, filters)

    def _handle_single_column_partition(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_info: Dict[str, str],
    ) -> Optional[List[str]]:
        """Handle single-column partition requirements."""
        column = partition_info.get("column")
        if not column:
            return None

        logger.info(f"Found single-column partition requirement: {column}")

        # Get column type
        column_types = self._get_column_types(table, project, schema, [column])
        col_type = column_types.get(column, "STRING").upper()

        # Generate filter based on column type
        filter_str = self._generate_filter_for_column(
            table, project, schema, column, col_type
        )
        logger.info(f"Generated single-column filter: {filter_str}")

        # For external tables, use the filter without verification
        if table.external:
            return [filter_str]

        # For regular tables, verify the filter works
        return self._verify_and_return_filters(table, project, schema, [filter_str])

    def _generate_filter_for_column(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        column: str,
        col_type: str,
    ) -> str:
        """Generate filter for a single column based on its type."""
        now = datetime.now()

        if col_type in ["DATE", "DATETIME", "TIMESTAMP"]:
            if col_type == "DATE":
                return f"`{column}` = DATE '{now.strftime('%Y-%m-%d')}'"
            else:
                return f"`{column}` = TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif col_type.startswith("INT") or col_type in ["NUMERIC", "FLOAT"]:
            # For numeric types, try recent year if column name suggests a date component
            if "year" in column.lower():
                return f"`{column}` = {now.year}"
            elif "month" in column.lower():
                return f"`{column}` = {now.month}"
            elif "day" in column.lower():
                return f"`{column}` = {now.day}"
            else:
                # Default numeric filter
                return f"`{column}` >= 0"
        else:
            # For string columns, try to find a valid value
            value = self._find_valid_string_value(table, project, schema, column)
            if value:
                return f"`{column}` = '{value}'"
            else:
                return f"`{column}` IS NOT NULL"

    def _verify_and_return_filters(
        self, table: BigqueryTable, project: str, schema: str, filters: List[str]
    ) -> Optional[List[str]]:
        """Verify filters work and return them or handle external tables."""
        if self._filter_builder.verify_partition_has_data(
            table, project, schema, filters, timeout=30
        ):
            logger.info(
                f"Verified partition filters for {project}.{schema}.{table.name}: {filters}"
            )
            return filters
        else:
            logger.warning(f"Partition filters don't return data: {filters}")
            # Still use it for external tables
            if table.external:
                logger.info("Using filters anyway for external table")
                return filters
            return None

    def _generate_filters_for_required_columns(
        self, table: BigqueryTable, project: str, schema: str, required_cols: List[str]
    ) -> List[str]:
        """Generate filters for required columns based on their types."""
        # Get accurate column types
        column_types = self._get_column_types(table, project, schema, required_cols)

        # Generate appropriate filters for each column
        filters = []
        now = datetime.now()

        for col in required_cols:
            col_type = column_types.get(col, "STRING").upper()

            # Handle different column types with appropriate values
            if col.lower() == "year" or col_type.startswith("INT"):
                filters.append(f"`{col}` = {now.year}")
            elif col.lower() == "month":
                filters.append(f"`{col}` = {now.month}")
            elif col.lower() == "day":
                filters.append(f"`{col}` = {now.day}")
            elif col_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                if col_type == "DATE":
                    filters.append(f"`{col}` = DATE '{now.strftime('%Y-%m-%d')}'")
                else:
                    filters.append(
                        f"`{col}` = TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}'"
                    )
            elif col.lower() == "feedhandler" or col_type in [
                "STRING",
                "VARCHAR",
            ]:
                # Try to find a valid value for the column
                value = self._find_valid_string_value(table, project, schema, col)
                if value:
                    filters.append(f"`{col}` = '{value}'")
                else:
                    filters.append(f"`{col}` IS NOT NULL")
            else:
                # Generic fallback for any other column type
                filters.append(f"`{col}` IS NOT NULL")

        return filters

    def _try_last_resort_approach(
        self, table: BigqueryTable, project: str, schema: str, metadata: Dict[str, Any]
    ) -> Optional[List[str]]:
        """Try last resort approach when all else fails."""
        logger.debug(
            f"Trying last resort partition approach for {project}.{schema}.{table.name}"
        )

        filters = self._last_resort_partition_search(
            table, project, schema, metadata.get("partition_columns", {}), timeout=30
        )

        if filters:
            logger.debug(
                f"Found last resort partition filters for {project}.{schema}.{table.name}"
            )
            return filters

        return None

    def _last_resort_partition_search(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """
        Last resort approach for finding partition filters when standard methods fail.
        Attempts to find ANY partition value that has data to make profiling possible.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_columns: Dictionary mapping column names to data types
            timeout: Query timeout in seconds

        Returns:
            List of partition filter strings or None if no valid filters found
        """
        logger.warning(
            f"Standard approach failed for large table {table.name}, trying last-resort options"
        )

        # First try to get partition info from any previous error messages in the thread
        error_info = getattr(self, "_last_error_message", None)
        if error_info:
            partition_info = self._extract_partition_info_from_error(error_info)
            if partition_info:
                filters = []
                for col, value in partition_info.items():
                    if col in partition_columns:
                        # Handle different data types appropriately
                        if partition_columns[col].upper() in ("STRING", "VARCHAR"):
                            filters.append(f"`{col}` = '{value}'")
                        else:
                            filters.append(f"`{col}` = {value}")

                # Verify these filters work
                if filters and self._filter_builder.verify_partition_has_data(
                    table, project, schema, filters, timeout // 2
                ):
                    logger.info(f"Found working filters from error message: {filters}")
                    return filters

        # If that didn't work, continue with existing last resort approach
        for col_name, col_type in partition_columns.items():
            try:
                # Find the value with ANY data (not necessarily the most data)
                query = f"""
                SELECT DISTINCT {col_name} as value
                FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                WHERE {col_name} IS NOT NULL
                LIMIT 5
                """

                try:
                    distinct_results = self._execute_query(
                        query,
                        f"last_resort_{project}_{schema}_{table.name}_{col_name}",
                        timeout // 3,
                    )

                    for row in distinct_results:
                        val = row.value
                        if val is not None:
                            if isinstance(val, str):
                                filter_str = f"`{col_name}` = '{val}'"
                            elif isinstance(val, (int, float)):
                                filter_str = f"`{col_name}` = {val}"
                            elif isinstance(val, datetime):
                                if col_type in ("DATE", "TIMESTAMP", "DATETIME"):
                                    filter_str = f"`{col_name}` = TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
                                else:
                                    # For non-date columns with datetime values, use string formatting
                                    filter_str = f"`{col_name}` = '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
                            else:
                                # Default fallback for other types
                                filter_str = f"`{col_name}` = '{val}'"

                            # Verify this filter works
                            if self._filter_builder.verify_partition_has_data(
                                table, project, schema, [filter_str], timeout // 3
                            ):
                                logger.info(f"Last resort filter found: {filter_str}")
                                return [filter_str]
                except Exception as e:
                    logger.debug(f"Error in inner try block for column {col_name}: {e}")
                    continue  # Try next value
            except Exception as e:
                logger.debug(f"Error in outer try block for column {col_name}: {e}")
                continue  # Try next column

        # Final fallback - IS NOT NULL filter on the first partition column
        if partition_columns:
            first_col = next(iter(partition_columns.keys()))
            fallback_filter = f"`{first_col}` IS NOT NULL"
            logger.warning(f"Using fallback filter as last resort: {fallback_filter}")
            return [fallback_filter]

        # If all else fails, return None
        logger.warning(
            "Last resort approach failed to find any usable partition filters"
        )
        return None

    def _get_column_types(
        self, table: BigqueryTable, project: str, schema: str, columns: List[str]
    ) -> Dict[str, str]:
        """
        Get the data types for specified columns.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            columns: List of column names

        Returns:
            Dictionary mapping column names to their data types
        """
        if not columns:
            return {}

        column_str = ", ".join([f"'{col}'" for col in columns])
        query = f"""
        SELECT 
            column_name, 
            data_type 
        FROM 
            `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS` 
        WHERE 
            table_name = '{table.name}' 
            AND column_name IN ({column_str})
        """

        results = self._execute_query(query, timeout=30)

        column_types = {}
        for row in results:
            if hasattr(row, "column_name") and hasattr(row, "data_type"):
                column_types[row.column_name] = row.data_type

        return column_types

    def _find_valid_string_value(
        self, table: BigqueryTable, project: str, schema: str, column: str
    ) -> Optional[str]:
        """
        Find a valid string value for a column to use in a partition filter.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            column: Column name

        Returns:
            A valid string value or None if not found
        """
        query = f"""
        SELECT DISTINCT `{column}`
        FROM `{project}.{schema}.{table.name}`
        WHERE `{column}` IS NOT NULL
        LIMIT 1
        """

        try:
            results = self._execute_query(query, timeout=15)
            if results and len(results) > 0:
                # Get the first value
                row = results[0]
                if hasattr(row, column):
                    return str(getattr(row, column))
                # For dict-like results
                elif isinstance(row, dict) and column in row:
                    return str(row[column])
                # For tuple-like results
                elif hasattr(row, "__getitem__"):
                    return str(row[0])
        except Exception as e:
            logger.debug(f"Error finding valid string value for column {column}: {e}")

        return None

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

    def _try_time_hierarchy_approach(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """
        Try to create partition filters using a time hierarchy approach.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_columns: Dictionary mapping partition column names to types

        Returns:
            List of partition filter expressions, or None if not applicable
        """
        # For test compatibility
        if not partition_columns:
            return None

        # Check for time columns
        time_hierarchy = ["year", "month", "day", "hour"]
        time_columns = {
            col_name: col_type
            for col_name, col_type in partition_columns.items()
            if col_name.lower() in time_hierarchy
        }

        if not time_columns:
            return None

        # Create a filter for each time column
        filters = []
        now = datetime.now(timezone.utc)

        # Add time filters based on current time
        for col_name, _col_type in time_columns.items():
            col_lower = col_name.lower()
            if col_lower == "year":
                filters.append(f"`{col_name}` = {now.year}")
            elif col_lower == "month":
                filters.append(f"`{col_name}` = {now.month}")
            elif col_lower == "day":
                filters.append(f"`{col_name}` = {now.day}")
            elif col_lower == "hour":
                filters.append(f"`{col_name}` = {now.hour}")

        return filters if filters else None

    def _try_date_columns_approach(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """
        Try to create partition filters based on date columns.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_columns: Dictionary mapping partition column names to types

        Returns:
            List of partition filter expressions, or None if not applicable
        """
        # Look for date/time columns
        date_columns = {
            col_name: col_type
            for col_name, col_type in partition_columns.items()
            if col_type in ("DATE", "DATETIME", "TIMESTAMP")
        }

        if not date_columns:
            return None

        # Create filters for recent dates
        filters = []
        now = datetime.now(timezone.utc)
        thirty_days_ago = now - timedelta(days=30)

        for col_name, col_type in date_columns.items():
            if col_type == "DATE":
                filters.append(
                    f"`{col_name}` >= DATE '{thirty_days_ago.strftime('%Y-%m-%d')}'"
                )
            else:
                filters.append(
                    f"`{col_name}` >= TIMESTAMP '{thirty_days_ago.strftime('%Y-%m-%d')}'"
                )

        return filters if filters else None

    def _try_date_based_filtering_for_external(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
        timeout: int = 30,
    ) -> Optional[List[str]]:
        """
        Try date-based filtering specifically for external tables.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_columns: Dictionary mapping partition column names to types
            timeout: Query timeout in seconds

        Returns:
            List of partition filter expressions, or None if not applicable
        """
        # First try standard date approach
        date_filters = self._try_date_columns_approach(
            table, project, schema, partition_columns, timeout
        )
        if date_filters:
            return date_filters

        # For external tables, we also look for string columns that might contain dates
        string_columns = {
            col_name: col_type
            for col_name, col_type in partition_columns.items()
            if col_type in ("STRING", "VARCHAR")
        }

        if not string_columns:
            return None

        # Check for common date column names
        date_column_names = ["date", "dt", "partition_date", "created_at", "event_date"]
        likely_date_columns = [
            col for col in string_columns if col.lower() in date_column_names
        ]

        if not likely_date_columns:
            return None

        # Create a simple filter for the first likely date column
        col_name = likely_date_columns[0]
        now = datetime.now(timezone.utc)
        return [f"`{col_name}` = '{now.strftime('%Y-%m-%d')}'"]

    def _try_standard_approach_for_external(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: Dict[str, str],
    ) -> Optional[List[str]]:
        """
        Standard approach for handling external table partitions.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            partition_columns: Dictionary mapping partition column names to types

        Returns:
            List of partition filter expressions, or None if not applicable
        """
        if not partition_columns:
            return None

        # For external tables, we create simple equality filters
        sample_filters = []

        for col_name, col_type in partition_columns.items():
            if col_type in ("STRING", "VARCHAR"):
                # For string columns, use a simple value
                sample_filters.append(f"`{col_name}` = 'partition_value'")
                break
            elif col_type in ("INT64", "INTEGER"):
                # For integer columns, use 0
                sample_filters.append(f"`{col_name}` = 0")
                break

        return sample_filters if sample_filters else None

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
        partition_values = self.get_partition_values(
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

        # Create filter strings using the robust method
        filters = self._filter_builder.create_partition_filters(
            table, project, schema, partition_columns, partition_values
        )

        # Verify these filters together
        if filters and self._filter_builder.verify_partition_has_data(
            table, project, schema, filters, timeout
        ):
            logger.info(f"Using combined filters: {filters}")
            return filters

        # Try individual filters if combined don't work
        if filters:
            working_filters = []
            for filter_str in filters:
                if self._filter_builder.verify_partition_has_data(
                    table, project, schema, [filter_str], timeout // 2
                ):
                    working_filters.append(filter_str)
                    break  # One working filter is enough

            if working_filters:
                logger.info(f"Using individual filters: {working_filters}")
                return working_filters

        # Last resort for large tables
        if is_large_table or is_external:
            last_resort_filters = self._last_resort_partition_search(
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

    def _get_external_table_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        metadata: Dict[str, Any],
        timeout: int,
    ) -> Optional[List[str]]:
        """
        Get partition filters specifically for external tables.
        External tables require special handling due to their different structure.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name
            metadata: Table metadata dictionary
            timeout: Query timeout in seconds

        Returns:
            List of partition filter strings or None if not applicable
        """
        partition_columns = metadata.get("partition_columns", {})
        if not partition_columns:
            logger.info(f"External table {table.name} has no partition columns")
            return []

        # First try date-based filtering for external tables
        date_filters = self._try_date_columns_approach(
            table, project, schema, partition_columns, timeout
        )
        if date_filters:
            logger.info(f"Using date-based filters for external table: {date_filters}")
            return date_filters

        # For external tables, we also look for string columns that might contain dates
        string_columns = {
            col_name: col_type
            for col_name, col_type in partition_columns.items()
            if col_type in ("STRING", "VARCHAR")
        }

        if string_columns:
            # Check for common date column names
            date_column_names = [
                "date",
                "dt",
                "partition_date",
                "created_at",
                "event_date",
            ]
            likely_date_columns = [
                col for col in string_columns if col.lower() in date_column_names
            ]

            if likely_date_columns:
                # Create a simple filter for the first likely date column
                col_name = likely_date_columns[0]
                now = datetime.now(timezone.utc)
                filter_str = f"`{col_name}` = '{now.strftime('%Y-%m-%d')}'"

                # Verify this filter works
                if self._filter_builder.verify_partition_has_data(
                    table, project, schema, [filter_str], timeout // 2
                ):
                    logger.info(
                        f"Found working date filter for external table: {filter_str}"
                    )
                    return [filter_str]

        # Try a simplified approach with any partition column
        for col_name, col_type in partition_columns.items():
            if col_type in ("STRING", "VARCHAR"):
                # Try to find any value that works
                try:
                    query = f"""
                    SELECT DISTINCT {col_name} as value
                    FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                    WHERE {col_name} IS NOT NULL
                    LIMIT 1
                    """
                    results = self._execute_query(
                        query,
                        f"ext_sample_{project}_{schema}_{table.name}_{col_name}",
                        timeout // 3,
                    )

                    if results and results[0].value:
                        val = results[0].value
                        filter_str = f"`{col_name}` = '{val}'"

                        # Verify this filter works
                        if self._filter_builder.verify_partition_has_data(
                            table, project, schema, [filter_str], timeout // 2
                        ):
                            logger.info(
                                f"Found working string filter for external table: {filter_str}"
                            )
                            return [filter_str]
                except Exception as e:
                    logger.debug(f"Error finding string column value: {e}")

            elif col_type in ("INT64", "INTEGER"):
                # For integer columns, try finding a value
                try:
                    query = f"""
                    SELECT DISTINCT {col_name} as value
                    FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                    WHERE {col_name} IS NOT NULL
                    LIMIT 1
                    """
                    results = self._execute_query(
                        query,
                        f"ext_int_{project}_{schema}_{table.name}_{col_name}",
                        timeout // 3,
                    )

                    if results and results[0].value is not None:
                        val = results[0].value
                        filter_str = f"`{col_name}` = {val}"

                        # Verify this filter works
                        if self._filter_builder.verify_partition_has_data(
                            table, project, schema, [filter_str], timeout // 2
                        ):
                            logger.info(
                                f"Found working integer filter for external table: {filter_str}"
                            )
                            return [filter_str]
                except Exception as e:
                    logger.debug(f"Error finding integer column value: {e}")

        # If we couldn't find specific values, return empty list for external tables
        # This allows profiling to proceed with full table scan
        logger.info(
            f"No specific partition filters for external table {table.name}, using empty filter list"
        )
        return []

    def _get_emergency_partition_filter(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Optional[str]:
        """
        Get an emergency partition filter as a final fallback.
        This is used when all other approaches fail but we still need to profile the table.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name

        Returns:
            A single filter string or None if not possible
        """
        # Try to find ANY column that looks like a date
        try:
            date_query = """
            SELECT 
                column_name,
                data_type
            FROM 
                `{}.{}.INFORMATION_SCHEMA.COLUMNS` 
            WHERE 
                table_name = '{}'
                AND (
                    LOWER(column_name) LIKE '%date%'
                    OR LOWER(column_name) LIKE '%time%'
                    OR LOWER(column_name) LIKE '%day%'
                    OR data_type IN ('DATE', 'TIMESTAMP', 'DATETIME')
                )
            LIMIT 5
            """.format(project, schema, table.name)

            date_results = self._execute_query(
                date_query, f"emergency_{project}_{schema}_{table.name}", timeout=20
            )

            # First try with standard date columns
            for row in date_results:
                col_name = row.column_name
                data_type = row.data_type

                # Use 30 days ago for date filtering
                thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

                if data_type == "DATE":
                    filter_str = (
                        f"`{col_name}` >= DATE '{thirty_days_ago.strftime('%Y-%m-%d')}'"
                    )
                elif data_type in ("TIMESTAMP", "DATETIME"):
                    filter_str = f"`{col_name}` >= TIMESTAMP '{thirty_days_ago.strftime('%Y-%m-%d')}'"
                else:
                    # For string columns that look like dates
                    filter_str = f"`{col_name}` IS NOT NULL"

                # Verify this filter works
                if self._filter_builder.verify_partition_has_data(
                    table, project, schema, [filter_str], 30
                ):
                    logger.info(f"Emergency filter found: {filter_str}")
                    return filter_str

            # If no date columns work, try any non-null integer column
            int_query = """
            SELECT 
                column_name
            FROM 
                `{}.{}.INFORMATION_SCHEMA.COLUMNS` 
            WHERE 
                table_name = '{}'
                AND data_type IN ('INT64', 'INTEGER')
            LIMIT 5
            """.format(project, schema, table.name)

            int_results = self._execute_query(
                int_query, f"emergency_int_{project}_{schema}_{table.name}", timeout=20
            )

            for row in int_results:
                col_name = row.column_name
                filter_str = f"`{col_name}` >= 0"

                # Verify this filter works
                if self._filter_builder.verify_partition_has_data(
                    table, project, schema, [filter_str], 20
                ):
                    logger.info(f"Emergency integer filter found: {filter_str}")
                    return filter_str

                # Try IS NOT NULL as last fallback
                filter_str = f"`{col_name}` IS NOT NULL"
                if self._filter_builder.verify_partition_has_data(
                    table, project, schema, [filter_str], 10
                ):
                    logger.info(f"Emergency IS NOT NULL filter found: {filter_str}")
                    return filter_str

        except Exception as e:
            logger.warning(f"Error in emergency filter logic: {e}")

        # If nothing else works, use _PARTITIONTIME if it's a partitioned table
        if hasattr(table, "partition_info") and table.partition_info:
            return "_PARTITIONTIME IS NOT NULL"

        return None

    def _get_table_metadata(
        self, table: BigqueryTable, project: str, schema: str
    ) -> Dict[str, Any]:
        """
        Get metadata for a table, including partition information.

        Args:
            table: BigqueryTable instance
            project: Project ID
            schema: Dataset name

        Returns:
            Dictionary with table metadata
        """
        # Check if table has columns
        if not hasattr(table, "columns") or not table.columns:
            return {}

        # Get partition column information
        partition_columns = {}
        for column in table.columns:
            # Look for likely partition columns
            col_name = column.name.lower()
            if (
                col_name
                in ["year", "month", "day", "hour", "date", "dt", "partition_date"]
                or "partition" in col_name
                or (
                    hasattr(column, "is_partition_column")
                    and column.is_partition_column
                )
            ):
                partition_columns[column.name] = column.data_type

        # Try to get additional partition information
        try:
            query = f"""
            SELECT 
                column_name,
                is_partitioning_column,
                data_type
            FROM 
                `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE 
                table_name = '{table.name}'
                AND (is_partitioning_column = 'YES' OR partition_ordinal_position IS NOT NULL)
            """

            results = self._execute_query(query, timeout=30)

            for row in results:
                if hasattr(row, "column_name") and hasattr(row, "data_type"):
                    partition_columns[row.column_name] = row.data_type
        except Exception as e:
            logger.debug(
                f"Error getting partition metadata from INFORMATION_SCHEMA: {e}"
            )

        return {
            "partition_columns": partition_columns,
            "table_name": table.name,
            "is_external": table.external,
            "size_bytes": table.size_in_bytes,
            "rows_count": table.rows_count,
        }
