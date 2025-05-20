import logging
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

    def get_required_partition_filters(
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

        # Get table metadata to find partition columns
        from datahub.ingestion.source.bigquery_v2.profiling_table_metadata_manager import (
            BigQueryTableMetadataManager,
        )

        metadata_manager = BigQueryTableMetadataManager(self._execute_query)
        metadata = metadata_manager.get_table_metadata(table, project, schema)
        partition_columns = metadata["partition_columns"]

        # If no partition columns found, this table doesn't require partition filters
        if not partition_columns:
            logger.debug(f"No partition columns found for table {table.name}")
            return None

        logger.info(f"Found partition columns for {table.name}: {partition_columns}")

        # Important change - External tables should always be considered "large" for profiling
        # This ensures we use proper strategies even if their reported size is 0
        is_small_table = False
        if table.external:
            logger.info(
                f"Table {table.name} is an external table, treating as large table for profiling"
            )
        elif table.rows_count == 0 or table.rows_count is None:
            logger.info(
                f"Table {table.name} reports 0 rows or unknown row count, treating more carefully"
            )
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
                results = self._execute_query(
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
                table, project, schema, metadata, timeout=60
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

    def _try_last_resort_approach(
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

        # Try to find ANY partition that has data - even a small amount - to make profiling possible
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
