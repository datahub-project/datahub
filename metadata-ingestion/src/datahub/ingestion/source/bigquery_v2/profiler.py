import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

from dateutil.relativedelta import relativedelta

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
        timeout: int = 120,
    ) -> Dict[str, Any]:
        """
        Find the most populated partitions for a table based on record count.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_columns: List of partition column names
            max_results: Maximum number of top partitions to return
            timeout: Query timeout in seconds

        Returns:
            Dictionary mapping partition column names to their values
        """
        result_values = {}

        # First try with all partition columns together to find combinations with data
        if len(partition_columns) > 1:
            try:
                partition_cols_select = ", ".join(
                    [f"{col}" for col in partition_columns]
                )
                partition_cols_group = ", ".join(partition_columns)

                query = f"""WITH PartitionStats AS (
    SELECT {partition_cols_select}, COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {" AND ".join([f"{col} IS NOT NULL" for col in partition_columns])}
    GROUP BY {partition_cols_group}
    HAVING record_count > 0
    ORDER BY record_count DESC
    LIMIT {max_results}
)
SELECT * FROM PartitionStats"""

                logger.debug(f"Executing combined partition query: {query}")
                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job.result())

                if results:
                    # Take the partition combination with the most records
                    best_result = results[0]
                    logger.info(
                        f"Found combined partition with {best_result.record_count} records"
                    )

                    # Extract values for each partition column
                    for col in partition_columns:
                        result_values[col] = getattr(best_result, col)

                    return result_values
            except Exception as e:
                logger.warning(f"Error finding combined partition values: {e}")
                # Continue to individual column approach if combined approach fails

        # If combined approach didn't work or returned no results, try each column individually
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
                    f"Executing query for partition column {col_name}: {query}"
                )
                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job.result())

                if not results or results[0].val is None:
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
                    chosen_result = results[0]  # Already sorted by date DESC
                else:
                    # Find the result with the most records
                    chosen_result = max(results, key=lambda r: r.record_count)

                result_values[col_name] = chosen_result.val
                logger.info(
                    f"Selected partition {col_name}={chosen_result.val} with {chosen_result.record_count} records"
                )

            except Exception as e:
                logger.error(f"Error getting partition value for {col_name}: {e}")

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
        query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""
        query_job = self.config.get_bigquery_client().query(query)
        results = list(query_job)

        return {row.column_name: row.data_type for row in results}

    def _get_partition_columns_from_ddl(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
    ) -> Dict[str, str]:
        """
        Extract partition columns from table DDL.

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

        # Look for PARTITION BY in DDL
        if "PARTITION BY" in table.ddl.upper():
            ddl_lines = table.ddl.upper().split("\n")
            for line in ddl_lines:
                if "PARTITION BY" in line:
                    # Extract column names from PARTITION BY clause
                    parts = line.split("PARTITION BY")[1].split("OPTIONS")[0].strip()
                    potential_cols = [col.strip(", `()") for col in parts.split()]

                    # Get data types for these columns
                    all_cols_query = f"""SELECT column_name, data_type
FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table.name}'"""
                    all_cols_job = self.config.get_bigquery_client().query(
                        all_cols_query
                    )
                    all_cols_results = list(all_cols_job)
                    all_cols_dict = {
                        row.column_name.upper(): row.data_type
                        for row in all_cols_results
                    }

                    # Add columns to our result
                    for col in potential_cols:
                        if col in all_cols_dict:
                            partition_cols_with_types[col] = all_cols_dict[col]

        return partition_cols_with_types

    def _create_filter_for_partition_column(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        col_name: str,
        data_type: str,
        timeout: int = 300,
    ) -> Optional[str]:
        """
        Create a filter for a single partition column.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            col_name: Partition column name
            data_type: Data type of the column
            timeout: Query timeout in seconds

        Returns:
            Filter string if a valid partition value is found, None otherwise
        """
        logger.debug(
            f"Processing external table partition column: {col_name} with type {data_type}"
        )

        # Query to find a partition value with data
        query = f"""WITH PartitionStats AS (
    SELECT {col_name} as val,
           COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {col_name} DESC
    LIMIT 1
)
SELECT val, record_count
FROM PartitionStats"""

        try:
            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job.result())

            if not results or results[0].val is None:
                logger.warning(
                    f"No non-empty partition values found for column {col_name}"
                )
                return None

            val = results[0].val
            record_count = results[0].record_count
            logger.info(
                f"Selected external partition {col_name}={val} with {record_count} records"
            )

            # Format filter based on data type
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

        except Exception as e:
            logger.warning(
                f"Error determining value for partition column {col_name}: {e}"
            )
            return None

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

            # Step 4: First try sampling approach (most efficient)
            sample_filters = self._get_partitions_with_sampling(table, project, schema)
            if sample_filters:
                return sample_filters

            # Step 5: For time partitions in external tables, try recent dates
            date_columns = {
                col: data_type
                for col, data_type in partition_cols_with_types.items()
                if (
                    col.lower()
                    in {"date", "dt", "day", "created_date", "partition_date"}
                )
                or (data_type.upper() == "DATE")
            }

            timestamp_columns = {
                col: data_type
                for col, data_type in partition_cols_with_types.items()
                if (
                    col.lower()
                    in {"timestamp", "time", "datetime", "created_at", "updated_at"}
                )
                or (data_type.upper() in {"TIMESTAMP", "DATETIME"})
            }

            # Handle date columns first - try working backwards from current date
            if date_columns:
                filters = self._try_recent_date_partitions(
                    table, project, schema, date_columns, current_time
                )
                if filters:
                    return filters

            # Next try timestamp columns similarly
            if timestamp_columns:
                filters = self._try_recent_timestamp_partitions(
                    table, project, schema, timestamp_columns, current_time
                )
                if filters:
                    return filters

            # Step 6: For non-time partitions, try small-scale sampling for each column
            for col_name, data_type in partition_cols_with_types.items():
                if col_name in date_columns or col_name in timestamp_columns:
                    continue  # Already tried these

                # Try to get a few distinct values with minimal scanning
                try:
                    query = f"""
                    SELECT DISTINCT {col_name}
                    FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.5 PERCENT) 
                    WHERE {col_name} IS NOT NULL
                    LIMIT 5
                    """

                    query_job = self.config.get_bigquery_client().query(query)
                    values = [getattr(row, col_name) for row in query_job.result()]

                    # Try each value to find one that works
                    for val in values:
                        filter_str = self._create_partition_filter_from_value(
                            col_name, val, data_type
                        )

                        # Verify this value returns data with minimal scanning
                        if self._verify_partition_has_data_with_limit(
                            table, project, schema, [filter_str], limit=10
                        ):
                            return [filter_str]
                except Exception as e:
                    logger.warning(f"Error sampling values for column {col_name}: {e}")

            # Last resort: fallback to regular combination finding with careful limits
            logger.warning(f"Using fallback approach for external table {table.name}")
            return self._find_valid_partition_combination_for_external(
                table, project, schema, partition_cols_with_types
            )

        except Exception as e:
            logger.error(f"Error checking external table partitioning: {e}")
            return None

    def _try_recent_date_partitions(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        date_columns: Dict[str, str],
        current_time: datetime,
    ) -> Optional[List[str]]:
        """
        Try recent date values for external table date partitions.
        """
        # Try dates in reverse chronological order
        test_dates = [
            current_time.date(),  # Today
            (current_time - timedelta(days=1)).date(),  # Yesterday
            (current_time - timedelta(days=7)).date(),  # Last week
            (current_time - timedelta(days=30)).date(),  # Last month
            (current_time - timedelta(days=90)).date(),  # Last quarter
            (current_time - timedelta(days=365)).date(),  # Last year
        ]

        for col_name, data_type in date_columns.items():
            for test_date in test_dates:
                filter_str = self._create_partition_filter_from_value(
                    col_name, test_date, data_type
                )

                # Check if this date filter returns data with minimal scanning
                if self._verify_partition_has_data_with_limit(
                    table, project, schema, [filter_str], limit=10
                ):
                    logger.info(
                        f"Found working date filter for external table: {filter_str}"
                    )
                    return [filter_str]

        return None

    def _try_recent_timestamp_partitions(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        timestamp_columns: Dict[str, str],
        current_time: datetime,
    ) -> Optional[List[str]]:
        """
        Try recent timestamp values for external table timestamp partitions.
        """
        # Create timestamp ranges - truncate to hour to increase matches
        current_hour = current_time.replace(minute=0, second=0, microsecond=0)

        test_times = [
            current_hour,  # This hour
            current_hour - timedelta(hours=1),  # Last hour
            current_hour - timedelta(hours=24),  # Yesterday same hour
            current_hour - timedelta(days=7),  # Last week same hour
        ]

        for col_name, _data_type in timestamp_columns.items():
            for test_time in test_times:
                # For timestamps, we can do a range check instead of exact match to increase chances
                # of finding data in a specific time window
                filter_str = (
                    f"`{col_name}` >= TIMESTAMP '{test_time.strftime('%Y-%m-%d %H:00:00')}' "
                    + f"AND `{col_name}` < TIMESTAMP '{(test_time + timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')}'"
                )

                # Check if this filter returns data with minimal scanning
                if self._verify_partition_has_data_with_limit(
                    table, project, schema, [filter_str], limit=10
                ):
                    logger.info(
                        f"Found working timestamp filter for external table: {filter_str}"
                    )
                    return [filter_str]

        return None

    def _verify_partition_has_data_with_limit(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        limit: int = 10,
    ) -> bool:
        """
        Verify partition filters return data using a LIMIT clause to avoid full scans.
        """
        if not filters:
            return False

        where_clause = " AND ".join(filters)

        # Use LIMIT 1 to minimize data scanned
        query = f"""
        SELECT 1
        FROM `{project}.{schema}.{table.name}`
        WHERE {where_clause}
        LIMIT {limit}
        """

        try:
            logger.debug(
                f"Verifying external table partition with minimal query: {query}"
            )
            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job.result())
            return len(results) > 0
        except Exception as e:
            logger.warning(f"Error verifying partition with limit: {e}")
            return False

    def _find_valid_partition_combination_for_external(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
    ) -> Optional[List[str]]:
        """
        Find valid partition combination for external tables with minimal scanning.
        """
        # For external tables, we'll be extra cautious to avoid large scans
        # Start by trying to get a single value for each partition column individually
        individual_filters = []

        for col_name, data_type in partition_cols_with_types.items():
            try:
                # Use TABLESAMPLE to minimize scanning
                query = f"""
                SELECT DISTINCT {col_name}
                FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (0.1 PERCENT)
                WHERE {col_name} IS NOT NULL
                LIMIT 5
                """

                query_job = self.config.get_bigquery_client().query(query)
                rows = list(query_job.result())

                if rows:
                    # Try each value to find one that works
                    for row in rows:
                        val = getattr(row, col_name)
                        filter_str = self._create_partition_filter_from_value(
                            col_name, val, data_type
                        )

                        if self._verify_partition_has_data_with_limit(
                            table, project, schema, [filter_str]
                        ):
                            individual_filters.append(filter_str)
                            logger.info(
                                f"Found working filter for external table: {filter_str}"
                            )
                            # For external tables, we might only need one partition column filter
                            # to get good performance
                            return [filter_str]
            except Exception as e:
                logger.warning(
                    f"Error finding valid partition for column {col_name}: {e}"
                )

        # If we have multiple individual filters, verify they work together
        if (
            len(individual_filters) > 1
            and self._verify_partition_has_data_with_limit(
                table, project, schema, individual_filters
            )
            or len(individual_filters) == 1
        ):
            return individual_filters

        # Last resort - return a dummy filter that will get at least some data
        logger.warning(f"Using catch-all approach for external table {table.name}")

        # Pick a non-time column if available
        non_time_cols = [
            col
            for col in partition_cols_with_types.keys()
            if not any(
                time_word in col.lower()
                for time_word in ["date", "time", "day", "month", "year", "hour"]
            )
        ]

        if non_time_cols:
            col_name = non_time_cols[0]
            try:
                # Try to get any value at all
                query = f"""
                SELECT DISTINCT {col_name}
                FROM `{project}.{schema}.{table.name}`
                WHERE {col_name} IS NOT NULL
                LIMIT 1
                """

                query_job = self.config.get_bigquery_client().query(query)
                rows = list(query_job.result())

                if rows and hasattr(rows[0], col_name):
                    val = getattr(rows[0], col_name)
                    data_type = partition_cols_with_types[col_name]
                    filter_str = self._create_partition_filter_from_value(
                        col_name, val, data_type
                    )
                    return [filter_str]
            except Exception as e:
                logger.warning(f"Error in last resort approach: {e}")

        # Empty list indicates no partitions found or usable
        return []

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

    def _try_partition_combinations(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols: List[str],
        partition_cols_with_types: Dict[str, str],
        timeout: int = 300,
    ) -> Optional[List[str]]:
        """
        Try to find combinations of partition values that return data.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_cols: List of partition column names
            partition_cols_with_types: Dictionary of column names to data types
            timeout: Parameter retained for API compatibility but not used

        Returns:
            List of filter strings if found, None otherwise
        """
        # If there are multiple partition columns, query for combinations with data
        partition_cols_select = ", ".join([f"{col}" for col in partition_cols])
        partition_cols_group = ", ".join(partition_cols)

        # Query to find partition combinations with most records
        query = f"""
        WITH PartitionStats AS (
            SELECT {partition_cols_select}, COUNT(*) as record_count
            FROM `{project}.{schema}.{table.name}`
            WHERE {" AND ".join([f"{col} IS NOT NULL" for col in partition_cols])}
            GROUP BY {partition_cols_group}
            HAVING record_count > 0
            ORDER BY record_count DESC
            LIMIT 10
        )
        SELECT * FROM PartitionStats
        """

        logger.debug(f"Finding partition combinations: {query}")

        try:
            query_job = self.config.get_bigquery_client().query(query)
            # Call result() without a timeout parameter
            results = list(query_job.result())
        except Exception as e:
            logger.warning(f"Error querying partition combinations: {e}")
            try:
                # Try a simpler version of the query
                simplified_query = f"""
                SELECT {partition_cols_select}, COUNT(*) as record_count
                FROM `{project}.{schema}.{table.name}` 
                WHERE {partition_cols[0]} IS NOT NULL
                GROUP BY {partition_cols_select}
                ORDER BY record_count DESC
                LIMIT 5
                """
                logger.info(f"Trying simplified query: {simplified_query}")
                simplified_job = self.config.get_bigquery_client().query(
                    simplified_query
                )
                results = list(simplified_job.result())
            except Exception as simplified_error:
                logger.warning(f"Simplified query also failed: {simplified_error}")
                return None

        # If the query returned no results
        if not results:
            logger.warning("No partition combinations found")
            return None

        # Try each combination, starting with the one with most records
        for result in results:
            filters = []
            for col_name in partition_cols:
                val = getattr(result, col_name)
                if val is not None:
                    data_type = partition_cols_with_types.get(col_name, "STRING")
                    filter_str = self._create_partition_filter_from_value(
                        col_name, val, data_type
                    )
                    filters.append(filter_str)

            # Verify this combination has data
            if self._verify_partition_has_data(table, project, schema, filters):
                logger.info(f"Found valid partition combination: {filters}")
                return filters

        return None

    def _try_fallback_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
        timeout: int = 600,
    ) -> Optional[List[str]]:
        """
        Attempt fallback approaches to find valid partition values.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_cols_with_types: Dictionary of column names to data types
            timeout: Query timeout in seconds

        Returns:
            List of filter strings if found, None otherwise
        """
        # 1. Try date columns first
        date_columns = [
            col
            for col in partition_cols_with_types.keys()
            if col.lower() in {"date", "day", "dt", "created_date", "partition_date"}
        ]

        if date_columns:
            logger.info(f"Found date columns to try first: {date_columns}")
            date_filters = self._try_date_column_fallback(
                table, project, schema, date_columns, partition_cols_with_types, timeout
            )
            if date_filters:
                return date_filters

        # 2. Try generic approach for all columns
        fallback_filters = self._try_generic_column_fallback(
            table, project, schema, partition_cols_with_types, timeout
        )

        # 3. If we found filters, verify them
        if fallback_filters:
            if self._verify_partition_has_data(
                table, project, schema, fallback_filters, timeout
            ):
                logger.info(f"Found valid fallback filters: {fallback_filters}")
                return fallback_filters
            else:
                logger.warning("Fallback filters validation failed")

        # 4. Last resort - try simplified approach
        return self._try_simplified_fallback(
            table, project, schema, partition_cols_with_types, timeout
        )

    def _try_date_column_fallback(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        date_columns: List[str],
        partition_cols_with_types: Dict[str, str],
        timeout: int,
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
                        table, project, schema, [filter_str], timeout
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
        timeout: int,
    ) -> List[str]:
        """Try generic fallback approach for any column type."""
        fallback_filters = []
        # Skip date columns that were already tried
        date_columns = [
            col
            for col in partition_cols_with_types.keys()
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
                    table, project, schema, col_name, data_type, timeout
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
        timeout: int,
    ) -> Optional[str]:
        """Try different ordering strategies to find a valid value for a column."""
        for order_by in ["DESC", "ASC", "record_count DESC"]:
            query = f"""
            WITH PartitionStats AS (
                SELECT {col_name} as val, COUNT(*) as record_count
                FROM `{project}.{schema}.{table.name}`
                WHERE {col_name} IS NOT NULL
                GROUP BY {col_name}
                HAVING record_count > 0
                ORDER BY {col_name} {order_by}
                LIMIT 10
            )
            SELECT val, record_count FROM PartitionStats
            """

            logger.info(f"Executing fallback query with {order_by} ordering")
            try:
                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job.result())

                if results:
                    logger.info(f"Query returned {len(results)} potential values")

                    for result in results:
                        val = result.val
                        if val is not None:
                            filter_str = self._create_partition_filter_from_value(
                                col_name, val, data_type
                            )

                            logger.info(f"Testing filter: {filter_str}")
                            # Test each filter individually
                            if self._verify_partition_has_data(
                                table, project, schema, [filter_str], timeout
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
        timeout: int,
    ) -> Optional[List[str]]:
        """Last resort approach when other methods fail."""
        logger.warning(
            "All approaches failed, trying with a single most populated partition"
        )

        # Try with simplified approach - just get most recent partition
        if self._try_get_most_recent_partition(
            table, project, schema, partition_cols_with_types, timeout
        ):
            return self._try_get_most_recent_partition(
                table, project, schema, partition_cols_with_types, timeout
            )

        # Try to find the most populated partition for any column
        return self._try_find_most_populated_partition(
            table, project, schema, partition_cols_with_types, timeout
        )

    def _try_get_most_recent_partition(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
        timeout: int,
    ) -> Optional[List[str]]:
        """Try to get the most recent partition."""
        logger.info("Trying simplified approach to find most recent partition")
        try:
            fallback_filters = []
            # This query just gets the most recent data for any partition column
            query = f"""
            SELECT *
            FROM `{project}.{schema}.{table.name}`
            ORDER BY {list(partition_cols_with_types.keys())[0]} DESC
            LIMIT 1
            """

            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job.result())

            if results and len(results) > 0:
                for col_name in partition_cols_with_types:
                    val = getattr(results[0], col_name, None)
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
                    table, project, schema, fallback_filters, timeout
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
        timeout: int,
    ) -> Optional[List[str]]:
        """Find the single most populated partition across all columns."""
        try:
            best_col = None
            best_val = None
            best_count = 0

            for col_name in partition_cols_with_types:
                query = f"""
                SELECT {col_name} as val, COUNT(*) as cnt
                FROM `{project}.{schema}.{table.name}`
                WHERE {col_name} IS NOT NULL
                GROUP BY {col_name}
                ORDER BY cnt DESC
                LIMIT 1
                """

                try:
                    query_job = self.config.get_bigquery_client().query(query)
                    results = list(query_job.result())

                    if (
                        results
                        and results[0].val is not None
                        and results[0].cnt > best_count
                    ):
                        best_col = col_name
                        best_val = results[0].val
                        best_count = results[0].cnt
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
        timeout: int = 300,  # Increased from 120
    ) -> bool:
        """
        Verify that the partition filters actually return data.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            filters: List of partition filter strings
            timeout: Query timeout in seconds

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

            # Set a longer timeout for this operation
            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job.result())

            if results and results[0].cnt > 0:
                logger.info(
                    f"Verified partition filters return {results[0].cnt} rows: {where_clause}"
                )
                return True
            else:
                logger.warning(f"Partition verification found no data: {where_clause}")
                return False
        except Exception as e:
            logger.warning(f"Error verifying partition data: {e}", exc_info=True)

            # Try with a simpler query as fallback
            try:
                simpler_query = f"""
                SELECT 1 
                FROM `{project}.{schema}.{table.name}`
                WHERE {where_clause}
                LIMIT 1
                """
                query_job = self.config.get_bigquery_client().query(simpler_query)
                results = list(query_job.result())

                return len(results) > 0
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
            sample_query = f"""
            SELECT *
            FROM `{project}.{schema}.{table.name}` TABLESAMPLE SYSTEM (1 PERCENT)
            LIMIT 100
            """

            query_job = self.config.get_bigquery_client().query(sample_query)
            results = list(query_job.result())

            if not results:
                logger.info("Sample query returned no results")
                return None

            # Extract values for partition columns
            filters = []
            for col_name, data_type in partition_cols_with_types.items():
                for row in results:
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
        timeout: int = 600,  # Increased from 300
    ) -> Optional[List[str]]:
        """
        Find a valid combination of partition filters that returns data.

        Args:
            table: BigqueryTable instance
            project: BigQuery project ID
            schema: BigQuery dataset name
            partition_cols_with_types: Dictionary of partition column names to data types
            timeout: Query timeout in seconds

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
                combined_filters = self._try_partition_combinations(
                    table,
                    project,
                    schema,
                    partition_cols,
                    partition_cols_with_types,
                    timeout,
                )
                if combined_filters:
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
                    filter_str = self._create_filter_for_partition_column(
                        table, project, schema, col_name, data_type, timeout
                    )
                    if filter_str:
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
                    table, project, schema, individual_filters, timeout
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
            table, project, schema, partition_cols_with_types, timeout
        )

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
            None if partition filters could not be determined
        """
        current_time = datetime.now(timezone.utc)
        partition_filters = []

        # First try sampling approach as it's most efficient
        sample_filters = self._get_partitions_with_sampling(table, project, schema)
        if sample_filters:
            return sample_filters

        # Get required partition columns from table info
        required_partition_columns = set()

        # Get from explicit partition_info if available
        if table.partition_info:
            if isinstance(table.partition_info.fields, list):
                required_partition_columns.update(table.partition_info.fields)

            if table.partition_info.columns:
                required_partition_columns.update(
                    col.name for col in table.partition_info.columns if col
                )

        # If no partition columns found from partition_info, query INFORMATION_SCHEMA
        if not required_partition_columns:
            try:
                query = f"""SELECT column_name
    FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table.name}' AND is_partitioning_column = 'YES'"""
                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job.result())
                required_partition_columns = {row.column_name for row in results}
                logger.debug(
                    f"Found partition columns from schema: {required_partition_columns}"
                )
            except Exception as e:
                logger.error(f"Error querying partition columns: {e}")

        # If still no partition columns found, check for external table partitioning
        if not required_partition_columns:
            logger.debug(f"No partition columns found for table {table.name}")
            if table.external:
                return self._get_external_table_partition_filters(
                    table, project, schema, current_time
                )
            else:
                return None

        logger.debug(f"Required partition columns: {required_partition_columns}")

        # Get column data types for the partition columns
        column_data_types = {}
        try:
            query = f"""SELECT column_name, data_type
    FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table.name}'"""
            query_job = self.config.get_bigquery_client().query(query)
            results = list(query_job.result())
            column_data_types = {row.column_name: row.data_type for row in results}
        except Exception as e:
            logger.error(f"Error fetching column data types: {e}")

        # Handle standard time-based columns without querying
        standard_time_columns = {"year", "month", "day", "hour"}
        time_based_columns = {
            col
            for col in required_partition_columns
            if col.lower() in standard_time_columns
        }
        other_columns = required_partition_columns - time_based_columns

        for col_name in time_based_columns:
            col_name_lower = col_name.lower()
            col_data_type = column_data_types.get(col_name, "STRING")

            if col_name_lower == "year":
                value = current_time.year
            elif col_name_lower == "month":
                value = current_time.month
            elif col_name_lower == "day":
                value = current_time.day
            elif col_name_lower == "hour":
                value = current_time.hour
            else:
                continue

            # Handle casting based on column type
            if col_data_type.upper() in {"STRING"}:
                partition_filters.append(f"`{col_name}` = '{value}'")
            else:
                partition_filters.append(f"`{col_name}` = {value}")

        # Fetch latest value for non-time-based partitions
        for col_name in other_columns:
            try:
                # Query to get latest non-empty partition
                query = f"""WITH PartitionStats AS (
    SELECT {col_name} as val,
           COUNT(*) as record_count
    FROM `{project}.{schema}.{table.name}`
    WHERE {col_name} IS NOT NULL
    GROUP BY {col_name}
    HAVING record_count > 0
    ORDER BY {col_name} DESC
    LIMIT 1
    )
    SELECT val, record_count
    FROM PartitionStats"""
                logger.debug(f"Executing query for partition value: {query}")

                query_job = self.config.get_bigquery_client().query(query)
                results = list(query_job.result())

                if not results or results[0].val is None:
                    logger.warning(
                        f"No non-empty partition values found for column {col_name}"
                    )
                    continue

                val = results[0].val
                record_count = results[0].record_count
                logger.info(
                    f"Selected partition {col_name}={val} with {record_count} records"
                )

                if isinstance(val, (int, float)):
                    partition_filters.append(f"`{col_name}` = {val}")
                else:
                    partition_filters.append(f"`{col_name}` = '{val}'")

            except Exception as e:
                logger.error(f"Error getting partition value for {col_name}: {e}")

        logger.debug(f"Final partition filters: {partition_filters}")
        return partition_filters if partition_filters else None

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
