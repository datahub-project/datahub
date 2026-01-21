"""BigQuery partition discovery and filter generation with enhanced security."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sqlglot
from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter
from sqlglot.expressions import (
    Anonymous,
    Date,
    DatetimeTrunc,
    Identifier,
    PartitionedByProperty,
    Property,
)

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    DEFAULT_INFO_SCHEMA_PARTITIONS_LIMIT,
    DEFAULT_MAX_PARTITION_VALUES,
    DEFAULT_PARTITION_STATS_LIMIT,
    DEFAULT_POPULATED_PARTITIONS_LIMIT,
    MAX_PARTITION_VALUES,
    PARTITION_FILTER_PATTERN,
    PARTITION_PATH_PATTERN,
    SAMPLING_LIMIT_ROWS,
    SAMPLING_PERCENT,
    TEST_QUERY_LIMIT_ROWS,
    VALID_COLUMN_NAME_PATTERN,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.date_utils import (
    DateUtils,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.filter_builder import (
    FilterBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.info_schema import (
    InfoSchemaQueries,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.types import (
    PartitionInfo,
    PartitionResult,
)
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_filter_expression,
)

logger = logging.getLogger(__name__)


class PartitionDiscovery:
    """
    Handles partition discovery and filter generation for BigQuery tables.

    This class encapsulates all the logic needed to:
    1. Discover partition columns from table metadata
    2. Find valid partition values that contain data
    3. Generate secure partition filters for profiling queries
    """

    def __init__(self, config: BigQueryV2Config):
        self.config = config

        # Thread-local storage for cached partition metadata during a single discovery operation
        self._current_cached_metadata: Optional[Dict] = None

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
        return InfoSchemaQueries.get_partition_columns_from_info_schema(
            table, project, schema, execute_query_func
        )

    def get_partition_columns_from_ddl(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        """
        Extract partition columns from table DDL using sqlglot parsing.

        This method uses sqlglot to parse BigQuery CREATE TABLE DDL
        and extract partition column information from the PARTITION BY clause.

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
            parsed = sqlglot.parse_one(table.ddl, dialect="bigquery")

            partition_by_expr = None
            for prop in parsed.find_all(Property):
                if isinstance(prop, PartitionedByProperty):
                    partition_by_expr = prop.this
                    break

            if not partition_by_expr:
                logger.debug(
                    f"No PARTITION BY clause found in DDL for table {table.name}"
                )
                return partition_cols_with_types

            column_names = self._extract_column_names_from_sqlglot_partition(
                partition_by_expr
            )

            if not column_names:
                logger.warning(
                    f"Could not extract column names from PARTITION BY clause for table {table.name}"
                )
                return partition_cols_with_types

            logger.debug(
                f"Extracted partition columns from DDL: {column_names} for table {table.name}"
            )

            return self._get_partition_column_types(
                table, project, schema, column_names, execute_query_func
            )

        except Exception as e:
            logger.warning(
                f"Error parsing DDL with sqlglot for table {table.name}: {e}. "
                f"This may indicate a complex or non-standard DDL format."
            )

        return partition_cols_with_types

    def get_most_populated_partitions(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = DEFAULT_POPULATED_PARTITIONS_LIMIT,
    ) -> PartitionResult:
        """Find the most populated partitions for a table."""
        if not partition_columns:
            return PartitionResult(partition_values={}, row_count=None)

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
            result = self._get_partition_info_from_information_schema(
                table,
                project,
                schema,
                partition_columns,
                execute_query_func,
                max_results * 10,  # Query more partitions from info schema
            )

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
        cached_partition_metadata: Optional[Dict] = None,
    ) -> Optional[List[str]]:
        current_time = datetime.now(timezone.utc)

        self._current_cached_metadata = cached_partition_metadata

        try:
            logger.info(
                f"Starting partition discovery for table {table.name} (may try multiple date formats to find data)"
            )
            logger.debug(
                "Attempting comprehensive partition discovery before falling back to sampling"
            )

            required_partition_columns = self._get_partition_columns_from_table_info(
                table
            )

            if not required_partition_columns and cached_partition_metadata:
                logger.debug(f"Using cached partition metadata for {table.name}")
                required_partition_columns = set(
                    cached_partition_metadata.get("partition_columns", [])
                )

            if not required_partition_columns:
                required_partition_columns = self._get_partition_columns_from_schema(
                    table, project, schema, execute_query_func
                )

            if not required_partition_columns:
                try:
                    safe_table_ref = build_safe_table_reference(
                        project, schema, table.name
                    )

                    test_query = (
                        f"""SELECT COUNT(*) FROM {safe_table_ref} LIMIT @limit_rows"""
                    )
                    job_config = QueryJobConfig(
                        query_parameters=[
                            ScalarQueryParameter(
                                "limit_rows", "INT64", TEST_QUERY_LIMIT_ROWS
                            )
                        ]
                    )
                    execute_query_func(test_query, job_config, "partition detection")

                    logger.debug(f"Table {table.name} is not partitioned")
                    return []

                except Exception as e:
                    error_info = self._extract_partition_info_from_error(str(e))
                    required_partition_columns = set(error_info.required_columns)

                    if required_partition_columns:
                        logger.debug(
                            f"Detected required partition columns from error: {required_partition_columns}"
                        )
                    else:
                        logger.debug(
                            f"No partition columns found for table {table.name}"
                        )
                        return []

            if not required_partition_columns:
                logger.debug(f"No partition columns found for table {table.name}")
                return self._handle_external_table_partitioning(
                    table, project, schema, current_time, execute_query_func
                )

            # Get column types once for all optimization paths
            column_types = self._get_partition_column_types(
                table,
                project,
                schema,
                list(required_partition_columns),
                execute_query_func,
            )

            # OPTIMIZATION: Try using table.max_partition_id first (zero-cost metadata)
            filters_from_metadata = None
            if table.max_partition_id:
                logger.debug(
                    f"Attempting to use pre-extracted max_partition_id: {table.max_partition_id}"
                )
                filters_from_metadata = (
                    self._get_partition_filters_from_max_partition_id(
                        table, list(required_partition_columns), column_types
                    )
                )
            if filters_from_metadata:
                logger.info(
                    f"Zero-scan optimization: Using max_partition_id from schema metadata for {table.name}"
                )
                return filters_from_metadata

            if not table.external:
                partition_filters = self._get_partition_filters_from_information_schema(
                    table,
                    project,
                    schema,
                    list(required_partition_columns),
                    execute_query_func,
                    column_types,
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

            partition_filters = self._find_real_partition_values(
                table,
                project,
                schema,
                list(required_partition_columns),
                execute_query_func,
            )

            if partition_filters:
                logger.debug(f"Found valid partition filters: {partition_filters}")
                return partition_filters
            else:
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

        finally:
            self._current_cached_metadata = None

    def _get_partition_column_types(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
        if self._current_cached_metadata:
            cached_types = self._current_cached_metadata.get("column_types", {})
            if all(col in cached_types for col in partition_columns):
                logger.debug(f"Using cached column types for {partition_columns}")
                return {col: cached_types[col] for col in partition_columns}

        return InfoSchemaQueries.get_partition_column_types(
            table, project, schema, partition_columns, execute_query_func
        )

    def _get_partition_info_from_information_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = DEFAULT_INFO_SCHEMA_PARTITIONS_LIMIT,
    ) -> PartitionResult:
        """Get partition information from INFORMATION_SCHEMA.PARTITIONS."""
        return InfoSchemaQueries.get_partition_info_from_information_schema(
            table, project, schema, partition_columns, execute_query_func, max_results
        )

    def _get_partition_info_from_table_query(
        self,
        table,
        project,
        schema,
        partition_columns,
        execute_query_func,
        max_results=5,
    ):
        """
        Get partition information by querying the actual table using parameterized queries.

        For tables with date/timestamp columns, uses a hierarchical approach:
        1. Find the latest date first
        2. Find most populated values for other columns within that latest date

        This ensures we get a valid combination that actually exists in the data.
        """
        if not partition_columns:
            return {}

        safe_table_ref = build_safe_table_reference(project, schema, table.name)
        column_types = self._get_partition_column_types(
            table, project, schema, partition_columns, execute_query_func
        )

        date_columns, date_component_columns, non_date_columns = (
            self._categorize_partition_columns(partition_columns, column_types)
        )

        result_values: Dict[str, Any] = {}
        latest_date_filters = []

        latest_date_filters.extend(
            self._process_regular_date_columns(
                date_columns,
                safe_table_ref,
                column_types,
                max_results,
                execute_query_func,
                table,
                result_values,
            )
        )

        # Process year/month/day components hierarchically
        latest_date_filters.extend(
            self._process_date_components_hierarchically(
                date_component_columns,
                safe_table_ref,
                execute_query_func,
                result_values,
                column_types,
            )
        )

        self._process_non_date_columns(
            non_date_columns,
            safe_table_ref,
            latest_date_filters,
            column_types,
            max_results,
            execute_query_func,
            table,
            result_values,
        )

        return result_values

    def _categorize_partition_columns(self, partition_columns, column_types):
        """Categorize partition columns into date, date components, and non-date columns."""
        date_columns = []
        date_component_columns = {"year": None, "month": None, "day": None}
        non_date_columns = []

        for col_name in partition_columns:
            col_data_type = column_types.get(col_name, "")
            if self._is_date_type_column(col_data_type):
                date_columns.append(col_name)
            elif col_name.lower() in ["year", "month", "day"]:
                date_component_columns[col_name.lower()] = col_name
            elif self._is_date_like_column(col_name):
                date_columns.append(col_name)
                if col_data_type:
                    logger.debug(
                        f"Column {col_name} detected as date-like based on name (data type: {col_data_type})"
                    )
                else:
                    logger.debug(
                        f"Column {col_name} detected as date-like based on name (no data type available)"
                    )
            else:
                non_date_columns.append(col_name)

        return date_columns, date_component_columns, non_date_columns

    def _process_regular_date_columns(
        self,
        date_columns,
        safe_table_ref,
        column_types,
        max_results,
        execute_query_func,
        table,
        result_values,
    ):
        """Process regular date columns (DATE, DATETIME, TIMESTAMP types)."""
        latest_date_filters = []

        for col_name in date_columns:
            try:
                if not VALID_COLUMN_NAME_PATTERN.match(col_name):
                    logger.warning(f"Invalid column name: {col_name}")
                    continue

                col_data_type = column_types.get(col_name, "")
                query, job_config = self._create_partition_stats_query(
                    safe_table_ref, col_name, max_results, col_data_type
                )

                self._log_partition_attempt("table query", table.name, [col_name])
                partition_values_results = execute_query_func(
                    query, job_config, f"partition column {col_name}"
                )

                if (
                    not partition_values_results
                    or partition_values_results[0].val is None
                ):
                    logger.warning(
                        f"No non-empty partition values found for date column {col_name}"
                    )
                    continue

                # For date columns, always take the latest (first result, ordered by date DESC)
                chosen_result = partition_values_results[0]
                logger.info(
                    f"Found latest date for {col_name}: {chosen_result.val} with {chosen_result.record_count} records (queried table directly for maximum date)"
                )

                result_values[col_name] = chosen_result.val
                latest_date_filters.append(
                    self._create_safe_filter(col_name, chosen_result.val, col_data_type)
                )
                self._log_partition_attempt(
                    "table query", table.name, [col_name], success=True
                )

            except Exception as e:
                logger.error(
                    f"Error getting partition value for date column {col_name}: {e}"
                )
                continue

        return latest_date_filters

    def _process_date_components_hierarchically(
        self,
        date_component_columns,
        safe_table_ref,
        execute_query_func,
        result_values,
        column_types,
    ):
        """Process year/month/day components hierarchically to avoid invalid future dates."""
        active_date_components = {
            k: v for k, v in date_component_columns.items() if v is not None
        }
        if not active_date_components:
            return []

        logger.info(
            f"Found date components: {list(active_date_components.keys())}. Using hierarchical approach to find valid latest date combination."
        )

        component_filters = []

        if "year" in active_date_components:
            component_filters.extend(
                self._find_max_year(
                    active_date_components["year"],
                    safe_table_ref,
                    execute_query_func,
                    result_values,
                    column_types,
                )
            )

        if "month" in active_date_components and component_filters:
            component_filters.extend(
                self._find_max_month_within_year(
                    active_date_components["month"],
                    safe_table_ref,
                    component_filters,
                    execute_query_func,
                    result_values,
                    column_types,
                )
            )

        if "day" in active_date_components and component_filters:
            component_filters.extend(
                self._find_max_day_within_year_month(
                    active_date_components["day"],
                    safe_table_ref,
                    component_filters,
                    execute_query_func,
                    result_values,
                    column_types,
                )
            )

        return component_filters

    def _find_max_year(
        self,
        year_col,
        safe_table_ref,
        execute_query_func,
        result_values,
        column_types=None,
    ):
        """Find the maximum year value."""
        try:
            col_type = column_types.get(year_col, "INT64") if column_types else "INT64"
            query, job_config = self._create_partition_stats_query(
                safe_table_ref,
                year_col,
                1,
                col_type,  # Get top 1 year
            )
            partition_values_results = execute_query_func(
                query, job_config, f"partition component {year_col}"
            )
            if partition_values_results and partition_values_results[0].val is not None:
                max_year = partition_values_results[0].val
                result_values[year_col] = max_year
                filter_expr = self._create_safe_filter(year_col, max_year, col_type)
                logger.info(f"Found latest year: {max_year}")
                return [filter_expr]
        except Exception as e:
            logger.error(f"Error getting max year for {year_col}: {e}")
        return []

    def _find_max_month_within_year(
        self,
        month_col,
        safe_table_ref,
        year_filters,
        execute_query_func,
        result_values,
        column_types=None,
    ):
        """Find the maximum month value within the maximum year."""
        try:
            col_type = column_types.get(month_col, "INT64") if column_types else "INT64"
            year_constraint = " AND ".join(year_filters)
            constrained_query = f"""WITH PartitionStats AS (
    SELECT `{month_col}` as val, COUNT(*) as record_count
    FROM {safe_table_ref}
    WHERE `{month_col}` IS NOT NULL AND {year_constraint}
    GROUP BY `{month_col}`
    HAVING record_count > 0
    ORDER BY `{month_col}` DESC
    LIMIT 1
)
SELECT val, record_count FROM PartitionStats"""

            job_config = QueryJobConfig()
            partition_values_results = execute_query_func(
                constrained_query, job_config, f"partition component {month_col}"
            )
            if partition_values_results and partition_values_results[0].val is not None:
                max_month = partition_values_results[0].val
                result_values[month_col] = max_month
                filter_expr = self._create_safe_filter(month_col, max_month, col_type)
                logger.info(f"Found latest month within max year: {max_month}")
                return [filter_expr]
        except Exception as e:
            logger.error(f"Error getting max month for {month_col}: {e}")
        return []

    def _find_max_day_within_year_month(
        self,
        day_col,
        safe_table_ref,
        year_month_filters,
        execute_query_func,
        result_values,
        column_types=None,
    ):
        """Find the maximum day value within the maximum year/month."""
        try:
            col_type = column_types.get(day_col, "INT64") if column_types else "INT64"
            year_month_constraint = " AND ".join(year_month_filters)
            constrained_query = f"""WITH PartitionStats AS (
    SELECT `{day_col}` as val, COUNT(*) as record_count
    FROM {safe_table_ref}
    WHERE `{day_col}` IS NOT NULL AND {year_month_constraint}
    GROUP BY `{day_col}`
    HAVING record_count > 0
    ORDER BY `{day_col}` DESC
    LIMIT 1
)
SELECT val, record_count FROM PartitionStats"""

            job_config = QueryJobConfig()
            partition_values_results = execute_query_func(
                constrained_query, job_config, f"partition component {day_col}"
            )
            if partition_values_results and partition_values_results[0].val is not None:
                max_day = partition_values_results[0].val
                result_values[day_col] = max_day
                filter_expr = self._create_safe_filter(day_col, max_day, col_type)
                logger.info(f"Found latest day within max year/month: {max_day}")
                return [filter_expr]
        except Exception as e:
            logger.error(f"Error getting max day for {day_col}: {e}")
        return []

    def _process_non_date_columns(
        self,
        non_date_columns,
        safe_table_ref,
        latest_date_filters,
        column_types,
        max_results,
        execute_query_func,
        table,
        result_values,
    ):
        """Process non-date columns within the latest date constraints."""
        for col_name in non_date_columns:
            try:
                if not VALID_COLUMN_NAME_PATTERN.match(col_name):
                    logger.warning(f"Invalid column name: {col_name}")
                    continue

                if latest_date_filters:
                    date_constraint = " AND ".join(latest_date_filters)
                    constrained_query = f"""WITH PartitionStats AS (
    SELECT `{col_name}` as val, COUNT(*) as record_count
    FROM {safe_table_ref}
    WHERE `{col_name}` IS NOT NULL AND {date_constraint}
    GROUP BY `{col_name}`
    HAVING record_count > 0
    ORDER BY record_count DESC
    LIMIT @max_results
)
SELECT val, record_count FROM PartitionStats"""

                    job_config = QueryJobConfig(
                        query_parameters=[
                            ScalarQueryParameter("max_results", "INT64", max_results)
                        ]
                    )
                else:
                    constrained_query, job_config = self._create_partition_stats_query(
                        safe_table_ref,
                        col_name,
                        max_results,
                        column_types.get(col_name, ""),
                    )

                self._log_partition_attempt("table query", table.name, [col_name])
                partition_values_results = execute_query_func(
                    constrained_query, job_config, f"partition column {col_name}"
                )

                if (
                    not partition_values_results
                    or partition_values_results[0].val is None
                ):
                    logger.warning(
                        f"No non-empty partition values found for non-date column {col_name}"
                    )
                    continue

                # For non-date columns, take the most populated (first result, ordered by record_count DESC)
                chosen_result = partition_values_results[0]
                logger.info(
                    f"Found most populated partition for {col_name}: {chosen_result.val} with {chosen_result.record_count} records (within latest date, queried table directly for partition statistics)"
                )

                result_values[col_name] = chosen_result.val
                self._log_partition_attempt(
                    "table query", table.name, [col_name], success=True
                )

            except Exception as e:
                logger.error(
                    f"Error getting partition value for non-date column {col_name}: {e}"
                )
                continue

    def _create_partition_stats_query(
        self,
        table_ref: str,
        col_name: str,
        max_results: int = DEFAULT_PARTITION_STATS_LIMIT,
        data_type: str = "",
    ) -> Tuple[str, QueryJobConfig]:
        """
        Create a standardized partition statistics query with parameterized limit.
        """
        order_by = self._get_column_ordering_strategy(col_name, data_type)
        safe_max_results = max(1, min(int(max_results), MAX_PARTITION_VALUES))

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
        Get the appropriate ORDER BY strategy for a column based on its data type and name.
        Prioritizes data type over column name for reliable detection.
        """
        return DateUtils.get_column_ordering_strategy(col_name, data_type)

    def _is_date_like_column(self, col_name: str) -> bool:
        """
        Check if a column name suggests it contains date/time data.

        This is used as a fallback when column type information is unavailable or when
        date columns are stored as STRING/INT64 in external tables.

        See DATE_LIKE_COLUMN_NAMES in constants.py for the full list of patterns.
        """
        return DateUtils.is_date_like_column(col_name)

    def _is_date_type_column(self, data_type: str) -> bool:
        """
        Check if a column's data type is a date/time type in BigQuery.

        See DATE_TIME_TYPES in constants.py for the full list of recognized types.
        """
        return DateUtils.is_date_type_column(data_type)

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

    def _extract_column_names_from_sqlglot_partition(
        self, partition_expr: Any
    ) -> List[str]:
        """
        Extract column names from a sqlglot PARTITION BY expression.

        This method handles various BigQuery partitioning patterns:
        - DATE(column_name)
        - DATETIME_TRUNC(column_name, DAY)
        - TIMESTAMP_TRUNC(column_name, DAY)
        - RANGE_BUCKET(column_name, GENERATE_ARRAY(...))
        - column1, column2, column3 (simple columns)
        - Complex expressions with nested functions

        Args:
            partition_expr: sqlglot expression from PARTITION BY clause

        Returns:
            List of column names used in partitioning
        """
        column_names = []

        try:
            expressions = []
            if hasattr(partition_expr, "expressions") and partition_expr.expressions:
                expressions = partition_expr.expressions
            elif partition_expr:
                expressions = [partition_expr]

            for expr in expressions:
                column_name = None

                if isinstance(expr, (Date, DatetimeTrunc)):
                    if hasattr(expr, "this") and expr.this:
                        column_name = str(expr.this)

                elif isinstance(expr, Anonymous):
                    # Try to extract the first argument (usually the column)
                    if hasattr(expr, "expressions") and expr.expressions:
                        first_arg = expr.expressions[0]
                        if isinstance(first_arg, Identifier):
                            column_name = str(first_arg)
                        elif hasattr(first_arg, "this"):
                            column_name = str(first_arg.this)

                elif isinstance(expr, Identifier):
                    column_name = str(expr)

                elif hasattr(expr, "this") and expr.this:
                    if isinstance(expr.this, Identifier):
                        column_name = str(expr.this)
                    else:
                        # Try to extract column from nested expression
                        column_name = str(expr.this)

                if column_name:
                    column_name = column_name.strip().strip("`").strip('"').strip("'")
                    if column_name and column_name not in column_names:
                        column_names.append(column_name)
                        logger.debug(f"Extracted partition column: {column_name}")

        except Exception as e:
            logger.warning(
                f"Error extracting column names from sqlglot partition expression: {e}"
            )

        return column_names

    # Note: Regex-based partition parsing methods have been replaced with sqlglot-based parsing
    # in _extract_column_names_from_sqlglot_partition() for improved reliability and maintainability.
    # The old regex methods (_extract_column_names_from_partition_clause, _get_function_patterns, etc.)
    # have been removed as they are no longer needed.

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
        required_partition_columns: set[str] = set()

        if table.partition_info:
            if isinstance(table.partition_info.fields, (list, tuple)):
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
                    query_parameters=[
                        ScalarQueryParameter(
                            "limit_rows", "INT64", TEST_QUERY_LIMIT_ROWS
                        )
                    ]
                )
                execute_query_func(test_query, job_config, "partition error detection")
            except Exception as e:
                error_info = self._extract_partition_info_from_error(str(e))
                required_partition_columns = set(error_info.required_columns)

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
                        ScalarQueryParameter(
                            "limit_rows", "INT64", TEST_QUERY_LIMIT_ROWS
                        ),
                    ]
                )
            else:
                # Fall back to random sampling for non-date partitioned tables
                sample_query = f"""SELECT *
FROM {safe_table_ref} TABLESAMPLE SYSTEM (@sample_percent PERCENT)
LIMIT @limit_rows"""

                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter(
                            "sample_percent", "FLOAT64", SAMPLING_PERCENT
                        ),
                        ScalarQueryParameter(
                            "limit_rows", "INT64", SAMPLING_LIMIT_ROWS
                        ),
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
        return self._create_safe_filter(col_name, val, data_type)

    def _create_safe_filter(
        self, col_name: str, val: Union[str, int, float], col_type: Optional[str] = None
    ) -> str:
        """
        Create a safe partition filter with upstream validation of inputs.
        Uses appropriate quoting based on column type to avoid type mismatch errors.
        """
        return FilterBuilder.create_safe_filter(col_name, val, col_type)

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
            table,
            project,
            schema,
            list(partition_cols_with_types.keys()),
            partition_cols_with_types,
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

        # Strategy 1: For tables with date/timestamp columns, use direct table query
        # Prioritize data types over column names for reliable detection
        has_date_types = any(
            self._is_date_type_column(column_types.get(col, ""))
            for col in required_columns
        )
        has_date_components = any(
            col.lower() in ["year", "month", "day"] for col in required_columns
        )
        # Only use column name patterns as fallback when data type is unknown
        has_date_like_names = any(
            not column_types.get(col, "") and self._is_date_like_column(col)
            for col in required_columns
        )

        if has_date_types or has_date_components or has_date_like_names:
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
                        filters.append(
                            self._create_safe_filter(col, date_str, col_data_type)
                        )
                    elif col.lower() == "year":
                        filters.append(
                            self._create_safe_filter(
                                col, str(test_date.year), col_data_type
                            )
                        )
                    elif col.lower() == "month":
                        filters.append(
                            self._create_safe_filter(
                                col, f"{test_date.month:02d}", col_data_type
                            )
                        )
                    elif col.lower() == "day":
                        filters.append(
                            self._create_safe_filter(
                                col, f"{test_date.day:02d}", col_data_type
                            )
                        )
                    else:
                        # For other non-date columns, use fallback values from config
                        if col in self.config.profiling.fallback_partition_values:
                            fallback_val = (
                                self.config.profiling.fallback_partition_values[col]
                            )
                            filters.append(
                                self._create_safe_filter(
                                    col, fallback_val, col_data_type
                                )
                            )
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
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """
        Generate fallback partition filters based on configuration when regular methods time out.
        """
        logger.debug(f"Using fallback partition values for {table.name}")

        fallback_date = datetime.now(timezone.utc) - timedelta(days=1)
        logger.debug(
            f"Configured fallback values: {self.config.profiling.fallback_partition_values}"
        )

        column_types = column_types or {}

        # Generate filters for each required column
        fallback_filters = []
        for col_name in required_columns:
            col_type = column_types.get(col_name, "")
            filter_str = self._create_fallback_filter_for_column(
                col_name, fallback_date, col_type
            )
            if filter_str:
                fallback_filters.append(filter_str)

        logger.debug(f"Generated fallback partition filters: {fallback_filters}")
        return fallback_filters

    def _create_fallback_filter_for_column(
        self, col_name: str, fallback_date: datetime, col_type: str = ""
    ) -> str:
        """Create a fallback filter for a specific column using safe filter creation."""
        # Check for explicit fallback value in config
        if col_name in self.config.profiling.fallback_partition_values:
            fallback_value = self.config.profiling.fallback_partition_values[col_name]
            try:
                return self._create_safe_filter(col_name, fallback_value, col_type)
            except ValueError as e:
                logger.warning(f"Invalid fallback value for {col_name}: {e}")
                return f"`{col_name}` IS NOT NULL"

        # Use date-based fallbacks for date-like columns
        try:
            if self._is_date_like_column(col_name) or self._is_date_type_column(
                col_type
            ):
                # For date columns, if specific dates don't work, use IS NOT NULL as last resort
                logger.warning(
                    f"Specific date values failed for column {col_name}, using IS NOT NULL for broader partition coverage"
                )
                return f"`{col_name}` IS NOT NULL"
            elif col_name.lower() == "year":
                return self._create_safe_filter(
                    col_name, str(fallback_date.year), col_type
                )
            elif col_name.lower() == "month":
                return self._create_safe_filter(
                    col_name, f"{fallback_date.month:02d}", col_type
                )
            elif col_name.lower() == "day":
                return self._create_safe_filter(
                    col_name, f"{fallback_date.day:02d}", col_type
                )
            else:
                # Last resort - use IS NOT NULL
                logger.warning(
                    f"No fallback value for partition column {col_name}, using IS NOT NULL"
                )
                return f"`{col_name}` IS NOT NULL"
        except ValueError as e:
            logger.warning(f"Error creating fallback filter for {col_name}: {e}")
            return f"`{col_name}` IS NOT NULL"

    def _get_partition_filters_from_max_partition_id(
        self,
        table: BigqueryTable,
        required_columns: List[str],
        column_types: Dict[str, str],
    ) -> Optional[List[str]]:
        if not table.max_partition_id or table.max_partition_id in (
            "__NULL__",
            "__UNPARTITIONED__",
            "__STREAMING_UNPARTITIONED__",
        ):
            return None

        try:
            filters = FilterBuilder.convert_partition_id_to_filters(
                table.max_partition_id, required_columns, column_types
            )
            if filters:
                logger.debug(
                    f"Extracted partition filters from max_partition_id '{table.max_partition_id}': {filters}"
                )
            return filters

        except Exception as e:
            logger.warning(
                f"Error parsing max_partition_id '{table.max_partition_id}': {e}"
            )
            return None

    def _get_partition_filters_from_information_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        column_types: Dict[str, str],
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
            column_types: Mapping of column names to BigQuery types

        Returns:
            List of partition filter strings, or None if method fails
        """
        return InfoSchemaQueries.get_partition_filters_from_information_schema(
            table,
            project,
            schema,
            required_columns,
            execute_query_func,
            self._verify_partition_has_data,
            column_types,
        )

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
        return FilterBuilder.convert_partition_id_to_filters(
            partition_id, required_columns
        )

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
                            ScalarQueryParameter(
                                "max_values", "INT64", DEFAULT_MAX_PARTITION_VALUES
                            ),
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
        return DateUtils.get_strategic_candidate_dates()

    def _extract_partition_info_from_error(self, error_message: str) -> PartitionInfo:
        """Extract partition information from error messages."""
        result = PartitionInfo(required_columns=[], partition_values={})

        # Look for "filter over column(s)" pattern which lists required partition columns
        column_match = PARTITION_FILTER_PATTERN.search(error_message)

        if column_match:
            required_columns = []
            for i in range(1, 5):  # Check up to 4 matched groups
                if column_match.group(i):
                    required_columns.append(column_match.group(i))

            if required_columns:
                result.required_columns = required_columns
                logger.debug(
                    f"Extracted required partition columns: {required_columns}"
                )

        # Look for partition path patterns like feed=value/year=value/month=value/day=value
        path_matches = PARTITION_PATH_PATTERN.findall(error_message)

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
                result.partition_values = partition_values
                logger.debug(
                    f"Extracted partition values from path: {partition_values}"
                )

        return result
