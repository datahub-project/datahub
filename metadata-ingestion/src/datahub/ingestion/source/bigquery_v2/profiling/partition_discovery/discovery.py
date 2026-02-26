"""BigQuery partition discovery and filter generation."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from datahub.ingestion.source.bigquery_v2.common import BQ_SPECIAL_PARTITION_IDS
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
    """Handles partition discovery and filter generation for BigQuery tables."""

    def __init__(self, config: BigQueryV2Config):
        self.config = config
        self._current_cached_metadata: Optional[Dict] = None

    @staticmethod
    def get_partition_range_from_partition_id(
        partition_id: str, partition_datetime: Optional[datetime]
    ) -> Tuple[datetime, datetime]:
        """Get the (start, end) datetime range for a BigQuery partition ID (YYYY/YYYYMM/YYYYMMDD/YYYYMMDDHH)."""
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
        """Extract partition columns from table DDL using sqlglot parsing."""
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
                f"Error parsing DDL with sqlglot for table {table.name}: {e}"
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

        if table.external:
            return PartitionResult(
                partition_values=self._get_partition_info_from_table_query(
                    table,
                    project,
                    schema,
                    partition_columns,
                    execute_query_func,
                    max_results,
                )
            )
        else:
            result = self._get_partition_info_from_information_schema(
                table,
                project,
                schema,
                partition_columns,
                execute_query_func,
                max_results * 10,  # Query more partitions from info schema
            )

            if not result:
                result = PartitionResult(
                    partition_values=self._get_partition_info_from_table_query(
                        table,
                        project,
                        schema,
                        partition_columns,
                        execute_query_func,
                        max_results,
                    )
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
        """Return partition filter expressions needed to query this table.

        Returns [] for unpartitioned tables, a list of filter strings for partitioned
        tables, or None if filters are required but could not be determined (caller
        should skip the table to avoid full scans).

        Discovery strategy (in priority order):
        1. table.partition_info / table.schema fields (zero-cost, already fetched)
        2. Caller-supplied cached INFORMATION_SCHEMA.COLUMNS metadata
        3. Direct INFORMATION_SCHEMA.COLUMNS query
        4. Probe with a test query and parse the resulting partition-filter error
        5. max_partition_id from table metadata (zero-scan)
        6. INFORMATION_SCHEMA.PARTITIONS (internal tables only)
        7. Direct table query for latest date values (_find_real_partition_values)
        8. Sampling fallback (_get_partitions_with_sampling)
        """
        current_time = datetime.now(timezone.utc)

        self._current_cached_metadata = cached_partition_metadata

        try:
            logger.info(
                f"Starting partition discovery for table {table.name} "
                f"(project={project}, dataset={schema})"
            )

            required_partition_columns = self._get_partition_columns_from_table_info(
                table
            )

            if not required_partition_columns and cached_partition_metadata:
                required_partition_columns = set(
                    cached_partition_metadata.get("partition_columns", [])
                )

            if not required_partition_columns:
                required_partition_columns = self._get_partition_columns_from_schema(
                    table, project, schema, execute_query_func
                )

            if not required_partition_columns:
                # Last resort: probe with a cheap query. BigQuery returns a specific
                # error ("filter over column(s) ...") when a partitioned table is queried
                # without a partition filter. If the query succeeds the table is not
                # partitioned; if it fails we parse the required columns from the error.
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

            column_types = self._get_partition_column_types(
                table,
                project,
                schema,
                list(required_partition_columns),
                execute_query_func,
            )

            filters_from_metadata = None
            if table.max_partition_id:
                filters_from_metadata = (
                    self._get_partition_filters_from_max_partition_id(
                        table, list(required_partition_columns), column_types
                    )
                )
            if filters_from_metadata:
                logger.info(
                    f"Zero-scan optimization: using max_partition_id from schema metadata for {table.name}"
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
                    return partition_filters
                else:
                    logger.debug(
                        f"INFORMATION_SCHEMA approach failed for {table.name}, falling back to strategic dates"
                    )

            partition_filters = self._find_real_partition_values(
                table,
                project,
                schema,
                list(required_partition_columns),
                execute_query_func,
            )

            if partition_filters:
                return partition_filters
            else:
                sample_filters = self._get_partitions_with_sampling(
                    table, project, schema, execute_query_func
                )
                if sample_filters:
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
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = 5,
    ) -> Dict[str, Any]:
        """Get partition information by querying the actual table."""
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

    def _categorize_partition_columns(
        self,
        partition_columns: List[str],
        column_types: Dict[str, str],
    ) -> Tuple[List[str], Dict[str, Optional[str]], List[str]]:
        """Categorize partition columns into date, date components, and non-date columns."""
        date_columns = []
        date_component_columns: Dict[str, Optional[str]] = {
            "year": None,
            "month": None,
            "day": None,
        }
        non_date_columns = []

        for col_name in partition_columns:
            col_data_type = column_types.get(col_name, "")
            if self._is_date_type_column(col_data_type):
                date_columns.append(col_name)
            elif col_name.lower() in ["year", "month", "day"]:
                date_component_columns[col_name.lower()] = col_name
            elif self._is_date_like_column(col_name):
                date_columns.append(col_name)
                logger.debug(
                    f"Column {col_name} detected as date-like based on name (data type: {col_data_type or 'unknown'})"
                )
            else:
                non_date_columns.append(col_name)

        return date_columns, date_component_columns, non_date_columns

    def _process_regular_date_columns(
        self,
        date_columns: List[str],
        safe_table_ref: str,
        column_types: Dict[str, str],
        max_results: int,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        table: BigqueryTable,
        result_values: Dict[str, Any],
    ) -> List[str]:
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

                chosen_result = partition_values_results[0]
                logger.info(
                    f"Found latest date for {col_name}: {chosen_result.val} "
                    f"({chosen_result.record_count} records, queried table directly)"
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
        date_component_columns: Dict[str, Optional[str]],
        safe_table_ref: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, Any],
        column_types: Dict[str, str],
    ) -> List[str]:
        """Process year/month/day component columns hierarchically."""
        active_date_components = {
            k: v for k, v in date_component_columns.items() if v is not None
        }
        if not active_date_components:
            return []

        logger.info(
            f"Found date components: {list(active_date_components.keys())} - using hierarchical approach"
        )

        # Process hierarchically: find max year first, then constrain month to that
        # year, then constrain day to that year+month. This avoids selecting e.g.
        # month=12 from a previous year when the current year only has months 1-3.
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
        year_col: str,
        safe_table_ref: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, Any],
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Find the maximum year value."""
        try:
            col_type = column_types.get(year_col, "INT64") if column_types else "INT64"
            query, job_config = self._create_partition_stats_query(
                safe_table_ref, year_col, 1, col_type
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
        month_col: str,
        safe_table_ref: str,
        year_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, Any],
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
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
        day_col: str,
        safe_table_ref: str,
        year_month_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, Any],
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
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
        non_date_columns: List[str],
        safe_table_ref: str,
        latest_date_filters: List[str],
        column_types: Dict[str, str],
        max_results: int,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        table: BigqueryTable,
        result_values: Dict[str, Any],
    ) -> None:
        """Find the most-populated value for each non-date partition column.

        When date filters are already established (e.g. from processing a companion
        date column), the query is constrained to that date partition so we get the
        most common non-date value *within the latest date*, not globally. Without
        this constraint we might pick a stale value from a historical partition.
        """
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

                chosen_result = partition_values_results[0]
                context = " within latest date partition" if latest_date_filters else ""
                logger.info(
                    f"Found most populated partition for {col_name}: {chosen_result.val} "
                    f"({chosen_result.record_count} records{context}, queried table directly)"
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
        return DateUtils.get_column_ordering_strategy(col_name, data_type)

    def _is_date_like_column(self, col_name: str) -> bool:
        return DateUtils.is_date_like_column(col_name)

    def _is_date_type_column(self, data_type: str) -> bool:
        return DateUtils.is_date_type_column(data_type)

    def _log_partition_attempt(
        self,
        method: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        success: Optional[bool] = None,
    ) -> None:
        """Log a partition discovery attempt at debug level."""
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
        """Extract column names from a sqlglot PARTITION BY expression."""
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

            query_results = execute_query_func(
                query, job_config, "partition columns from schema"
            )
            required_partition_columns = {row.column_name for row in query_results}
            logger.debug(
                f"Found partition columns from schema: {required_partition_columns}"
            )
        except Exception as e:
            logger.warning(f"Error querying partition columns: {e}")
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
        """Get partition filters by sampling the table.

        Used as a last resort when INFORMATION_SCHEMA and direct date queries both fail.
        For date-partitioned tables uses ORDER BY date DESC (cheap, returns latest data);
        for non-date tables falls back to TABLESAMPLE SYSTEM (random sample).
        """
        try:
            partition_cols_with_types = self.get_partition_columns_from_info_schema(
                table, project, schema, execute_query_func
            )

            if not partition_cols_with_types:
                partition_cols_with_types = self.get_partition_columns_from_ddl(
                    table, project, schema, execute_query_func
                )

            if not partition_cols_with_types:
                return None

            date_columns = [
                col
                for col, data_type in partition_cols_with_types.items()
                if self._is_date_like_column(col)
                or self._is_date_type_column(data_type)
            ]

            safe_table_ref = build_safe_table_reference(project, schema, table.name)

            if date_columns:
                primary_date_col = date_columns[0]
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

            if filters and self._verify_partition_has_data(
                table, project, schema, filters, execute_query_func
            ):
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
                    f"Found partition values via sampling for {table.name}: {partition_values_for_log}"
                )
                return filters

            return None

        except Exception as e:
            logger.warning(f"Error getting partition filters with sampling: {e}")
            return None

    def _create_partition_filter_from_value(
        self, col_name: str, val: Union[str, int, float], data_type: str
    ) -> str:
        return self._create_safe_filter(col_name, val, data_type)

    def _create_safe_filter(
        self, col_name: str, val: Union[str, int, float], col_type: Optional[str] = None
    ) -> str:
        return FilterBuilder.create_safe_filter(col_name, val, col_type)

    def _verify_partition_has_data(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> bool:
        """Verify that the partition filters return at least one row."""
        if not filters:
            return False

        validated_filters = [f for f in filters if validate_filter_expression(f)]

        if not validated_filters:
            return False

        safe_table_ref = build_safe_table_reference(project, schema, table.name)
        where_clause = " AND ".join(validated_filters)

        try:
            query = f"""SELECT 1 as exists_check
FROM {safe_table_ref}
WHERE {where_clause}
LIMIT 1"""

            results = execute_query_func(
                query, QueryJobConfig(), "partition verification"
            )
            return bool(results)
        except Exception as e:
            logger.warning(
                f"Error verifying partition data for {project}.{schema}.{table.name}: {e}"
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
        if table.external:
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
        """Get partition filters for external tables."""
        try:
            sample_filters = self._get_partitions_with_sampling(
                table, project, schema, execute_query_func
            )
            if sample_filters:
                return sample_filters

            partition_cols_with_types = self.get_partition_columns_from_info_schema(
                table, project, schema, execute_query_func
            )

            if not partition_cols_with_types:
                partition_cols_with_types = self.get_partition_columns_from_ddl(
                    table, project, schema, execute_query_func
                )

            if not partition_cols_with_types:
                logger.debug(
                    f"No partition columns found for external table {table.name}"
                )
                return []

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
        return self._get_fallback_partition_filters(
            table,
            project,
            schema,
            list(partition_cols_with_types.keys()),
            partition_cols_with_types,
        )

    def _test_date_candidate(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        test_date: datetime,
        description: str,
        required_columns: List[str],
        column_types: Dict[str, str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """Test a single date candidate, returning partition filters if data found."""
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
                    if col in self.config.profiling.fallback_partition_values:
                        fallback_val = self.config.profiling.fallback_partition_values[
                            col
                        ]
                        filters.append(
                            self._create_safe_filter(col, fallback_val, col_data_type)
                        )
                    else:
                        filters.append(f"`{col}` IS NOT NULL")
            except ValueError as e:
                logger.warning(f"Skipping invalid filter for column {col}: {e}")
                filters.append(f"`{col}` IS NOT NULL")

        try:
            if self._verify_partition_has_data(
                table, project, schema, filters, execute_query_func
            ):
                enhanced_filters = self._enhance_partition_filters_with_actual_values(
                    table,
                    project,
                    schema,
                    required_columns,
                    filters,
                    execute_query_func,
                )

                if enhanced_filters:
                    return enhanced_filters
                else:
                    return filters
            else:
                return None
        except Exception as e:
            logger.warning(
                f"Error testing date candidate {description} for table {table.name}: {e}"
            )
            return None

    def _find_real_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        """Find real partition values by querying the table directly.

        For date/timestamp columns queries for the actual latest value (MAX query).
        For other columns falls back to testing strategic date candidates (today,
        yesterday) in parallel threads - parallel because each candidate requires a
        round-trip to BigQuery and sequential testing would be slow.
        """
        if not required_columns:
            return []

        column_types = self._get_partition_column_types(
            table, project, schema, required_columns, execute_query_func
        )

        # Always try direct table query first - it handles date, date-component, AND
        # non-date columns correctly via _process_non_date_columns. Falling through
        # directly to strategic date candidates for non-date columns (INT64, STRING, etc.)
        # only produces unhelpful IS NOT NULL filters that don't prune partitions.
        actual_partition_values = self._get_partition_info_from_table_query(
            table, project, schema, required_columns, execute_query_func
        )

        if actual_partition_values:
            actual_filters = []
            for col, val in actual_partition_values.items():
                try:
                    col_type = column_types.get(col, "")
                    filter_expr = self._create_safe_filter(col, val, col_type)
                    actual_filters.append(filter_expr)
                except ValueError as e:
                    logger.warning(f"Skipping invalid filter for {col}={val}: {e}")
                    continue

            logger.info(
                f"Successfully found partition values for {table.name}: {dict(actual_partition_values)}"
            )
            logger.info(
                f"Generated partition filters from direct table query for {table.name}: {actual_filters}"
            )
            return actual_filters

        # Direct table query failed. For date/timestamp columns try strategic date
        # candidates (today, yesterday) in parallel as a last resort.
        has_date_types = any(
            self._is_date_type_column(column_types.get(col, ""))
            for col in required_columns
        )
        has_date_components = any(
            col.lower() in ["year", "month", "day"] for col in required_columns
        )
        has_date_like_names = any(
            not column_types.get(col, "") and self._is_date_like_column(col)
            for col in required_columns
        )

        if not (has_date_types or has_date_components or has_date_like_names):
            logger.debug(
                f"No date-type columns in {table.name} and direct query returned nothing; "
                f"skipping strategic date candidates"
            )
            return None

        logger.debug(
            f"Direct table query failed for {table.name}, falling back to strategic dates"
        )

        candidate_dates = self._get_strategic_candidate_dates()

        logger.info(
            f"Testing {len(candidate_dates)} date candidates in parallel for table {table.name}"
        )
        with ThreadPoolExecutor(max_workers=min(len(candidate_dates), 5)) as executor:
            future_to_date = {
                executor.submit(
                    self._test_date_candidate,
                    table,
                    project,
                    schema,
                    test_date,
                    description,
                    required_columns,
                    column_types,
                    execute_query_func,
                ): (test_date, description)
                for test_date, description in candidate_dates
            }

            for future in as_completed(future_to_date):
                test_date, description = future_to_date[future]
                try:
                    result = future.result()
                    if result:
                        for remaining_future in future_to_date:
                            if remaining_future != future:
                                remaining_future.cancel()
                        return result
                except Exception as e:
                    logger.debug(
                        f"Exception testing date {description} for table {table.name}: {e}"
                    )

        logger.info(
            f"All strategic date candidates exhausted for {table.name}; querying table directly as final fallback"
        )
        actual_partition_values = self._get_partition_info_from_table_query(
            table, project, schema, required_columns, execute_query_func
        )

        if actual_partition_values:
            actual_filters = []
            for col, val in actual_partition_values.items():
                try:
                    col_type = column_types.get(col, "")
                    filter_expr = self._create_safe_filter(col, val, col_type)
                    actual_filters.append(filter_expr)
                except ValueError as e:
                    logger.warning(f"Skipping invalid filter for {col}={val}: {e}")
                    continue

            logger.info(
                f"Successfully found partition values for {table.name}: {dict(actual_partition_values)}"
            )
            logger.info(
                f"Generated partition filters from direct table query for {table.name}: {actual_filters}"
            )
            return actual_filters

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
        """Generate fallback partition filters based on configuration."""
        logger.debug(f"Using fallback partition values for {table.name}")

        fallback_date = datetime.now(timezone.utc) - timedelta(days=1)
        column_types = column_types or {}

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
        """Build a last-resort filter for one partition column.

        Checks for a user-configured override value first. Falls back to a
        date-derived value for known date-like column types, then to IS NOT NULL
        so profiling can still proceed (at the cost of a less targeted scan).
        """
        if col_name in self.config.profiling.fallback_partition_values:
            fallback_value = self.config.profiling.fallback_partition_values[col_name]
            try:
                return self._create_safe_filter(col_name, fallback_value, col_type)
            except ValueError as e:
                logger.warning(f"Invalid fallback value for {col_name}: {e}")
                return f"`{col_name}` IS NOT NULL"

        try:
            if self._is_date_like_column(col_name) or self._is_date_type_column(
                col_type
            ):
                logger.warning(
                    f"Specific date values failed for column {col_name}, using IS NOT NULL"
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
        if (
            not table.max_partition_id
            or table.max_partition_id in BQ_SPECIAL_PARTITION_IDS
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
        """Get partition filters from INFORMATION_SCHEMA.PARTITIONS (metadata-only, no data scanning)."""
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
        """Convert a partition_id from INFORMATION_SCHEMA.PARTITIONS to filter expressions."""
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
        """Discover actual values for non-date partition columns given working date filters.

        When a table has compound partitioning (e.g. date + region), the date candidate
        test in _test_date_candidate uses IS NOT NULL as a placeholder for non-date
        columns. This method replaces those placeholders with the real most-common value
        for each non-date column so the final WHERE clause is specific and efficient.
        """
        try:
            safe_table_ref = build_safe_table_reference(project, schema, table.name)

            where_conditions = []
            date_filters = []

            for filter_expr in initial_filters:
                if "IS NOT NULL" not in filter_expr and (
                    "=" in filter_expr or "BETWEEN" in filter_expr
                ):
                    date_filters.append(filter_expr)
                    where_conditions.append(filter_expr)

            if not where_conditions:
                return None

            where_clause = " AND ".join(where_conditions)
            enhanced_filters = list(date_filters)

            column_types = self._get_partition_column_types(
                table, project, schema, required_columns, execute_query_func
            )

            for col_name in required_columns:
                col_data_type = column_types.get(col_name, "")

                if self._is_date_like_column(col_name) or self._is_date_type_column(
                    col_data_type
                ):
                    continue

                if any(f"`{col_name}` =" in f for f in date_filters):
                    continue

                try:
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
                        actual_value = results[0].col_value
                        enhanced_filter = self._create_safe_filter(
                            col_name, actual_value, col_data_type
                        )
                        enhanced_filters.append(enhanced_filter)
                        logger.debug(
                            f"Found actual value for {col_name}: {actual_value} ({results[0].row_count} rows)"
                        )
                    else:
                        enhanced_filters.append(f"`{col_name}` IS NOT NULL")

                except Exception as e:
                    logger.warning(
                        f"Error discovering values for column {col_name}: {e}"
                    )
                    enhanced_filters.append(f"`{col_name}` IS NOT NULL")

            return enhanced_filters

        except Exception as e:
            logger.warning(f"Error enhancing partition filters: {e}")
            return None

    def _get_strategic_candidate_dates(self) -> List[Tuple[datetime, str]]:
        return DateUtils.get_strategic_candidate_dates()

    def _extract_partition_info_from_error(self, error_message: str) -> PartitionInfo:
        """Extract required partition columns and values from BigQuery partition error messages."""
        result = PartitionInfo(required_columns=[], partition_values={})

        column_match = PARTITION_FILTER_PATTERN.search(error_message)

        if column_match:
            required_columns = []
            for i in range(1, 5):
                if column_match.group(i):
                    required_columns.append(column_match.group(i))

            if required_columns:
                result.required_columns = required_columns
                logger.debug(
                    f"Extracted required partition columns: {required_columns}"
                )

        path_matches = PARTITION_PATH_PATTERN.findall(error_message)

        if path_matches:
            partition_values = {}
            for key, value in path_matches:
                clean_value = value.rstrip(".,;'\"")
                if clean_value.isdigit():
                    clean_value = int(clean_value)
                partition_values[key] = clean_value

            if partition_values:
                result.partition_values = partition_values
                logger.debug(
                    f"Extracted partition values from path: {partition_values}"
                )

        return result
