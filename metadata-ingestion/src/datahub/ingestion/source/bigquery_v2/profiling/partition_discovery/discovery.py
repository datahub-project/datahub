import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Set, Tuple

import sqlglot
from dateutil.relativedelta import relativedelta
from google.cloud.bigquery import QueryJobConfig, Row, ScalarQueryParameter
from sqlglot.expressions import (
    Anonymous,
    Date,
    DatetimeTrunc,
    Expression,
    Identifier,
    PartitionedByProperty,
    Property,
)

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.common import BQ_SPECIAL_PARTITION_IDS
from datahub.ingestion.source.bigquery_v2.profiling import queries
from datahub.ingestion.source.bigquery_v2.profiling.constants import (
    DATE_COMPONENT_COLUMNS,
    DEFAULT_MAX_PARTITION_VALUES,
    DEFAULT_PARTITION_STATS_LIMIT,
    MAX_PARTITION_VALUES,
    PARTITION_FILTER_PATTERN,
    PARTITIONING_COLUMN_FLAG,
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
    CachedPartitionMetadata,
    ExtractedPartitionInfo,
    PartitionValue,
)
from datahub.ingestion.source.bigquery_v2.profiling.reporting import warn
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_filter_expression,
)

logger = logging.getLogger(__name__)


class PartitionDiscovery:
    def __init__(
        self, config: BigQueryV2Config, report: Optional[BigQueryV2Report] = None
    ):
        self.config = config
        self.report = report
        self.info_schema = InfoSchemaQueries(report)

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
        return self.info_schema.get_partition_columns_from_info_schema(
            table, project, schema, execute_query_func
        )

    def get_partition_columns_from_ddl(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Dict[str, str]:
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
                warn(
                    self.report,
                    logger,
                    title="Partition columns from DDL failed",
                    message="Found a PARTITION BY clause but could not extract any column "
                    "names from it; the table may be treated as unpartitioned and "
                    "full-scanned or skipped.",
                    context=f"{table.name}",
                )
                return partition_cols_with_types

            logger.debug(
                f"Extracted partition columns from DDL: {column_names} for table {table.name}"
            )

            return self._get_partition_column_types(
                table, project, schema, column_names, execute_query_func
            )

        except Exception as e:
            warn(
                self.report,
                logger,
                title="Partition columns from DDL failed",
                message="Could not parse the table DDL to find partition columns; the "
                "table may be treated as unpartitioned and full-scanned or skipped.",
                context=f"{table.name}: {e}",
            )

        return partition_cols_with_types

    def get_required_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_partition_metadata: Optional[CachedPartitionMetadata] = None,
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
        logger.info(
            f"Starting partition discovery for table {table.name} "
            f"(project={project}, dataset={schema})"
        )

        required_partition_columns = self._get_partition_columns_from_table_info(table)

        if not required_partition_columns and cached_partition_metadata:
            required_partition_columns = set(
                cached_partition_metadata.get("partition_columns", [])
            )

        if not required_partition_columns:
            required_partition_columns = self._get_partition_columns_from_schema(
                table, project, schema, execute_query_func
            )

        probe_error: Optional[str] = None
        if not required_partition_columns:
            # Last resort: probe with a cheap query and parse the partition-filter error.
            required_partition_columns, probe_error = (
                self._probe_required_partition_columns(
                    table, project, schema, execute_query_func, "partition detection"
                )
            )

        if not required_partition_columns:
            if table.external:
                # External (e.g. hive-partitioned) tables often expose partition columns
                # only via DDL, which the schema and probe checks above don't read.
                return self._handle_external_table_partitioning(
                    table, project, schema, execute_query_func
                )
            if probe_error is not None:
                # A probe timeout / IAM / quota error must not be silently reclassified
                # as "unpartitioned"; mirror _get_partition_columns_from_schema.
                warn(
                    self.report,
                    logger,
                    title="Partition column detection failed",
                    message="Could not determine partition columns from the fallback "
                    "probe query; the table will be treated as unpartitioned and may be "
                    "full-scanned or skipped",
                    context=f"{table.name}: probe error={probe_error}",
                )
            logger.debug(f"No partition columns found for table {table.name}")
            return []

        column_types = self._get_partition_column_types(
            table,
            project,
            schema,
            list(required_partition_columns),
            execute_query_func,
            cached_metadata=cached_partition_metadata,
        )

        filters_from_metadata = None
        if table.max_partition_id:
            filters_from_metadata = self._get_partition_filters_from_max_partition_id(
                table, list(required_partition_columns), column_types
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
            cached_metadata=cached_partition_metadata,
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

    def _get_partition_column_types(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Dict[str, str]:
        # cached_metadata is passed down the discovery call chain rather than held as
        # instance state so a single PartitionDiscovery can be shared across the parallel
        # external-table threads without racing on it.
        if cached_metadata:
            cached_types = cached_metadata.get("column_types", {})
            if all(col in cached_types for col in partition_columns):
                return {col: cached_types[col] for col in partition_columns}

        return self.info_schema.get_partition_column_types(
            table, project, schema, partition_columns, execute_query_func
        )

    def _get_partition_info_from_table_query(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        max_results: int = 5,
        cached_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Dict[str, PartitionValue]:
        if not partition_columns:
            return {}

        safe_table_ref = build_safe_table_reference(project, schema, table.name)
        column_types = self._get_partition_column_types(
            table,
            project,
            schema,
            partition_columns,
            execute_query_func,
            cached_metadata=cached_metadata,
        )

        date_columns, date_component_columns, non_date_columns = (
            self._categorize_partition_columns(partition_columns, column_types)
        )

        result_values: Dict[str, PartitionValue] = {}
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
        date_columns = []
        date_component_columns: Dict[str, Optional[str]] = {
            c: None for c in DATE_COMPONENT_COLUMNS
        }
        non_date_columns = []

        for col_name in partition_columns:
            col_data_type = column_types.get(col_name, "")
            if self._is_date_type_column(col_data_type):
                date_columns.append(col_name)
            elif col_name.lower() in DATE_COMPONENT_COLUMNS:
                date_component_columns[col_name.lower()] = col_name
            elif self._is_date_like_column(col_name):
                date_columns.append(col_name)
                logger.debug(
                    f"Column {col_name} detected as date-like based on name (data type: {col_data_type or 'unknown'})"
                )
            else:
                non_date_columns.append(col_name)

        return date_columns, date_component_columns, non_date_columns

    @staticmethod
    def _date_component_value(col_name: str, dt: datetime) -> Optional[str]:
        # month/day are zero-padded to match BigQuery integer partition values.
        component = col_name.lower()
        if component == "year":
            return str(dt.year)
        if component == "month":
            return f"{dt.month:02d}"
        if component == "day":
            return f"{dt.day:02d}"
        return None

    def _warn_partition_column_discovery_failed(
        self, context: str, error: Exception
    ) -> None:
        warn(
            self.report,
            logger,
            title="Partition value discovery failed for column",
            message="Could not read the latest value for a partition column; its filter "
            "is dropped, so the resulting partition scan may be broader than intended.",
            context=f"{context}: {error}",
        )

    def _fetch_top_partition_row(
        self,
        col_name: str,
        query: str,
        job_config: Optional[QueryJobConfig],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        table_name: str,
    ) -> Optional[Row]:
        # Runs one partition-stats probe and returns the top row, or None (with a
        # warning) when the column has no populated value. Shared by the date and
        # non-date column loops, which differ only in how they build the query.
        self._log_partition_attempt("table query", table_name, [col_name])
        results = execute_query_func(query, job_config, f"partition column {col_name}")

        if not results or results[0].val is None:
            warn(
                self.report,
                logger,
                title="Partition value discovery found no values",
                message="A partition column returned no values; its filter is dropped, "
                "so the resulting partition scan may be broader than intended.",
                context=f"{table_name}.{col_name}",
            )
            return None

        self._log_partition_attempt("table query", table_name, [col_name], success=True)
        return results[0]

    def _process_regular_date_columns(
        self,
        date_columns: List[str],
        safe_table_ref: str,
        column_types: Dict[str, str],
        max_results: int,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        table: BigqueryTable,
        result_values: Dict[str, PartitionValue],
    ) -> List[str]:
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

                chosen_result = self._fetch_top_partition_row(
                    col_name, query, job_config, execute_query_func, table.name
                )
                if chosen_result is None:
                    continue

                logger.info(
                    f"Found latest date for {col_name}: {chosen_result.val} "
                    f"({chosen_result.record_count} records, queried table directly)"
                )

                result_values[col_name] = chosen_result.val
                latest_date_filters.append(
                    self._create_safe_filter(col_name, chosen_result.val, col_data_type)
                )

            except Exception as e:
                self._warn_partition_column_discovery_failed(
                    f"{table.name}.{col_name}", e
                )
                continue

        return latest_date_filters

    def _process_date_components_hierarchically(
        self,
        date_component_columns: Dict[str, Optional[str]],
        safe_table_ref: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, PartitionValue],
        column_types: Dict[str, str],
    ) -> List[str]:
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
        result_values: Dict[str, PartitionValue],
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
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
            self._warn_partition_column_discovery_failed(
                f"{safe_table_ref}.{year_col}", e
            )
        return []

    def _find_max_month_within_year(
        self,
        month_col: str,
        safe_table_ref: str,
        year_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, PartitionValue],
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        return self._find_max_component_within_constraint(
            component_col=month_col,
            safe_table_ref=safe_table_ref,
            constraint_filters=year_filters,
            execute_query_func=execute_query_func,
            result_values=result_values,
            column_types=column_types,
            scope_label="max year",
        )

    def _find_max_day_within_year_month(
        self,
        day_col: str,
        safe_table_ref: str,
        year_month_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, PartitionValue],
        column_types: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        return self._find_max_component_within_constraint(
            component_col=day_col,
            safe_table_ref=safe_table_ref,
            constraint_filters=year_month_filters,
            execute_query_func=execute_query_func,
            result_values=result_values,
            column_types=column_types,
            scope_label="max year/month",
        )

    def _find_max_component_within_constraint(
        self,
        component_col: str,
        safe_table_ref: str,
        constraint_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        result_values: Dict[str, PartitionValue],
        column_types: Optional[Dict[str, str]],
        scope_label: str,
    ) -> List[str]:
        try:
            col_type = (
                column_types.get(component_col, "INT64") if column_types else "INT64"
            )
            constrained_query = self._build_partition_stats_cte(
                safe_table_ref,
                component_col,
                order_by=f"`{component_col}` DESC",
                limit_clause="1",
                extra_where=" AND ".join(constraint_filters),
            )

            job_config = QueryJobConfig()
            partition_values_results = execute_query_func(
                constrained_query, job_config, f"partition component {component_col}"
            )
            if partition_values_results and partition_values_results[0].val is not None:
                max_value = partition_values_results[0].val
                result_values[component_col] = max_value
                filter_expr = self._create_safe_filter(
                    component_col, max_value, col_type
                )
                logger.info(
                    f"Found latest {component_col} within {scope_label}: {max_value}"
                )
                return [filter_expr]
        except Exception as e:
            self._warn_partition_column_discovery_failed(
                f"{safe_table_ref}.{component_col}", e
            )
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
        result_values: Dict[str, PartitionValue],
    ) -> None:
        # When date filters exist, constrain to them so the most-common non-date value
        # is taken within the latest date, not globally (which could be stale).
        for col_name in non_date_columns:
            try:
                if not VALID_COLUMN_NAME_PATTERN.match(col_name):
                    logger.warning(f"Invalid column name: {col_name}")
                    continue

                if latest_date_filters:
                    constrained_query = self._build_partition_stats_cte(
                        safe_table_ref,
                        col_name,
                        order_by="record_count DESC",
                        limit_clause="@max_results",
                        extra_where=" AND ".join(latest_date_filters),
                    )

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

                chosen_result = self._fetch_top_partition_row(
                    col_name,
                    constrained_query,
                    job_config,
                    execute_query_func,
                    table.name,
                )
                if chosen_result is None:
                    continue

                context = " within latest date partition" if latest_date_filters else ""
                logger.info(
                    f"Found most populated partition for {col_name}: {chosen_result.val} "
                    f"({chosen_result.record_count} records{context}, queried table directly)"
                )

                result_values[col_name] = chosen_result.val

            except Exception as e:
                self._warn_partition_column_discovery_failed(
                    f"{table.name}.{col_name}", e
                )
                continue

    @staticmethod
    def _build_partition_stats_cte(
        table_ref: str,
        col_name: str,
        order_by: str,
        limit_clause: str,
        extra_where: str = "",
    ) -> str:
        # Shared "most-populated partition value" CTE: group by the column, keep populated
        # groups, order, take the top. Probes differ only in extra_where/order_by/limit.
        where = FilterBuilder.is_not_null(col_name)
        if extra_where:
            where += f" AND {extra_where}"

        return queries.PARTITION_STATS_CTE.format(
            col_name=col_name,
            table_ref=table_ref,
            where=where,
            order_by=order_by,
            limit_clause=limit_clause,
        )

    def _create_partition_stats_query(
        self,
        table_ref: str,
        col_name: str,
        max_results: int = DEFAULT_PARTITION_STATS_LIMIT,
        data_type: str = "",
    ) -> Tuple[str, QueryJobConfig]:
        order_by = self._get_column_ordering_strategy(col_name, data_type)
        safe_max_results = max(1, min(int(max_results), MAX_PARTITION_VALUES))

        query = self._build_partition_stats_cte(
            table_ref, col_name, order_by, "@max_results"
        )

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
        col_info = f" for columns {columns}" if columns else ""
        if success is None:
            logger.debug(f"Attempting {method} for table {table_name}{col_info}")
        elif success:
            logger.debug(f"{method} succeeded for table {table_name}{col_info}")
        else:
            logger.debug(f"{method} failed for table {table_name}{col_info}")

    def _extract_column_names_from_sqlglot_partition(
        self, partition_expr: Expression
    ) -> List[str]:
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

    def _get_partition_columns_from_table_info(self, table: BigqueryTable) -> Set[str]:
        required_partition_columns: Set[str] = set()

        if table.partition_info:
            required_partition_columns.update(table.partition_info.fields)
            if table.partition_info.columns is not None:
                required_partition_columns.update(
                    col.name for col in table.partition_info.columns
                )

        return required_partition_columns

    def _probe_required_partition_columns(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        purpose: str,
    ) -> Tuple[Set[str], Optional[str]]:
        # Cheap COUNT(*) probe: BigQuery raises "requires filter over column(s) ..." for a
        # partitioned table queried without a filter. Success => not partitioned (empty set,
        # no error); failure (including a bad identifier) => columns parsed from the error,
        # plus the raw error for reporting.
        try:
            safe_table_ref = build_safe_table_reference(project, schema, table.name)
            test_query = queries.COUNT_STAR_PROBE.format(table_ref=safe_table_ref)
            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("limit_rows", "INT64", TEST_QUERY_LIMIT_ROWS)
                ]
            )
            execute_query_func(test_query, job_config, purpose)
            return set(), None
        except Exception as e:
            cols = set(self._extract_partition_info_from_error(str(e)).required_columns)
            return cols, str(e)

    def _get_partition_columns_from_schema(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Set[str]:
        required_partition_columns: Set[str] = set()

        try:
            safe_info_schema_ref = build_safe_table_reference(
                project, schema, "INFORMATION_SCHEMA.COLUMNS"
            )

            query = queries.PARTITION_COLUMN_NAMES.format(
                info_schema_ref=safe_info_schema_ref, flag=PARTITIONING_COLUMN_FLAG
            )

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
            logger.debug(f"Error querying partition columns from schema: {e}")
            required_partition_columns, probe_error = (
                self._probe_required_partition_columns(
                    table,
                    project,
                    schema,
                    execute_query_func,
                    "partition error detection",
                )
            )
            # If the probe also errored without yielding the expected "requires partition
            # filter" columns, we learned nothing and the table will be treated as
            # unpartitioned. Surface that so operators can see the real cause.
            if probe_error is not None and not required_partition_columns:
                warn(
                    self.report,
                    logger,
                    title="Partition column detection failed",
                    message="Could not determine partition columns from "
                    "INFORMATION_SCHEMA or the fallback probe query; the table will "
                    "be treated as unpartitioned and may be full-scanned or skipped",
                    context=f"{table.name}: schema error={e}; probe error={probe_error}",
                )

        return required_partition_columns

    def _get_partitions_with_sampling(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        # Last resort when INFORMATION_SCHEMA and direct date queries both fail. Date
        # columns use ORDER BY date DESC (cheap); non-date tables use TABLESAMPLE SYSTEM.
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
                sample_query = queries.LATEST_BY_DATE_SAMPLE.format(
                    table_ref=safe_table_ref, date_col=primary_date_col
                )

                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter(
                            "limit_rows", "INT64", TEST_QUERY_LIMIT_ROWS
                        ),
                    ]
                )
            else:
                sample_query = queries.TABLESAMPLE_SAMPLE.format(
                    table_ref=safe_table_ref, sample_percent=SAMPLING_PERCENT
                )

                job_config = QueryJobConfig(
                    query_parameters=[
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
                        filter_str = self._create_safe_filter(col_name, val, data_type)
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
            warn(
                self.report,
                logger,
                title="Partition sampling failed",
                message="Sampling-based partition discovery failed; the table may be "
                "profiled without a partition filter or skipped.",
                context=f"{table.name}: {e}",
            )
            return None

    def _create_safe_filter(
        self, col_name: str, val: PartitionValue, col_type: Optional[str] = None
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
        if not filters:
            return False

        validated_filters = [f for f in filters if validate_filter_expression(f)]

        if not validated_filters:
            return False

        safe_table_ref = build_safe_table_reference(project, schema, table.name)
        where_clause = " AND ".join(validated_filters)

        try:
            query = queries.PARTITION_EXISTS_CHECK.format(
                table_ref=safe_table_ref, where=where_clause
            )

            results = execute_query_func(
                query, QueryJobConfig(), "partition verification"
            )
            return bool(results)
        except Exception as e:
            # A timed-out/errored verification is indistinguishable from a genuinely
            # empty partition here, so a correct filter can be dropped; surface it.
            warn(
                self.report,
                logger,
                title="Partition verification failed",
                message="Could not verify that the discovered partition filter matches "
                "rows; the filter may be dropped and the partition scan widened.",
                context=f"{project}.{schema}.{table.name}: {e}",
            )
            return False

    def _handle_external_table_partitioning(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
        return self._get_external_table_partition_filters(
            table, project, schema, execute_query_func
        )

    def _get_external_table_partition_filters(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
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
                if not table.ddl:
                    # With no DDL and nothing from INFORMATION_SCHEMA we can't tell if an
                    # external table is partitioned, so surface it instead of assuming not.
                    warn(
                        self.report,
                        logger,
                        title="External table partition status undetermined",
                        message="No DDL was available and INFORMATION_SCHEMA returned no "
                        "partition columns for this external table; it will be treated "
                        "as unpartitioned and may be full-scanned.",
                        context=f"{project}.{schema}.{table.name}",
                    )
                else:
                    logger.debug(
                        f"No partition columns found for external table {table.name}"
                    )
                return []

            return self._find_valid_partition_combination(
                table, project, schema, partition_cols_with_types, execute_query_func
            )

        except Exception as e:
            warn(
                self.report,
                logger,
                title="External table partition discovery failed",
                message="Could not determine partition filters for this external table; "
                "profiling may fall back to a full scan or skip the table.",
                context=f"{table.name}: {e}",
            )
            return None

    def _find_valid_partition_combination(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        partition_cols_with_types: Dict[str, str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
    ) -> Optional[List[str]]:
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
        cached_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Optional[List[str]]:
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
                    continue

                component_value = self._date_component_value(col, test_date)
                if component_value is not None:
                    filters.append(
                        self._create_safe_filter(col, component_value, col_data_type)
                    )
                elif col in self.config.profiling.fallback_partition_values:
                    fallback_val = self.config.profiling.fallback_partition_values[col]
                    filters.append(
                        self._create_safe_filter(col, fallback_val, col_data_type)
                    )
                else:
                    filters.append(FilterBuilder.is_not_null(col))
            except ValueError as e:
                logger.warning(f"Skipping invalid filter for column {col}: {e}")
                filters.append(FilterBuilder.is_not_null(col))

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
                    cached_metadata=cached_metadata,
                )

                if enhanced_filters:
                    return enhanced_filters
                else:
                    return filters
            else:
                return None
        except Exception as e:
            # Report rather than debug-log: if every candidate fails this way (e.g. a
            # systemic IAM/timeout error) it must not look identical to "found nothing".
            warn(
                self.report,
                logger,
                title="Strategic date candidate failed",
                message="Could not test a strategic partition date candidate; if all "
                "candidates fail the table may be treated as unpartitioned.",
                context=f"{table.name} ({description}): {e}",
            )
            return None

    def _filters_from_partition_values(
        self,
        table: BigqueryTable,
        actual_partition_values: Dict[str, PartitionValue],
        column_types: Dict[str, str],
    ) -> List[str]:
        actual_filters = []
        for col, val in actual_partition_values.items():
            try:
                col_type = column_types.get(col, "")
                actual_filters.append(self._create_safe_filter(col, val, col_type))
            except ValueError as e:
                # Dropping a column here widens the scan on that dimension, so surface it.
                warn(
                    self.report,
                    logger,
                    title="Partition filter dropped",
                    message="Could not build a filter for a discovered partition value; "
                    "that column is left unfiltered and its partitions may be full-scanned.",
                    context=f"{table.name}: {col}={val}: {e}",
                )
                continue

        logger.info(
            f"Successfully found partition values for {table.name}: {dict(actual_partition_values)}"
        )
        logger.info(
            f"Generated partition filters from direct table query for {table.name}: {actual_filters}"
        )
        return actual_filters

    def _find_real_partition_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_metadata: Optional[CachedPartitionMetadata] = None,
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
            table,
            project,
            schema,
            required_columns,
            execute_query_func,
            cached_metadata=cached_metadata,
        )

        # Always try direct table query first - it handles date, date-component, AND
        # non-date columns correctly via _process_non_date_columns. Falling through
        # directly to strategic date candidates for non-date columns (INT64, STRING, etc.)
        # only produces unhelpful IS NOT NULL filters that don't prune partitions.
        actual_partition_values = self._get_partition_info_from_table_query(
            table,
            project,
            schema,
            required_columns,
            execute_query_func,
            cached_metadata=cached_metadata,
        )

        if actual_partition_values:
            return self._filters_from_partition_values(
                table, actual_partition_values, column_types
            )

        # Direct table query failed. For date/timestamp columns try strategic date
        # candidates (today, yesterday) in parallel as a last resort.
        has_date_types = any(
            self._is_date_type_column(column_types.get(col, ""))
            for col in required_columns
        )
        has_date_components = any(
            col.lower() in DATE_COMPONENT_COLUMNS for col in required_columns
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
                    cached_metadata,
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

        # Strategic candidates don't mutate the direct-query result, so re-querying is wasted.
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
        logger.debug(f"Using fallback partition values for {table.name}")

        fallback_date = datetime.now(timezone.utc) - timedelta(days=1)
        column_types = column_types or {}

        fallback_filters = []
        uses_is_not_null = False
        for col_name in required_columns:
            col_type = column_types.get(col_name, "")
            filter_str = self._create_fallback_filter_for_column(
                col_name, fallback_date, col_type
            )
            if filter_str:
                fallback_filters.append(filter_str)
                if "IS NOT NULL" in filter_str:
                    uses_is_not_null = True

        logger.debug(f"Generated fallback partition filters: {fallback_filters}")

        # An `IS NOT NULL` fallback does not prune partitions, so profiling will scan
        # the whole table and produce stats describing all partitions rather than the
        # intended one. Warn so operators can distinguish fallback-profiled tables from
        # correctly-discovered ones (this is the exact full-scan this feature avoids).
        if fallback_filters and uses_is_not_null:
            warn(
                self.report,
                logger,
                title="Partition discovery fell back to full scan",
                message="Partition discovery failed; profiling with an unpruned "
                "'IS NOT NULL' fallback filter. The resulting profile describes the whole "
                "table, not a single partition, and scans all partitions. Set "
                "profiling.fallback_partition_values to target a specific partition, or "
                "the table will be skipped if fallbacks are disabled",
                context=f"{table.name}: filters={fallback_filters}",
            )

        return fallback_filters

    def _create_fallback_filter_for_column(
        self, col_name: str, fallback_date: datetime, col_type: str = ""
    ) -> str:
        # Prefer a user-configured override, then a date-derived value for date-like
        # columns, then IS NOT NULL so profiling still runs (with a less targeted scan).
        if col_name in self.config.profiling.fallback_partition_values:
            fallback_value = self.config.profiling.fallback_partition_values[col_name]
            try:
                return self._create_safe_filter(col_name, fallback_value, col_type)
            except ValueError as e:
                logger.warning(f"Invalid fallback value for {col_name}: {e}")
                return FilterBuilder.is_not_null(col_name)

        try:
            if self._is_date_like_column(col_name) or self._is_date_type_column(
                col_type
            ):
                logger.warning(
                    f"Specific date values failed for column {col_name}, using IS NOT NULL"
                )
                return FilterBuilder.is_not_null(col_name)

            component_value = self._date_component_value(col_name, fallback_date)
            if component_value is not None:
                return self._create_safe_filter(col_name, component_value, col_type)

            logger.warning(
                f"No fallback value for partition column {col_name}, using IS NOT NULL"
            )
            return FilterBuilder.is_not_null(col_name)
        except ValueError as e:
            logger.warning(f"Error creating fallback filter for {col_name}: {e}")
            return FilterBuilder.is_not_null(col_name)

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
            warn(
                self.report,
                logger,
                title="max_partition_id parse failed",
                message="Could not derive a partition filter from the table's "
                "max_partition_id; falling back to query-based partition discovery.",
                context=f"{table.name} (max_partition_id={table.max_partition_id}): {e}",
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
        return self.info_schema.get_partition_filters_from_information_schema(
            table,
            project,
            schema,
            required_columns,
            execute_query_func,
            self._verify_partition_has_data,
            column_types,
        )

    def _enhance_partition_filters_with_actual_values(
        self,
        table: BigqueryTable,
        project: str,
        schema: str,
        required_columns: List[str],
        initial_filters: List[str],
        execute_query_func: Callable[[str, Optional[QueryJobConfig], str], List[Row]],
        cached_metadata: Optional[CachedPartitionMetadata] = None,
    ) -> Optional[List[str]]:
        # For compound partitioning (e.g. date + region), _test_date_candidate uses
        # IS NOT NULL as a placeholder for non-date columns; replace each with the real
        # most-common value so the final WHERE clause is specific and efficient.
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
            unresolved_cols: List[str] = []

            column_types = self._get_partition_column_types(
                table,
                project,
                schema,
                required_columns,
                execute_query_func,
                cached_metadata=cached_metadata,
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
                    discover_query = queries.TOP_VALUES_BY_COUNT.format(
                        col_name=col_name,
                        table_ref=safe_table_ref,
                        where=where_clause,
                    )

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
                        enhanced_filters.append(FilterBuilder.is_not_null(col_name))
                        unresolved_cols.append(col_name)

                except Exception as e:
                    logger.warning(
                        f"Error discovering values for column {col_name}: {e}"
                    )
                    enhanced_filters.append(FilterBuilder.is_not_null(col_name))
                    unresolved_cols.append(col_name)

            # These non-date dimensions kept an unpruned IS NOT NULL: the WHERE clause
            # looks specific but still full-scans those dimensions. This path bypasses
            # _get_fallback_partition_filters, so warn here too (same title) to stay
            # consistent with the fallback full-scan warning.
            if unresolved_cols:
                warn(
                    self.report,
                    logger,
                    title="Partition discovery fell back to full scan",
                    message="Could not resolve a specific value for one or more non-date "
                    "partition columns; profiling uses an unpruned 'IS NOT NULL' filter "
                    "for them, so those dimensions are full-scanned. Set "
                    "profiling.fallback_partition_values to target specific values.",
                    context=f"{table.name}: columns={unresolved_cols}",
                )

            return enhanced_filters

        except Exception as e:
            # Returning None makes the caller fall back to a full scan, bypassing the
            # dedicated warning above, so surface it here too.
            warn(
                self.report,
                logger,
                title="Partition discovery fell back to full scan",
                message="Failed to resolve actual values for the partition filter; "
                "profiling falls back to a broader (often unpruned) filter, so the "
                "partition scan may be wider than intended.",
                context=f"{table.name}: {e}",
            )
            return None

    def _get_strategic_candidate_dates(self) -> List[Tuple[datetime, str]]:
        return DateUtils.get_strategic_candidate_dates()

    def _extract_partition_info_from_error(
        self, error_message: str
    ) -> ExtractedPartitionInfo:
        result = ExtractedPartitionInfo(required_columns=[])

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

        return result
