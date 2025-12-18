"""Custom SQLAlchemy-based profiler to replace Great Expectations."""

import collections
import concurrent.futures
import dataclasses
import json
import logging
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import sqlalchemy as sa
import sqlalchemy.sql.compiler
import sqlalchemy.types as sa_types
from sqlalchemy.engine import Connection, Engine

from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.profiling.common import (
    Cardinality,
    convert_to_cardinality,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql_profiler.stats_calculator import (
    StatsCalculator,
)
from datahub.ingestion.source.sql_profiler.temp_table_handler import (
    create_athena_temp_table,
    create_bigquery_temp_table,
    drop_temp_table,
)
from datahub.ingestion.source.sql_profiler.type_mapping import (
    NORMALIZE_TYPE_PATTERN,
    ProfilerDataType,
    _get_column_types_to_ignore,
    get_column_profiler_type,
    resolve_profiler_type_with_fallback,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import (
    PartitionTypeClass,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    PartitionSpecClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sqlalchemy_query_combiner import (
    IS_SQLALCHEMY_1_4,
    SQLAlchemyQueryCombiner,
)


def _is_single_row_query_method(query: Any) -> bool:
    """
    Determine if a query method returns a single row.

    This is used by SQLAlchemyQueryCombiner to optimize query batching.
    For the custom profiler, we assume all our stat methods return single rows
    except for histogram and value frequencies which return multiple rows.
    """
    import traceback

    # Methods that return single rows (scalar results)
    SINGLE_ROW_QUERY_METHODS = {
        "get_row_count",
        "get_column_min",
        "get_column_max",
        "get_column_mean",
        "get_column_stdev",
        "get_column_unique_count",
        "get_column_non_null_count",
        "get_column_median",
    }

    # Methods that return multiple rows
    MULTI_ROW_QUERY_METHODS = {
        "get_column_histogram",
        "get_column_value_frequencies",
        "get_column_distinct_value_frequencies",
        "get_column_quantiles",  # May return multiple values but as a single row array
    }

    # Check the call stack to see which method is calling
    stack = traceback.extract_stack()
    for frame in reversed(stack):
        if frame.name in MULTI_ROW_QUERY_METHODS:
            return False
        if frame.name in SINGLE_ROW_QUERY_METHODS:
            return True

    # Default: assume single row for optimization
    return True


if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

logger: logging.Logger = logging.getLogger(__name__)

BIGQUERY = "bigquery"
ATHENA = "athena"
TRINO = "trino"


@dataclasses.dataclass
class DatahubSQLProfiler:
    """Custom SQLAlchemy-based profiler replacing Great Expectations."""

    report: SQLSourceReport
    config: GEProfilingConfig
    times_taken: List[float]
    total_row_count: int

    base_engine: Engine
    platform: str  # passed from parent source config
    env: str

    def __init__(
        self,
        conn: Union[Engine, Connection],
        report: SQLSourceReport,
        config: GEProfilingConfig,
        platform: str,
        env: str = "PROD",
    ):
        self.report = report
        self.config = config
        self.times_taken = []
        self.total_row_count = 0

        self.env = env

        # TRICKY: The call to `.engine` is quite important here. Connection.connect()
        # returns a "branched" connection, which does not actually use a new underlying
        # DB-API object from the connection pool. Engine.connect() does what we want to
        # make the threading code work correctly. As such, we need to make sure we've
        # got an engine here.
        self.base_engine = conn.engine

        if IS_SQLALCHEMY_1_4:
            # SQLAlchemy 1.4 added a statement "linter", which issues warnings about cartesian products in SELECT statements.
            # Changelog: https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#change-4737.
            # Code: https://github.com/sqlalchemy/sqlalchemy/blob/2f91dd79310657814ad28b6ef64f91fff7a007c9/lib/sqlalchemy/sql/compiler.py#L549
            #
            # The query combiner does indeed produce queries with cartesian products, but they are
            # safe because each "FROM" clause only returns one row, so the cartesian product
            # is also always a single row. As such, we disable the linter here.

            # Modified from https://github.com/sqlalchemy/sqlalchemy/blob/2f91dd79310657814ad28b6ef64f91fff7a007c9/lib/sqlalchemy/engine/create.py#L612
            self.base_engine.dialect.compiler_linting &= (  # type: ignore[attr-defined]
                ~sqlalchemy.sql.compiler.COLLECT_CARTESIAN_PRODUCTS  # type: ignore[attr-defined]
            )

        self.platform = platform.lower()

    def _get_columns_to_profile(self, table: sa.Table, dataset_name: str) -> List[str]:
        """Get list of columns to profile based on config and patterns."""
        if not self.config.any_field_level_metrics_enabled():
            return []

        columns_to_profile: List[str] = []
        ignored_columns_by_pattern: List[str] = []
        ignored_columns_by_type: List[str] = []

        for column in table.columns:
            col_name = column.name

            # Check allow/deny patterns (format: <table>.<column>)
            if not self.config._allow_deny_patterns.allowed(
                f"{dataset_name}.{col_name}"
            ):
                ignored_columns_by_pattern.append(col_name)
                continue

            # Check nested fields
            if not self.config.profile_nested_fields and "." in col_name:
                ignored_columns_by_pattern.append(col_name)
                continue

            # Check column type
            if self._should_ignore_column(column.type, column.name):
                ignored_columns_by_type.append(col_name)
                continue

            columns_to_profile.append(col_name)

        # Report ignored columns
        if ignored_columns_by_pattern:
            self.report.report_dropped(
                f"The profile of columns by pattern {dataset_name}({', '.join(sorted(ignored_columns_by_pattern))})"
            )
        if ignored_columns_by_type:
            self.report.report_dropped(
                f"The profile of columns by type {dataset_name}({', '.join(sorted(ignored_columns_by_type))})"
            )

        # Apply max_number_of_fields_to_profile limit
        if self.config.max_number_of_fields_to_profile is not None:
            if len(columns_to_profile) > self.config.max_number_of_fields_to_profile:
                columns_being_dropped = columns_to_profile[
                    self.config.max_number_of_fields_to_profile :
                ]
                columns_to_profile = columns_to_profile[
                    : self.config.max_number_of_fields_to_profile
                ]
                if self.config.report_dropped_profiles:
                    self.report.report_dropped(
                        f"The max_number_of_fields_to_profile={self.config.max_number_of_fields_to_profile} "
                        f"reached. Profile of columns {dataset_name}({', '.join(sorted(columns_being_dropped))})"
                    )

        return columns_to_profile

    def _should_ignore_column(
        self, sqlalchemy_type: sa_types.TypeEngine, column_name: str
    ) -> bool:
        """Check if column should be ignored based on type."""
        if str(sqlalchemy_type) == "NULL":
            return True

        sql_type = str(sqlalchemy_type)
        match = re.match(NORMALIZE_TYPE_PATTERN, sql_type)
        if match:
            sql_type = match.group(1)

        return sql_type in _get_column_types_to_ignore(self.platform)

    def generate_profiles(
        self,
        requests: List["GEProfilerRequest"],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[Tuple["GEProfilerRequest", Optional[DatasetProfileClass]]]:
        """
        Generate dataset profiles for a list of requests.

        This is the main entry point, matching DatahubGEProfiler.generate_profiles() signature.
        """
        max_workers = min(max_workers, len(requests)) if requests else max_workers
        if max_workers <= 0:
            max_workers = 1
        logger.info(
            f"Will profile {len(requests)} table(s) with {max_workers} worker(s) - this may take a while"
        )

        with (
            PerfTimer() as timer,
            concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as async_executor,
            SQLAlchemyQueryCombiner(
                enabled=self.config.query_combiner_enabled,
                catch_exceptions=self.config.catch_exceptions,
                is_single_row_query_method=_is_single_row_query_method,
                serial_execution_fallback_enabled=True,
            ).activate() as query_combiner,
        ):
            # Submit the profiling requests to the thread pool executor.
            async_profiles = collections.deque(
                async_executor.submit(
                    self._generate_profile_from_request,
                    query_combiner,
                    request,
                    platform=platform,
                    profiler_args=profiler_args,
                )
                for request in requests
            )

            # Avoid using as_completed so that the results are yielded in the
            # same order as the requests.
            while len(async_profiles) > 0:
                async_profile = async_profiles.popleft()
                yield async_profile.result()

        total_time_taken = timer.elapsed_seconds()
        logger.info(
            f"Profiling {len(requests)} table(s) finished in {total_time_taken:.3f} seconds"
        )

        time_percentiles: Dict[str, float] = {}

        if len(self.times_taken) > 0:
            percentiles = [50, 75, 95, 99]
            percentile_values = stats.calculate_percentiles(
                self.times_taken, percentiles
            )

            time_percentiles = {
                f"table_time_taken_p{percentile}": stats.discretize(
                    percentile_values[percentile]
                )
                for percentile in percentiles
            }

        telemetry.telemetry_instance.ping(
            "sql_profiling_summary",
            {
                "total_time_taken": stats.discretize(total_time_taken),
                "count": stats.discretize(len(self.times_taken)),
                "total_row_count": stats.discretize(self.total_row_count),
                "platform": self.platform,
                **time_percentiles,
            },
        )

        self.report.report_from_query_combiner(query_combiner.report)

    def _generate_profile_from_request(
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        request: "GEProfilerRequest",
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Tuple["GEProfilerRequest", Optional[DatasetProfileClass]]:
        return request, self._generate_single_profile(
            query_combiner=query_combiner,
            pretty_name=request.pretty_name,
            platform=platform,
            profiler_args=profiler_args,
            **request.batch_kwargs,
        )

    def _generate_single_profile(  # noqa: C901
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        pretty_name: str,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        partition: Optional[str] = None,
        custom_sql: Optional[str] = None,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
        **kwargs: Any,
    ) -> Optional[DatasetProfileClass]:
        """Generate a single dataset profile."""
        logger.debug(
            f"Received single profile request for {pretty_name} for {schema}, {table}, {custom_sql}"
        )

        platform = platform or self.platform
        bigquery_temp_table: Optional[str] = None
        temp_view: Optional[str] = None

        with PerfTimer() as timer:
            try:
                logger.info(f"Profiling {pretty_name}")
                with self.base_engine.connect() as conn:
                    # Handle custom SQL and temp tables
                    if platform.upper() == ATHENA and custom_sql:
                        # Get raw DBAPI connection
                        raw_conn = getattr(conn, "connection", None)
                        if raw_conn is None:
                            raw_conn = getattr(conn, "dbapi_connection", None)
                        if raw_conn is None:
                            raw_conn = conn  # Fallback to connection itself
                        temp_view = create_athena_temp_table(
                            self, custom_sql, pretty_name, raw_conn
                        )
                        if temp_view:
                            table = temp_view
                            schema = None
                            custom_sql = None

                    if platform == BIGQUERY and (
                        custom_sql or self.config.limit or self.config.offset
                    ):
                        if custom_sql:
                            bq_sql = custom_sql
                        else:
                            bq_sql = f"SELECT * FROM `{table}`"
                            if self.config.limit:
                                bq_sql += f" LIMIT {self.config.limit}"
                            if self.config.offset:
                                bq_sql += f" OFFSET {self.config.offset}"
                        # Get raw DBAPI connection
                        raw_conn = getattr(conn, "connection", None)
                        if raw_conn is None:
                            raw_conn = getattr(conn, "dbapi_connection", None)
                        if raw_conn is None:
                            raw_conn = conn  # Fallback to connection itself
                        bigquery_temp_table = create_bigquery_temp_table(
                            self, bq_sql, pretty_name, raw_conn
                        )
                        if bigquery_temp_table:
                            table = bigquery_temp_table
                            schema = None

                    # Create SQLAlchemy table object
                    # Note: custom_sql should already be handled via temp tables for Athena/BigQuery
                    # For other platforms with custom_sql, we'd need to create a view or use a subquery
                    if custom_sql and platform.upper() not in (ATHENA, BIGQUERY):
                        # For platforms other than Athena/BigQuery, custom SQL needs special handling
                        # This is a limitation - we'd need to create a view or use a subquery
                        logger.warning(
                            f"Custom SQL profiling for {platform} not fully supported for {pretty_name}. "
                            "Consider using Athena or BigQuery for custom SQL profiling."
                        )
                        return None

                    if not table:
                        logger.warning(
                            f"No table name provided for profiling {pretty_name}"
                        )
                        return None

                    # Get table metadata
                    metadata = sa.MetaData()
                    try:
                        sql_table = sa.Table(
                            table,
                            metadata,
                            schema=schema,
                            autoload_with=self.base_engine,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to load table metadata for {pretty_name}: {e}"
                        )
                        if not self.config.catch_exceptions:
                            raise
                        return None

                    # Initialize stats calculator with query combiner
                    stats_calc = StatsCalculator(
                        conn=conn, platform=platform, query_combiner=query_combiner
                    )

                    # Create profile
                    profile = DatasetProfileClass(timestampMillis=get_sys_time())
                    profile.columnCount = len(sql_table.columns)

                    # Handle partition spec
                    if partition:
                        profile.partitionSpec = PartitionSpecClass(partition=partition)
                    elif self.config.limit:
                        profile.partitionSpec = PartitionSpecClass(
                            type=PartitionTypeClass.QUERY,
                            partition=json.dumps(
                                dict(limit=self.config.limit, offset=self.config.offset)
                            ),
                        )
                    elif custom_sql:
                        profile.partitionSpec = PartitionSpecClass(
                            type=PartitionTypeClass.QUERY, partition="SAMPLE"
                        )

                    # Get row count - following GE profiler pattern:
                    # 1. Call method that modifies profile (queues query)
                    # 2. Flush to execute queries
                    # 3. Read result from profile
                    use_estimation = (
                        self.config.profile_table_row_count_estimate_only
                        and platform in ("postgresql", "mysql")
                    )
                    logger.debug(
                        f"Getting row count for {pretty_name}: "
                        f"use_estimation={use_estimation}, platform={platform}"
                    )

                    # Queue the row count query by calling the method
                    # The query will be executed when we flush
                    def _get_row_count_wrapper() -> None:
                        # Call the internal implementation directly to avoid double-wrapping
                        if use_estimation:
                            result = stats_calc._get_row_count_estimate(sql_table)
                        else:
                            result = stats_calc._get_row_count_impl(sql_table)
                        profile.rowCount = result
                        logger.debug(
                            f"_get_row_count_wrapper set profile.rowCount: {result}"
                        )

                    if query_combiner:
                        query_combiner.run(_get_row_count_wrapper)
                    else:
                        _get_row_count_wrapper()

                    # Flush to ensure the query executes (following GE profiler pattern)
                    if query_combiner:
                        query_combiner.flush()

                    row_count = profile.rowCount
                    logger.debug(
                        f"Row count result for {pretty_name}: {row_count}, type: {type(row_count)}"
                    )
                    self.total_row_count += row_count if row_count is not None else 0

                    # Handle BigQuery sampling if enabled
                    bigquery_sample_table: Optional[str] = None
                    if (
                        platform == BIGQUERY
                        and self.config.use_sampling
                        and not self.config.limit
                        and row_count
                        and row_count > self.config.sample_size
                    ):
                        """
                        According to BigQuery Sampling Docs, BigQuery does not cache the results
                        of a query that includes a TABLESAMPLE clause. However, for a simple
                        SELECT * query with TABLESAMPLE, results are cached and stored in a
                        temporary table. This can be (ab)used and all column level profiling
                        calculations can be performed against it.
                        """
                        sample_pc = 100 * self.config.sample_size / row_count
                        table_ref = f"{schema}.{table}" if schema else table
                        sql = (
                            f"SELECT * FROM `{table_ref}` "
                            f"TABLESAMPLE SYSTEM ({sample_pc:.8f} percent)"
                        )
                        # Get raw DBAPI connection
                        raw_conn = getattr(conn, "connection", None)
                        if raw_conn is None:
                            raw_conn = getattr(conn, "dbapi_connection", None)
                        if raw_conn is None:
                            raw_conn = conn  # Fallback to connection itself
                        bigquery_sample_table = create_bigquery_temp_table(
                            self, sql, pretty_name, raw_conn
                        )
                        if bigquery_sample_table:
                            # Update table reference to use sampled table
                            table = bigquery_sample_table
                            schema = None
                            # Recreate SQLAlchemy table object with sampled table
                            metadata = sa.MetaData()
                            sql_table = sa.Table(
                                table,
                                metadata,
                                schema=schema,
                                autoload_with=self.base_engine,
                            )
                            # Update partition spec to indicate sampling
                            if (
                                profile.partitionSpec
                                and profile.partitionSpec.type
                                == PartitionTypeClass.FULL_TABLE
                            ):
                                profile.partitionSpec = PartitionSpecClass(
                                    type=PartitionTypeClass.QUERY, partition="SAMPLE"
                                )
                            elif (
                                profile.partitionSpec
                                and profile.partitionSpec.type
                                == PartitionTypeClass.PARTITION
                            ):
                                profile.partitionSpec.partition += " SAMPLE"
                            # Recalculate row count for sampled table
                            # Note: We can't just use the original row_count because the actual
                            # sample size may differ from the configured sample_size
                            sampled_row_count = stats_calc.get_row_count(sql_table)
                            row_count = sampled_row_count
                            profile.rowCount = sampled_row_count
                            if profile.partitionSpec:
                                profile.partitionSpec.partition += (
                                    f" (sample rows {sampled_row_count})"
                                )

                    # Get columns to profile
                    columns_to_profile = self._get_columns_to_profile(
                        sql_table, pretty_name
                    )

                    # Check for tags to ignore sampling
                    from datahub.ingestion.source.sql_profiler.utils import (
                        _get_columns_to_ignore_sampling,
                    )

                    (
                        ignore_table_sampling,
                        columns_list_to_ignore_sampling,
                    ) = _get_columns_to_ignore_sampling(
                        pretty_name,
                        self.config.tags_to_ignore_sampling,
                        platform,
                        self.env,
                    )

                    # Profile columns
                    field_profiles = []
                    for column in sql_table.columns:
                        col_name = column.name
                        column_profile = DatasetFieldProfileClass(fieldPath=col_name)
                        field_profiles.append(column_profile)

                        if col_name not in columns_to_profile:
                            continue

                        # Get column type
                        col_type = get_column_profiler_type(column.type, platform)
                        if col_type == ProfilerDataType.UNKNOWN:
                            col_type = resolve_profiler_type_with_fallback(
                                column.type, platform, str(column.type)
                            )

                        # Get non-null count and null count - following GE profiler pattern
                        # Queue the query, then flush to execute
                        non_null_count_container: Dict[str, Optional[int]] = {
                            "value": None
                        }

                        def _get_non_null_count_wrapper(
                            container: Dict[
                                str, Optional[int]
                            ] = non_null_count_container,
                            name: str = col_name,
                        ) -> None:
                            # Call the internal implementation directly to avoid double-wrapping
                            container["value"] = (
                                stats_calc._get_column_non_null_count_impl(
                                    sql_table, name
                                )
                            )

                        if query_combiner:
                            query_combiner.run(_get_non_null_count_wrapper)
                            # Flush immediately to get the result (needed for subsequent calculations)
                            query_combiner.flush()
                        else:
                            _get_non_null_count_wrapper()

                        non_null_count = non_null_count_container["value"]
                        null_count = (
                            max(0, row_count - non_null_count)
                            if row_count is not None and non_null_count is not None
                            else None
                        )
                        if self.config.include_field_null_count:
                            column_profile.nullCount = null_count
                            if (
                                row_count is not None
                                and row_count > 0
                                and null_count is not None
                            ):
                                column_profile.nullProportion = min(
                                    1, null_count / row_count
                                )

                        # Get unique count
                        if self.config.include_field_distinct_count:
                            unique_count_container: Dict[str, Optional[int]] = {
                                "value": None
                            }

                            def _get_unique_count_wrapper(
                                container: Dict[
                                    str, Optional[int]
                                ] = unique_count_container,
                                name: str = col_name,
                            ) -> None:
                                # Call the internal implementation directly to avoid double-wrapping
                                container["value"] = (
                                    stats_calc._get_column_unique_count_impl(
                                        sql_table, name
                                    )
                                )

                            if query_combiner:
                                query_combiner.run(_get_unique_count_wrapper)
                                query_combiner.flush()
                            else:
                                _get_unique_count_wrapper()

                            unique_count = unique_count_container["value"]
                            column_profile.uniqueCount = unique_count
                            if (
                                non_null_count is not None
                                and non_null_count > 0
                                and unique_count is not None
                            ):
                                column_profile.uniqueProportion = min(
                                    1, unique_count / non_null_count
                                )
                            cardinality = convert_to_cardinality(
                                unique_count,
                                float(unique_count) / non_null_count
                                if non_null_count is not None
                                and non_null_count > 0
                                and unique_count is not None
                                else None,
                            )
                        else:
                            unique_count = None
                            cardinality = None

                        # Type-specific stats
                        # Only calculate stats if not ignoring sampling for this column
                        if (
                            not ignore_table_sampling
                            and col_name not in columns_list_to_ignore_sampling
                        ):
                            # Add sample values for all types
                            if self.config.include_field_sample_values:
                                sample_values = stats_calc.get_column_value_frequencies(
                                    sql_table,
                                    col_name,
                                    top_k=self.config.field_sample_values_limit,
                                )
                                # Filter out None values and convert to strings
                                column_profile.sampleValues = [
                                    str(value)
                                    for value, _ in sample_values
                                    if value is not None
                                ]

                            if col_type in (
                                ProfilerDataType.INT,
                                ProfilerDataType.FLOAT,
                            ):
                                if self.config.include_field_min_value:
                                    min_val = stats_calc._get_column_min_impl(
                                        sql_table, col_name
                                    )
                                    if min_val is not None:
                                        column_profile.min = str(min_val)

                                if self.config.include_field_max_value:
                                    max_val = stats_calc._get_column_max_impl(
                                        sql_table, col_name
                                    )
                                    if max_val is not None:
                                        column_profile.max = str(max_val)

                                if self.config.include_field_mean_value:
                                    mean_val = stats_calc._get_column_mean_impl(
                                        sql_table, col_name
                                    )
                                    if mean_val is not None:
                                        column_profile.mean = str(mean_val)

                                if self.config.include_field_stddev_value:
                                    stdev_val = stats_calc._get_column_stdev_impl(
                                        sql_table, col_name
                                    )
                                    if stdev_val is not None:
                                        column_profile.stdev = str(stdev_val)

                                if self.config.include_field_median_value:
                                    median_val = stats_calc._get_column_median_impl(
                                        sql_table, col_name
                                    )
                                    if median_val is not None:
                                        column_profile.median = str(median_val)

                                if self.config.include_field_quantiles:
                                    quantiles = stats_calc.get_column_quantiles(
                                        sql_table,
                                        col_name,
                                        [0.05, 0.25, 0.5, 0.75, 0.95],
                                    )
                                    column_profile.quantiles = [
                                        QuantileClass(quantile=str(q), value=str(v))
                                        for q, v in zip(
                                            [0.05, 0.25, 0.5, 0.75, 0.95], quantiles
                                        )
                                        if v is not None
                                    ]

                                    # Add histogram for numeric columns with high cardinality
                                    if (
                                        self.config.include_field_histogram
                                        and cardinality
                                        and cardinality
                                        in {
                                            Cardinality.FEW,
                                            Cardinality.MANY,
                                            Cardinality.VERY_MANY,
                                        }
                                    ):
                                        histogram = stats_calc.get_column_histogram(
                                            sql_table, col_name
                                        )
                                        if histogram:
                                            # Convert to HistogramClass format
                                            # boundaries: bucket boundaries (k+1 values for k buckets)
                                            # heights: counts per bucket (k values)
                                            boundaries = [
                                                str(start) for start, _, _ in histogram
                                            ]
                                            # Add the last bucket end as final boundary
                                            if histogram:
                                                boundaries.append(str(histogram[-1][1]))
                                            heights = [
                                                float(count)
                                                for _, _, count in histogram
                                            ]
                                            column_profile.histogram = HistogramClass(
                                                boundaries=boundaries, heights=heights
                                            )

                                    # Add distinct value frequencies for low cardinality numeric columns
                                    if (
                                        self.config.include_field_distinct_value_frequencies
                                        and cardinality
                                        and cardinality
                                        in {
                                            Cardinality.ONE,
                                            Cardinality.TWO,
                                            Cardinality.VERY_FEW,
                                        }
                                    ):
                                        frequencies = stats_calc.get_column_distinct_value_frequencies(
                                            sql_table, col_name
                                        )
                                        column_profile.distinctValueFrequencies = [
                                            ValueFrequencyClass(
                                                value=str(value), frequency=freq
                                            )
                                            for value, freq in frequencies
                                        ]

                            elif col_type == ProfilerDataType.STRING:
                                # For string columns, add distinct value frequencies for low cardinality
                                if (
                                    self.config.include_field_distinct_value_frequencies
                                    and cardinality
                                    and cardinality
                                    in {
                                        Cardinality.ONE,
                                        Cardinality.TWO,
                                        Cardinality.VERY_FEW,
                                        Cardinality.FEW,
                                    }
                                ):
                                    frequencies = stats_calc.get_column_distinct_value_frequencies(
                                        sql_table, col_name
                                    )
                                    column_profile.distinctValueFrequencies = [
                                        ValueFrequencyClass(
                                            value=str(value), frequency=freq
                                        )
                                        for value, freq in frequencies
                                    ]

                            elif col_type == ProfilerDataType.DATETIME:
                                # For datetime columns, add min/max
                                if self.config.include_field_min_value:
                                    min_val = stats_calc.get_column_min(
                                        sql_table, col_name
                                    )
                                    if min_val is not None:
                                        column_profile.min = str(min_val)

                                if self.config.include_field_max_value:
                                    max_val = stats_calc.get_column_max(
                                        sql_table, col_name
                                    )
                                    if max_val is not None:
                                        column_profile.max = str(max_val)

                                # Add distinct value frequencies for low cardinality datetime columns
                                if (
                                    self.config.include_field_distinct_value_frequencies
                                    and cardinality
                                    and cardinality
                                    in {
                                        Cardinality.ONE,
                                        Cardinality.TWO,
                                        Cardinality.VERY_FEW,
                                        Cardinality.FEW,
                                    }
                                ):
                                    frequencies = stats_calc.get_column_distinct_value_frequencies(
                                        sql_table, col_name
                                    )
                                    column_profile.distinctValueFrequencies = [
                                        ValueFrequencyClass(
                                            value=str(value), frequency=freq
                                        )
                                        for value, freq in frequencies
                                    ]

                            else:
                                # For other types, add distinct value frequencies for low cardinality
                                if (
                                    self.config.include_field_distinct_value_frequencies
                                    and cardinality
                                    and cardinality
                                    in {
                                        Cardinality.ONE,
                                        Cardinality.TWO,
                                        Cardinality.VERY_FEW,
                                        Cardinality.FEW,
                                    }
                                ):
                                    frequencies = stats_calc.get_column_distinct_value_frequencies(
                                        sql_table, col_name
                                    )
                                    column_profile.distinctValueFrequencies = [
                                        ValueFrequencyClass(
                                            value=str(value), frequency=freq
                                        )
                                        for value, freq in frequencies
                                    ]

                    profile.fieldProfiles = field_profiles

                    time_taken = timer.elapsed_seconds()
                    logger.info(
                        f"Finished profiling {pretty_name}; took {time_taken:.3f} seconds"
                    )
                    self.times_taken.append(time_taken)

                    return profile

            except Exception as e:
                if not self.config.catch_exceptions:
                    raise e

                error_message = str(e).lower()
                if "permission denied" in error_message:
                    self.report.warning(
                        title="Unauthorized to extract data profile statistics",
                        message="We were denied access while attempting to generate profiling statistics for some assets. Please ensure the provided user has permission to query these tables and views.",
                        context=f"Asset: {pretty_name}",
                        exc=e,
                    )
                else:
                    self.report.warning(
                        title="Failed to extract statistics for some assets",
                        message="Caught unexpected exception while attempting to extract profiling statistics for some assets.",
                        context=f"Asset: {pretty_name}",
                        exc=e,
                    )
                return None
            finally:
                # Cleanup temp tables
                if temp_view and platform.upper() in (ATHENA, TRINO):
                    drop_temp_table(self, temp_view)
                # Note: BigQuery temp tables (cached results) are automatically cleaned up
                # by BigQuery after 24 hours, so we don't need to explicitly drop them
