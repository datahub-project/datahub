"""Statistical calculation methods for the custom SQLAlchemy profiler."""

import functools
import logging
from typing import Any, Callable, List, Optional, ParamSpec, Tuple, TypeVar

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from typing_extensions import Concatenate

from datahub.ingestion.source.sql_profiler.database_handlers import (
    DatabaseHandlers,
)

logger: logging.Logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _run_with_query_combiner(
    method: Callable[Concatenate["StatsCalculator", P], R],
) -> Callable[Concatenate["StatsCalculator", P], R]:
    """Decorator to run method with query combiner."""

    @functools.wraps(method)
    def inner(self: "StatsCalculator", *args: P.args, **kwargs: P.kwargs) -> R:
        if self.query_combiner:
            return self.query_combiner.run(lambda: method(self, *args, **kwargs))
        return method(self, *args, **kwargs)

    return inner


class StatsCalculator:
    """Calculates statistical metrics for table columns."""

    def __init__(
        self,
        conn: Connection,
        platform: str,
        query_combiner: Optional[Any] = None,
    ):
        self.conn = conn
        self.platform = platform.lower()
        self.query_combiner = query_combiner

    def _get_row_count_impl(
        self,
        table: sa.Table,
        sample_clause: Optional[str] = None,
        use_estimation: bool = False,
    ) -> int:
        """Internal implementation of get_row_count (not decorated)."""
        logger.debug(
            f"_get_row_count_impl called: table={table.name}, schema={table.schema}, "
            f"use_estimation={use_estimation}, sample_clause={sample_clause}"
        )
        if use_estimation:
            estimated_result = self._get_row_count_estimate(table)
            logger.debug(f"_get_row_count_impl (estimation) result: {estimated_result}")
            return estimated_result

        query = sa.select([sa.func.count()]).select_from(table)
        if sample_clause:
            query = query.suffix_with(sample_clause)
        count_result: Any = self.conn.execute(query).scalar()
        logger.debug(
            f"_get_row_count_impl query result (raw): {count_result}, type: {type(count_result)}"
        )
        # scalar() can return Any | None, so we need to handle None
        if count_result is None:
            return 0
        final_result = int(count_result)
        logger.debug(f"_get_row_count_impl final result: {final_result}")
        return final_result

    @_run_with_query_combiner
    def get_row_count(
        self,
        table: sa.Table,
        sample_clause: Optional[str] = None,
        use_estimation: bool = False,
    ) -> int:
        """Get row count with optional sampling or estimation."""
        return self._get_row_count_impl(table, sample_clause, use_estimation)

    def _get_row_count_estimate(self, table: sa.Table) -> int:
        """Get estimated row count for PostgreSQL/MySQL."""
        if self.platform == "postgresql":
            # Use pg_class.reltuples
            schema = table.schema or "public"
            query = sa.text(
                f"SELECT reltuples::bigint FROM pg_class "
                f"WHERE oid = '{schema}.{table.name}'::regclass"
            )
        elif self.platform == "mysql":
            # Use information_schema.tables.table_rows
            schema = table.schema or "information_schema"
            query = sa.text(
                f"SELECT table_rows FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table.name}'"
            )
        else:
            raise ValueError(f"Row count estimation not supported for {self.platform}")
        result = self.conn.execute(query).scalar()
        return int(result) if result is not None else 0

    def _get_column_non_null_count_impl(self, table: sa.Table, column: str) -> int:
        """Internal implementation of get_column_non_null_count (not decorated)."""
        query = sa.select([sa.func.count(sa.column(column))]).select_from(table)
        result = self.conn.execute(query).scalar()
        return int(result) if result is not None else 0

    @_run_with_query_combiner
    def get_column_non_null_count(self, table: sa.Table, column: str) -> int:
        """
        Get non-null count (avoids GE's problematic IN (NULL) pattern).

        Uses COUNT(column) which automatically excludes NULLs.
        """
        return self._get_column_non_null_count_impl(table, column)

    def _get_column_min_impl(self, table: sa.Table, column: str) -> Any:
        """Internal implementation of get_column_min (not decorated)."""
        query = sa.select([sa.func.min(sa.column(column))]).select_from(table)
        return self.conn.execute(query).scalar()

    @_run_with_query_combiner
    def get_column_min(self, table: sa.Table, column: str) -> Any:
        """Get minimum value for a column."""
        return self._get_column_min_impl(table, column)

    def _get_column_max_impl(self, table: sa.Table, column: str) -> Any:
        """Internal implementation of get_column_max (not decorated)."""
        query = sa.select([sa.func.max(sa.column(column))]).select_from(table)
        return self.conn.execute(query).scalar()

    @_run_with_query_combiner
    def get_column_max(self, table: sa.Table, column: str) -> Any:
        """Get maximum value for a column."""
        return self._get_column_max_impl(table, column)

    def _get_column_mean_impl(self, table: sa.Table, column: str) -> Optional[float]:
        """Internal implementation of get_column_mean (not decorated)."""
        query = sa.select([sa.func.avg(sa.column(column))]).select_from(table)
        result = self.conn.execute(query).scalar()
        return float(result) if result is not None else None

    @_run_with_query_combiner
    def get_column_mean(self, table: sa.Table, column: str) -> Optional[float]:
        """Get average value for a column."""
        return self._get_column_mean_impl(table, column)

    def _get_column_stdev_impl(self, table: sa.Table, column: str) -> Optional[float]:
        """Internal implementation of get_column_stdev (not decorated)."""
        query = sa.select([sa.func.stddev(sa.column(column))]).select_from(table)
        result = self.conn.execute(query).scalar()
        return float(result) if result is not None else None

    @_run_with_query_combiner
    def get_column_stdev(self, table: sa.Table, column: str) -> Optional[float]:
        """Get standard deviation for a column."""
        return self._get_column_stdev_impl(table, column)

    def _get_column_unique_count_impl(
        self, table: sa.Table, column: str, use_approx: bool = True
    ) -> int:
        """Internal implementation of get_column_unique_count (not decorated)."""
        if use_approx:
            expr = DatabaseHandlers.get_approx_unique_count_expr(self.platform, column)
        else:
            expr = sa.func.count(sa.func.distinct(sa.column(column)))

        query = sa.select([expr]).select_from(table)
        result = self.conn.execute(query).scalar()
        return int(result) if result is not None else 0

    @_run_with_query_combiner
    def get_column_unique_count(
        self, table: sa.Table, column: str, use_approx: bool = True
    ) -> int:
        """Get unique count (approximate if use_approx=True)."""
        return self._get_column_unique_count_impl(table, column, use_approx)

    def _get_column_median_impl(self, table: sa.Table, column: str) -> Optional[float]:
        """Internal implementation of get_column_median (not decorated)."""
        expr = DatabaseHandlers.get_median_expr(self.platform, column)
        if expr is None:
            return None

        query = sa.select([expr]).select_from(table)
        result = self.conn.execute(query).scalar()
        return float(result) if result is not None else None

    @_run_with_query_combiner
    def get_column_median(self, table: sa.Table, column: str) -> Optional[float]:
        """Get median value for a column (database-specific)."""
        return self._get_column_median_impl(table, column)

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        if quantiles is None:
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
        """Get quantile values for a column (approximate where possible)."""
        # Note: This method doesn't use the decorator because it may execute
        # multiple queries or a single query with array results
        return DatabaseHandlers.get_quantiles(
            self.conn, self.platform, table, column, quantiles
        )

    def get_column_histogram(
        self,
        table: sa.Table,
        column: str,
        num_buckets: int = 10,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> List[Tuple[float, float, int]]:
        """
        Generate histogram for a column.

        Returns: List of (bucket_start, bucket_end, count) tuples.
        """
        # Get min/max if not provided
        if min_val is None:
            min_val = self.get_column_min(table, column)
        if max_val is None:
            max_val = self.get_column_max(table, column)

        if min_val is None or max_val is None:
            return []

        # Calculate bucket size
        bucket_size = (max_val - min_val) / num_buckets if max_val != min_val else 1.0

        # Generate SQL to count values in each bucket using CASE WHEN
        buckets = []
        for i in range(num_buckets):
            bucket_start = min_val + i * bucket_size
            bucket_end = min_val + (i + 1) * bucket_size

            # Create case expression for this bucket
            if i < num_buckets - 1:
                bucket_case_expr: Any = sa.case(
                    [
                        (
                            sa.and_(
                                sa.column(column) >= bucket_start,
                                sa.column(column) < bucket_end,
                            ),
                            1,
                        )
                    ],
                    else_=0,
                )
            else:
                # Last bucket includes the max value
                bucket_case_expr = sa.case(
                    [
                        (
                            sa.and_(
                                sa.column(column) >= bucket_start,
                                sa.column(column) <= bucket_end,
                            ),
                            1,
                        )
                    ],
                    else_=0,
                )
            buckets.append(sa.func.sum(bucket_case_expr).label(f"bucket_{i}"))

        query = sa.select(buckets).select_from(table)
        result = self.conn.execute(query).fetchone()

        # Convert to list of tuples
        histogram: List[Tuple[float, float, int]] = []
        if result is None:
            return histogram
        for i, count in enumerate(result):
            bucket_start = min_val + i * bucket_size
            bucket_end = min_val + (i + 1) * bucket_size
            histogram.append((bucket_start, bucket_end, int(count or 0)))

        return histogram

    def get_column_value_frequencies(
        self,
        table: sa.Table,
        column: str,
        top_k: int = 10,
    ) -> List[Tuple[Any, int]]:
        """
        Get top-K most frequent values and their counts.

        Returns: List of (value, count) tuples, sorted by count descending.
        """
        count_expr = sa.func.count().label("count")
        query = (
            sa.select([sa.column(column), count_expr])
            .select_from(table)
            .group_by(sa.column(column))
            .order_by(count_expr.desc())
            .limit(top_k)
        )

        result = self.conn.execute(query).fetchall()
        return [(row[0], int(row[1])) for row in result]

    def get_column_distinct_value_frequencies(
        self,
        table: sa.Table,
        column: str,
    ) -> List[Tuple[Any, int]]:
        """
        Get all distinct values with their counts (sorted by value).

        Returns: List of (value, count) tuples, sorted by value.
        """
        count_expr = sa.func.count().label("count")
        query = (
            sa.select([sa.column(column), count_expr])
            .select_from(table)
            .group_by(sa.column(column))
            .order_by(sa.column(column))
        )

        result = self.conn.execute(query).fetchall()
        return [(row[0], int(row[1])) for row in result]
