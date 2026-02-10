"""Query combiner runner for executing profiling queries with batching optimization."""

import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Optional,
    ParamSpec,
    Tuple,
    TypeVar,
)

from sqlalchemy.engine import Connection
from typing_extensions import Concatenate

if TYPE_CHECKING:
    import sqlalchemy as sa

    from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
        PlatformAdapter,
    )

logger: logging.Logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _run_with_query_combiner(
    method: Callable[Concatenate["QueryCombinerRunner", P], R],
) -> Callable[Concatenate["QueryCombinerRunner", P], R]:
    """Decorator to run method with query combiner."""

    @functools.wraps(method)
    def inner(self: "QueryCombinerRunner", *args: P.args, **kwargs: P.kwargs) -> R:
        if self.query_combiner:
            return self.query_combiner.run(lambda: method(self, *args, **kwargs))
        return method(self, *args, **kwargs)

    return inner


class QueryCombinerRunner:
    """
    Facade for executing profiling queries with optional query combining optimization.

    This class wraps a PlatformAdapter and provides query combining functionality
    to batch multiple SQL queries into a single database round-trip.

    The class delegates all actual query logic to the adapter, its sole responsibility
    is applying the @_run_with_query_combiner decorator to batch queries when possible.

    Method Decoration Strategy:
        - Decorated methods: Single-value scalar queries (e.g., COUNT, MIN, MAX, AVG, STDDEV)
          These return a single scalar value and can be efficiently batched together.

        - Non-decorated methods: Complex queries (e.g., histograms, frequencies, quantiles)
          These methods either:
          1. Execute multiple queries internally (e.g., quantiles may query each percentile)
          2. Return complex result sets (lists of tuples, arrays)
          3. Already perform their own batching/optimization
          Attempting to batch these would add unnecessary complexity.

    Architecture:
        User -> QueryCombinerRunner (applies query combiner) -> PlatformAdapter (executes SQL)
    """

    def __init__(
        self,
        conn: Connection,
        platform: str,
        adapter: "PlatformAdapter",
        query_combiner: Optional[Any] = None,
    ):
        """
        Initialize the query combiner runner.

        Args:
            conn: Active database connection
            platform: Database platform name (for logging/debugging)
            adapter: Platform-specific adapter that executes queries
            query_combiner: Optional query combiner for batching queries
        """
        self.conn = conn
        self.platform = platform.lower()
        self.adapter = adapter
        self.query_combiner = query_combiner

    @_run_with_query_combiner
    def get_row_count(
        self,
        table: "sa.Table",
        sample_clause: Optional[str] = None,
        use_estimation: bool = False,
    ) -> int:
        """Get row count with optional sampling or estimation."""
        return self.adapter.get_row_count(
            table, self.conn, sample_clause, use_estimation
        )

    @_run_with_query_combiner
    def get_column_non_null_count(self, table: "sa.Table", column: str) -> int:
        """Get non-null count (avoids GE's problematic IN (NULL) pattern)."""
        return self.adapter.get_column_non_null_count(table, column, self.conn)

    @_run_with_query_combiner
    def get_column_min(self, table: "sa.Table", column: str) -> Any:
        """Get minimum value for a column."""
        return self.adapter.get_column_min(table, column, self.conn)

    @_run_with_query_combiner
    def get_column_max(self, table: "sa.Table", column: str) -> Any:
        """Get maximum value for a column."""
        return self.adapter.get_column_max(table, column, self.conn)

    @_run_with_query_combiner
    def get_column_mean(self, table: "sa.Table", column: str) -> Optional[float]:
        """Get average value for a column."""
        return self.adapter.get_column_mean(table, column, self.conn)

    @_run_with_query_combiner
    def get_column_stdev(self, table: "sa.Table", column: str) -> Optional[float]:
        """Get standard deviation for a column."""
        return self.adapter.get_column_stdev(table, column, self.conn)

    @_run_with_query_combiner
    def get_column_unique_count(
        self, table: "sa.Table", column: str, use_approx: bool = True
    ) -> int:
        """Get unique count (approximate if use_approx=True)."""
        return self.adapter.get_column_unique_count(
            table, column, self.conn, use_approx
        )

    @_run_with_query_combiner
    def get_column_median(self, table: "sa.Table", column: str) -> Any:
        """Get median value for a column (database-specific)."""
        return self.adapter.get_column_median(table, column, self.conn)

    def get_column_quantiles(
        self,
        table: "sa.Table",
        column: str,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column (approximate where possible).

        Note: This method doesn't use the decorator because it may execute
        multiple queries or a single query with array results.
        """
        return self.adapter.get_column_quantiles(table, column, self.conn, quantiles)

    def get_column_histogram(
        self,
        table: "sa.Table",
        column: str,
        num_buckets: int = 10,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> List[Tuple[float, float, int]]:
        """
        Generate histogram for a column.

        Note: This method doesn't use the decorator because it executes
        multiple queries to compute bucket ranges and counts.

        Returns: List of (bucket_start, bucket_end, count) tuples.
        """
        return self.adapter.get_column_histogram(
            table, column, self.conn, num_buckets, min_val, max_val
        )

    def get_column_value_frequencies(
        self,
        table: "sa.Table",
        column: str,
        top_k: int = 10,
    ) -> List[Tuple[Any, int]]:
        """
        Get top-K most frequent values and their counts.

        Note: This method doesn't use the decorator because it returns
        a complex result set (list of tuples).

        Returns: List of (value, count) tuples, sorted by count descending.
        """
        return self.adapter.get_column_value_frequencies(
            table, column, self.conn, top_k
        )

    def get_column_distinct_value_frequencies(
        self,
        table: "sa.Table",
        column: str,
    ) -> List[Tuple[Any, int]]:
        """
        Get all distinct values with their counts (sorted by value).

        Note: This method doesn't use the decorator because it returns
        a complex result set (list of tuples).

        Returns: List of (value, count) tuples, sorted by value.
        """
        return self.adapter.get_column_distinct_value_frequencies(
            table, column, self.conn
        )

    def get_column_sample_values(
        self,
        table: "sa.Table",
        column: str,
        limit: int = 20,
    ) -> List[Any]:
        """
        Get actual sample rows from the table (not distinct values).

        Note: This method doesn't use the decorator because it returns
        a complex result set (list of values).

        Returns: List of sample values (may contain duplicates).
        """
        return self.adapter.get_column_sample_values(table, column, self.conn, limit)
