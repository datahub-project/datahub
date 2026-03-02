"""Query combiner runner for executing profiling queries with batching optimization."""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, List, Optional, Tuple, TypeVar

from sqlalchemy.engine import Connection

if TYPE_CHECKING:
    import sqlalchemy as sa

    from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
        PlatformAdapter,
    )
    from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombiner

logger: logging.Logger = logging.getLogger(__name__)

T = TypeVar("T")

# Sentinel value to distinguish "not set" from "set to None"
_NOT_SET = object()


@dataclass
class _ResultContainer(Generic[T]):
    """Container for deferred query result. Internal implementation detail."""

    value: Any = _NOT_SET  # Use sentinel instead of Optional[T]
    exc: Optional[Exception] = None  # Store exception if query fails


class FutureResult(Generic[T]):
    """
    Represents a query result that will be available after query_combiner.flush().

    This makes batching behavior explicit in the type system:
    - Methods returning FutureResult[T] are batched via query combiner
    - Call flush() to execute all scheduled queries
    - Call result() to extract the value after flush()

    Example:
        # Schedule multiple queries
        row_count = runner.get_row_count(table)
        min_val = runner.get_column_min(table, "col")
        max_val = runner.get_column_max(table, "col")

        # Execute all 3 queries in one batch
        query_combiner.flush()

        # Extract results
        print(row_count.result())
        print(min_val.result())
        print(max_val.result())
    """

    def __init__(self, container: _ResultContainer[T]):
        """
        Initialize with result container.

        Args:
            container: Container that will hold the result after flush()
        """
        self._container = container

    def result(self) -> T:
        """
        Get the query result.

        Must be called after query_combiner.flush() has executed the query.

        Returns:
            The query result value (can be None if query returned NULL)

        Raises:
            Exception: The actual database exception if query failed
            ValueError: If flush() hasn't been called yet
        """
        # Re-raise the actual exception if query failed
        if self._container.exc is not None:
            raise self._container.exc

        if self._container.value is _NOT_SET:
            raise ValueError(
                "Result not available yet. Must call query_combiner.flush() first."
            )
        return self._container.value

    def __repr__(self) -> str:
        if self._container.exc is not None:
            return f"FutureResult(error={self._container.exc!r})"
        if self._container.value is _NOT_SET:
            return "FutureResult(pending)"
        return f"FutureResult(value={self._container.value!r})"


class QueryCombinerRunner:
    """
    Facade for executing profiling queries with batching optimization via query combiner.

    This class wraps a PlatformAdapter and provides query combining functionality
    to batch multiple SQL queries into a single database round-trip.

    Methods that return FutureResult[T]:
        - Single-value scalar queries (e.g., COUNT, MIN, MAX, AVG, STDDEV)
        - These return FutureResult[T] which makes batching explicit in the type system
        - Schedule multiple queries, call query_combiner.flush(), then extract results

    Methods that return direct values:
        - Complex queries (e.g., histograms, frequencies, quantiles)
        - These either execute multiple queries internally or return complex result sets
        - Cannot be efficiently batched

    Usage pattern:
        # Schedule queries (returns FutureResult immediately)
        row_count_future = runner.get_row_count(table)
        min_future = runner.get_column_min(table, "col")
        max_future = runner.get_column_max(table, "col")

        # Execute all 3 queries in one batch
        query_combiner.flush()

        # Extract results
        row_count = row_count_future.result()
        min_val = min_future.result()
        max_val = max_future.result()

    TODO: Future enhancement - Add context manager for automatic flush:
        with runner.batch() as batch:
            row_count = batch.get_row_count(table)
            min_val = batch.get_column_min(table, "col")
            # Auto-flushes on exit
        print(row_count.result())

    Architecture:
        User -> QueryCombinerRunner (schedules via query combiner) -> PlatformAdapter (executes SQL)
    """

    def __init__(
        self,
        conn: Connection,
        platform: str,
        adapter: "PlatformAdapter",
        query_combiner: "SQLAlchemyQueryCombiner",
    ):
        """
        Initialize the query combiner runner.

        Args:
            conn: Active database connection
            platform: Database platform name (for logging/debugging)
            adapter: Platform-specific adapter that executes queries
            query_combiner: Query combiner for batching queries (required)
        """
        self.conn = conn
        self.platform = platform.lower()
        self.adapter = adapter
        self.query_combiner = query_combiner

    def get_row_count(
        self,
        table: "sa.Table",
        sample_clause: Optional[str] = None,
        use_estimation: bool = False,
    ) -> FutureResult[int]:
        """
        Get row count with optional sampling or estimation.

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[int]()

        def execute():
            try:
                container.value = self.adapter.get_row_count(
                    table, self.conn, sample_clause, use_estimation
                )
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_non_null_count(
        self, table: "sa.Table", column: str
    ) -> FutureResult[int]:
        """
        Get non-null count (avoids GE's problematic IN (NULL) pattern).

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[int]()

        def execute():
            try:
                container.value = self.adapter.get_column_non_null_count(
                    table, column, self.conn
                )
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_min(self, table: "sa.Table", column: str) -> FutureResult[Any]:
        """
        Get minimum value for a column.

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[Any]()

        def execute():
            try:
                container.value = self.adapter.get_column_min(table, column, self.conn)
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_max(self, table: "sa.Table", column: str) -> FutureResult[Any]:
        """
        Get maximum value for a column.

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[Any]()

        def execute():
            try:
                container.value = self.adapter.get_column_max(table, column, self.conn)
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_mean(
        self, table: "sa.Table", column: str
    ) -> FutureResult[Optional[float]]:
        """
        Get average value for a column.

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[Optional[float]]()

        def execute():
            try:
                container.value = self.adapter.get_column_mean(table, column, self.conn)
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_stdev(
        self, table: "sa.Table", column: str
    ) -> FutureResult[Optional[float]]:
        """
        Get standard deviation for a column.

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[Optional[float]]()

        def execute():
            try:
                container.value = self.adapter.get_column_stdev(
                    table, column, self.conn
                )
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_unique_count(
        self, table: "sa.Table", column: str, use_approx: bool = True
    ) -> FutureResult[int]:
        """
        Get unique count (approximate if use_approx=True).

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[int]()

        def execute():
            try:
                container.value = self.adapter.get_column_unique_count(
                    table, column, self.conn, use_approx
                )
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

    def get_column_median(self, table: "sa.Table", column: str) -> FutureResult[Any]:
        """
        Get median value for a column (database-specific).

        Returns FutureResult that resolves after query_combiner.flush().
        """
        container = _ResultContainer[Any]()

        def execute():
            try:
                container.value = self.adapter.get_column_median(
                    table, column, self.conn
                )
            except Exception as e:
                container.exc = e

        self.query_combiner.run(execute)
        return FutureResult(container=container)

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
