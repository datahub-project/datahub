"""Abstract base class for platform-specific profiling adapters."""

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)

logger = logging.getLogger(__name__)

# Default quantiles for statistical profiling
DEFAULT_QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]


class PlatformAdapter(ABC):
    """
    Abstract base class for platform-specific profiling logic.

    Each database platform (BigQuery, Snowflake, etc.) implements this interface
    to provide platform-specific SQL expressions, temp table handling, and
    optimizations.

    This design keeps all platform-specific code in one place per platform,
    making the codebase easier to understand and maintain.

    Some methods (eg `get_column_max`, `get_column_min`, etc) return `Any` to preserve native
    type formatting to match GE behavior.
    Instead, this `PlatformAdapter` should be more opinionated on the expected data type for those methods, so:
     - we can have consistent formatting across different sources
     - no complex formatting depending on native data types, as we currently do in `sqlalchemy_profiler.py`
       in order to match GE profiler
    """

    def __init__(
        self,
        config: ProfilingConfig,
        report: SQLSourceReport,
        base_engine: Engine,
    ):
        """
        Initialize the platform adapter.

        Args:
            config: Profiling configuration
            report: Report object for warnings/errors
            base_engine: SQLAlchemy engine for database connections
        """
        self.config = config
        self.report = report
        self.base_engine = base_engine

    # =========================================================================
    # Setup & Teardown
    # =========================================================================

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Platform-specific setup before profiling.

        This method handles:
        - Creating temp tables/views for custom SQL or partitions
        - Setting up table sampling for large tables
        - Creating the SQLAlchemy Table object

        Default implementation: Creates SQLAlchemy table object with no special handling.
        Platforms like BigQuery, Athena, and Trino override this for custom behavior.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context with sql_table ready for profiling

        Raises:
            ValueError: If table name is not provided
        """
        if not context.table:
            raise ValueError(
                f"Cannot profile {context.pretty_name}: table name required"
            )

        # Create SQLAlchemy table object
        context.sql_table = self._create_sqlalchemy_table(
            schema=context.schema,
            table=context.table,
        )

        logger.debug(
            f"Setup profiling for {context.pretty_name}: "
            f"schema={context.schema}, table={context.table}"
        )

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Cleanup temp resources created during profiling.

        Default implementation: No cleanup needed (no temp resources created).
        Platforms that create temp resources (like BigQuery) should override this.

        Args:
            context: Profiling context with resources to clean up
        """
        # Default implementation: no temp resources to clean up
        return

    # =========================================================================
    # Identifier Quoting
    # =========================================================================

    def quote_identifier(self, identifier: str) -> str:
        """
        Quote a SQL identifier using the platform's dialect.

        This method handles both simple identifiers (my_table, my_column) and
        multi-part identifiers (my_schema.my_table, my_db.my_schema.my_table).

        Default implementation splits on dots and quotes each part separately,
        which works for most platforms. Platforms with special requirements
        (like BigQuery) can override this method.

        Args:
            identifier: The identifier to quote (table name, column name, etc.)

        Returns:
            Properly quoted identifier safe for use in SQL queries

        Examples:
            >>> adapter.quote_identifier("my_table")
            '"my_table"'
            >>> adapter.quote_identifier("my_schema.my_table")
            '"my_schema"."my_table"'
        """
        preparer = self.base_engine.dialect.identifier_preparer
        # Split on dots to handle multi-part identifiers (schema.table, etc.)
        parts = identifier.split(".")
        # Quote each part separately using SQLAlchemy's dialect-specific identifier preparer
        # Use quoted_name to force quoting (quote() doesn't quote unless necessary)
        quoted_parts = [
            preparer.quote(sa.sql.quoted_name(part, True)) for part in parts
        ]
        return ".".join(quoted_parts)

    # =========================================================================
    # SQL Expression Builders (used by query execution methods)
    # =========================================================================

    @abstractmethod
    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        Get platform-specific approximate distinct count expression.

        Examples:
        - BigQuery: APPROX_COUNT_DISTINCT(column)
        - Snowflake: APPROX_COUNT_DISTINCT(column)
        - Redshift: APPROXIMATE count(distinct column)
        - Generic: COUNT(DISTINCT column)

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approximate unique count
        """
        pass

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Get platform-specific median expression.

        Default implementation returns None (not supported).
        Platforms that support median should override this.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for median, or None if unsupported
        """
        return None

    def get_quantiles_expr(
        self, column: str, quantiles: List[float]
    ) -> Optional[ColumnElement[Any]]:
        """
        Get platform-specific quantiles expression.

        Default implementation returns None (not supported).
        Platforms that support quantiles should override this.

        Args:
            column: Column name
            quantiles: List of quantile values (e.g., [0.25, 0.5, 0.75])

        Returns:
            SQLAlchemy expression for quantiles, or None if unsupported
        """
        return None

    def get_sample_clause(self, sample_size: int) -> Optional[str]:
        """
        Get platform-specific TABLESAMPLE clause.

        Args:
            sample_size: Number of rows to sample

        Returns:
            SQL TABLESAMPLE clause string, or None if unsupported
        """
        return None

    def get_mean_expr(self, column: str) -> ColumnElement[Any]:
        """
        Get platform-specific mean (AVG) expression.

        Default implementation returns AVG(column).
        Some platforms (e.g., Redshift) need to cast columns for full precision.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for AVG
        """
        return sa.func.avg(sa.column(column))

    def get_stdev_null_value(self) -> Optional[float]:
        """
        Get value to return for standard deviation when all values are NULL.

        Different platforms have different behaviors:
        - Most platforms: return None (NULL)
        - Redshift: return 0.0 (to match GE behavior)

        Returns:
            Value to use for all-NULL columns (None or 0.0)
        """
        return None

    # =========================================================================
    # Row Count Estimation
    # =========================================================================

    def supports_row_count_estimation(self) -> bool:
        """
        Whether platform supports fast row count estimation.

        Returns:
            True if platform can provide fast approximate row counts
        """
        return False

    def get_estimated_row_count(
        self, table: sa.Table, conn: Connection
    ) -> Optional[int]:
        """
        Get fast row count estimate without full table scan.

        Examples:
        - PostgreSQL: Uses pg_class.reltuples
        - MySQL: Uses information_schema.tables.table_rows

        Args:
            table: SQLAlchemy table object
            conn: Active database connection

        Returns:
            Estimated row count, or None if not supported
        """
        return None

    # =========================================================================
    # Query Execution Methods
    # =========================================================================
    # These methods execute SQL queries and return results.
    # Default implementations work for most platforms.
    # Override in platform-specific adapters when needed.

    def get_row_count(
        self,
        table: sa.Table,
        conn: Connection,
        sample_clause: Optional[str] = None,
        use_estimation: bool = False,
    ) -> int:
        """
        Get row count with optional sampling or estimation.

        Args:
            table: SQLAlchemy table object
            conn: Active database connection
            sample_clause: Optional SQL suffix for sampling
            use_estimation: Use fast estimation if available

        TODO: performance optimization: get from system tables
        Current approach uses SELECT COUNT(*) or APPROX_COUNT(*) which may be slow on large tables.
        Better approach would be to fetch from system tables, in those platforms that provide that. Eg BigQuery

                SELECT row_count
                FROM `project.dataset.INFORMATION_SCHEMA.TABLES`
                WHERE table_name = 'table_name'

        Even, we could fetch all row counts for a given dataset/schema and cache them!

        Returns:
            Row count
        """
        if use_estimation:
            if not self.supports_row_count_estimation():
                raise ValueError(
                    f"Row count estimation not supported for {self.__class__.__name__}"
                )
            result = self.get_estimated_row_count(table, conn)
            return int(result) if result is not None else 0

        query = sa.select([sa.func.count()]).select_from(table)
        if sample_clause:
            query = query.suffix_with(sample_clause)
        count_result: Any = conn.execute(query).scalar()
        # scalar() can return Any | None, so we need to handle None
        if count_result is None:
            return 0
        return int(count_result)

    def get_column_non_null_count(
        self, table: sa.Table, column: str, conn: Connection
    ) -> int:
        """
        Get non-null count for a column.

        Uses COUNT(column) which automatically excludes NULLs.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            Non-null count
        """
        query = sa.select([sa.func.count(sa.column(column))]).select_from(table)
        result = conn.execute(query).scalar()
        return int(result) if result is not None else 0

    def get_column_min(self, table: sa.Table, column: str, conn: Connection) -> Any:
        """
        Get minimum value for a column.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            Minimum value
        """
        query = sa.select([sa.func.min(sa.column(column))]).select_from(table)
        return conn.execute(query).scalar()

    def get_column_max(self, table: sa.Table, column: str, conn: Connection) -> Any:
        """
        Get maximum value for a column.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            Maximum value
        """
        query = sa.select([sa.func.max(sa.column(column))]).select_from(table)
        return conn.execute(query).scalar()

    def get_column_mean(
        self, table: sa.Table, column: str, conn: Connection
    ) -> Optional[Any]:
        """
        Get average value for a column.

        Returns the raw database result to preserve native type formatting
        (e.g., DECIMAL precision) to match GE behavior.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            Mean value
        """
        # Use adapter to get platform-specific mean expression
        # (e.g., Redshift needs CAST to preserve precision)
        avg_expr = self.get_mean_expr(column)

        query = sa.select([avg_expr]).select_from(table)
        result = conn.execute(query).scalar()

        # Return raw result to preserve database-native formatting (like GE does)
        return result

    def get_column_stdev(
        self, table: sa.Table, column: str, conn: Connection
    ) -> Optional[Any]:
        """
        Get standard deviation for a column.

        Returns the raw database result to preserve native type formatting.
        For all-null columns, behavior is database-specific (handled by adapter).

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            Standard deviation
        """
        query = sa.select([sa.func.stddev(sa.column(column))]).select_from(table)
        result = conn.execute(query).scalar()
        # Some databases return NULL for STDDEV when there's only one row
        # For a single value, standard deviation is mathematically undefined (None)
        # For multiple values with no variation, stdev is 0.0
        if result is None:
            # Check if there's at least one non-null value
            non_null_count = self.get_column_non_null_count(table, column, conn)
            if non_null_count == 1:
                # Single value: stdev is undefined, return None (matches GE behavior)
                return None
            elif non_null_count > 1:
                # Multiple values but database returned NULL (all same value): stdev is 0.0
                return 0.0
            # No non-null values: use adapter to get platform-specific behavior
            # (e.g., Redshift returns 0.0, PostgreSQL returns None)
            return self.get_stdev_null_value()
        # Return raw result to preserve database-native formatting (like GE does)
        return result

    def get_column_unique_count(
        self, table: sa.Table, column: str, conn: Connection, use_approx: bool = True
    ) -> int:
        """
        Get unique count (approximate if use_approx=True).

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection
            use_approx: Use approximate count if available

        Returns:
            Unique count
        """
        if use_approx:
            expr = self.get_approx_unique_count_expr(column)
        else:
            expr = sa.func.count(sa.func.distinct(sa.column(column)))

        query = sa.select([expr]).select_from(table)
        result = conn.execute(query).scalar()
        return int(result) if result is not None else 0

    def get_column_median(self, table: sa.Table, column: str, conn: Connection) -> Any:
        """
        Get median value for a column (database-specific).

        Returns raw database result to preserve native type formatting.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            Median value, or None if not supported
        """
        expr = self.get_median_expr(column)
        if expr is None:
            return None

        query = sa.select([expr]).select_from(table)
        result = conn.execute(query).scalar()
        # Return raw result to preserve database-native formatting (like GE does)
        return result

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column (approximate where possible).

        Default implementation uses PERCENTILE_CONT which is supported by:
        - PostgreSQL
        - Redshift
        - SQL Server
        - Oracle
        - And other SQL:2011 compliant databases

        Platform-specific adapters (BigQuery, Snowflake, etc.) override this
        method to use their native approximate percentile functions.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection
            quantiles: List of quantile values (default: DEFAULT_QUANTILES)

        Returns:
            List of quantile values (None for unavailable quantiles)
        """
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES

        # Validate quantile range
        for q in quantiles:
            if not (0 <= q <= 1):
                raise ValueError(
                    f"Quantiles must be in [0, 1], got {q}. "
                    f"Quantiles represent percentiles as decimals (e.g., 0.5 for median)."
                )

        # Fallback: Use exact PERCENTILE_CONT if supported
        logger.debug(
            f"Using PERCENTILE_CONT fallback for {self.__class__.__name__} on column {column}, "
            f"quantiles={quantiles}"
        )
        results = []
        for q in quantiles:
            try:
                quoted_column = self.quote_identifier(column)
                # Use literal_column with label() to preserve column metadata
                # which is needed for the query combiner to work correctly.
                percentile_expr = sa.literal_column(
                    f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {quoted_column})"
                ).label("percentile")
                query = sa.select([percentile_expr]).select_from(table)
                result = conn.execute(query).scalar()
                logger.debug(
                    f"Quantile {q} for {column}: result type={type(result)}, value={result}"
                )
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {type(e).__name__}: {str(e)}",
                )
                results.append(None)
        logger.debug(f"Final quantile results for {column}: {results}")
        return results

    def get_column_histogram(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        num_buckets: int = 10,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> List[Tuple[float, float, int]]:
        """
        Generate histogram for a column.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection
            num_buckets: Number of histogram buckets
            min_val: Minimum value (optional, will be queried if not provided)
            max_val: Maximum value (optional, will be queried if not provided)

        Returns:
            List of (bucket_start, bucket_end, count) tuples
        """
        # Get min/max if not provided
        if min_val is None:
            min_val = self.get_column_min(table, column, conn)
        if max_val is None:
            max_val = self.get_column_max(table, column, conn)

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
        result = conn.execute(query).fetchone()

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
        conn: Connection,
        top_k: int = 10,
    ) -> List[Tuple[Any, int]]:
        """
        Get top-K most frequent values and their counts.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection
            top_k: Number of top values to return

        Returns:
            List of (value, count) tuples, sorted by count descending
        """
        count_expr = sa.func.count().label("count")
        query = (
            sa.select([sa.column(column), count_expr])
            .select_from(table)
            .group_by(sa.column(column))
            .order_by(count_expr.desc())
            .limit(top_k)
        )

        result = conn.execute(query).fetchall()
        logger.debug(
            f"get_column_value_frequencies for {column}: got {len(result)} rows"
        )
        # Assertion: SQL query selects exactly 2 columns (value, count)
        # This should always be true given our query construction above
        assert all(len(row) == 2 for row in result), (
            f"Expected 2 columns from value_frequencies query for {column}. "
            f"This indicates a bug in query construction."
        )
        return [(row[0], int(row[1])) for row in result]

    def get_column_distinct_value_frequencies(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
    ) -> List[Tuple[Any, int]]:
        """
        Get all distinct values with their counts (sorted by value).

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection

        Returns:
            List of (value, count) tuples, sorted by value
        """
        count_expr = sa.func.count().label("count")
        query = (
            sa.select([sa.column(column), count_expr])
            .select_from(table)
            .group_by(sa.column(column))
            .order_by(sa.column(column))
        )

        result = conn.execute(query).fetchall()
        logger.debug(
            f"get_column_distinct_value_frequencies for {column}: got {len(result)} rows"
        )
        # Assertion: SQL query selects exactly 2 columns (value, count)
        # This should always be true given our query construction above
        assert all(len(row) == 2 for row in result), (
            f"Expected 2 columns from distinct_value_frequencies query for {column}. "
            f"This indicates a bug in query construction."
        )
        return [(row[0], int(row[1])) for row in result]

    def get_column_sample_values(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        limit: int = 20,
    ) -> List[Any]:
        """
        Get actual sample rows from the table (not distinct values).

        This matches GE profiler behavior which uses expect_column_values_to_be_in_set
        with an empty set to get actual sample rows with duplicates.

        Args:
            table: SQLAlchemy table object
            column: Column name
            conn: Active database connection
            limit: Number of sample values to return

        Returns:
            List of sample values (may contain duplicates)
        """
        query = (
            sa.select([sa.column(column)])
            .select_from(table)
            .where(sa.column(column).isnot(None))
            .limit(limit)
        )

        result = conn.execute(query).fetchall()
        logger.debug(
            f"get_column_sample_values for {column}: got {len(result)} rows, limit={limit}"
        )
        if result and len(result) > 0:
            if len(result[0]) < 1:
                logger.error(
                    f"Invalid result row structure for {column} in sample_values: "
                    f"row has {len(result[0])} columns, expected at least 1. "
                    f"First row: {result[0]}"
                )
        return [row[0] for row in result]

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _create_sqlalchemy_table(
        self,
        schema: Optional[str],
        table: str,
        autoload_with: Optional[Engine] = None,
    ) -> sa.Table:
        """
        Create a SQLAlchemy Table object.

        Args:
            schema: Schema name (optional)
            table: Table name
            autoload_with: Engine to autoload columns from (optional)

        Returns:
            SQLAlchemy Table object
        """
        metadata = sa.MetaData()
        return sa.Table(
            table,
            metadata,
            schema=schema,
            autoload_with=autoload_with or self.base_engine,
        )
