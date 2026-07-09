"""Snowflake-specific profiling adapter."""

import logging
import random
import string
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
)
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)

logger = logging.getLogger(__name__)


class SnowflakeAdapter(PlatformAdapter):
    """
    Snowflake-specific profiling adapter.

    Snowflake optimizations:
    1. APPROX_COUNT_DISTINCT for fast unique counts
    2. Native MEDIAN() function for median calculation
    3. Temporary tables with TABLESAMPLE for large table profiling
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup Snowflake profiling with temp tables and sampling.

        For large tables (> sample_size rows), creates a temporary table
        with sampled data to avoid massive cross-join queries on production tables.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context ready for profiling
        """
        logger.debug(f"Snowflake setup_profiling called for {context.pretty_name}")
        logger.debug(
            f"Config: use_sampling={self.config.use_sampling}, sample_size={self.config.sample_size}"
        )
        logger.debug(
            f"Context: table={context.table}, schema={context.schema}, row_count={context.row_count}"
        )

        # Setup sampling for large tables
        should_sample = self._should_sample_table(context, conn)
        logger.debug(f"Should sample table: {should_sample}")
        if should_sample:
            context = self._setup_sampling(context, conn)

        # Create SQLAlchemy table object
        if context.temp_table:
            # Use temp table - CRITICAL: must use same connection that created it
            # since temp tables are session-scoped in Snowflake
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.temp_schema,
                table=context.temp_table,
                autoload_with=conn,
            )
        else:
            # Use original table
            if not context.table:
                raise ValueError(
                    f"Cannot profile {context.pretty_name}: table name required"
                )
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.schema,
                table=context.table,
            )

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Cleanup Snowflake temp resources.

        Snowflake temporary tables are session-scoped and automatically dropped when
        the connection closes. Since cleanup() is called after the profiling connection
        has closed (in the finally block), the temp tables are already gone.

        No explicit DROP is needed.

        Args:
            context: Profiling context
        """
        # Snowflake temp tables are automatically dropped when connection closes
        # No explicit cleanup needed
        pass

    # =========================================================================
    # Sampling
    # =========================================================================

    def _should_sample_table(self, context: ProfilingContext, conn: Connection) -> bool:
        """
        Check if table should be sampled based on size and configuration.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            True if table should be sampled
        """
        logger.debug(f"Checking if sampling needed for {context.pretty_name}")

        if not self.config.use_sampling:
            logger.debug("use_sampling=False, skipping sampling")
            return False

        if context.custom_sql:
            logger.debug("custom_sql provided, skipping sampling")
            # Don't sample custom SQL
            return False

        if not context.table:
            logger.debug("No table name, skipping sampling")
            # Can't sample without a table
            return False

        # Check if table is large enough to warrant sampling
        # Get row count (only if not already cached in context)
        if context.row_count is None:
            logger.debug("row_count is None, calling _get_quick_row_count")
            context.row_count = self._get_quick_row_count(context, conn)
            logger.debug(f"_get_quick_row_count returned: {context.row_count}")

        if context.row_count is None:
            # Failed to get row count - default to sampling (conservative approach)
            # This prevents profiling failures on large tables where metadata is unavailable
            logger.info(
                f"Row count unavailable for {context.pretty_name}, defaulting to sampling"
            )
            return True

        # Only skip sampling if we successfully determined the table is small
        should_sample = context.row_count > self.config.sample_size
        logger.debug(
            f"row_count={context.row_count}, sample_size={self.config.sample_size}, should_sample={should_sample}"
        )
        return should_sample

    def _get_quick_row_count(
        self, context: ProfilingContext, conn: Connection
    ) -> Optional[int]:
        """
        Get row count for sampling decision using Snowflake metadata.

        Uses INFORMATION_SCHEMA.TABLES.ROW_COUNT for instant approximate counts
        instead of SELECT COUNT(*) which requires full table scans.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Approximate row count from Snowflake metadata, or None if unavailable
        """
        try:
            if not context.table:
                return None

            # Query INFORMATION_SCHEMA for instant row count
            # This is approximate but sufficient for sampling decisions
            # Must filter by TABLE_CATALOG (database) to avoid ambiguity
            query = sa.text(
                """
                SELECT ROW_COUNT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = CURRENT_DATABASE()
                AND TABLE_SCHEMA = :schema_name
                AND TABLE_NAME = :table_name
                """
            )

            result = conn.execute(
                query,
                {"schema_name": context.schema, "table_name": context.table},
            ).scalar()

            return int(result) if result is not None else None
        except SQLAlchemyError as e:
            self.report.warning(
                title="Failed to get row count from metadata",
                message=f"Could not retrieve row count for sampling decision: {type(e).__name__}",
                context=f"{context.pretty_name}: {str(e)}",
            )
            return None

    def _setup_sampling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup Snowflake TABLESAMPLE for large tables.

        Creates a temporary table with sampled data using Snowflake's
        TABLESAMPLE syntax. Matches GE profiler's conditional logic:
        - BLOCK + BERNOULLI for very large tables (>50M rows)
        - BERNOULLI only for smaller tables

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context with sampled temp table
        """
        logger.debug(f"Setting up sampling for {context.pretty_name}")

        # Get row count for calculating sample percentage
        row_count = context.row_count or self._get_quick_row_count(context, conn)
        logger.debug(f"row_count={row_count}, sample_size={self.config.sample_size}")
        if not row_count or row_count <= self.config.sample_size:
            logger.debug("Table too small or no row count, skipping sampling")
            return context

        # Table must be present for sampling
        if not context.table:
            raise ValueError(
                f"Cannot sample {context.pretty_name}: table name is required"
            )

        # Build fully qualified table name
        table_fqn = (
            f'"{context.schema}"."{context.table}"'
            if context.schema
            else f'"{context.table}"'
        )

        # Generate unique temp table name with quotes to preserve case
        # CRITICAL: Snowflake stores unquoted identifiers as UPPERCASE, but we need
        # lowercase for SQLAlchemy reflection to work correctly. Using quotes in the
        # CREATE statement tells Snowflake to preserve the exact case.
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        temp_table_name_unquoted = f"datahub_profiling_sample_{random_suffix}"
        temp_table_name_quoted = f'"{temp_table_name_unquoted}"'

        # GE's sampling strategy: use BLOCK + BERNOULLI for very large tables
        # to improve performance, otherwise use BERNOULLI only
        estimated_block_row_count = 500_000
        block_profiling_min_rows = 100 * estimated_block_row_count  # 50M rows
        overgeneration_factor = 1000

        # Calculate base sample percentage
        sample_pc = self.config.sample_size / row_count

        # Decide sampling strategy based on table size
        if (
            row_count > block_profiling_min_rows
            and row_count > self.config.sample_size * overgeneration_factor
        ):
            # Very large table: use BLOCK + BERNOULLI
            # First pass with BLOCK to get 1000x the target sample size,
            # then BERNOULLI to reduce to final size
            block_sample_pc = 100 * overgeneration_factor * sample_pc
            bernoulli_sample_pc = 100 / overgeneration_factor

            create_sql = (
                f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name_quoted} AS "
                f"SELECT * FROM ("
                f"  SELECT * FROM {table_fqn} "
                f"  TABLESAMPLE BLOCK ({block_sample_pc:.8f})"
                f") TABLESAMPLE BERNOULLI ({bernoulli_sample_pc:.8f})"
            )
            sampling_strategy = f"BLOCK ({block_sample_pc:.2f}%) + BERNOULLI ({bernoulli_sample_pc:.2f}%)"
        else:
            # Smaller table: use BERNOULLI only
            bernoulli_sample_pc = 100 * sample_pc

            create_sql = (
                f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name_quoted} AS "
                f"SELECT * FROM {table_fqn} "
                f"TABLESAMPLE BERNOULLI ({bernoulli_sample_pc:.8f})"
            )
            sampling_strategy = f"BERNOULLI ({bernoulli_sample_pc:.2f}%)"

        logger.info(
            f"Creating temp table for {context.pretty_name} "
            f"({row_count:,} rows -> {self.config.sample_size:,} sample)"
        )
        logger.debug(f"Sampling strategy: {sampling_strategy}")
        logger.debug(f"Sampling SQL: {create_sql}")

        try:
            # Execute using the same connection that will be used for profiling
            # CRITICAL: Temp tables in Snowflake are session-scoped and must be created
            # on the same connection that will query them. Creating on a separate
            # raw_connection() and closing it would drop the temp table immediately.
            conn.execute(sa.text(create_sql))

            # Update context to use temp table (store unquoted name - SQLAlchemy adds quotes)
            context.temp_table = temp_table_name_unquoted
            context.temp_schema = context.schema  # Temp table inherits schema
            context.is_sampled = True
            context.sample_percentage = sample_pc * 100  # Store as percentage
            context.add_temp_resource("snowflake_temp_table", temp_table_name_unquoted)

            logger.debug(
                f"Created Snowflake temp table: "
                f"{context.temp_schema}.{context.temp_table}"
            )

            return context

        except SQLAlchemyError as e:
            error_msg = (
                f"Cannot profile {context.pretty_name}: Snowflake temp table creation failed. "
                f"{type(e).__name__}: {str(e)}"
            )
            self.report.warning(
                title="Failed to create Snowflake temporary table",
                message="Profiling will continue on full table (may have memory issues)",
                context=f"{context.pretty_name}: {type(e).__name__}: {str(e)}",
                exc=e,
            )
            if not self.config.catch_exceptions:
                raise
            # Continue without sampling (fallback to full table)
            logger.warning(error_msg)
            return context

    # =========================================================================
    # Table Creation
    # =========================================================================

    def _create_sqlalchemy_table(
        self, schema: Optional[str], table: str, autoload_with: Optional[Any] = None
    ) -> "sa.Table":
        """
        Create SQLAlchemy Table object for Snowflake with proper identifier handling.

        Snowflake identifier behavior:
        - Unquoted identifiers are case-insensitive and stored as UPPERCASE
        - Quoted identifiers are case-sensitive and stored with exact case
        - The Snowflake source connector may lowercase identifiers for URN generation

        Strategy - try in this order based on table name pattern:
        1. If table has lowercase chars -> Try WITH quoting first
           - Lowercase suggests table was created with quotes: "lcase_table"
           - Quoting preserves exact case needed to find these tables
        2. If that fails OR table is all uppercase -> Try WITHOUT quoting
           - Standard Snowflake tables are stored as UPPERCASE
           - Unquoted reflection lets Snowflake auto-uppercase: errortypes -> ERRORTYPES

        Args:
            schema: Schema name from metadata (may be lowercase if convert_urns_to_lowercase=True)
            table: Table name from metadata (may be lowercase if convert_urns_to_lowercase=True)
            autoload_with: Engine or Connection for metadata reflection (optional)

        Returns:
            SQLAlchemy Table object

        Raises:
            NoSuchTableError: If table cannot be found with either approach
        """
        metadata = sa.MetaData()
        engine = autoload_with or self.base_engine

        # Determine if table name suggests it was created with quotes
        # Mixed case or lowercase letters indicate quoted creation
        has_lowercase = any(c.islower() for c in table)
        has_lowercase_schema = schema and any(c.islower() for c in schema)

        # Try quoted first if name has lowercase (likely created with quotes)
        if has_lowercase or has_lowercase_schema:
            try:
                return sa.Table(
                    table,
                    metadata,
                    schema=schema,
                    autoload_with=engine,
                    quote=True,
                    quote_schema=bool(schema),
                )
            except SQLAlchemyError as e:
                logger.debug(
                    f"Failed to reflect {schema}.{table} with quoting, "
                    f"trying without quotes: {type(e).__name__}: {str(e)}"
                )

        # Try unquoted (standard Snowflake - auto-uppercase)
        try:
            return sa.Table(
                table,
                metadata,
                schema=schema,
                autoload_with=engine,
                quote=False,
                quote_schema=False,
            )
        except SQLAlchemyError as e:
            # If unquoted failed and we haven't tried quoted yet, try it now
            if not (has_lowercase or has_lowercase_schema):
                logger.debug(
                    f"Failed to reflect {schema}.{table} without quoting, "
                    f"trying with quotes: {type(e).__name__}: {str(e)}"
                )
                return sa.Table(
                    table,
                    metadata,
                    schema=schema,
                    autoload_with=engine,
                    quote=True,
                    quote_schema=bool(schema),
                )
            # Already tried both, re-raise
            raise

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        Snowflake uses APPROX_COUNT_DISTINCT for fast unique counts.

        This matches GE profiler behavior (ge_data_profiler.py:213-221)
        where Snowflake is grouped with BigQuery for approximate counts.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for APPROX_COUNT_DISTINCT
        """
        return sa.func.APPROX_COUNT_DISTINCT(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Snowflake has native MEDIAN() function.

        This matches GE profiler behavior (ge_data_profiler.py:676-683).

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for MEDIAN
        """
        return sa.func.median(sa.column(column))

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using Snowflake's APPROX_PERCENTILE.

        Snowflake: APPROX_PERCENTILE(col, quantile) computes a single percentile.
        We execute one query per quantile.

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

        # Snowflake: APPROX_PERCENTILE(col, quantile)
        results = []
        for q in quantiles:
            try:
                snowflake_expr = sa.func.approx_percentile(sa.column(column), q)
                query = sa.select([snowflake_expr]).select_from(table)
                result = conn.execute(query).scalar()
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {type(e).__name__}: {str(e)}"
                )
                results.append(None)
        return results
