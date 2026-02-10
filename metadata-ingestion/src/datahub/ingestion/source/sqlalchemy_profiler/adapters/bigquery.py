"""BigQuery-specific profiling adapter."""

import logging
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)

logger = logging.getLogger(__name__)


def _quote_bigquery_identifier(identifier: str) -> str:
    """
    Safely quote a BigQuery identifier to prevent SQL injection.

    BigQuery uses backticks for identifiers. To escape a backtick within
    an identifier, double it (` -> ``).

    Args:
        identifier: The identifier to quote (table name, column name, etc.)

    Returns:
        Properly quoted identifier safe for use in SQL queries

    Examples:
        >>> _quote_bigquery_identifier("my_table")
        '`my_table`'
        >>> _quote_bigquery_identifier("table`with`backticks")
        '`table``with``backticks`'
        >>> _quote_bigquery_identifier("schema.table")
        '`schema`.`table`'
    """
    # Split on dots to handle schema.table notation
    parts = identifier.split(".")
    # Escape backticks in each part by doubling them, then wrap in backticks
    quoted_parts = [f"`{part.replace('`', '``')}`" for part in parts]
    return ".".join(quoted_parts)


class BigQueryAdapter(PlatformAdapter):
    """
    BigQuery-specific profiling adapter.

    BigQuery has several special requirements:
    1. Temporary tables via cached results (no CREATE TABLE permission needed)
    2. TABLESAMPLE SYSTEM for sampling large tables
    3. APPROX_COUNT_DISTINCT, approx_quantiles for fast statistics
    4. Special handling for LIMIT/OFFSET queries
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup BigQuery profiling with temp tables and sampling.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context ready for profiling
        """
        logger.debug(f"BigQuery setup for {context.pretty_name}")

        # Step 1: Handle custom SQL or LIMIT/OFFSET via temp table
        if context.custom_sql or self.config.limit or self.config.offset:
            context = self._create_temp_table_for_query(context)

        # Step 2: Setup sampling for large tables
        if self._should_sample_table(context, conn):
            context = self._setup_sampling(context, conn)

        # Step 3: Create SQLAlchemy table object
        if context.temp_table:
            # Use temp table
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.temp_schema,
                table=context.temp_table,
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
        Cleanup BigQuery temp resources.

        BigQuery cached result tables auto-expire in 24 hours, so no
        explicit cleanup is needed.

        Args:
            context: Profiling context
        """
        # BigQuery temp tables auto-expire, no cleanup needed
        if context.temp_table:
            logger.debug(
                f"BigQuery temp table {context.temp_table} will auto-expire in 24h"
            )

    def _create_temp_table_for_query(
        self, context: ProfilingContext
    ) -> ProfilingContext:
        """
        Create BigQuery temp table using cached results feature.

        On BigQuery, we (ab)use the cached results feature to create temp tables
        without requiring CREATE/DELETE TABLE permissions. BigQuery stores query
        results in a temporary, cached results table that's per-user and per-project.

        Args:
            context: Current profiling context

        Returns:
            Updated context with temp_table populated
        """
        if context.custom_sql:
            bq_sql = context.custom_sql
        else:
            # Table must be present if we're not using custom_sql
            if not context.table:
                raise ValueError(
                    f"Cannot profile {context.pretty_name}: table name is required"
                )

            # Safely quote table identifier to prevent SQL injection
            table_identifier = (
                f"{context.schema}.{context.table}" if context.schema else context.table
            )
            quoted_table = _quote_bigquery_identifier(table_identifier)
            bq_sql = f"SELECT * FROM {quoted_table}"

            # Validate and add LIMIT/OFFSET as integers to prevent SQL injection
            if self.config.limit:
                # Pydantic validates this is an int, but ensure it's positive
                limit_val = int(self.config.limit)
                if limit_val <= 0:
                    raise ValueError(
                        f"Invalid LIMIT value: {limit_val}. Must be positive."
                    )
                bq_sql += f" LIMIT {limit_val}"

            if self.config.offset:
                # Pydantic validates this is an int, but ensure it's non-negative
                offset_val = int(self.config.offset)
                if offset_val < 0:
                    raise ValueError(
                        f"Invalid OFFSET value: {offset_val}. Must be non-negative."
                    )
                bq_sql += f" OFFSET {offset_val}"

        # Execute query to create cached temp table
        try:
            import google.cloud.bigquery.job.query
            from google.cloud.bigquery.dbapi.cursor import Cursor as BigQueryCursor

            # Get raw DBAPI connection
            raw_conn = self.base_engine.raw_connection()
            cursor: "BigQueryCursor" = raw_conn.cursor()  # type: ignore[assignment]

            logger.debug(
                f"Creating BigQuery temp table for {context.pretty_name}: {bq_sql}"
            )
            cursor.execute(bq_sql)

            # Extract the name of the cached results table from the query job
            query_job: Optional["google.cloud.bigquery.job.query.QueryJob"] = (
                # In google-cloud-bigquery 3.15.0, the _query_job attribute was
                # made public and renamed to query_job.
                cursor.query_job if hasattr(cursor, "query_job") else cursor._query_job  # type: ignore[attr-defined]
            )

            if query_job and query_job.destination:
                temp_destination_table = query_job.destination
                context.temp_table = temp_destination_table.table_id
                context.temp_schema = f"{temp_destination_table.project}.{temp_destination_table.dataset_id}"
                context.add_temp_resource("bigquery_temp_table", context.temp_table)
                logger.debug(
                    f"Created BigQuery temp table: {context.temp_schema}.{context.temp_table}"
                )
            else:
                logger.warning(
                    f"Failed to get BigQuery temp table destination for {context.pretty_name}"
                )

        except Exception as e:
            if not self.config.catch_exceptions:
                raise
            logger.exception(f"Failed to create BigQuery temp table: {e}")
            self.report.warning(
                title="Failed to create BigQuery temporary table",
                message=f"Profiling exception when running custom sql: {bq_sql}",
                context=f"Asset: {context.pretty_name}",
                exc=e,
            )

        return context

    def _should_sample_table(self, context: ProfilingContext, conn: Connection) -> bool:
        """
        Check if table should be sampled based on size and configuration.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            True if table should be sampled
        """
        if not self.config.use_sampling:
            return False

        if self.config.limit:
            # Already limited by LIMIT clause
            return False

        if context.custom_sql:
            # Don't sample custom SQL
            return False

        if not context.table:
            # Can't sample without a table
            return False

        # Check if table is large enough to warrant sampling
        # Get quick row count estimate
        row_count = self._get_quick_row_count(context, conn)
        if row_count is None:
            return False

        return row_count > self.config.sample_size

    def _get_quick_row_count(
        self, context: ProfilingContext, conn: Connection
    ) -> Optional[int]:
        """
        Get quick row count estimate for sampling decision.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Estimated row count, or None if unavailable
        """
        try:
            if context.sql_table is not None:
                query: Any = sa.select([sa.func.count()]).select_from(context.sql_table)
            else:
                # Build quick count query
                if not context.table:
                    return None
                table_identifier = (
                    f"{context.schema}.{context.table}"
                    if context.schema
                    else context.table
                )
                quoted_table = _quote_bigquery_identifier(table_identifier)
                query = sa.text(f"SELECT COUNT(*) FROM {quoted_table}")

            result = conn.execute(query).scalar()
            return int(result) if result is not None else None
        except Exception as e:
            logger.debug(f"Failed to get quick row count: {e}")
            return None

    def _setup_sampling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup BigQuery TABLESAMPLE for large tables.

        According to BigQuery Sampling Docs, BigQuery does not cache the results
        of a query that includes a TABLESAMPLE clause. However, for a simple
        SELECT * query with TABLESAMPLE, results are cached and stored in a
        temporary table. This can be (ab)used for column level profiling.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context with sampled temp table
        """
        # Get row count for calculating sample percentage
        row_count = context.row_count or self._get_quick_row_count(context, conn)
        if not row_count or row_count <= self.config.sample_size:
            return context

        # Calculate sample percentage (validated as float from config)
        sample_pc = 100 * self.config.sample_size / row_count
        # Ensure sample_pc is within valid range [0, 100]
        sample_pc = max(0.0, min(100.0, sample_pc))

        # Table must be present for sampling
        if not context.table:
            raise ValueError(
                f"Cannot sample {context.pretty_name}: table name is required"
            )

        # Safely quote table identifier to prevent SQL injection
        table_identifier = (
            f"{context.schema}.{context.table}" if context.schema else context.table
        )
        quoted_table = _quote_bigquery_identifier(table_identifier)

        # Build query with properly escaped identifiers and validated percentage
        sql = (
            f"SELECT * FROM {quoted_table} TABLESAMPLE SYSTEM ({sample_pc:.8f} percent)"
        )

        logger.debug(
            f"Creating sampled BigQuery temp table for {context.pretty_name}: {sql}"
        )

        # Create sampled temp table (reuse temp table creation logic)
        old_custom_sql = context.custom_sql
        context.custom_sql = sql
        context = self._create_temp_table_for_query(context)
        context.custom_sql = old_custom_sql  # Restore

        context.is_sampled = True
        context.sample_percentage = sample_pc

        return context

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> Any:
        """BigQuery uses APPROX_COUNT_DISTINCT."""
        return sa.func.APPROX_COUNT_DISTINCT(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[Any]:
        """
        BigQuery median via APPROX_QUANTILES.

        APPROX_QUANTILES(column, 2) returns [min, median, max].
        We use OFFSET(1) to get the median value.
        """
        return sa.func.APPROX_QUANTILES(sa.column(column), 2)[sa.text("OFFSET(1)")]

    def get_quantiles_expr(self, column: str, quantiles: List[float]) -> Optional[Any]:
        """
        BigQuery quantiles via approx_quantiles.

        Returns array of quantile values.
        """
        num_quantiles = len(quantiles)
        return sa.func.approx_quantiles(sa.column(column), num_quantiles)

    def get_sample_clause(self, sample_size: int) -> Optional[str]:
        """BigQuery uses TABLESAMPLE SYSTEM."""
        return f"TABLESAMPLE SYSTEM ({sample_size} ROWS)"
