"""Snowflake-specific profiling adapter."""

import logging
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

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

    TODO: While Snowflake supports TABLESAMPLE SYSTEM for sampling large tables,
    it's not implemented yet to maintain alignment with GE profiler behavior
    (GE profiler only uses TABLESAMPLE for BigQuery, not Snowflake).
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Snowflake setup - create SQL table object.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context with sql_table populated
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
            f"Snowflake setup for {context.pretty_name}: "
            f"schema={context.schema}, table={context.table}"
        )

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Snowflake cleanup - nothing to clean up.

        Args:
            context: Profiling context
        """
        # No temp resources created in Snowflake setup
        pass

    # =========================================================================
    # Table Creation
    # =========================================================================

    def _create_sqlalchemy_table(
        self, schema: Optional[str], table: str, autoload_with: Optional[Any] = None
    ) -> "sa.Table":
        """
        Create SQLAlchemy Table object for Snowflake with proper identifier quoting.

        Snowflake has case-sensitive identifiers when quoted. To handle tables created
        with quotes (e.g., CREATE TABLE "lcase_table" ...), we must preserve the exact
        case from metadata by quoting all identifiers.

        Without quotes:
            SELECT * FROM PUBLIC.lcase_table
            -> Snowflake uppercases: PUBLIC.LCASE_TABLE (may not exist)

        With quotes:
            SELECT * FROM "PUBLIC"."lcase_table"
            -> Snowflake uses exact case: PUBLIC.lcase_table (correct)

        Args:
            schema: Schema name (preserve case from metadata, optional)
            table: Table name (preserve case from metadata)
            autoload_with: Engine or Connection for metadata reflection (optional)

        Returns:
            SQLAlchemy Table object with quoted identifiers
        """
        metadata = sa.MetaData()
        return sa.Table(
            table,
            metadata,
            schema=schema,
            autoload_with=autoload_with or self.base_engine,
            quote=True,  # Quote table name to preserve case
            quote_schema=bool(schema),  # Quote schema name to preserve case
        )

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> Any:
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

    def get_median_expr(self, column: str) -> Optional[Any]:
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
            except Exception as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {e}",
                    exc_info=True,
                )
                results.append(None)
        return results
