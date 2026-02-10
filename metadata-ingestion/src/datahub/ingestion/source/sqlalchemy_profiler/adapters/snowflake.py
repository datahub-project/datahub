"""Snowflake-specific profiling adapter."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter
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
