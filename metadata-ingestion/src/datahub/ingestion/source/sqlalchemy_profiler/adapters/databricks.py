"""Databricks-specific profiling adapter."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)

logger = logging.getLogger(__name__)


class DatabricksAdapter(PlatformAdapter):
    """
    Databricks-specific profiling adapter.

    Databricks optimizations:
    1. approx_count_distinct for fast unique counts
    2. approx_percentile for median calculation

    Note: Databricks uses lowercase function names (approx_count_distinct, approx_percentile)
    unlike some other platforms.
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Databricks setup - create SQL table object.

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
            f"Databricks setup for {context.pretty_name}: "
            f"schema={context.schema}, table={context.table}"
        )

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Databricks cleanup - nothing to clean up.

        Args:
            context: Profiling context
        """
        # No temp resources created in Databricks setup
        pass

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> Any:
        """
        Databricks uses approx_count_distinct for fast unique counts.

        This matches GE profiler behavior (ge_data_profiler.py:233-239).
        Note: Databricks uses lowercase function name.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approx_count_distinct
        """
        return sa.func.approx_count_distinct(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[Any]:
        """
        Databricks uses approx_percentile for median.

        This matches GE profiler behavior (ge_data_profiler.py:684-693).
        approx_percentile(column, 0.5) computes the approximate median.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approx_percentile(column, 0.5)
        """
        return sa.func.approx_percentile(sa.column(column), 0.5)
