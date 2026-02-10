"""Generic/default adapter for databases without special handling."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)

logger = logging.getLogger(__name__)


class GenericAdapter(PlatformAdapter):
    """
    Generic adapter for databases without platform-specific optimizations.

    This is the fallback adapter used when no specialized adapter exists
    for a platform. It provides standard SQL implementations that work
    across most databases.
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Generic setup - just create the SQL table object.

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
            f"Generic setup for {context.pretty_name}: "
            f"schema={context.schema}, table={context.table}"
        )

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Generic cleanup - nothing to clean up.

        Args:
            context: Profiling context
        """
        # No temp resources created in generic setup
        pass

    def get_approx_unique_count_expr(self, column: str) -> Any:
        """
        Generic approximate unique count - uses exact COUNT(DISTINCT).

        Most databases don't have approximate count distinct, so we fall
        back to exact count which is slower but accurate.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for COUNT(DISTINCT column)
        """
        return sa.func.count(sa.func.distinct(sa.column(column)))

    def get_median_expr(self, column: str) -> Optional[Any]:
        """
        Generic median - not supported.

        Standard SQL doesn't have a MEDIAN function. Platforms that support
        it (BigQuery, Snowflake, Redshift) override this method.

        Args:
            column: Column name

        Returns:
            None (not supported)
        """
        return None
