"""MySQL-specific profiling adapter."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter

logger = logging.getLogger(__name__)


class MySQLAdapter(PlatformAdapter):
    """
    MySQL-specific profiling adapter.

    MySQL features:
    1. Fast row count estimation via information_schema.tables.table_rows
    2. Standard SQL for most operations
    3. No native MEDIAN function

    Uses default setup_profiling and cleanup from PlatformAdapter.
    """

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> Any:
        """
        MySQL approximate unique count - uses exact COUNT(DISTINCT).

        MySQL doesn't have a built-in approximate count distinct function,
        so we fall back to exact count.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for COUNT(DISTINCT column)
        """
        return sa.func.count(sa.func.distinct(sa.column(column)))

    def get_median_expr(self, column: str) -> Optional[Any]:
        """
        MySQL median - not supported by default.

        Standard MySQL doesn't have a MEDIAN function. It can be
        implemented using subqueries but adds complexity.

        Args:
            column: Column name

        Returns:
            None (not supported)
        """
        return None

    # =========================================================================
    # Row Count Estimation
    # =========================================================================

    def supports_row_count_estimation(self) -> bool:
        """
        MySQL supports fast row count estimation.

        Returns:
            True - MySQL has information_schema.tables.table_rows
        """
        return True

    def get_estimated_row_count(
        self, table: sa.Table, conn: Connection
    ) -> Optional[int]:
        """
        Get fast row count estimate using information_schema.tables.table_rows.

        This avoids a full table scan by using MySQL's statistics.
        The estimate may be slightly out of date but is very fast.

        Args:
            table: SQLAlchemy table object
            conn: Active database connection

        Returns:
            Estimated row count, or None if query fails
        """
        try:
            schema = table.schema or "information_schema"

            # Use parameterized query to prevent SQL injection
            query = sa.text(
                "SELECT table_rows FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = :table_name"
            ).bindparams(schema=schema, table_name=table.name)

            result = conn.execute(query).scalar()
            return int(result) if result is not None else None

        except Exception as e:
            logger.debug(
                f"Failed to get MySQL row count estimate: {type(e).__name__}: {str(e)}"
            )
            return None
