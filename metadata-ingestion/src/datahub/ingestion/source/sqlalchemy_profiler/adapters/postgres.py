"""PostgreSQL-specific profiling adapter."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(PlatformAdapter):
    """
    PostgreSQL-specific profiling adapter.

    PostgreSQL features:
    1. Fast row count estimation via pg_class.reltuples
    2. Standard SQL for most operations
    3. No native MEDIAN function (would need custom implementation)

    Uses default setup_profiling and cleanup from PlatformAdapter.
    """

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> Any:
        """
        PostgreSQL approximate unique count - uses exact COUNT(DISTINCT).

        PostgreSQL doesn't have a built-in approximate count distinct function,
        so we fall back to exact count.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for COUNT(DISTINCT column)
        """
        return sa.func.count(sa.func.distinct(sa.column(column)))

    def get_median_expr(self, column: str) -> Optional[Any]:
        """
        PostgreSQL median via PERCENTILE_CONT.

        PostgreSQL doesn't have a native MEDIAN function, but supports
        PERCENTILE_CONT which can compute the median (0.5 percentile).

        Args:
            column: Column name (from database schema, already validated)

        Returns:
            SQLAlchemy expression for PERCENTILE_CONT(0.5)
        """
        # Use PERCENTILE_CONT which is supported in PostgreSQL
        # Column name is from database schema (validated), not user input
        # IMPORTANT: label() is required whenever using literal_column()
        # Without label(), the column name would be the entire SQL string, which breaks query combiner result extraction
        return sa.literal_column(
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column})"
        ).label("median")

    # =========================================================================
    # Row Count Estimation
    # =========================================================================

    def supports_row_count_estimation(self) -> bool:
        """
        PostgreSQL supports fast row count estimation.

        Returns:
            True - PostgreSQL has pg_class.reltuples
        """
        return True

    def get_estimated_row_count(
        self, table: sa.Table, conn: Connection
    ) -> Optional[int]:
        """
        Get fast row count estimate using pg_class.reltuples.

        This avoids a full table scan by using PostgreSQL's statistics.
        The estimate may be slightly out of date but is very fast.

        Args:
            table: SQLAlchemy table object
            conn: Active database connection

        Returns:
            Estimated row count, or None if query fails
        """
        try:
            schema = table.schema or "public"
            table_ref = f"{schema}.{table.name}"

            # Use parameterized query to prevent SQL injection
            query = sa.text(
                "SELECT reltuples::bigint FROM pg_class "
                "WHERE oid = :table_ref::regclass"
            ).bindparams(table_ref=table_ref)

            result = conn.execute(query).scalar()
            return int(result) if result is not None else None

        except Exception as e:
            logger.debug(
                f"Failed to get PostgreSQL row count estimate: {type(e).__name__}: {str(e)}"
            )
            return None
