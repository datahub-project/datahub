"""Generic/default adapter for databases without special handling."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter

logger = logging.getLogger(__name__)


class GenericAdapter(PlatformAdapter):
    """
    Generic adapter for databases without platform-specific optimizations.

    This is the fallback adapter used when no specialized adapter exists
    for a platform. It provides standard SQL implementations that work
    across most databases.

    Uses default setup_profiling and cleanup from PlatformAdapter.
    """

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
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

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Generic adapter has no assumption about MEDIAN support.

        Platforms with a native MEDIAN function (Snowflake, Oracle, SQLite with
        custom aggregate) should provide their own adapter that overrides this.
        Unknown platforms fall through to the Python OFFSET/LIMIT fallback in
        `base_adapter.get_column_median`.
        """
        return None
