"""MSSQL (SQL Server) specific profiling adapter."""

import logging
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
)

logger = logging.getLogger(__name__)


class MSSQLAdapter(PlatformAdapter):
    """
    MSSQL (SQL Server) specific profiling adapter.

    MSSQL quirks handled:
      - `STDEV()` instead of `STDDEV_SAMP()` (MSSQL has no `stddev_samp`).
      - `PERCENTILE_DISC` requires an explicit `OVER ()` window.
      - No native APPROX_COUNT_DISTINCT — falls back to exact COUNT DISTINCT.
      - No native MEDIAN — falls through to base adapter's OFFSET/LIMIT
        Python fallback. MSSQL 2012+ supports `OFFSET ... ROWS FETCH NEXT ...
        ROWS ONLY`, which SQLAlchemy emits automatically for `.limit().offset()`.

    Mean (AVG) float promotion is handled by the base adapter via `AVG(col * 1.0)`,
    which works for MSSQL (where it prevents integer truncation) and all other
    dialects (where it preserves precision).

    Mirrors GE's MSSQL handling in great_expectations.dataset.sqlalchemy_dataset:
      - `get_column_stdev` (MSSQL branch uses `sa.func.stdev`).
      - `_get_column_quantiles_mssql` (PERCENTILE_DISC ... WITHIN GROUP OVER ()).
    """

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        MSSQL has no built-in approximate distinct count function, so we fall
        back to exact COUNT(DISTINCT col).
        """
        return sa.func.count(sa.func.distinct(sa.column(column)))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        MSSQL has no native MEDIAN function. Returning None triggers the
        base adapter's OFFSET/LIMIT Python fallback, which works on
        SQL Server 2012+ (SQLAlchemy translates `.limit().offset()` to
        `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY`).
        """
        return None

    # =========================================================================
    # Query Execution Methods (overrides)
    # =========================================================================

    def get_column_stdev(
        self, table: sa.Table, column: str, conn: Connection
    ) -> Optional[Any]:
        """
        Get sample standard deviation using MSSQL's `STDEV()` function.

        MSSQL doesn't have `stddev_samp` — its sample-stddev function is named
        `STDEV` (population variant is `STDEVP`). NULL disambiguation mirrors
        the base adapter: single value → None, multiple-equal rows → 0.0,
        all-null → adapter-specific `get_stdev_null_value()` hook.
        """
        query = sa.select([sa.func.stdev(sa.column(column))]).select_from(table)
        result = conn.execute(query).scalar()
        if result is None:
            non_null_count = self.get_column_non_null_count(table, column, conn)
            if non_null_count == 1:
                return None
            if non_null_count > 1:
                return 0.0
            return self.get_stdev_null_value()
        return result

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantiles using MSSQL's `PERCENTILE_DISC(q) WITHIN GROUP (...) OVER ()`.

        MSSQL requires an explicit (empty) `OVER ()` window for `PERCENTILE_DISC`,
        and since the windowed form returns one row per input row, we use
        `SELECT DISTINCT` to dedupe. This mirrors GE's `_get_column_quantiles_mssql`.
        """
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES

        for q in quantiles:
            if not (0 <= q <= 1):
                raise ValueError(
                    f"Quantiles must be in [0, 1], got {q}. "
                    f"Quantiles represent percentiles as decimals (e.g., 0.5 for median)."
                )

        quoted_column = self.quote_identifier(column)
        selects = [
            sa.literal_column(
                f"PERCENTILE_DISC({q}) WITHIN GROUP (ORDER BY {quoted_column} ASC) OVER ()"
            ).label(f"q_{i}")
            for i, q in enumerate(quantiles)
        ]
        # DISTINCT is required because the windowed PERCENTILE_DISC would
        # otherwise return one identical row per input row.
        query = sa.select(selects).select_from(table).distinct()

        try:
            row = conn.execute(query).fetchone()
        except SQLAlchemyError as e:
            logger.warning(
                f"Failed to compute MSSQL quantiles for {column}: "
                f"{type(e).__name__}: {str(e)}",
            )
            return [None] * len(quantiles)

        if row is None:
            return [None] * len(quantiles)
        return [float(v) if v is not None else None for v in row]
