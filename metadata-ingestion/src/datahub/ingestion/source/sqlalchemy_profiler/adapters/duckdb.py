"""DuckDB-specific profiling adapter."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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


@dataclass(frozen=True)
class _ColumnSummary:
    min: Any  # post-conversion: int | float | str | None
    max: Any
    avg: Optional[float]
    std: Optional[float]
    q50: Optional[float]
    approx_unique: int
    non_null: int


_INTEGER_TYPE_PREFIXES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
)
_FLOAT_TYPE_PREFIXES = ("DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL")


def _convert_bound(value: Any, column_type: Optional[str]) -> Any:
    """Convert a SUMMARIZE min/max (always VARCHAR) to a numeric Python type for
    numeric columns, so downstream arithmetic (e.g. histogram bucketing) works.
    Non-numeric columns (VARCHAR, DATE, TIMESTAMP, BOOLEAN) are left as-is."""
    if value is None:
        return None
    t = (column_type or "").upper()
    try:
        if t.startswith(_INTEGER_TYPE_PREFIXES):
            return int(value)
        if t.startswith(_FLOAT_TYPE_PREFIXES):
            return float(value)
    except (TypeError, ValueError):
        return value
    return value


class DuckDBAdapter(PlatformAdapter):
    """
    DuckDB profiling adapter.

    DuckDB optimizations:
    1. approx_count_distinct for fast unique counts (HyperLogLog).
    2. quantile_cont for median and quantiles in a single call.
    3. SUMMARIZE fast-path: computes min/max/avg/std/approx_unique/null_pct for
       all columns in one table scan, cached per-table for subsequent metric reads.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Per-table cache: column_name -> parsed SUMMARIZE stats. A fresh adapter is
        # created per table in _generate_single_profile, so instance state is safe.
        self._summary: Dict[str, _ColumnSummary] = {}
        self._row_count: Optional[int] = None

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Run SUMMARIZE once and cache per-column stats before per-metric queries.

        Falls back gracefully (empty cache) if SUMMARIZE is unavailable for the
        relation (e.g., views, external tables in some DuckDB versions).
        """
        context = super().setup_profiling(context, conn)
        try:
            self._run_summarize(context, conn)
        except Exception as e:
            logger.debug(f"DuckDB SUMMARIZE failed for {context.pretty_name}: {e}")
            self._summary = {}
            self._row_count = None
        return context

    def _run_summarize(self, context: ProfilingContext, conn: Connection) -> None:
        """Execute SUMMARIZE and populate the per-column stats cache."""
        identifier = self.quote_identifier(context.get_table_identifier())
        rows = conn.execute(sa.text(f"SUMMARIZE {identifier}")).fetchall()
        summary: Dict[str, _ColumnSummary] = {}
        for row in rows:
            m = row._mapping
            name = m["column_name"]
            null_pct = (
                float(m["null_percentage"]) if m["null_percentage"] is not None else 0.0
            )
            count = int(m["count"]) if m["count"] is not None else 0
            col_type = m["column_type"]
            summary[name] = _ColumnSummary(
                min=_convert_bound(m["min"], col_type),
                max=_convert_bound(m["max"], col_type),
                avg=self._to_float(m["avg"]),
                std=self._to_float(m["std"]),
                q50=self._to_float(m["q50"]),
                approx_unique=(
                    int(m["approx_unique"]) if m["approx_unique"] is not None else 0
                ),
                non_null=round(count * (1.0 - null_pct / 100.0)),
            )
            self._row_count = count
        self._summary = summary

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        """Convert a SUMMARIZE VARCHAR stat to float, returning None on failure."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        return sa.func.approx_count_distinct(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        return sa.func.quantile_cont(sa.column(column), 0.5)

    def get_quantiles_expr(
        self, column: str, quantiles: List[float]
    ) -> Optional[ColumnElement[Any]]:
        """
        DuckDB's quantile_cont accepts a list argument and returns all quantiles
        in a single function call, avoiding one round-trip per quantile.

        Args:
            column: Column name
            quantiles: List of quantile values (e.g., [0.25, 0.5, 0.75])

        Returns:
            SQLAlchemy expression for quantile_cont with list argument
        """
        return sa.func.quantile_cont(sa.column(column), sa.literal(quantiles))

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using DuckDB's quantile_cont.

        DuckDB's quantile_cont accepts a list of quantile fractions and returns
        all results in a single query — one table scan regardless of how many
        quantiles are requested. This is more efficient than the base-class
        PERCENTILE_CONT fallback which issues one query per quantile.

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
        # DuckDB returns all quantiles in one call: quantile_cont(col, [..]) -> list.
        expr = sa.func.quantile_cont(sa.column(column), sa.literal(quantiles))
        query = sa.select([expr]).select_from(table)
        try:
            result = conn.execute(query).scalar()
        except SQLAlchemyError as e:
            logger.warning(
                f"Failed to compute quantiles for {column}: {type(e).__name__}: {e}"
            )
            return [None] * len(quantiles)
        # quantile_cont(col, [..]) yields a list; guard against an unexpected
        # non-iterable scalar so we degrade gracefully instead of raising.
        if not isinstance(result, (list, tuple)):
            return [None] * len(quantiles)
        return [float(v) if v is not None else None for v in result]

    def get_sample_clause(self, sample_size: int) -> Optional[str]:
        # DuckDB reservoir sampling by absolute row count.
        return f"USING SAMPLE {int(sample_size)} ROWS"

    # =========================================================================
    # Cache-backed execution overrides (SUMMARIZE fast-path)
    # =========================================================================

    def get_row_count(
        self,
        table: sa.Table,
        conn: Connection,
        sample_clause: Optional[str] = None,
        use_estimation: bool = False,
    ) -> int:
        """Return cached SUMMARIZE row count when no sampling is requested."""
        if sample_clause is None and self._row_count is not None:
            return self._row_count
        return super().get_row_count(table, conn, sample_clause, use_estimation)

    def get_column_non_null_count(
        self, table: sa.Table, column: str, conn: Connection
    ) -> int:
        """Return cached non-null count derived from SUMMARIZE null_percentage."""
        s = self._summary.get(column)
        if s is not None:
            return int(s.non_null)
        return super().get_column_non_null_count(table, column, conn)

    def get_column_unique_count(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        use_approx: bool = True,
    ) -> int:
        """Return cached approx_unique from SUMMARIZE (HyperLogLog)."""
        s = self._summary.get(column)
        if use_approx and s is not None:
            return int(s.approx_unique)
        return super().get_column_unique_count(table, column, conn, use_approx)

    def get_column_min(self, table: sa.Table, column: str, conn: Connection) -> Any:
        """Return cached SUMMARIZE min (VARCHAR); falls back to SQL on cache miss."""
        s = self._summary.get(column)
        return s.min if s is not None else super().get_column_min(table, column, conn)

    def get_column_max(self, table: sa.Table, column: str, conn: Connection) -> Any:
        """Return cached SUMMARIZE max (VARCHAR); falls back to SQL on cache miss."""
        s = self._summary.get(column)
        return s.max if s is not None else super().get_column_max(table, column, conn)

    def get_column_mean(
        self, table: sa.Table, column: str, conn: Connection
    ) -> Optional[Any]:
        """Return cached SUMMARIZE avg (float); falls back to SQL on cache miss."""
        s = self._summary.get(column)
        return s.avg if s is not None else super().get_column_mean(table, column, conn)

    def get_column_stdev(
        self, table: sa.Table, column: str, conn: Connection
    ) -> Optional[Any]:
        """Return cached SUMMARIZE std (float); falls back to SQL on cache miss or when std is None."""
        s = self._summary.get(column)
        if s is not None and s.std is not None:
            return s.std
        return super().get_column_stdev(table, column, conn)

    def get_column_median(self, table: sa.Table, column: str, conn: Connection) -> Any:
        """Return cached SUMMARIZE q50 (p50 median); falls back to quantile_cont on cache miss or when q50 is None."""
        s = self._summary.get(column)
        if s is not None and s.q50 is not None:
            return s.q50
        return super().get_column_median(table, column, conn)
