"""ClickHouse-specific profiling adapter."""

from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
)


class ClickHouseAdapter(PlatformAdapter):
    """Profiling adapter for ClickHouse's non-standard SQL aggregates."""

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        # uniq() is HyperLogLog, materially faster than COUNT(DISTINCT) on large columns.
        return sa.func.uniq(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        # ClickHouse uses two-call quantile(level)(col) syntax that sa.func cannot
        # express; .label() is required because the query combiner extracts result
        # columns by name.
        quoted_column = self.quote_identifier(column)
        return sa.literal_column(f"quantile(0.5)({quoted_column})").label("median")

    def get_mean_expr(self, column: str) -> ColumnElement[Any]:
        return sa.func.avg(sa.column(column))

    def get_column_stdev(
        self, table: sa.Table, column: str, conn: Connection
    ) -> Optional[Any]:
        # ClickHouse's `stddev` is an alias for `stddevPop` (population), so we
        # call `stddevSamp` explicitly to match sample-stddev semantics.
        try:
            query = sa.select([sa.func.stddevSamp(sa.column(column))]).select_from(
                table
            )
            result = conn.execute(query).scalar()
        except SQLAlchemyError as e:
            self.report.warning(
                title="Profiling: failed to compute stdev",
                message="ClickHouse stddevSamp() query failed; column stdev unavailable",
                context=_format_context(table, column),
                exc=e,
            )
            return None

        if result is not None:
            return result

        # NULL stddev → disambiguate undefined (≤1 row) from zero variance.
        # Reported separately so a failure here is not blamed on stddevSamp above.
        try:
            non_null_count = self.get_column_non_null_count(table, column, conn)
        except SQLAlchemyError as e:
            self.report.warning(
                title="Profiling: failed to disambiguate stdev null result",
                message="Non-null count query failed after stddevSamp() returned NULL",
                context=_format_context(table, column),
                exc=e,
            )
            return None
        return None if non_null_count <= 1 else 0.0

    def supports_row_count_estimation(self) -> bool:
        return True

    def get_estimated_row_count(
        self, table: sa.Table, conn: Connection
    ) -> Optional[int]:
        """Read MergeTree row count from system.tables; fall back to count() otherwise."""
        # `database = NULL` never matches, so skip the fast path entirely when
        # schema is unset rather than waste a round-trip.
        if table.schema is not None:
            try:
                result = conn.execute(
                    sa.text(
                        "SELECT total_rows FROM system.tables "
                        "WHERE database = :db AND name = :tbl"
                    ),
                    {"db": table.schema, "tbl": table.name},
                ).scalar()
                if result is not None:
                    return int(result)
            except (SQLAlchemyError, ValueError, TypeError) as e:
                self.report.warning(
                    title="Profiling: ClickHouse row count estimate query failed",
                    message="system.tables lookup failed; falling back to count()",
                    context=_format_context(table),
                    exc=e,
                )

        try:
            result = conn.execute(
                sa.select([sa.func.count()]).select_from(table)
            ).scalar()
            return int(result) if result is not None else None
        except (SQLAlchemyError, ValueError, TypeError) as e:
            self.report.warning(
                title="Profiling: ClickHouse count() fallback failed",
                message="Both system.tables and count() row count queries failed",
                context=_format_context(table),
                exc=e,
            )
            return None

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """Batched quantiles() with per-quantile fallback. Returns list of len(quantiles)."""
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES

        # `quantiles()()` is invalid SQL — short-circuit before emitting a
        # misleading "batched failed" warning.
        if not quantiles:
            return []

        # Validate before string-interpolating into SQL.
        for q in quantiles:
            if not (0 <= q <= 1):
                raise ValueError(
                    f"Quantiles must be in [0, 1], got {q}. "
                    "Quantiles represent percentiles as decimals (e.g. 0.5 for median)."
                )

        quoted_column = self.quote_identifier(column)

        # Catches are scoped narrowly per operation so programming bugs propagate.
        raw: Any = None
        batched_failed = False
        try:
            levels = ", ".join(str(q) for q in quantiles)
            expr = sa.literal_column(f"quantiles({levels})({quoted_column})").label(
                "quantiles"
            )
            query = sa.select([expr]).select_from(table)
            raw = conn.execute(query).scalar()
        except SQLAlchemyError as e:
            batched_failed = True
            self.report.warning(
                title="Profiling: ClickHouse batched quantiles failed, using fallback",
                message="quantiles() aggregate failed; running per-quantile queries",
                context=_format_context(table, column),
                exc=e,
            )

        if not batched_failed:
            if raw is None:
                return [None] * len(quantiles)
            try:
                values = [float(v) if v is not None else None for v in raw]
            except (ValueError, TypeError) as e:
                batched_failed = True
                self.report.warning(
                    title="Profiling: ClickHouse batched quantiles returned non-numeric data, using fallback",
                    message="quantiles() result was not iterable / numeric; running per-quantile queries",
                    context=_format_context(table, column),
                    exc=e,
                )
            else:
                if len(values) != len(quantiles):
                    raise RuntimeError(
                        f"ClickHouse quantiles() returned {len(values)} values for "
                        f"{len(quantiles)} requested quantiles"
                    )
                return values

        results: List[Optional[float]] = []
        failures: List[float] = []
        first_exc: Optional[BaseException] = None
        for q in quantiles:
            try:
                expr = sa.literal_column(f"quantile({q})({quoted_column})").label(
                    "quantile"
                )
                query = sa.select([expr]).select_from(table)
                result = conn.execute(query).scalar()
            except SQLAlchemyError as e:
                if first_exc is None:
                    first_exc = e
                failures.append(q)
                results.append(None)
                continue
            try:
                results.append(float(result) if result is not None else None)
            except (ValueError, TypeError) as e:
                if first_exc is None:
                    first_exc = e
                failures.append(q)
                results.append(None)

        if failures:
            self.report.warning(
                title="Profiling: some ClickHouse quantiles unavailable",
                message=(
                    f"{len(failures)}/{len(quantiles)} quantile queries failed "
                    "(after batched-form fallback)"
                ),
                context=f"{_format_context(table, column)}: failed={failures}",
                exc=first_exc,
            )
        return results


def _format_context(table: sa.Table, column: Optional[str] = None) -> str:
    parts = [p for p in (table.schema, table.name, column) if p is not None]
    return ".".join(parts) if parts else "<unknown>"
