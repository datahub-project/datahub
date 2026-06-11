"""DuckDB-specific profiling adapter."""

import logging
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
)

logger = logging.getLogger(__name__)


class DuckDBAdapter(PlatformAdapter):
    """
    DuckDB profiling adapter.

    DuckDB optimizations:
    1. approx_count_distinct for fast unique counts (HyperLogLog).
    2. quantile_cont for median and quantiles in a single call.
    3. (later task) a SUMMARIZE fast-path computing the base numeric +
       cardinality + null block for all columns in one table scan.
    """

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        return sa.func.approx_count_distinct(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        return sa.func.quantile_cont(sa.column(column), 0.5)

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Any,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES
        # DuckDB returns all quantiles in one call: quantile_cont(col, [..]) -> list.
        expr = sa.func.quantile_cont(sa.column(column), sa.literal(quantiles))
        query = sa.select([expr]).select_from(table)
        result = conn.execute(query).scalar()
        if result is None:
            return [None] * len(quantiles)
        return [float(v) if v is not None else None for v in result]

    def get_sample_clause(self, sample_size: int) -> Optional[str]:
        # DuckDB reservoir sampling by absolute row count.
        return f"USING SAMPLE {int(sample_size)} ROWS"
