"""Databricks-specific profiling adapter."""

import logging
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
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

    Uses default setup_profiling and cleanup from PlatformAdapter.
    """

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
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

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
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

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using Databricks' approx_percentile.

        Databricks: approx_percentile(col, array(0.05, 0.25, ...)) returns an array.

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

        quoted_column = self.quote_identifier(column)
        # Databricks: Similar to Athena/Trino but uses array() syntax
        array_str = f"array({', '.join(str(q) for q in quantiles)})"
        databricks_expr = sa.literal_column(
            f"approx_percentile({quoted_column}, {array_str})"
        ).label("quantiles")
        query = sa.select([databricks_expr]).select_from(table)
        result = conn.execute(query).scalar()
        logger.debug(
            f"Databricks quantiles for {column}: result type={type(result)}, "
            f"value={result}, expected_length={len(quantiles)}"
        )
        # Result is an array, convert to list
        if isinstance(result, list):
            if len(result) != len(quantiles):
                logger.warning(
                    f"Quantile result length mismatch: got {len(result)}, expected {len(quantiles)}"
                )
            return [float(v) if v is not None else None for v in result]
        logger.warning(
            f"Quantile result is not a list: type={type(result)}, value={result}"
        )
        return [None] * len(quantiles)
