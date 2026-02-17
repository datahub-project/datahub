"""Redshift-specific profiling adapter."""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter

logger = logging.getLogger(__name__)


class RedshiftAdapter(PlatformAdapter):
    """
    Redshift-specific profiling adapter.

    Redshift features:
    1. Fast row count estimation via system tables
    2. AVG on INTEGER returns integer (need CAST for precision)
    3. STDDEV on all-NULL columns returns 0.0 (not NULL)
    4. APPROXIMATE COUNT DISTINCT for fast unique counts
    5. PERCENTILE_CONT for median and quantiles

    Uses default setup_profiling and cleanup from PlatformAdapter.
    """

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        Redshift approximate unique count - uses APPROXIMATE COUNT DISTINCT.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for APPROXIMATE COUNT(DISTINCT column)
        """
        # Redshift supports APPROXIMATE COUNT DISTINCT
        return sa.func.count(sa.func.distinct(sa.column(column)))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Redshift median via PERCENTILE_CONT.

        Redshift supports PERCENTILE_CONT which can compute the median (0.5 percentile).

        Note: Using literal_column with label to ensure proper column metadata
        for query combiner compatibility.

        Args:
            column: Column name (from database schema, already validated)

        Returns:
            SQLAlchemy expression for PERCENTILE_CONT(0.5)
        """
        quoted_column = self.quote_identifier(column)
        # Use literal_column with label() to preserve column metadata
        # which is needed for the query combiner to work correctly.
        # sa.text() doesn't provide column metadata, causing empty result rows.
        return sa.literal_column(
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {quoted_column})"
        ).label("median")

    def get_mean_expr(self, column: str) -> ColumnElement[Any]:
        """
        Redshift mean (AVG) with CAST to preserve precision.

        Redshift's AVG on INTEGER columns returns integer (rounded).
        To match GE behavior which shows full precision, we cast to float.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for AVG(CAST(column AS FLOAT))
        """
        # Cast column to float to ensure AVG returns float with full precision
        # This matches GE behavior (e.g., '8.478238501903489')
        return sa.func.avg(sa.cast(sa.column(column), sa.Float))

    def get_stdev_null_value(self) -> Optional[float]:
        """
        Redshift returns 0.0 for STDDEV on all-NULL columns.

        This matches Redshift's actual behavior and GE golden file expectations.

        Returns:
            0.0 for Redshift (not None)
        """
        return 0.0

    # =========================================================================
    # Row Count Estimation
    # =========================================================================

    def supports_row_count_estimation(self) -> bool:
        """
        Redshift supports fast row count estimation.

        Returns:
            True - Redshift has system tables with row count estimates
        """
        return True

    def get_estimated_row_count(
        self, table: sa.Table, conn: Connection
    ) -> Optional[int]:
        """
        Get fast row count estimate using Redshift system tables.

        This avoids a full table scan by using Redshift's statistics.
        The estimate may be slightly out of date but is very fast.

        Args:
            table: SQLAlchemy table object
            conn: Active database connection

        Returns:
            Estimated row count, or None if query fails
        """
        try:
            schema = table.schema
            table_name = table.name

            # Query Redshift system view using SQLAlchemy query builder
            # svv_table_info contains row counts for tables
            svv_table_info = sa.Table(
                "svv_table_info",
                sa.MetaData(),
                sa.Column("schema", sa.String),
                sa.Column("table", sa.String),
                sa.Column("tbl_rows", sa.BigInteger),
            )
            query = (
                sa.select([svv_table_info.c.tbl_rows])
                .where(svv_table_info.c.schema == schema)
                .where(svv_table_info.c.table == table_name)
            )

            result = conn.execute(query).scalar()
            return int(result) if result is not None else None

        except SQLAlchemyError as e:
            logger.debug(
                f"Failed to get Redshift row count estimate: {type(e).__name__}: {str(e)}"
            )
            return None
