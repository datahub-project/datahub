"""PostgreSQL-specific profiling adapter."""

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

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
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

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        PostgreSQL median via PERCENTILE_CONT.

        PostgreSQL doesn't have a native MEDIAN function, but supports
        PERCENTILE_CONT which can compute the median (0.5 percentile).

        Args:
            column: Column name (from database schema, already validated)

        Returns:
            SQLAlchemy expression for PERCENTILE_CONT(0.5)
        """
        quoted_column = self.quote_identifier(column)
        # IMPORTANT: label() is required whenever using literal_column()
        # Without label(), the column name would be the entire SQL string, which breaks query combiner result extraction
        return sa.literal_column(
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {quoted_column})"
        ).label("median")

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        PostgreSQL quantiles via PERCENTILE_DISC.

        Matches GE behavior (sqlalchemy_dataset.py:_get_column_quantiles_generic_sqlalchemy)
        which uses PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY col ASC). The base adapter's
        PERCENTILE_CONT interpolates between values; PERCENTILE_DISC returns the actual
        value at the discrete percentile. These return different values for sparse data.
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
        results: List[Optional[float]] = []
        for q in quantiles:
            try:
                percentile_expr = sa.literal_column(
                    f"PERCENTILE_DISC({q}) WITHIN GROUP (ORDER BY {quoted_column} ASC)"
                ).label("percentile")
                query = sa.select([percentile_expr]).select_from(table)
                result = conn.execute(query).scalar()
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: "
                    f"{type(e).__name__}: {str(e)}"
                )
                results.append(None)
        return results

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
            schema = table.schema
            table_name = table.name

            # Query pg_class and pg_namespace directly using SQLAlchemy query builder
            pg_class = sa.Table(
                "pg_class",
                sa.MetaData(),
                sa.Column("relname", sa.String),
                sa.Column("relnamespace", sa.Integer),
                sa.Column("reltuples", sa.Float),
                schema="pg_catalog",
            )
            pg_namespace = sa.Table(
                "pg_namespace",
                sa.MetaData(),
                sa.Column("oid", sa.Integer),
                sa.Column("nspname", sa.String),
                schema="pg_catalog",
            )
            query = (
                sa.select([sa.cast(pg_class.c.reltuples, sa.BigInteger)])
                .select_from(
                    pg_class.join(
                        pg_namespace, pg_class.c.relnamespace == pg_namespace.c.oid
                    )
                )
                .where(pg_namespace.c.nspname == schema)
                .where(pg_class.c.relname == table_name)
            )

            result = conn.execute(query).scalar()
            return int(result) if result is not None else None

        except SQLAlchemyError as e:
            logger.debug(
                f"Failed to get PostgreSQL row count estimate: {type(e).__name__}: {str(e)}"
            )
            return None
