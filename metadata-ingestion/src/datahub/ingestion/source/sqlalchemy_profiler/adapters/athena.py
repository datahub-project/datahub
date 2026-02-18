"""Athena-specific profiling adapter."""

import logging
import uuid
from typing import Any, List, Optional

import sqlalchemy as sa
from pyathena import error as athena_errors
from pyathena.cursor import Cursor
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


class AthenaAdapter(PlatformAdapter):
    """
    Athena-specific profiling adapter.

    Athena features:
    1. Temp views for custom SQL (no CREATE TABLE permission needed)
    2. approx_distinct() for fast unique count estimation
    3. approx_percentile() for median and quantiles
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup Athena profiling with temp views for custom SQL.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context ready for profiling
        """
        logger.debug(f"Athena setup for {context.pretty_name}")

        # Step 1: Handle custom SQL via temp view
        if context.custom_sql:
            context = self._create_temp_view_for_custom_sql(context)

        # Step 2: Create SQLAlchemy table object
        if context.temp_view:
            # Use temp view
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.schema,
                table=context.temp_view,
            )
        else:
            # Use original table
            if not context.table:
                raise ValueError(
                    f"Cannot profile {context.pretty_name}: table name required"
                )
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.schema,
                table=context.table,
            )

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Cleanup Athena temp views.

        Args:
            context: Profiling context
        """
        if context.temp_view:
            self._drop_temp_view(context.temp_view, context.schema)

    def _create_temp_view_for_custom_sql(
        self, context: ProfilingContext
    ) -> ProfilingContext:
        """
        Create Athena temp view for custom SQL.

        Athena doesn't require CREATE TABLE permissions for views,
        making this a lightweight operation.

        Args:
            context: Current profiling context

        Returns:
            Updated context with temp_view populated
        """
        if not context.custom_sql:
            return context

        try:
            # Get raw DBAPI connection
            raw_conn = self.base_engine.raw_connection()
            cursor: Cursor = raw_conn.cursor()  # type: ignore[assignment]

            # Generate unique view name
            temp_view = f"ge_{uuid.uuid4().hex[:8]}"

            logger.debug(
                f"Creating Athena temp view for {context.pretty_name}: {context.custom_sql}"
            )

            # Create view with custom SQL
            cursor.execute(
                f'CREATE OR REPLACE VIEW "{temp_view}" AS {context.custom_sql}'
            )

            context.temp_view = temp_view
            context.add_temp_resource("athena_temp_view", temp_view)

            logger.debug(f"Created Athena temp view: {temp_view}")

        except athena_errors.DatabaseError as e:
            error_msg = (
                f"Cannot profile {context.pretty_name}: Athena temp view creation failed. "
                f"Temp view is required for custom SQL queries. "
                f"{type(e).__name__}: {str(e)}"
            )
            self.report.warning(
                title="Failed to create Athena temporary view",
                message=f"Profiling exception when running custom sql: {context.custom_sql}",
                context=f"Asset: {context.pretty_name}",
                exc=e,
            )
            if not self.config.catch_exceptions:
                raise
            # Even with catch_exceptions, we must fail here because temp view is REQUIRED
            # Without it, we'd silently profile the full table instead of the requested custom SQL
            raise RuntimeError(error_msg) from e

        return context

    def _drop_temp_view(self, view_name: str, schema: Optional[str]) -> None:
        """
        Drop Athena temp view.

        Args:
            view_name: Name of view to drop
            schema: Schema name (optional)
        """
        try:
            with self.base_engine.connect() as connection:
                full_view_name = (
                    self.quote_identifier(f"{schema}.{view_name}")
                    if schema
                    else self.quote_identifier(view_name)
                )
                connection.execute(sa.text(f"DROP VIEW IF EXISTS {full_view_name}"))
                logger.debug(f"Dropped Athena temp view: {full_view_name}")
        except SQLAlchemyError as e:
            logger.warning(
                f"Unable to drop Athena temp view {view_name}: {type(e).__name__}: {str(e)}"
            )

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        Athena uses approx_distinct() for fast unique count estimation.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approx_distinct(column)
        """
        return sa.func.approx_distinct(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Athena median via approx_percentile(col, 0.5).

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for approx_percentile(column, 0.5)
        """
        return sa.func.approx_percentile(sa.column(column), 0.5)

    def get_quantiles_expr(
        self, column: str, quantiles: List[float]
    ) -> Optional[ColumnElement[Any]]:
        """
        Athena quantiles via approx_percentile(col, ARRAY[...]).

        Returns array of quantile values.

        Args:
            column: Column name
            quantiles: List of quantile values (e.g., [0.25, 0.5, 0.75])

        Returns:
            SQLAlchemy expression for approx_percentile with array of quantiles
        """
        quoted_column = self.quote_identifier(column)
        # Build ARRAY[0.05, 0.25, 0.5, 0.75, 0.95] string
        array_str = f"ARRAY[{', '.join(str(q) for q in quantiles)}]"
        return sa.literal_column(
            f"approx_percentile({quoted_column}, {array_str})"
        ).label("quantiles")

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using Athena's approx_percentile.

        Athena: approx_percentile(col, ARRAY[0.05, 0.25, ...]) returns an array.

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
        # Athena/Trino: approx_percentile(col, ARRAY[0.05, 0.25, ...])
        array_str = f"ARRAY[{', '.join(str(q) for q in quantiles)}]"
        athena_expr = sa.literal_column(
            f"approx_percentile({quoted_column}, {array_str})"
        ).label("quantiles")
        query = sa.select([athena_expr]).select_from(table)
        result = conn.execute(query).scalar()
        logger.debug(
            f"Athena quantiles for {column}: result type={type(result)}, "
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
