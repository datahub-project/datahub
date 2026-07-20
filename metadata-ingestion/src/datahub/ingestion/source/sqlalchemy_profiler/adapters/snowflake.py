"""Snowflake-specific profiling adapter."""

import logging
import uuid
from typing import Any, List, Optional

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


class SnowflakeAdapter(PlatformAdapter):
    """
    Snowflake-specific profiling adapter.

    Snowflake optimizations:
    1. APPROX_COUNT_DISTINCT for fast unique counts
    2. Native MEDIAN() function for median calculation
    3. Temp table materialization for large table sampling
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        if not context.table:
            raise ValueError(
                f"Cannot profile {context.pretty_name}: table name required"
            )

        # custom_sql is only set on the GE profiler path (snowflake_profiler.py
        # gates it behind `method == "ge"`). The SQLAlchemy adapter should never
        # receive it — if it does, something upstream changed without updating this code.
        assert not context.custom_sql, (
            f"custom_sql is not supported on the SQLAlchemy profiler path "
            f"(table: {context.pretty_name})"
        )

        # Determine if sampling is needed
        row_count = self._get_row_count_from_metadata(context, conn)
        if not self.config.limit and self.config.use_sampling:
            if row_count is not None and row_count <= self.config.sample_size:
                # Table is small enough — profile directly without sampling
                context.sql_table = self._create_sqlalchemy_table(
                    schema=context.schema,
                    table=context.table,
                )
            else:
                # Either the table is confirmed large, or we couldn't get the
                # row count. Be conservative and sample in both cases.
                effective_row_count = (
                    row_count
                    if row_count is not None
                    else (self.config.sample_size * 10)
                )
                context = self._create_sampled_temp_table(
                    context, conn, effective_row_count
                )
        else:
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.schema,
                table=context.table,
            )

        return context

    def _get_row_count_from_metadata(
        self, context: ProfilingContext, conn: Connection
    ) -> Optional[int]:
        """Get row count from INFORMATION_SCHEMA.TABLES (instant, no table scan)."""
        try:
            query = sa.text(
                "SELECT ROW_COUNT FROM INFORMATION_SCHEMA.TABLES"
                " WHERE TABLE_CATALOG = CURRENT_DATABASE()"
                " AND TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name"
            )
            result = conn.execute(
                query,
                {"schema_name": context.schema, "table_name": context.table},
            ).scalar()
            row_count = int(result) if result is not None else None
            logger.debug(
                f"INFORMATION_SCHEMA row count for {context.pretty_name}: {row_count}"
            )
            return row_count
        except SQLAlchemyError as e:
            logger.debug(
                f"Failed to get row count from metadata for {context.pretty_name}: {type(e).__name__}: {e}"
            )
            return None

    def _create_sampled_temp_table(
        self, context: ProfilingContext, conn: Connection, row_count: int
    ) -> ProfilingContext:
        """
        Materialize a TABLESAMPLE into a session-scoped temp table.

        The temp table is created once and all subsequent profiling queries
        operate on this small materialized table, avoiding the 70-800x
        performance penalty of re-evaluating TABLESAMPLE on every query.

        No SEED is needed because the sample is materialized once — all
        profiling queries see the exact same rows. Without SEED,
        TABLESAMPLE BERNOULLI also works on views.
        """
        temp_name = f"dh_sample_{uuid.uuid4().hex[:8]}"
        sample_pc = self.config.sample_size / row_count

        estimated_block_row_count = 500_000
        block_profiling_min_rows = 100 * estimated_block_row_count
        overgeneration_factor = 1000

        tablename = f'"{context.schema}"."{context.table}"'
        use_block_presample = (
            row_count > block_profiling_min_rows
            and row_count > self.config.sample_size * overgeneration_factor
        )

        if use_block_presample:
            # Two-tier: BLOCK first to reduce to ~1000x sample, then BERNOULLI
            block_pc = min(100 * overgeneration_factor * sample_pc, 100)
            bernoulli_pc = 100 / overgeneration_factor
            sample_sql = (
                f"SELECT * FROM"
                f" (SELECT * FROM {tablename} TABLESAMPLE BLOCK ({block_pc:.8f}))"
                f" TABLESAMPLE BERNOULLI ({bernoulli_pc:.8f})"
            )
        else:
            bernoulli_pc = 100 * sample_pc
            sample_sql = (
                f"SELECT * FROM {tablename} TABLESAMPLE BERNOULLI ({bernoulli_pc:.8f})"
            )

        # Try the sampling SQL; if BLOCK fails (views), fall back to BERNOULLI only
        # Temp table name is unquoted so Snowflake stores it as uppercase,
        # matching SQLAlchemy's unquoted references in generated SQL.
        create_sql = f"CREATE OR REPLACE TEMPORARY TABLE {temp_name} AS {sample_sql}"

        try:
            logger.info(
                f"Creating sampled temp table for {context.pretty_name} (rows: {row_count:,}, "
                f"sample: {self.config.sample_size:,})"
            )
            logger.debug(f"SQL: {create_sql}")
            conn.execute(sa.text(create_sql))
        except SQLAlchemyError as e:
            if use_block_presample:
                # BLOCK sampling may fail on views; retry with BERNOULLI only
                logger.info(
                    f"BLOCK sampling failed for {context.pretty_name}, falling back to BERNOULLI: "
                    f"{type(e).__name__}: {e}"
                )
                bernoulli_pc = 100 * sample_pc
                fallback_sql = (
                    f"SELECT * FROM {tablename}"
                    f" TABLESAMPLE BERNOULLI ({bernoulli_pc:.8f})"
                )
                create_sql = (
                    f"CREATE OR REPLACE TEMPORARY TABLE {temp_name} AS {fallback_sql}"
                )
                logger.debug(f"Fallback SQL: {create_sql}")
                conn.execute(sa.text(create_sql))
            else:
                raise

        # Reflect the temp table as a real sa.Table
        metadata = sa.MetaData()
        context.sql_table = sa.Table(temp_name, metadata, autoload_with=conn)
        context.is_sampled = True
        context.temp_table = temp_name

        logger.info(f"Created temp table {temp_name} for {context.pretty_name}")

        return context

    def cleanup(self, context: ProfilingContext) -> None:
        # Snowflake temp tables are session-scoped and auto-drop when the
        # connection closes. No explicit cleanup needed.
        if context.temp_table:
            logger.debug(
                f"Temp table {context.temp_table} will be auto-dropped at session end"
            )

    def _create_sqlalchemy_table(
        self, schema: Optional[str], table: str, autoload_with: Optional[Any] = None
    ) -> sa.Table:
        """
        Create SQLAlchemy Table object for Snowflake with proper identifier handling.

        Snowflake identifier behavior:
        - Unquoted identifiers are case-insensitive and stored as UPPERCASE
        - Quoted identifiers are case-sensitive and stored with exact case

        Strategy - try in this order based on table name pattern:
        1. If table has lowercase chars -> Try WITH quoting first
        2. If that fails OR table is all uppercase -> Try WITHOUT quoting
        """
        metadata = sa.MetaData()
        engine = autoload_with or self.base_engine

        has_lowercase = any(c.islower() for c in table)
        has_lowercase_schema = schema and any(c.islower() for c in schema)

        if has_lowercase or has_lowercase_schema:
            try:
                return sa.Table(
                    table,
                    metadata,
                    schema=schema,
                    autoload_with=engine,
                    quote=True,
                    quote_schema=bool(schema),
                )
            except SQLAlchemyError as e:
                logger.debug(
                    f"Failed to reflect {schema}.{table} with quoting, "
                    f"trying without quotes: {type(e).__name__}: {str(e)}"
                )

        try:
            return sa.Table(
                table,
                metadata,
                schema=schema,
                autoload_with=engine,
                quote=False,
                quote_schema=False,
            )
        except SQLAlchemyError as e:
            if not (has_lowercase or has_lowercase_schema):
                logger.debug(
                    f"Failed to reflect {schema}.{table} without quoting, "
                    f"trying with quotes: {type(e).__name__}: {str(e)}"
                )
                return sa.Table(
                    table,
                    metadata,
                    schema=schema,
                    autoload_with=engine,
                    quote=True,
                    quote_schema=bool(schema),
                )
            raise

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        return sa.func.APPROX_COUNT_DISTINCT(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        return sa.func.median(sa.column(column))

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES

        results = []
        for q in quantiles:
            try:
                snowflake_expr = sa.func.approx_percentile(sa.column(column), q)
                query = sa.select(snowflake_expr).select_from(table)
                result = conn.execute(query).scalar()
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {type(e).__name__}: {str(e)}"
                )
                results.append(None)
        return results
