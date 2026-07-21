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
    3. Sampling of large tables via a materialized temporary table (see
       ``setup_profiling``), matching the legacy Great Expectations profiler.
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Snowflake setup - create SQL table object.

        When the connector requested sampling for a large table,
        ``SnowflakeProfiler.get_batch_kwargs`` supplies a ``TABLESAMPLE`` query
        as ``context.custom_sql``. We materialize that sample into a
        session-scoped temporary table and profile it instead of the base
        table. This mirrors the legacy Great Expectations profiler, which
        turned the same ``custom_sql`` into a ``CREATE TEMPORARY TABLE ... AS``.
        Without it every column statistic (min/max/mean/median/distinct/…) would
        scan every row of the base table, which is prohibitively slow on large
        Snowflake tables.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context with sql_table populated
        """
        if not context.table:
            raise ValueError(
                f"Cannot profile {context.pretty_name}: table name required"
            )

        # Reflect the real (permanent) table to obtain its column names/types.
        # This is safe on any pooled connection because the source table is a
        # persistent object.
        source_table = self._create_sqlalchemy_table(
            schema=context.schema,
            table=context.table,
        )

        if context.custom_sql:
            sampled_table = self._materialize_sample(context, source_table, conn)
            if sampled_table is not None:
                context.sql_table = sampled_table
                context.is_sampled = True
                logger.debug(
                    f"Snowflake setup for {context.pretty_name}: "
                    f"profiling sampled temp table {sampled_table.name}"
                )
                return context
            # _materialize_sample already reported a warning; fall back to
            # profiling the full table so the run does not fail outright.

        context.sql_table = source_table

        logger.debug(
            f"Snowflake setup for {context.pretty_name}: "
            f"schema={context.schema}, table={context.table}"
        )

        return context

    def _materialize_sample(
        self,
        context: ProfilingContext,
        source_table: "sa.Table",
        conn: Connection,
    ) -> Optional["sa.Table"]:
        """
        Materialize ``context.custom_sql`` (a ``TABLESAMPLE`` query) into a
        session-scoped temporary table and return a Table object pointing at it.

        The temporary table MUST be created on ``conn`` — the same connection
        the ``QueryCombinerRunner`` uses to run the profiling queries — because
        Snowflake ``TEMPORARY`` tables are visible only within the session that
        created them. Using ``base_engine.raw_connection()`` (as the Athena /
        Trino / BigQuery adapters do for their global views/cached tables) would
        create the table in a different pooled session where the profiling
        queries could not see it.

        Returns None if the sample could not be created, signalling the caller
        to fall back to full-table profiling.
        """
        # All-uppercase, unquoted-safe identifier. Snowflake stores unquoted
        # identifiers upper-cased, so an all-caps name reflects/quotes
        # consistently whether or not the dialect decides to quote it.
        temp_name = f"GE_TMP_SAMPLE_{uuid.uuid4().hex[:12].upper()}"

        # Created unqualified (in the session's current schema), matching the
        # legacy GE profiler's behavior. The sample query itself references the
        # fully-qualified source table, so no schema needs to be in scope for
        # the SELECT.
        ddl = f'CREATE OR REPLACE TEMPORARY TABLE "{temp_name}" AS {context.custom_sql}'
        try:
            conn.exec_driver_sql(ddl)
        except SQLAlchemyError as e:
            self.report.warning(
                title="Profiling: falling back to full-table scan",
                message=(
                    "Failed to create a sampled temporary table for profiling; "
                    "profiling will run against the full table, which may be slow "
                    "on large tables."
                ),
                context=f"Asset: {context.pretty_name}",
                exc=e,
            )
            return None

        context.add_temp_resource("snowflake_temp_table", temp_name)

        # Reuse the reflected columns of the source table (CREATE TABLE AS SELECT
        # preserves exact column names/casing) rather than reflecting the temp
        # table: Snowflake reflection is information-schema based and does not
        # reliably surface session-scoped temporary tables.
        try:
            return source_table.to_metadata(sa.MetaData(), schema=None, name=temp_name)
        except SQLAlchemyError as e:
            self.report.warning(
                title="Profiling: falling back to full-table scan",
                message=(
                    "Failed to build a table object for the sampled temporary "
                    "table; profiling will run against the full table."
                ),
                context=f"Asset: {context.pretty_name}",
                exc=e,
            )
            return None

    def cleanup(self, context: ProfilingContext) -> None:
        """
        Snowflake cleanup.

        Any temporary sample table created in ``setup_profiling`` is
        session-scoped and is dropped automatically when the underlying
        connection is returned to the pool and closed, so no explicit DROP is
        issued here (a DROP on a fresh pooled connection would target a
        different session and be a no-op anyway).
        """
        pass

    # =========================================================================
    # Table Creation
    # =========================================================================

    def _create_sqlalchemy_table(
        self, schema: Optional[str], table: str, autoload_with: Optional[Any] = None
    ) -> "sa.Table":
        """
        Create SQLAlchemy Table object for Snowflake with proper identifier handling.

        Snowflake identifier behavior:
        - Unquoted identifiers are case-insensitive and stored as UPPERCASE
        - Quoted identifiers are case-sensitive and stored with exact case
        - The Snowflake source connector may lowercase identifiers for URN generation

        Strategy - try in this order based on table name pattern:
        1. If table has lowercase chars -> Try WITH quoting first
           - Lowercase suggests table was created with quotes: "lcase_table"
           - Quoting preserves exact case needed to find these tables
        2. If that fails OR table is all uppercase -> Try WITHOUT quoting
           - Standard Snowflake tables are stored as UPPERCASE
           - Unquoted reflection lets Snowflake auto-uppercase: errortypes -> ERRORTYPES

        Args:
            schema: Schema name from metadata (may be lowercase if convert_urns_to_lowercase=True)
            table: Table name from metadata (may be lowercase if convert_urns_to_lowercase=True)
            autoload_with: Engine or Connection for metadata reflection (optional)

        Returns:
            SQLAlchemy Table object

        Raises:
            NoSuchTableError: If table cannot be found with either approach
        """
        metadata = sa.MetaData()
        engine = autoload_with or self.base_engine

        # Determine if table name suggests it was created with quotes
        # Mixed case or lowercase letters indicate quoted creation
        has_lowercase = any(c.islower() for c in table)
        has_lowercase_schema = schema and any(c.islower() for c in schema)

        # Try quoted first if name has lowercase (likely created with quotes)
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

        # Try unquoted (standard Snowflake - auto-uppercase)
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
            # If unquoted failed and we haven't tried quoted yet, try it now
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
            # Already tried both, re-raise
            raise

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """
        Snowflake uses APPROX_COUNT_DISTINCT for fast unique counts.

        This matches GE profiler behavior (ge_data_profiler.py:213-221)
        where Snowflake is grouped with BigQuery for approximate counts.

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for APPROX_COUNT_DISTINCT
        """
        return sa.func.APPROX_COUNT_DISTINCT(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        Snowflake has native MEDIAN() function.

        This matches GE profiler behavior (ge_data_profiler.py:676-683).

        Args:
            column: Column name

        Returns:
            SQLAlchemy expression for MEDIAN
        """
        return sa.func.median(sa.column(column))

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using Snowflake's APPROX_PERCENTILE.

        Snowflake: APPROX_PERCENTILE(col, quantile) computes a single percentile.
        We execute one query per quantile.

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

        # Snowflake: APPROX_PERCENTILE(col, quantile)
        results = []
        for q in quantiles:
            try:
                snowflake_expr = sa.func.approx_percentile(sa.column(column), q)
                query = sa.select([snowflake_expr]).select_from(table)
                result = conn.execute(query).scalar()
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {type(e).__name__}: {str(e)}"
                )
                results.append(None)
        return results
