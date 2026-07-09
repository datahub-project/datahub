"""Snowflake-specific profiling adapter."""

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
    3. Inline TABLESAMPLE for large table profiling (matches GE profiler behavior)
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup Snowflake profiling using inline TABLESAMPLE.

        IMPORTANT ARCHITECTURAL NOTE:
        For Snowflake, sampling is controlled by snowflake_profiler.py which sets
        context.custom_sql with a TABLESAMPLE query. This is not ideal architecture
        (sampling decision should be in this adapter layer), but we keep it this way
        for backwards compatibility with the GE profiler.

        FUTURE CONSIDERATION:
        Consider using temp tables for custom_sql like BigQuery and Athena adapters do.
        This would provide:
        - Consistent samples across all profiling queries
        - Better separation of concerns (adapter handles all sampling)
        However, this adds complexity around schema prefixes and connection lifecycle
        in Snowflake's session-scoped temp table model.

        For now, we follow GE's proven approach: inline TABLESAMPLE queries.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context ready for profiling
        """
        logger.debug(f"Snowflake setup_profiling called for {context.pretty_name}")

        # If custom_sql is provided (e.g., TABLESAMPLE query from snowflake_profiler.py),
        # use it as the profiling source
        if context.custom_sql:
            logger.info(
                f"Using custom SQL for {context.pretty_name} (sampling via inline TABLESAMPLE)"
            )
            context.sql_table = self._create_table_from_custom_sql(context, conn)
            context.is_sampled = True
            return context

        # Otherwise, profile the original table
        if not context.table:
            raise ValueError(
                f"Cannot profile {context.pretty_name}: table name required"
            )

        context.sql_table = self._create_sqlalchemy_table(
            schema=context.schema,
            table=context.table,
        )

        return context

    def _create_table_from_custom_sql(
        self, context: ProfilingContext, conn: Connection
    ) -> sa.Table:
        """
        Create a SQLAlchemy Table object from a custom SQL query (e.g., TABLESAMPLE).

        This creates a Table that wraps the custom SQL as a subquery, allowing the
        query combiner to profile it just like a regular table.

        Args:
            context: Profiling context with custom_sql set
            conn: Active database connection for metadata reflection

        Returns:
            SQLAlchemy Table object representing the custom SQL query
        """
        # First, get the column metadata from the original table
        # This is needed to create the Table object with proper columns
        if not context.table:
            raise ValueError(
                f"Cannot create table from custom SQL for {context.pretty_name}: "
                "table name required for column metadata"
            )

        # Reflect the original table to get column definitions
        original_table = self._create_sqlalchemy_table(
            schema=context.schema,
            table=context.table,
        )

        # Create a text query from the custom SQL
        custom_query = sa.text(context.custom_sql).columns(
            *[sa.column(c.name, c.type) for c in original_table.columns]
        )

        # Create a subquery that can be used like a table
        # We create a Table object with a custom selectable
        subquery = custom_query.subquery(name=context.table)

        # Create a Table from the subquery with the same columns as the original
        table = sa.Table(
            context.table,
            sa.MetaData(),
            *[
                sa.Column(
                    c.name,
                    c.type,
                    key=c.name,
                )
                for c in original_table.columns
            ],
            schema=context.schema,
        )

        # Override the table's select to use our subquery
        # This makes profiling queries execute against the TABLESAMPLE query
        table = subquery.alias(context.table)

        logger.debug(
            f"Created table from custom SQL for {context.pretty_name}: {context.custom_sql[:100]}..."
        )

        return table

    def _create_sqlalchemy_table(
        self, schema: Optional[str], table: str, autoload_with: Optional[Any] = None
    ) -> sa.Table:
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
                query = sa.select(snowflake_expr).select_from(table)
                result = conn.execute(query).scalar()
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {type(e).__name__}: {str(e)}"
                )
                results.append(None)
        return results
