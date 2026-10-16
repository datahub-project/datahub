"""Snowflake-specific profiling adapter."""

import logging
import uuid
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
    ProfilingConnection,
)
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)

logger = logging.getLogger(__name__)

# Below this a table spans too few micro-partitions for BLOCK sampling to be
# anything but coarse: ~100 partitions at a guessed ~500k rows each. The
# per-partition figure is an order-of-magnitude estimate, not a measurement --
# only the product is meaningful.
BLOCK_SAMPLE_MIN_ROWS = 50_000_000

# How much larger the BLOCK pre-sample is than the final target sample size.
BLOCK_OVERGENERATION_FACTOR = 1000

# Snowflake rejects a fixed-size TABLESAMPLE asking for more than this:
# https://docs.snowflake.com/en/sql-reference/constructs/sample
MAX_FIXED_SIZE_SAMPLE_ROWS = 1_000_000


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

        # custom_sql is not supported on the SQLAlchemy profiler path.
        # If it arrives here, something upstream changed without updating this code.
        assert not context.custom_sql, (
            f"custom_sql is not supported on the SQLAlchemy profiler path "
            f"(table: {context.pretty_name})"
        )

        # Sampling and limit/offset are two answers to the same question -- how
        # to profile a big table cheaply -- and they do not compose: sampling an
        # already-bounded slice pays sampling's cost and loses the validity it
        # buys. Sampling wins; the profiler warns that the limit went unapplied.
        if self.config.use_sampling:
            return self._setup_sampling(context, conn)

        # An offset with no limit is declined: honouring it would materialize
        # every row after the offset, which costs more than leaving it unset.
        if self.config.limit:
            return self._create_limited_temp_table(context, conn)

        context.sql_table = self._create_sqlalchemy_table(
            schema=context.schema,
            table=context.table,
        )
        return context

    def _setup_sampling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        # Prefer the row count already on the context (from the schema crawl)
        # to avoid a redundant INFORMATION_SCHEMA round-trip per table.
        row_count = (
            context.row_count
            if context.row_count is not None
            else self._get_row_count_from_metadata(context, conn)
        )
        if row_count is not None and row_count <= self.config.sample_size:
            # Table is small enough — profile directly without sampling
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.schema,
                table=context.table,
            )
            return context
        if row_count is None:
            # A sampling fraction needs a denominator. Rather than invent
            # one, hand Snowflake a row count and let it size the sample.
            return self._create_fixed_size_sampled_temp_table(context, conn)
        return self._create_sampled_temp_table(context, conn, row_count)

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

        Snowflake spells system sampling BLOCK (SYSTEM and BLOCK are synonyms,
        as are BERNOULLI and ROW), and the two methods trade off against each
        other:
          - BLOCK samples whole micro-partitions, so it skips most of the table
            unread -- but the sample is clustered. Micro-partitions form in load
            order, so a BLOCK sample of a date-ordered table is a handful of date
            ranges, which skews exactly what profiling measures: min/max,
            distinct counts, quantiles.
          - BERNOULLI is an independent per-row coin flip: statistically sound,
            but it scans.
        Large tables therefore get both -- BLOCK cuts the candidate set down to
        BLOCK_OVERGENERATION_FACTOR x the target, then BERNOULLI reduces it the
        rest of the way. That dilutes the clustering bias rather than removing it.

        Both tiers are fraction-based rather than fixed-size (`SAMPLE (<n> ROWS)`)
        because fixed-size sampling "prevents some query optimization":
        https://docs.snowflake.com/en/sql-reference/constructs/sample#performance-considerations

        No SEED is needed because the sample is materialized once — all
        profiling queries see the exact same rows. Without SEED,
        TABLESAMPLE BERNOULLI also works on views.
        """
        temp_name = self._temp_table_name("sample")
        sample_pc = self.config.sample_size / row_count

        assert context.schema is not None, (
            f"schema is required for sampling {context.pretty_name}"
        )
        tablename = SnowflakeIdentifierBuilder.get_quoted_identifier_for_table(
            db_name=None, schema_name=context.schema, table_name=context.table
        )
        # Two conditions, not one: the table must span enough micro-partitions
        # for BLOCK to be meaningful, and there must be a real reduction left for
        # BERNOULLI to do afterwards. The second only bites when sample_size
        # exceeds 50_000 -- below that BLOCK_SAMPLE_MIN_ROWS already implies it.
        use_block_presample = (
            row_count > BLOCK_SAMPLE_MIN_ROWS
            and row_count > self.config.sample_size * BLOCK_OVERGENERATION_FACTOR
        )

        # The percentages are formatted to 8 decimal places throughout, and that
        # precision is load-bearing: at 10T rows block_pc is 1e-4 (as is the
        # BERNOULLI-only retry's bernoulli_pc at 10B rows), and coarser formatting
        # would round the sample down to zero rows.
        if use_block_presample:
            # The guard above holds sample_pc below 1/BLOCK_OVERGENERATION_FACTOR,
            # so block_pc is always under 100 and needs no clamp.
            block_pc = 100 * BLOCK_OVERGENERATION_FACTOR * sample_pc
            bernoulli_pc = 100 / BLOCK_OVERGENERATION_FACTOR
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
        try:
            self._materialize_temp_table(
                conn,
                temp_name,
                sample_sql,
                f"Creating sampled temp table for {context.pretty_name} "
                f"(rows: {row_count:,}, sample: {self.config.sample_size:,})",
            )
        except SQLAlchemyError as e:
            if use_block_presample:
                self.report.warning(
                    title="BLOCK+BERNOULLI sampling failed, retrying with BERNOULLI-only",
                    message="Initial two-tier sampling query failed. "
                    "This can happen on views, with insufficient permissions, "
                    "or when the warehouse is suspended. Retrying with BERNOULLI-only.",
                    context=f"{context.pretty_name}: {type(e).__name__}: {e}",
                )
                bernoulli_pc = 100 * sample_pc
                fallback_sql = (
                    f"SELECT * FROM {tablename}"
                    f" TABLESAMPLE BERNOULLI ({bernoulli_pc:.8f})"
                )
                self._materialize_temp_table(
                    conn,
                    temp_name,
                    fallback_sql,
                    f"Retrying {context.pretty_name} with BERNOULLI-only sampling",
                )
            else:
                raise

        context.applied.sample_percentage = bernoulli_pc
        return self._reflect_temp_table_into_context(context, conn, temp_name)

    def _create_fixed_size_sampled_temp_table(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Materialize a fixed-size TABLESAMPLE into a session-scoped temp table.

        This is the path taken when the row count is unavailable -- external
        tables carry no ROW_COUNT, and INFORMATION_SCHEMA may be inaccessible.
        Fixed-size sampling needs no denominator, so it replaces what would
        otherwise be a guess at the size of the table.

        It is single-tier by necessity: Snowflake rejects `<n> ROWS` together
        with BLOCK/SYSTEM and with SEED. It does work on views and subqueries.
        Fixed-size sampling also "prevents some query optimization", which is
        why the known-row-count path stays fraction-based:
        https://docs.snowflake.com/en/sql-reference/constructs/sample#performance-considerations
        """
        sample_rows = min(self.config.sample_size, MAX_FIXED_SIZE_SAMPLE_ROWS)
        if sample_rows < self.config.sample_size:
            self.report.warning(
                title="Sample size reduced to Snowflake's fixed-size sampling limit",
                message="The row count for this table was unavailable, so it is sampled by "
                "row count rather than by fraction. Snowflake caps that at "
                f"{MAX_FIXED_SIZE_SAMPLE_ROWS:,} rows, below the configured sample_size.",
                context=f"{context.pretty_name} "
                f"({self.config.sample_size:,} -> {sample_rows:,} rows)",
            )

        assert context.schema is not None, (
            f"schema is required for sampling {context.pretty_name}"
        )
        tablename = SnowflakeIdentifierBuilder.get_quoted_identifier_for_table(
            db_name=None, schema_name=context.schema, table_name=context.table
        )
        temp_name = self._temp_table_name("sample")
        self._materialize_temp_table(
            conn,
            temp_name,
            f"SELECT * FROM {tablename} TABLESAMPLE BERNOULLI ({sample_rows} ROWS)",
            f"Creating fixed-size sampled temp table for {context.pretty_name} "
            f"(row count unavailable, sample: {sample_rows:,} rows)",
        )

        # A fixed row count out of an unknown total is an unknown fraction.
        context.applied.sample_rows = sample_rows
        return self._reflect_temp_table_into_context(context, conn, temp_name)

    def _create_limited_temp_table(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Materialize `LIMIT`/`OFFSET` rows into a session-scoped temp table.

        Unlike TABLESAMPLE this is not a sample: it is whatever rows Snowflake
        returns first, so it is reported as a bound rather than as a sample and
        the profile is labelled a bounded query.
        """
        assert context.schema is not None, (
            f"schema is required for limiting {context.pretty_name}"
        )
        # quote=True matches the quoted identifiers the sampling paths build, so
        # both paths treat context.schema/table as the exact stored case.
        table_obj = sa.Table(
            context.table,
            sa.MetaData(),
            schema=context.schema,
            quote=True,
            quote_schema=True,
        )
        # Only reached with a limit set, which also keeps the dialect off its
        # bare-offset path -- that compiles to the invalid `LIMIT -1 OFFSET n`.
        query = sa.select(sa.text("*")).select_from(table_obj).limit(self.config.limit)
        if self.config.offset:
            query = query.offset(self.config.offset)

        select_sql = str(
            query.compile(
                dialect=self.base_engine.dialect,
                compile_kwargs={"literal_binds": True},
            )
        )
        temp_name = self._temp_table_name("limit")
        self._materialize_temp_table(
            conn,
            temp_name,
            select_sql,
            f"Creating limited temp table for {context.pretty_name} "
            f"(limit: {self.config.limit}, offset: {self.config.offset})",
        )

        context.applied.limit = self.config.limit
        context.applied.offset = self.config.offset
        return self._reflect_temp_table_into_context(context, conn, temp_name)

    @staticmethod
    def _temp_table_name(prefix: str) -> str:
        # Unquoted so Snowflake stores it uppercase, matching SQLAlchemy's
        # unquoted references in the SQL it generates against the temp table.
        return f"dh_{prefix}_{uuid.uuid4().hex[:8]}"

    def _materialize_temp_table(
        self, conn: Connection, temp_name: str, select_sql: str, log_message: str
    ) -> None:
        """Fill a session-scoped temp table from `select_sql`."""
        create_sql = f"CREATE OR REPLACE TEMPORARY TABLE {temp_name} AS {select_sql}"
        logger.info(log_message)
        logger.debug(f"SQL: {create_sql}")
        conn.execute(sa.text(create_sql))

    def _reflect_temp_table_into_context(
        self, context: ProfilingContext, conn: Connection, temp_name: str
    ) -> ProfilingContext:
        # Reflect the temp table as a real sa.Table. CTAS carries case-only
        # duplicate columns through to the sample, so this needs the same
        # case-folding repair as a directly reflected table.
        metadata = sa.MetaData()
        context.sql_table = self._use_stored_column_names(
            sa.Table(temp_name, metadata, autoload_with=conn), conn
        )
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
                return self._use_stored_column_names(
                    sa.Table(
                        table,
                        metadata,
                        schema=schema,
                        autoload_with=engine,
                        quote=True,
                        quote_schema=bool(schema),
                    ),
                    engine,
                )
            except SQLAlchemyError as e:
                logger.debug(
                    f"Failed to reflect {schema}.{table} with quoting, "
                    f"trying without quotes: {type(e).__name__}: {str(e)}"
                )

        try:
            return self._use_stored_column_names(
                sa.Table(
                    table,
                    metadata,
                    schema=schema,
                    autoload_with=engine,
                    quote=False,
                    quote_schema=False,
                ),
                engine,
            )
        except SQLAlchemyError as e:
            if not (has_lowercase or has_lowercase_schema):
                logger.debug(
                    f"Failed to reflect {schema}.{table} without quoting, "
                    f"trying with quotes: {type(e).__name__}: {str(e)}"
                )
                return self._use_stored_column_names(
                    sa.Table(
                        table,
                        metadata,
                        schema=schema,
                        autoload_with=engine,
                        quote=True,
                        quote_schema=bool(schema),
                    ),
                    engine,
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
        conn: ProfilingConnection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES

        results = []
        for q in quantiles:
            try:
                snowflake_expr = sa.func.approx_percentile(sa.column(column), q)
                query = sa.select(snowflake_expr).select_from(table)
                result = conn.execute_rows(query).scalar()
                results.append(float(result) if result is not None else None)
            except SQLAlchemyError as e:
                logger.warning(
                    f"Failed to compute quantile {q} for {column}: {type(e).__name__}: {str(e)}"
                )
                results.append(None)
        return results
