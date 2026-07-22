"""BigQuery-specific profiling adapter."""

import logging
from typing import Any, List, Optional

import google.cloud.bigquery.job.query
import sqlalchemy as sa
from google.cloud.bigquery.dbapi import exceptions as bq_exceptions
from google.cloud.bigquery.dbapi.cursor import Cursor as BigQueryCursor
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


class BigQueryAdapter(PlatformAdapter):
    """
    BigQuery-specific profiling adapter.

    BigQuery has several special requirements:
    1. Temporary tables via cached results (no CREATE TABLE permission needed)
    2. TABLESAMPLE SYSTEM for sampling large tables
    3. APPROX_COUNT_DISTINCT, approx_quantiles for fast statistics
    4. Special handling for LIMIT/OFFSET queries
    5. Supports 3-part identifiers (project.dataset.table) via inherited quote_identifier()
    """

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Setup BigQuery profiling with temp tables and sampling.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context ready for profiling
        """
        logger.debug(f"BigQuery setup for {context.pretty_name}")

        # Step 1: Handle custom SQL or LIMIT/OFFSET via temp table
        if context.custom_sql or self.config.limit or self.config.offset:
            context = self._create_temp_table_for_query(context)

        # Step 2: Sample large tables (and large partitions) into a temp table.
        # Skip when a LIMIT is configured — the row set is already bounded.
        if self.config.use_sampling and not self.config.limit:
            context = self._setup_sampling(context, conn)

        # Step 3: Create SQLAlchemy table object
        if context.temp_table:
            # Use temp table
            context.sql_table = self._create_sqlalchemy_table(
                schema=context.temp_schema,
                table=context.temp_table,
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
        Cleanup BigQuery temp resources.

        BigQuery cached result tables auto-expire in 24 hours, so no
        explicit cleanup is needed.

        Args:
            context: Profiling context
        """
        # BigQuery temp tables auto-expire, no cleanup needed
        if context.temp_table:
            logger.debug(
                f"BigQuery temp table {context.temp_table} will auto-expire in 24h"
            )
        return

    def _create_temp_table_for_query(
        self, context: ProfilingContext
    ) -> ProfilingContext:
        """
        Create BigQuery temp table using cached results feature.

        On BigQuery, we (ab)use the cached results feature to create temp tables
        without requiring CREATE/DELETE TABLE permissions. BigQuery stores query
        results in a temporary, cached results table that's per-user and per-project.

        Args:
            context: Current profiling context

        Returns:
            Updated context with temp_table populated
        """
        if context.custom_sql:
            # User-provided SQL - pass through as-is
            bq_sql = context.custom_sql
        else:
            # Build query using SQLAlchemy query builder to prevent SQL injection
            # Table must be present if we're not using custom_sql
            if not context.table:
                raise ValueError(
                    f"Cannot profile {context.pretty_name}: table name is required"
                )

            # Create SQLAlchemy Table object for safe query construction
            table_obj = sa.Table(
                context.table,
                sa.MetaData(),
                schema=context.schema,
            )

            # Build SELECT * query using query builder
            query = sa.select(sa.text("*")).select_from(table_obj)

            # Add LIMIT if configured
            if self.config.limit:
                # Pydantic validates this is an int, but ensure it's positive
                limit_val = int(self.config.limit)
                if limit_val <= 0:
                    raise ValueError(
                        f"Invalid LIMIT value: {limit_val}. Must be positive."
                    )
                query = query.limit(limit_val)

            # Add OFFSET if configured
            if self.config.offset:
                # Pydantic validates this is an int, but ensure it's non-negative
                offset_val = int(self.config.offset)
                if offset_val < 0:
                    raise ValueError(
                        f"Invalid OFFSET value: {offset_val}. Must be non-negative."
                    )
                query = query.offset(offset_val)

            # Compile query to SQL string for DBAPI execution
            bq_sql = str(
                query.compile(
                    dialect=self.base_engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )

        # Execute query to create cached temp table
        raw_conn = self.base_engine.raw_connection()
        try:
            cursor: BigQueryCursor = raw_conn.cursor()  # type: ignore[assignment]

            logger.debug(
                f"Creating BigQuery temp table for {context.pretty_name}: {bq_sql}"
            )
            cursor.execute(bq_sql)

            # Extract the name of the cached results table from the query job
            query_job: Optional[google.cloud.bigquery.job.query.QueryJob] = (
                # In google-cloud-bigquery 3.15.0, the _query_job attribute was
                # made public and renamed to query_job.
                cursor.query_job if hasattr(cursor, "query_job") else cursor._query_job  # type: ignore[attr-defined]
            )

            if query_job and query_job.destination:
                temp_destination_table = query_job.destination
                context.temp_table = temp_destination_table.table_id
                context.temp_schema = f"{temp_destination_table.project}.{temp_destination_table.dataset_id}"
                context.add_temp_resource("bigquery_temp_table", context.temp_table)
                logger.debug(
                    f"Created BigQuery temp table: {context.temp_schema}.{context.temp_table}"
                )
                return context
            else:
                # No destination means BigQuery didn't cache results (too large, security settings, etc)
                error_msg = (
                    f"Cannot profile {context.pretty_name}: BigQuery did not create cached results table. "
                    "This typically happens when query results exceed 10GB or table has row-level security."
                )
                self.report.warning(
                    title="BigQuery temporary table required but not created",
                    message="Cannot profile with custom SQL/LIMIT/OFFSET - temp table creation required",
                    context=f"{context.pretty_name}: No cached results table",
                )
                raise RuntimeError(error_msg)

        except bq_exceptions.DatabaseError as e:
            error_msg = (
                f"Cannot profile {context.pretty_name}: BigQuery temp table creation failed. "
                f"Temp table is required for custom SQL/LIMIT/OFFSET queries. "
                f"{type(e).__name__}: {str(e)}"
            )
            self.report.warning(
                title="Failed to create BigQuery temporary table",
                message="Cannot profile with custom SQL/LIMIT/OFFSET - temp table creation required",
                context=f"{context.pretty_name}: {type(e).__name__}: {str(e)}",
                exc=e,
            )
            if not self.config.catch_exceptions:
                raise
            # Even with catch_exceptions, we must fail here because temp table is REQUIRED
            # Without it, we'd silently profile the full table instead of the requested sample
            raise RuntimeError(error_msg) from e
        finally:
            # Guard the close so a teardown error can't replace the in-flight
            # exception (the actionable RuntimeError/DatabaseError above).
            try:
                raw_conn.close()
            except Exception as close_err:
                logger.debug(
                    f"Failed to close raw connection after temp-table creation: {close_err}"
                )

    def _get_quick_row_count(
        self, context: ProfilingContext, conn: Connection
    ) -> Optional[int]:
        """
        Get a quick row count for the current profiling target.

        Counts whichever target is active: a temp table materialized in step 1
        (e.g. a partition) takes precedence, then an already-built sql_table,
        then the original table. On BigQuery a COUNT(*) is a metadata-only,
        0-bytes-billed query.

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Row count, or None if unavailable
        """
        try:
            if context.temp_table:
                # A partition/query temp table exists — count it, not the
                # original table (its size differs from the full table).
                table_obj: Any = sa.Table(
                    context.temp_table,
                    sa.MetaData(),
                    schema=context.temp_schema,
                )
            elif context.sql_table is not None:
                table_obj = context.sql_table
            elif context.table:
                table_obj = sa.Table(
                    context.table,
                    sa.MetaData(),
                    schema=context.schema,
                )
            else:
                return None

            query = sa.select([sa.func.count()]).select_from(table_obj)
            result = conn.execute(query).scalar()
            return int(result) if result is not None else None
        except SQLAlchemyError as e:
            self.report.warning(
                title="Profiling: unable to get row count for sampling",
                message="Could not determine row count; the table will be "
                "profiled without sampling.",
                context=context.pretty_name,
                exc=e,
            )
            return None

    def _setup_sampling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        """
        Sample a large table (or partition) into a cached temp table.

        Mirrors the GE profiler (update_dataset_batch_use_sampling): run
        `SELECT * FROM <target> TABLESAMPLE SYSTEM (pc PERCENT)` and reuse
        BigQuery's cached-results table as the profiling target. Because
        TABLESAMPLE must be applied to a table (not a subquery), a partitioned
        table is sampled via the partition temp table materialized in step 1,
        not by wrapping the partition SQL.

        Note on custom_sql: for BigQuery, `custom_sql` (the partition filter
        built in bigquery_v2/profiler.py) feeds BOTH the GE and SQLAlchemy
        engines — unlike Snowflake, where #18253 gated custom_sql to the GE
        path. Once the GE profiler is removed, partition selection should be
        pushed down into this adapter (see the deprecation note on
        ProfilingContext.custom_sql).

        Args:
            context: Current profiling context
            conn: Active database connection

        Returns:
            Updated context with sampled temp table
        """
        # Resolve the profiling target and its row count once.
        if context.temp_table:
            # Step 1 materialized a partition/query temp table. Sample THAT
            # table: context.row_count is the full-table count from the source,
            # not this partition, so measure the partition directly via
            # _get_quick_row_count (which targets the temp table).
            target = self.quote_identifier(
                f"{context.temp_schema}.{context.temp_table}"
            )
            row_count = self._get_quick_row_count(context, conn)
        elif context.table:
            # Prefer the row count the BigQuery source already knows
            # (INFORMATION_SCHEMA / __TABLES__); COUNT(*) only when it is
            # missing (e.g. views, external tables).
            target = self.quote_identifier(
                f"{context.schema}.{context.table}" if context.schema else context.table
            )
            row_count = context.row_count or self._get_quick_row_count(context, conn)
        else:
            return context

        if not row_count or row_count <= self.config.sample_size:
            return context

        # SQL Injection Safety: `target` is quote_identifier() output over
        # metadata / BigQuery-API identifiers (never user input); sample_pc is a
        # validated float in [0, 100]. We use an f-string here because
        # SQLAlchemy's tablesample() omits the PERCENT keyword BigQuery requires.
        sample_pc = max(0.0, min(100.0, 100 * self.config.sample_size / row_count))
        sql = f"SELECT * FROM {target} TABLESAMPLE SYSTEM ({sample_pc:.8f} PERCENT)"

        logger.debug(
            f"Creating sampled BigQuery temp table for {context.pretty_name}: {sql}"
        )

        # Materialize the sample into a cached temp table (reuse the query path).
        old_custom_sql = context.custom_sql
        context.custom_sql = sql
        context = self._create_temp_table_for_query(context)
        context.custom_sql = old_custom_sql  # Restore

        context.is_sampled = True
        context.sample_percentage = sample_pc

        return context

    # =========================================================================
    # SQL Expression Builders
    # =========================================================================

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        """BigQuery uses APPROX_COUNT_DISTINCT."""
        return sa.func.APPROX_COUNT_DISTINCT(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        """
        BigQuery median via APPROX_QUANTILES.

        APPROX_QUANTILES(column, 2) returns [min, median, max].
        We use OFFSET(1) to get the median value.

        Note: BigQuery's SQLAlchemy dialect doesn't support the [] operator on expressions,
        so we generate the full SQL string using literal_column().
        """
        # Column name is from database schema (validated), not user input
        # IMPORTANT: label() is required whenever using literal_column()
        # Without label(), the column name would be the entire SQL string, which breaks query combiner result extraction
        return sa.literal_column(f"APPROX_QUANTILES(`{column}`, 2)[OFFSET(1)]").label(
            "median"
        )

    def get_quantiles_expr(
        self, column: str, quantiles: List[float]
    ) -> Optional[ColumnElement[Any]]:
        """
        BigQuery quantiles via approx_quantiles.

        Returns array of quantile values.
        """
        num_quantiles = len(quantiles)
        return sa.func.approx_quantiles(sa.column(column), num_quantiles)

    def get_sample_clause(self, sample_size: int) -> Optional[str]:
        """BigQuery uses TABLESAMPLE SYSTEM."""
        return f"TABLESAMPLE SYSTEM ({sample_size} ROWS)"

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Connection,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        """
        Get quantile values for a column using BigQuery's approx_quantiles.

        BigQuery: approx_quantiles(col, 100) returns 101 values (0th to 100th percentile).
        We map quantiles to indices to extract the desired percentiles.

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

        # BigQuery: approx_quantiles(col, 100) returns 101 values
        indices = [int(q * 100) for q in quantiles]
        selects = [
            sa.literal_column(
                f"approx_quantiles(`{column}`, 100)[OFFSET({idx})]"
            ).label(f"q_{int(q * 100)}")
            for q, idx in zip(quantiles, indices, strict=False)
        ]
        query = sa.select(selects).select_from(table)
        result = conn.execute(query).fetchone()
        if result is None:
            return [None] * len(quantiles)
        return [float(v) if v is not None else None for v in result]
