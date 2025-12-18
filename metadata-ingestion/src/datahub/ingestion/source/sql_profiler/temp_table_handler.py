"""Temporary table and view handling for custom SQL and partitions."""

import logging
import uuid
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from datahub.ingestion.source.sql_profiler.datahub_sql_profiler import (
        DatahubSQLProfiler,
    )

logger: logging.Logger = logging.getLogger(__name__)


def create_athena_temp_table(
    instance: "DatahubSQLProfiler",
    sql: str,
    table_pretty_name: str,
    raw_connection: Any,
) -> Optional[str]:
    """Create a temporary view in Athena for custom SQL queries."""
    try:
        from pyathena.cursor import Cursor

        cursor: "Cursor" = raw_connection.cursor()
        logger.debug(f"Creating view for {table_pretty_name}: {sql}")
        temp_view = f"ge_{uuid.uuid4().hex[:8]}"
        if "." in table_pretty_name:
            schema_part = table_pretty_name.split(".")[-1]
            schema_part_quoted = ".".join(
                [f'"{part}"' for part in str(schema_part).split(".")]
            )
            temp_view = f"{schema_part_quoted}_{temp_view}"

        temp_view = f"ge_{uuid.uuid4().hex[:8]}"
        cursor.execute(f'create or replace view "{temp_view}" as {sql}')
        return temp_view
    except Exception as e:
        if not instance.config.catch_exceptions:
            raise e
        logger.exception(f"Encountered exception while profiling {table_pretty_name}")
        instance.report.report_warning(
            table_pretty_name,
            f"Profiling exception {e} when running custom sql {sql}",
        )
        return None


def create_bigquery_temp_table(
    instance: "DatahubSQLProfiler",
    bq_sql: str,
    table_pretty_name: str,
    raw_connection: Any,
) -> Optional[str]:
    """
    Create a temporary table in BigQuery using cached results.

    On BigQuery, we need to bypass GE's mechanism for creating temporary tables because
    it requires create/delete table permissions. We (ab)use the BigQuery cached results
    feature to avoid creating temporary tables ourselves.

    For almost all queries, BigQuery will store the results in a temporary, cached results
    table when an explicit destination table is not provided. These tables are per-user
    and per-project, so there's no risk of permissions escalation.

    Risks:
    1. If the query results are larger than the maximum response size, BigQuery will
       not cache the results (max 10 GB compressed).
    2. The cache lifetime of 24 hours is "best-effort" and hence not guaranteed.
    3. Tables with column-level security may not be cached, and tables with row-level
       security will not be cached.
    4. BigQuery "discourages" using cached results directly, but notes that
       the current semantics do allow it.
    """
    try:
        import google.cloud.bigquery.job.query
        from google.cloud.bigquery.dbapi.cursor import Cursor as BigQueryCursor

        cursor: "BigQueryCursor" = raw_connection.cursor()
        try:
            logger.debug(f"Creating temporary table for {table_pretty_name}: {bq_sql}")
            cursor.execute(bq_sql)
        except Exception as e:
            if not instance.config.catch_exceptions:
                raise e
            logger.exception(
                f"Encountered exception while profiling {table_pretty_name}"
            )
            instance.report.report_warning(
                table_pretty_name,
                f"Profiling exception {e} when running custom sql {bq_sql}",
            )
            return None

        # Extract the name of the cached results table from the query job
        query_job: Optional["google.cloud.bigquery.job.query.QueryJob"] = (
            # In google-cloud-bigquery 3.15.0, the _query_job attribute was
            # made public and renamed to query_job.
            cursor.query_job if hasattr(cursor, "query_job") else cursor._query_job  # type: ignore[attr-defined]
        )
        if query_job is None:
            return None

        temp_destination_table = query_job.destination
        bigquery_temp_table = (
            f"{temp_destination_table.project}."
            f"{temp_destination_table.dataset_id}."
            f"{temp_destination_table.table_id}"
        )
        return bigquery_temp_table
    except Exception as e:
        if not instance.config.catch_exceptions:
            raise e
        logger.exception(f"Failed to create BigQuery temp table: {e}")
        return None
    finally:
        raw_connection.close()


def drop_temp_table(
    instance: "DatahubSQLProfiler",
    table_name: str,
    schema: Optional[str] = None,
) -> None:
    """Drop a temporary table or view."""
    try:
        with instance.base_engine.connect() as connection:
            full_table_name = (
                f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
            )
            connection.execute(f"drop view if exists {full_table_name}")
            logger.debug(f"View {full_table_name} was dropped.")
    except Exception as e:
        logger.warning(f"Unable to delete temporary table {table_name}: {e}")
