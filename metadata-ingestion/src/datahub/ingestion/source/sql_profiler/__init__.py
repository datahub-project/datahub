"""Custom SQLAlchemy-based profiler to replace Great Expectations dependency."""

from datahub.ingestion.source.sql_profiler.datahub_sql_profiler import (
    DatahubSQLProfiler,
)

__all__ = ["DatahubSQLProfiler"]
