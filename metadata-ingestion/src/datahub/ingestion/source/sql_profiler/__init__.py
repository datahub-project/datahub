"""Custom SQLAlchemy-based profiler to replace Great Expectations dependency."""

from datahub.ingestion.source.sql_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)

__all__ = ["SQLAlchemyProfiler"]
