"""Helpers for loading the optional Great Expectations profiler.

This module is intentionally GE-free so it can be imported by any code path
that might run in an environment where `great_expectations` is not installed.
"""

GE_PROFILER_MISSING_MESSAGE = (
    "The Great Expectations profiler is not installed. Either install "
    "the optional dependency with `pip install 'acryl-datahub[profiling-ge]'`, "
    "or switch to the SQLAlchemy profiler by setting "
    "`profiling.method: sqlalchemy` in your recipe."
)
