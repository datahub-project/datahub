"""Performance and behavior tests for Doris connector."""

from datahub.ingestion.source import ge_data_profiler
from datahub.ingestion.source.sql.doris import (
    _get_column_types_to_ignore_with_doris,
    _patch_profiler,
)


def test_profiler_patch_idempotence():
    """Verify profiler patch can be called multiple times safely (idempotent)."""
    # Patch profiler
    _patch_profiler()
    first_fn = ge_data_profiler._get_column_types_to_ignore

    # Verify it's using our wrapped function
    assert first_fn is _get_column_types_to_ignore_with_doris

    # Call patch 10 more times
    for _ in range(10):
        _patch_profiler()

    # Should still be the same function (idempotent)
    assert ge_data_profiler._get_column_types_to_ignore is first_fn
