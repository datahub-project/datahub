from datahub.ingestion.source import ge_data_profiler
from datahub.ingestion.source.sql.doris import (
    _get_column_types_to_ignore_with_doris,
    _patch_profiler,
)


def test_profiler_patch_idempotence():
    """Verifies profiler patch is idempotent (safe to call multiple times)."""
    _patch_profiler()
    first_fn = ge_data_profiler._get_column_types_to_ignore

    assert first_fn is _get_column_types_to_ignore_with_doris

    for _ in range(10):
        _patch_profiler()

    assert ge_data_profiler._get_column_types_to_ignore is first_fn
