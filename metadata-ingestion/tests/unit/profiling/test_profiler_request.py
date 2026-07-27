import pytest

from datahub.ingestion.source.profiling.common import ProfilerRequest


def test_profiler_request_importable_from_new_location():
    req = ProfilerRequest(pretty_name="my_table", batch_kwargs={"schema": "s"})
    assert req.pretty_name == "my_table"
    assert req.batch_kwargs == {"schema": "s"}


def test_ge_data_profiler_reexports_for_backcompat():
    # The back-compat aliases live on ge_data_profiler.py, which has top-level
    # great_expectations imports. Skip the assertion entirely when GE isn't
    # installed (the back-compat shim is irrelevant in that environment).
    pytest.importorskip("great_expectations")

    # Late import is justified: ge_data_profiler imports great_expectations at
    # module load, so we can only attempt it AFTER importorskip confirms GE is
    # available.
    from datahub.ingestion.source.ge_data_profiler import (
        GEProfilerRequest,
        ProfilerRequest as LegacyProfilerRequest,
    )

    assert GEProfilerRequest is ProfilerRequest
    assert LegacyProfilerRequest is ProfilerRequest
