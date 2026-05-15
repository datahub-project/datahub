def test_profiler_request_importable_from_new_location():
    from datahub.ingestion.source.profiling.common import ProfilerRequest

    req = ProfilerRequest(pretty_name="my_table", batch_kwargs={"schema": "s"})
    assert req.pretty_name == "my_table"
    assert req.batch_kwargs == {"schema": "s"}


def test_ge_data_profiler_reexports_for_backcompat():
    from datahub.ingestion.source.ge_data_profiler import (
        GEProfilerRequest,
        ProfilerRequest,
    )
    from datahub.ingestion.source.profiling.common import (
        ProfilerRequest as NewProfilerRequest,
    )

    assert GEProfilerRequest is NewProfilerRequest
    assert ProfilerRequest is NewProfilerRequest
