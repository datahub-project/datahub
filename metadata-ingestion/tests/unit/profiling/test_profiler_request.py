from datahub.ingestion.source.profiling.common import ProfilerRequest


def test_profiler_request_importable_from_new_location():
    req = ProfilerRequest(pretty_name="my_table", batch_kwargs={"schema": "s"})
    assert req.pretty_name == "my_table"
    assert req.batch_kwargs == {"schema": "s"}
