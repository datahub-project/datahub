import pytest

from datahub.ingestion.source.matillion_dpc.matillion_utils import (
    extract_folder_segments,
    extract_pipeline_file_name,
)


@pytest.mark.parametrize(
    "job_name,expected",
    [
        (
            "ingest/staging/orders/load.orch.yaml",
            ["ingest", "staging", "orders"],
        ),
        ("folder/pipeline.tran.yaml", ["folder"]),
        ("pipeline.orch.yaml", []),
        ("pipeline", []),
    ],
)
def test_extract_folder_segments(job_name: str, expected: list) -> None:
    assert extract_folder_segments(job_name) == expected


@pytest.mark.parametrize(
    "job_name,expected",
    [
        ("ingest/staging/orders/load.orch.yaml", "load.orch.yaml"),
        ("folder/pipeline.tran.yaml", "pipeline.tran.yaml"),
        ("pipeline.orch.yaml", "pipeline.orch.yaml"),
    ],
)
def test_extract_pipeline_file_name(job_name: str, expected: str) -> None:
    assert extract_pipeline_file_name(job_name) == expected
