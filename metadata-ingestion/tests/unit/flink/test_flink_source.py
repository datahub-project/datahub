import json
import pathlib
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-02-12 00:00:00"
TEST_RESOURCES_DIR = pathlib.Path(__file__).parent / "golden_files"


def _load_json(filename: str) -> dict:
    return json.loads((TEST_RESOURCES_DIR / filename).read_text())


def _mock_flink_api(path: str) -> dict:
    """Route mock API calls to appropriate golden file responses."""
    if path == "/v1/config":
        return _load_json("flink_cluster_config.json")
    elif path == "/v1/jobs/overview":
        return _load_json("flink_jobs_overview.json")
    elif path == "/v1/jobs/d4b4a1234567890abcdef1234567890a":
        return _load_json("flink_job_running.json")
    elif path == "/v1/jobs/e5c5b2345678901bcdef2345678901b":
        return _load_json("flink_job_finished.json")
    elif path == "/v1/jobs/f6d6c3456789012cdef3456789012c":
        # test_job is CANCELED and should be included
        return {
            "jid": "f6d6c3456789012cdef3456789012c",
            "name": "test_job",
            "state": "CANCELED",
            "start-time": 1707504000000,
            "end-time": 1707505000000,
            "duration": 1000000,
            "plan": {"nodes": []},
        }
    elif "checkpoints/config" in path:
        if "d4b4a1234567890abcdef1234567890a" in path:
            return _load_json("flink_checkpoint_running.json")
        elif "e5c5b2345678901bcdef2345678901b" in path:
            return _load_json("flink_checkpoint_finished.json")
        return {}
    return {}


@freeze_time(FROZEN_TIME)
def test_flink_source_golden(pytestconfig, tmp_path):
    """End-to-end golden file test with mocked Flink REST API."""
    output_file = tmp_path / "flink_mces.json"
    golden_file = TEST_RESOURCES_DIR / "flink_mces_golden.json"

    with patch(
        "datahub.ingestion.source.flink.flink_client.FlinkRestClient._get",
        side_effect=_mock_flink_api,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test",
                "source": {
                    "type": "flink",
                    "config": {
                        "rest_endpoint": "http://localhost:8081",
                        "include_lineage": True,
                        "include_run_history": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(output_file),
                    },
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_file),
        golden_path=str(golden_file),
    )


@freeze_time(FROZEN_TIME)
def test_flink_source_with_name_filter(tmp_path):
    """Test that job name filtering works correctly."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.flink_client.FlinkRestClient._get",
        side_effect=_mock_flink_api,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-filtered",
                "source": {
                    "type": "flink",
                    "config": {
                        "rest_endpoint": "http://localhost:8081",
                        "job_name_pattern": {
                            "allow": ["^fraud.*"],
                        },
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(output_file),
                    },
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()

    # Read output and check only fraud_detection job was emitted
    output_data = json.loads(output_file.read_text())
    entity_urns = {
        item.get("entityUrn", "") for item in output_data if "entityUrn" in item
    }
    # Should have fraud_detection DataFlow and DataJob
    assert any("fraud_detection" in urn for urn in entity_urns)
    # Should NOT have etl_pipeline or test_job
    assert not any("etl_pipeline" in urn for urn in entity_urns)
    assert not any("test_job" in urn for urn in entity_urns)
