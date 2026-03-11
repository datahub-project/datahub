import json
import pathlib
from typing import Any, Callable, Dict, Set
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.flink.source import FlinkSource

_FLINK_REST_CLIENT_GET = "datahub.ingestion.source.flink.client.FlinkRestClient._get"


def _mock_flink_api(path: str) -> dict:
    """Route mock API calls to golden file responses."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    if path == "/v1/jobs/overview":
        return {
            "jobs": [
                {
                    "jid": "abc123",
                    "name": "fraud_detection",
                    "state": "RUNNING",
                    "start-time": 1707676800000,
                    "end-time": -1,
                    "duration": 3600000,
                    "last-modification": 1707676800000,
                },
                {
                    "jid": "def456",
                    "name": "etl_pipeline",
                    "state": "FINISHED",
                    "start-time": 1707590400000,
                    "end-time": 1707594000000,
                    "duration": 3600000,
                },
            ]
        }
    if path == "/v1/jobs/abc123":
        return {
            "jid": "abc123",
            "name": "fraud_detection",
            "state": "RUNNING",
            "start-time": 1707676800000,
            "end-time": -1,
            "duration": 3600000,
            "job-type": "STREAMING",
            "maxParallelism": 128,
            "plan": {
                "nodes": [
                    {
                        "id": "1",
                        "description": "Source: KafkaSource-transactions -> Filter",
                        "operator": "KafkaSource",
                        "parallelism": 4,
                    },
                    {
                        "id": "2",
                        "description": "Sink: KafkaSink-alerts",
                        "operator": "KafkaSink",
                        "parallelism": 2,
                    },
                ]
            },
        }
    if path == "/v1/jobs/def456":
        return {
            "jid": "def456",
            "name": "etl_pipeline",
            "state": "FINISHED",
            "start-time": 1707590400000,
            "end-time": 1707594000000,
            "duration": 3600000,
            "plan": {
                "nodes": [
                    {
                        "id": "1",
                        "description": "Source: KafkaSource-orders -> Map",
                        "operator": "KafkaSource",
                        "parallelism": 2,
                    },
                    {
                        "id": "2",
                        "description": "Sink: KafkaSink-enriched-orders",
                        "operator": "KafkaSink",
                        "parallelism": 2,
                    },
                ]
            },
        }
    if "checkpoints/config" in path:
        return {
            "mode": "exactly_once",
            "interval": 60000,
            "timeout": 120000,
            "state_backend": "rocksdb",
        }
    return {}


def _create_pipeline(
    output_file: pathlib.Path,
    source_config: Dict[str, Any],
    run_id: str = "flink-test",
) -> Pipeline:
    config = {"connection": {"rest_api_url": "http://localhost:8081"}, **source_config}
    return Pipeline.create(
        {
            "run_id": run_id,
            "source": {"type": "flink", "config": config},
            "sink": {"type": "file", "config": {"filename": str(output_file)}},
        }
    )


def _collect_entity_urns(output_file: pathlib.Path) -> Set[str]:
    if not output_file.exists():
        return set()
    output = json.loads(output_file.read_text())
    return {item["entityUrn"] for item in output if "entityUrn" in item}


def _run_pipeline(
    tmp_path: pathlib.Path,
    mock_api: Callable[[str], dict],
    source_config: Dict[str, Any],
    *,
    expect_success: bool = True,
) -> Set[str]:
    """Run a Flink pipeline with a mocked REST client and return entity URNs."""
    output_file = tmp_path / "flink_mces.json"
    with patch(_FLINK_REST_CLIENT_GET, side_effect=mock_api):
        pipeline = _create_pipeline(output_file, source_config)
        pipeline.run()
        if expect_success:
            pipeline.raise_from_status()
    return _collect_entity_urns(output_file)


def test_flink_source_emits_dataflow_and_datajob(tmp_path: pathlib.Path) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api,
        {"include_lineage": True, "include_run_history": False},
    )
    assert any("dataFlow" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataFlow" in urn and "etl_pipeline" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "etl_pipeline" in urn for urn in entity_urns)


def test_flink_source_name_filter(tmp_path: pathlib.Path) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api,
        {"job_name_pattern": {"allow": ["^fraud.*"]}, "include_run_history": False},
    )
    assert any("fraud_detection" in urn for urn in entity_urns)
    assert not any("etl_pipeline" in urn for urn in entity_urns)


def test_flink_source_state_filter(tmp_path: pathlib.Path) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api,
        {"include_job_states": ["RUNNING"], "include_run_history": False},
    )
    assert any("fraud_detection" in urn for urn in entity_urns)
    assert not any("etl_pipeline" in urn for urn in entity_urns)


def _mock_flink_api_with_duplicate_jobs(path: str) -> dict:
    """Mock API with two jobs having the same name but different start times."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    if path == "/v1/jobs/overview":
        return {
            "jobs": [
                {
                    "jid": "old111",
                    "name": "my_job",
                    "state": "CANCELED",
                    "start-time": 1000000000000,
                    "end-time": 1000001000000,
                    "duration": 1000000,
                },
                {
                    "jid": "new222",
                    "name": "my_job",
                    "state": "RUNNING",
                    "start-time": 2000000000000,
                    "end-time": -1,
                    "duration": 500000,
                },
            ]
        }
    if path == "/v1/jobs/new222":
        return {
            "jid": "new222",
            "name": "my_job",
            "state": "RUNNING",
            "start-time": 2000000000000,
            "end-time": -1,
            "duration": 500000,
            "plan": {"nodes": []},
        }
    if "checkpoints/config" in path:
        return {}
    return {}


def test_flink_source_deduplicates_by_name(tmp_path: pathlib.Path) -> None:
    """When two jobs have the same name, keeps the most recently started."""
    output_file = tmp_path / "flink_mces.json"
    with patch(_FLINK_REST_CLIENT_GET, side_effect=_mock_flink_api_with_duplicate_jobs):
        pipeline = _create_pipeline(output_file, {"include_run_history": False})
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    dataflow_urns = {
        item["entityUrn"]
        for item in output
        if item.get("entityUrn", "").startswith("urn:li:dataFlow")
    }
    my_job_flows = {u for u in dataflow_urns if "my_job" in u}
    assert len(my_job_flows) == 1

    custom_props = [
        item["aspect"]["json"]["customProperties"]
        for item in output
        if item.get("aspectName") == "dataFlowInfo"
        and "my_job" in item.get("entityUrn", "")
    ]
    assert len(custom_props) == 1
    assert custom_props[0]["job_state"] == "RUNNING"


def test_flink_source_vertex_granularity(tmp_path: pathlib.Path) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api,
        {
            "operator_granularity": "vertex",
            "include_run_history": False,
            "include_job_states": ["RUNNING"],
        },
    )
    fraud_datajobs = {
        u for u in entity_urns if "dataJob" in u and "fraud_detection" in u
    }
    assert len(fraud_datajobs) == 2


def test_flink_source_dpi_failure_preserves_job_entities(
    tmp_path: pathlib.Path,
) -> None:
    """DPI construction failure does not lose DataFlow/DataJob entities."""
    output_file = tmp_path / "flink_mces.json"
    with (
        patch(_FLINK_REST_CLIENT_GET, side_effect=_mock_flink_api),
        patch(
            "datahub.ingestion.source.flink.entities.FlinkEntityBuilder.build_dpi_workunits",
            side_effect=RuntimeError("DPI broke"),
        ),
    ):
        pipeline = _create_pipeline(
            output_file,
            {"include_run_history": True, "include_job_states": ["RUNNING"]},
        )
        pipeline.run()
        pipeline.raise_from_status()

    entity_urns = _collect_entity_urns(output_file)
    assert any("dataFlow" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert not any("dataProcessInstance" in urn for urn in entity_urns)


def test_flink_source_lineage_failure_preserves_job_entities(
    tmp_path: pathlib.Path,
) -> None:
    """Lineage extraction failure does not lose DataFlow/DataJob entities."""
    output_file = tmp_path / "flink_mces.json"
    with (
        patch(_FLINK_REST_CLIENT_GET, side_effect=_mock_flink_api),
        patch(
            "datahub.ingestion.source.flink.lineage.FlinkLineageOrchestrator.extract",
            side_effect=RuntimeError("Lineage broke"),
        ),
    ):
        pipeline = _create_pipeline(
            output_file,
            {
                "include_lineage": True,
                "include_run_history": False,
                "include_job_states": ["RUNNING"],
            },
        )
        pipeline.run()
        pipeline.raise_from_status()

    entity_urns = _collect_entity_urns(output_file)
    assert any("dataFlow" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "fraud_detection" in urn for urn in entity_urns)


def _mock_flink_api_job_detail_fails(path: str) -> dict:
    """Mock where get_job_details fails for one of two jobs."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    if path == "/v1/jobs/overview":
        return {
            "jobs": [
                {
                    "jid": "abc123",
                    "name": "good_job",
                    "state": "RUNNING",
                    "start-time": 1707676800000,
                    "end-time": -1,
                    "duration": 3600000,
                },
                {
                    "jid": "fail999",
                    "name": "bad_job",
                    "state": "RUNNING",
                    "start-time": 1707676800000,
                    "end-time": -1,
                    "duration": 3600000,
                },
            ]
        }
    if path == "/v1/jobs/abc123":
        return {
            "jid": "abc123",
            "name": "good_job",
            "state": "RUNNING",
            "start-time": 1707676800000,
            "end-time": -1,
            "duration": 3600000,
            "plan": {"nodes": []},
        }
    if path == "/v1/jobs/fail999":
        raise RuntimeError("Flink API returned 500")
    if "checkpoints/config" in path:
        return {}
    return {}


def test_flink_source_job_detail_failure_continues_other_jobs(
    tmp_path: pathlib.Path,
) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api_job_detail_fails,
        {"include_run_history": False},
    )
    assert any("good_job" in urn for urn in entity_urns)


def test_flink_source_cluster_connect_failure(tmp_path: pathlib.Path) -> None:
    def _fail(path: str) -> dict:
        raise RuntimeError("Connection refused")

    entity_urns = _run_pipeline(
        tmp_path, _fail, {"include_run_history": False}, expect_success=False
    )
    assert not entity_urns


def test_include_run_history_false_suppresses_dpis(tmp_path: pathlib.Path) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api,
        {"include_run_history": False, "include_job_states": ["RUNNING"]},
    )
    assert any("dataFlow" in urn for urn in entity_urns)
    assert any("dataJob" in urn for urn in entity_urns)
    assert not any("dataProcessInstance" in urn for urn in entity_urns)


def test_include_lineage_false_suppresses_dataset_materialization(
    tmp_path: pathlib.Path,
) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api,
        {
            "include_lineage": False,
            "include_run_history": False,
            "include_job_states": ["RUNNING"],
        },
    )
    assert any("dataFlow" in urn for urn in entity_urns)
    assert any("dataJob" in urn for urn in entity_urns)
    assert not any("dataPlatform:kafka" in urn for urn in entity_urns)


def _mock_flink_api_jobs_overview_fails(path: str) -> dict:
    """Mock where cluster config succeeds but jobs overview fails."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    if path == "/v1/jobs/overview":
        raise RuntimeError("Failed to list jobs")
    return {}


def test_flink_source_jobs_overview_failure(tmp_path: pathlib.Path) -> None:
    entity_urns = _run_pipeline(
        tmp_path,
        _mock_flink_api_jobs_overview_fails,
        {"include_run_history": False},
        expect_success=False,
    )
    assert not entity_urns


class TestTestConnection:
    def test_reports_success_when_cluster_reachable(self) -> None:
        def mock_api(path: str) -> dict:
            if path == "/v1/config":
                return {"flink-version": "1.19.0"}
            if path == "/v1/jobs/overview":
                return {"jobs": []}
            return {}

        with patch(_FLINK_REST_CLIENT_GET, side_effect=mock_api):
            report = FlinkSource.test_connection(
                {"connection": {"rest_api_url": "http://localhost:8081"}}
            )
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True
        assert report.capability_report is not None

    def test_reports_failure_when_cluster_unreachable(self) -> None:
        with patch(
            _FLINK_REST_CLIENT_GET,
            side_effect=RuntimeError("Connection refused"),
        ):
            report = FlinkSource.test_connection(
                {"connection": {"rest_api_url": "http://localhost:8081"}}
            )
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
