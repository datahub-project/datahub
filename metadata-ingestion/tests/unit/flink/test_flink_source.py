import json
import pathlib
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline


def _mock_flink_api(path: str) -> dict:
    """Route mock API calls to golden file responses."""
    if path == "/v1/config":
        return {
            "flink-version": "1.19.0",
            "timezone-name": "UTC",
        }
    elif path == "/v1/jobs/overview":
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
    elif path == "/v1/jobs/abc123":
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
    elif path == "/v1/jobs/def456":
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
    elif "checkpoints/config" in path:
        return {
            "mode": "exactly_once",
            "interval": 60000,
            "timeout": 120000,
            "state_backend": "rocksdb",
        }
    return {}


def test_flink_source_emits_dataflow_and_datajob(tmp_path: pathlib.Path) -> None:
    """Pipeline test: verifies DataFlow and DataJob entities are emitted."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_lineage": True,
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    entity_urns = {item.get("entityUrn", "") for item in output if "entityUrn" in item}

    # Should have DataFlow and DataJob for both jobs
    assert any("dataFlow" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataFlow" in urn and "etl_pipeline" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "etl_pipeline" in urn for urn in entity_urns)


def test_flink_source_name_filter(tmp_path: pathlib.Path) -> None:
    """Job name pattern filters correctly."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-filtered",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "job_name_pattern": {"allow": ["^fraud.*"]},
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    entity_urns = {item.get("entityUrn", "") for item in output if "entityUrn" in item}

    assert any("fraud_detection" in urn for urn in entity_urns)
    assert not any("etl_pipeline" in urn for urn in entity_urns)


def test_flink_source_state_filter(tmp_path: pathlib.Path) -> None:
    """Job state filtering excludes non-matching states."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-state",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_job_states": ["RUNNING"],
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    entity_urns = {item.get("entityUrn", "") for item in output if "entityUrn" in item}

    # fraud_detection is RUNNING — should be included
    assert any("fraud_detection" in urn for urn in entity_urns)
    # etl_pipeline is FINISHED — should be excluded
    assert not any("etl_pipeline" in urn for urn in entity_urns)


def _mock_flink_api_with_duplicate_jobs(path: str) -> dict:
    """Mock API with two jobs having the same name but different start times."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    elif path == "/v1/jobs/overview":
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
    elif path == "/v1/jobs/new222":
        return {
            "jid": "new222",
            "name": "my_job",
            "state": "RUNNING",
            "start-time": 2000000000000,
            "end-time": -1,
            "duration": 500000,
            "plan": {"nodes": []},
        }
    elif "checkpoints/config" in path:
        return {}
    return {}


def test_flink_source_deduplicates_by_name(tmp_path: pathlib.Path) -> None:
    """When two jobs have the same name, keeps the most recently started."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api_with_duplicate_jobs,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-dedup",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    # Should have exactly one DataFlow for "my_job" (not two)
    dataflow_urns = {
        item.get("entityUrn", "")
        for item in output
        if "entityUrn" in item
        and item.get("entityUrn", "").startswith("urn:li:dataFlow")
    }
    my_job_flows = {u for u in dataflow_urns if "my_job" in u}
    assert len(my_job_flows) == 1
    # Verify the CORRECT job was kept (new222 with later start-time, not old111)
    custom_props = [
        item["aspect"]["json"]["customProperties"]
        for item in output
        if item.get("aspectName") == "dataFlowInfo"
        and "my_job" in item.get("entityUrn", "")
    ]
    assert len(custom_props) == 1
    assert custom_props[0]["job_state"] == "RUNNING"


def test_flink_source_vertex_granularity(tmp_path: pathlib.Path) -> None:
    """operator_granularity='vertex' creates one DataJob per plan node."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-vertex",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "operator_granularity": "vertex",
                        "include_run_history": False,
                        "include_job_states": ["RUNNING"],
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    datajob_urns = {
        item.get("entityUrn", "")
        for item in output
        if "entityUrn" in item and "dataJob" in item.get("entityUrn", "")
    }
    # fraud_detection has 2 plan nodes → 2 DataJobs in vertex mode
    fraud_datajobs = {u for u in datajob_urns if "fraud_detection" in u}
    assert len(fraud_datajobs) == 2


def test_flink_source_dpi_failure_preserves_job_entities(
    tmp_path: pathlib.Path,
) -> None:
    """DPI construction failure does not lose DataFlow/DataJob entities."""
    output_file = tmp_path / "flink_mces.json"

    with (
        patch(
            "datahub.ingestion.source.flink.client.FlinkRestClient._get",
            side_effect=_mock_flink_api,
        ),
        patch(
            "datahub.ingestion.source.flink.entities.FlinkEntityBuilder.build_dpi_workunits",
            side_effect=RuntimeError("DPI broke"),
        ),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-dpi-fail",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_run_history": True,
                        "include_job_states": ["RUNNING"],
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    entity_urns = {item.get("entityUrn", "") for item in output if "entityUrn" in item}
    # DataFlow and DataJob should still be emitted despite DPI failure
    assert any("dataFlow" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "fraud_detection" in urn for urn in entity_urns)
    # DPI should NOT be emitted
    assert not any("dataProcessInstance" in urn for urn in entity_urns)


def test_flink_source_lineage_failure_preserves_job_entities(
    tmp_path: pathlib.Path,
) -> None:
    """Lineage extraction failure does not lose DataFlow/DataJob entities."""
    output_file = tmp_path / "flink_mces.json"

    with (
        patch(
            "datahub.ingestion.source.flink.client.FlinkRestClient._get",
            side_effect=_mock_flink_api,
        ),
        patch(
            "datahub.ingestion.source.flink.lineage.FlinkLineageOrchestrator.extract",
            side_effect=RuntimeError("Lineage broke"),
        ),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-lineage-fail",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_lineage": True,
                        "include_run_history": False,
                        "include_job_states": ["RUNNING"],
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    entity_urns = {item.get("entityUrn", "") for item in output if "entityUrn" in item}
    # DataFlow and DataJob should still be emitted despite lineage failure
    assert any("dataFlow" in urn and "fraud_detection" in urn for urn in entity_urns)
    assert any("dataJob" in urn and "fraud_detection" in urn for urn in entity_urns)


def _mock_flink_api_job_detail_fails(path: str) -> dict:
    """Mock where get_cluster_config and get_jobs_overview succeed
    but get_job_details fails for one job."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    elif path == "/v1/jobs/overview":
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
    elif path == "/v1/jobs/abc123":
        return {
            "jid": "abc123",
            "name": "good_job",
            "state": "RUNNING",
            "start-time": 1707676800000,
            "end-time": -1,
            "duration": 3600000,
            "plan": {"nodes": []},
        }
    elif path == "/v1/jobs/fail999":
        raise RuntimeError("Flink API returned 500")
    elif "checkpoints/config" in path:
        return {}
    return {}


def test_flink_source_job_detail_failure_continues_other_jobs(
    tmp_path: pathlib.Path,
) -> None:
    """A single job's get_job_details failure does not prevent other jobs from being ingested."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api_job_detail_fails,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-thread-fail",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    output = json.loads(output_file.read_text())
    entity_urns = {item.get("entityUrn", "") for item in output if "entityUrn" in item}
    # good_job should be emitted despite bad_job failing
    assert any("good_job" in urn for urn in entity_urns)


def test_flink_source_cluster_connect_failure(tmp_path: pathlib.Path) -> None:
    """Cluster connection failure reports failure and emits no workunits."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=RuntimeError("Connection refused"),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-connect-fail",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()

    # Should produce an empty output file (or file with no entityUrns)
    if output_file.exists():
        output = json.loads(output_file.read_text())
        entity_urns = {
            item.get("entityUrn", "") for item in output if "entityUrn" in item
        }
        assert len(entity_urns) == 0


def _mock_flink_api_jobs_overview_fails(path: str) -> dict:
    """Mock where cluster config succeeds but jobs overview fails."""
    if path == "/v1/config":
        return {"flink-version": "1.19.0", "timezone-name": "UTC"}
    elif path == "/v1/jobs/overview":
        raise RuntimeError("Failed to list jobs")
    return {}


def test_flink_source_jobs_overview_failure(tmp_path: pathlib.Path) -> None:
    """Jobs overview failure reports failure and emits no workunits."""
    output_file = tmp_path / "flink_mces.json"

    with patch(
        "datahub.ingestion.source.flink.client.FlinkRestClient._get",
        side_effect=_mock_flink_api_jobs_overview_fails,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "flink-test-overview-fail",
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "include_run_history": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            }
        )
        pipeline.run()

    if output_file.exists():
        output = json.loads(output_file.read_text())
        entity_urns = {
            item.get("entityUrn", "") for item in output if "entityUrn" in item
        }
        assert len(entity_urns) == 0
