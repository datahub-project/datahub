import json
import pathlib
from typing import Any, Dict, List, Optional, Set
from unittest.mock import patch

import pytest
import requests
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2024-01-01 12:00:00"
BASE_URL = "https://api.hightouch.com/api/v1"
API_KEY = "test_api_key"
TEST_DIR = pathlib.Path(__file__).parent
GMS_SERVER = "http://localhost:8080"


def _paged(items: List[Dict[str, Any]], *, with_has_more: bool) -> Any:
    """Serve a list endpoint honouring limit/offset. Only /syncs and
    /syncs/{id}/runs carry hasMore in Hightouch's API; the rest return just data."""

    def callback(request: Any, context: Any) -> Dict[str, Any]:
        offset = int(request.qs.get("offset", ["0"])[0])
        limit = int(request.qs.get("limit", [str(len(items) or 1)])[0])
        body: Dict[str, Any] = {"data": items[offset : offset + limit]}
        if with_has_more:
            body["hasMore"] = offset + limit < len(items)
        return body

    return callback


def load_responses() -> Dict[str, Any]:
    return json.loads((TEST_DIR / "hightouch_api_responses.json").read_text())


def register_hightouch_api(
    mocker: requests_mock.Mocker, responses: Optional[Dict[str, Any]] = None
) -> None:
    responses = responses if responses is not None else load_responses()

    for endpoint in ("sources", "models", "destinations"):
        mocker.get(
            f"{BASE_URL}/{endpoint}",
            json=_paged(responses[endpoint], with_has_more=False),
        )
        for entity in responses[endpoint]:
            mocker.get(f"{BASE_URL}/{endpoint}/{entity['id']}", json=entity)

    mocker.get(
        f"{BASE_URL}/events/contracts",
        json=_paged(responses["contracts"], with_has_more=False),
    )
    mocker.get(
        f"{BASE_URL}/syncs",
        json=_paged(responses["syncs"], with_has_more=True),
    )
    for sync_id, runs in responses["sync_runs"].items():
        mocker.get(
            f"{BASE_URL}/syncs/{sync_id}/runs",
            json=_paged(runs, with_has_more=True),
        )


def _run_pipeline(tmp_path: pathlib.Path, output: str, config: Dict[str, Any]) -> str:
    output_path = str(tmp_path / output)
    pipeline = Pipeline.create(
        {
            "run_id": "hightouch-test",
            "source": {
                "type": "hightouch",
                "config": {
                    "api_config": {"api_key": API_KEY, "base_url": BASE_URL},
                    "env": "PROD",
                    **config,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    report = pipeline.source.get_report()
    assert not report.warnings, list(report.warnings)
    assert not report.failures, list(report.failures)
    return output_path


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_hightouch_source_basic(pytestconfig, tmp_path, requests_mock):
    register_hightouch_api(requests_mock)

    output_path = _run_pipeline(
        tmp_path,
        "hightouch_mces.json",
        {
            "emit_models_as_datasets": True,
            "include_sync_runs": True,
            "max_sync_runs_per_sync": 5,
            "parse_model_sql": True,
            "include_contracts": True,
            "include_sibling_relationships": False,
            # Hightouch ids are numbers in the API; the recipe keys them as strings.
            "destination_to_platform_instance": {
                "301": {"platform_instance": "prod-salesforce"}
            },
        },
    )

    # The pipeline may also probe a DataHub server configured in ~/.datahubenv.
    history = [r for r in requests_mock.request_history if r.url.startswith(BASE_URL)]
    assert all(r.headers["Authorization"] == f"Bearer {API_KEY}" for r in history)
    run_requests = [r for r in history if r.path.endswith("/runs")]
    assert len(run_requests) == 3
    assert all(r.qs["limit"] == ["5"] for r in run_requests)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=TEST_DIR / "hightouch_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_hightouch_source_with_patterns(pytestconfig, tmp_path, requests_mock):
    register_hightouch_api(requests_mock)

    output_path = _run_pipeline(
        tmp_path,
        "hightouch_filtered_mces.json",
        {
            "emit_models_as_datasets": True,
            "include_sync_runs": False,
            "include_contracts": False,
            "include_sibling_relationships": False,
            "sync_patterns": {
                "allow": ["product-events-.*"],
                "deny": [".*-contacts$"],
            },
            "model_patterns": {"deny": ["Active Customers"]},
        },
    )

    assert not any(
        r.path.endswith("/runs") or r.path.endswith("/events/contracts")
        for r in requests_mock.request_history
        if r.url.startswith(BASE_URL)
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=TEST_DIR / "hightouch_filtered_mces_golden.json",
    )


def _stateful_config(sink: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "run_id": "hightouch-stateful",
        "pipeline_name": "hightouch-stateful",
        "source": {
            "type": "hightouch",
            "config": {
                "api_config": {"api_key": API_KEY, "base_url": BASE_URL},
                "env": "PROD",
                "include_sibling_relationships": False,
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100.0,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            },
        },
        "sink": sink,
    }


def _soft_deleted_urns(output_path: str) -> Set[str]:
    return {
        record["entityUrn"]
        for record in json.loads(pathlib.Path(output_path).read_text())
        if record.get("aspectName") == "status"
        and record["aspect"]["json"].get("removed")
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_hightouch_stateful_removes_deleted_syncs_and_models(
    tmp_path, requests_mock, mock_datahub_graph
):
    register_hightouch_api(requests_mock)

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline_run1 = run_and_get_pipeline(_stateful_config({"type": "console"}))
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
        assert checkpoint1 and checkpoint1.state

        # Sync 403 and the model only it used are deleted in Hightouch.
        responses = load_responses()
        responses["syncs"] = [s for s in responses["syncs"] if s["id"] != 403]
        responses["models"] = [m for m in responses["models"] if m["id"] != 202]
        register_hightouch_api(requests_mock, responses)

        output_path = str(tmp_path / "hightouch_stateful.json")
        pipeline_run2 = run_and_get_pipeline(
            _stateful_config({"type": "file", "config": {"filename": output_path}})
        )
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
        assert checkpoint2 and checkpoint2.state

        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

    assert _soft_deleted_urns(output_path) == {
        "urn:li:dataFlow:(hightouch,403,PROD)",
        "urn:li:dataJob:(urn:li:dataFlow:(hightouch,403,PROD),403)",
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,active-customers,PROD)",
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_hightouch_stateful_keeps_contracts_it_could_not_read(
    tmp_path, requests_mock, mock_datahub_graph
):
    register_hightouch_api(requests_mock)

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        run_and_get_pipeline(_stateful_config({"type": "console"}))

        # The contracts endpoint is unreachable on the next run.
        requests_mock.get(
            f"{BASE_URL}/events/contracts", exc=requests.exceptions.ConnectionError
        )
        output_path = str(tmp_path / "hightouch_stateful_failed.json")
        pipeline = Pipeline.create(
            _stateful_config({"type": "file", "config": {"filename": output_path}})
        )
        pipeline.run()

    # The failure keeps stale-entity removal from soft-deleting the contracts.
    assert _soft_deleted_urns(output_path) == set()
    report = pipeline.source.get_report()
    assert [f.title for f in report.failures] == ["Could not fetch Event Contracts"]
