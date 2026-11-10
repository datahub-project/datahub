import json
import pathlib
from typing import Any, Dict, List

import pytest
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 12:00:00"
BASE_URL = "https://api.hightouch.com/api/v1"
API_KEY = "test_api_key"
TEST_DIR = pathlib.Path(__file__).parent


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


def register_hightouch_api(mocker: requests_mock.Mocker) -> None:
    responses = json.loads((TEST_DIR / "hightouch_api_responses.json").read_text())

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
