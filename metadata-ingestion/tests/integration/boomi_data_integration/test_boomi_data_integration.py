import json
from pathlib import Path
from typing import Any, Dict, Optional, Set
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2026-11-03 12:00:00"
ACCOUNT_ID = "acct000000000000000000001"
BASE_URL = f"https://api.rivery.io/v1/accounts/{ACCOUNT_ID}"
GOLDEN_DIR = Path(__file__).parent / "golden"
# Production responses are anonymised captures from a live trial account.
# Development responses are synthetic, built from the same shapes, for cases
# the API cannot create: actions rivers, rivers made in the console (whose GET
# returns 400), loop containers, and other target types. The three failed logic
# runs are live; the other runs are synthetic: a multi-table run, a canceled
# run, a run still in progress, and a river run triggered by a logic run.
RESPONSES_DIR = Path(__file__).parent / "api_responses"

ENV_PROD = "e00000000000000000000001"
ENV_DEV = "e00000000000000000000002"
ACTIONS_RIVER = "a00000000000000000000004"
BIGQUERY_CONNECTION = "6ae8eee7c23a3443ebd7abe4"
POSTGRES_CONNECTION = "a00000000000000000000092"


def _load(name: str) -> Dict[str, Any]:
    return json.loads((RESPONSES_DIR / name).read_text())


def _without_river(page: Dict[str, Any], river_id: str) -> Dict[str, Any]:
    items = [r for r in page["items"] if r["river_cross_id"] != river_id]
    return {**page, "items": items, "current_page_size": len(items)}


def _register_mocks(
    mock: requests_mock.Mocker, deleted_river: Optional[str] = None
) -> None:
    mock.get(f"{BASE_URL}/environments", json=_load("environments.json"))
    mock.get(
        f"{BASE_URL}/environments/{ENV_PROD}",
        json=_load("environment_production.json"),
    )
    mock.get(
        f"{BASE_URL}/environments/{ENV_DEV}",
        json=_load("environment_development.json"),
    )
    pages = {
        f"{ENV_PROD}/rivers?page=1": _load("rivers_production_page_1.json"),
        f"{ENV_PROD}/rivers?page=2": _load("rivers_production_page_2.json"),
        f"{ENV_DEV}/rivers?page=1": _load("rivers_development.json"),
    }
    for path, page in pages.items():
        if deleted_river:
            page = _without_river(page, deleted_river)
        mock.get(f"{BASE_URL}/environments/{path}", json=page)
        environment_id = path.split("/")[0]
        for summary in page["items"]:
            river_id = summary["river_cross_id"]
            url = f"{BASE_URL}/environments/{environment_id}/rivers/{river_id}"
            definition = RESPONSES_DIR / f"river_{river_id}.json"
            if definition.exists():
                mock.get(url, json=_load(definition.name))
            else:
                mock.get(url, status_code=400, json=_load("river_get_400.json"))
            runs = RESPONSES_DIR / f"runs_{river_id}.json"
            if runs.exists():
                mock.get(f"{url}/runs", json=_load(runs.name))
    for environment_id, name in [
        (ENV_PROD, "activities_production.json"),
        (ENV_DEV, "activities_development.json"),
    ]:
        mock.get(
            f"{BASE_URL}/environments/{environment_id}/activities", json=_load(name)
        )


def _recipe(output_path: Path) -> Dict[str, object]:
    return {
        "run_id": "boomi-di-test",
        "source": {
            "type": "boomi-data-integration",
            "config": {
                "account_id": ACCOUNT_ID,
                "environment_tokens": [
                    {"environment_id": ENV_PROD, "api_token": "token-prod"},
                    {"environment_id": ENV_DEV, "api_token": "token-dev"},
                ],
                "river_pattern": {"deny": ["^tmp_.*"]},
                "connection_to_platform": {
                    BIGQUERY_CONNECTION: {
                        "platform": "bigquery",
                        "database": "example-project-123456",
                    },
                    POSTGRES_CONNECTION: {
                        "platform": "postgres",
                        "platform_instance": "dw",
                        "database": "warehouse",
                        "convert_urns_to_lowercase": False,
                    },
                },
                "max_requests_per_second": 50,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_data_integration_ingestion(
    pytestconfig: pytest.Config, tmp_path: Path
) -> None:
    output_path = tmp_path / "boomi_di_mces.json"
    with requests_mock.Mocker() as mock:
        _register_mocks(mock)
        pipeline = Pipeline.create(_recipe(output_path))
        pipeline.run()
        pipeline.raise_from_status()

        dev_calls = [
            r for r in mock.request_history if f"/environments/{ENV_DEV}" in r.url
        ]
        assert {r.headers["Authorization"] for r in dev_calls} == {"Bearer token-dev"}

    report = pipeline.source.get_report()
    assert isinstance(report, BoomiDataIntegrationSourceReport)
    assert report.environments_emitted == 2
    assert list(report.environments_inaccessible) == []
    assert report.rivers_emitted == 10
    assert list(report.rivers_filtered) == ["tmp_scratch"]
    assert report.rivers_without_definition == 1
    assert report.logic_steps_emitted == 11
    assert report.logic_steps_disabled == 1
    assert report.targets_object_storage_skipped == 1
    assert len(report.targets_prefix_skipped) == 1
    assert [w.title for w in report.warnings] == ["River definitions unavailable"]
    assert report.runs_scanned == 9
    assert report.runs_emitted == 9
    assert report.runs_river_not_emitted == 2
    assert report.runs_unknown_status == 0

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_DIR / "boomi_data_integration_mces_golden.json",
    )


def _stateful_recipe(output_path: Path) -> Dict[str, object]:
    recipe = _recipe(output_path)
    source_config = recipe["source"]["config"]  # type: ignore[index]
    source_config["stateful_ingestion"] = {
        "enabled": True,
        "remove_stale_metadata": True,
        "fail_safe_threshold": 100.0,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": "http://localhost:8080"}},
        },
    }
    recipe["pipeline_name"] = "boomi_di_stateful_test"
    return recipe


def _soft_deleted(output_path: Path) -> Set[str]:
    return {
        mcp["entityUrn"]
        for mcp in json.loads(output_path.read_text())
        if mcp.get("aspectName") == "status" and mcp["aspect"]["json"]["removed"]
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_data_integration_stateful_removes_deleted_river(
    tmp_path: Path, mock_datahub_graph: MagicMock
) -> None:
    recipe = _stateful_recipe(tmp_path / "run1.json")

    with (
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
        requests_mock.Mocker() as mock,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        _register_mocks(mock)
        pipeline_run1 = run_and_get_pipeline(recipe)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        # The actions river is deleted between runs.
        _register_mocks(mock, deleted_river=ACTIONS_RIVER)
        recipe["sink"] = {
            "type": "file",
            "config": {"filename": str(tmp_path / "run2.json")},
        }
        pipeline_run2 = run_and_get_pipeline(recipe)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

    assert checkpoint1 and checkpoint1.state
    assert checkpoint2 and checkpoint2.state
    validate_all_providers_have_committed_successfully(pipeline_run1, 1)
    validate_all_providers_have_committed_successfully(pipeline_run2, 1)
    removed = set(
        checkpoint1.state.get_urns_not_in(
            type="dataFlow", other_checkpoint_state=checkpoint2.state
        )
    ) | set(
        checkpoint1.state.get_urns_not_in(
            type="dataJob", other_checkpoint_state=checkpoint2.state
        )
    )
    assert removed == {
        f"urn:li:dataFlow:(boomi-data-integration,{ACTIONS_RIVER},PROD)",
        f"urn:li:dataJob:(urn:li:dataFlow:(boomi-data-integration,{ACTIONS_RIVER},PROD),run)",
    }
    assert _soft_deleted(tmp_path / "run2.json") == removed


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_data_integration_unreadable_environment_keeps_its_entities(
    tmp_path: Path, mock_datahub_graph: MagicMock
) -> None:
    recipe = _stateful_recipe(tmp_path / "run1.json")
    with (
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
        requests_mock.Mocker() as mock,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        _register_mocks(mock)
        run_and_get_pipeline(recipe)

        # The development token stops working between runs.
        mock.get(
            f"{BASE_URL}/environments/{ENV_DEV}",
            status_code=403,
            json=_load("rivers_forbidden.json"),
        )
        recipe["sink"] = {
            "type": "file",
            "config": {"filename": str(tmp_path / "run2.json")},
        }
        pipeline_run2 = Pipeline.create(recipe)
        pipeline_run2.run()

    assert [f.title for f in pipeline_run2.source.get_report().failures] == [
        "Configured environment not accessible"
    ]
    assert _soft_deleted(tmp_path / "run2.json") == set()


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_data_integration_single_token_skips_out_of_scope_environment(
    tmp_path: Path,
) -> None:
    # A token is scoped to one environment, yet it lists every environment and
    # can read each one; only the rivers of the others return 403.
    recipe = _recipe(tmp_path / "out.json")
    source_config = recipe["source"]["config"]  # type: ignore[index]
    del source_config["environment_tokens"]
    source_config["api_token"] = "token-prod"
    with requests_mock.Mocker() as mock:
        _register_mocks(mock)
        mock.get(
            f"{BASE_URL}/environments/{ENV_DEV}/rivers",
            status_code=403,
            json=_load("rivers_forbidden.json"),
        )
        pipeline = Pipeline.create(recipe)
        pipeline.run()
        pipeline.raise_from_status()

    report = pipeline.source.get_report()
    assert isinstance(report, BoomiDataIntegrationSourceReport)
    assert report.environments_emitted == 1
    assert list(report.environments_inaccessible) == [ENV_DEV]
    assert report.rivers_emitted == 4
    assert [w.title for w in report.warnings] == ["Environment not accessible"]
