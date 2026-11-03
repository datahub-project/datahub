import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union

import pytest
import requests_mock
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.source import (
    BoomiDataIntegrationSource,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

ACCOUNT_ID = "acct-0001"
BASE_URL = f"https://api.rivery.io/v1/accounts/{ACCOUNT_ID}"
API_RESPONSES = (
    Path(__file__).parents[2]
    / "integration"
    / "boomi_data_integration"
    / "api_responses"
)


def _source(**overrides: object) -> BoomiDataIntegrationSource:
    config: Dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "api_token": "token-a",
        "max_requests_per_second": 50,
        # Run history has its own tests in test_run_history.py.
        "include_run_history": False,
    }
    config.update(overrides)
    return BoomiDataIntegrationSource.create(
        config, PipelineContext(run_id="boomi-di-unit")
    )


def _environment(cross_id: str, name: str) -> Dict[str, object]:
    return {
        "_id": cross_id,
        "account": ACCOUNT_ID,
        "cross_id": cross_id,
        "environment_name": name,
        "variables": {"Mail_Alert_Group": "svc@example.com"},
        "is_default": False,
        "is_deleted": False,
    }


def _summary(river_id: str, river_type: str = "source_to_target") -> Dict:
    return {
        "river_cross_id": river_id,
        "name": f"River {river_id}",
        "river_type": river_type,
        "river_status": "active",
        "group_id": "g-1",
        "group_name": "Sales",
        "datasource_id": "salesforce",
        "description": "From the list.",
        "river_schedulers": ["0 6 * * *"],
        "last_user_name_modified": "Example User",
        "is_api_v2": False,
        "created_at": "2026-11-02T20:14:55.701000Z",
        "last_updated_at": "2026-11-02T20:14:55.701000Z",
    }


def _page(items: List[Dict[str, object]]) -> Dict[str, object]:
    # The live API marks the last page with an empty next_page string.
    return {"items": items, "next_page": "", "current_page_size": len(items)}


# The live API's response to a token outside the environment's scope.
FORBIDDEN: Dict[str, object] = {
    "status_code": 403,
    "json": {"detail": "Insufficient permissions"},
}


def _entities(source: BoomiDataIntegrationSource) -> List[Entity]:
    stream: Iterable[Union[MetadataWorkUnit, Entity]] = source.get_workunits_internal()
    return [e for e in stream if isinstance(e, Entity)]


def test_forbidden_environment_is_skipped_with_one_warning() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments",
            json=_page(
                [_environment("env-a", "Prod"), _environment("env-b", "Locked")]
            ),
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([]))
        mock.get(f"{BASE_URL}/environments/env-b/rivers", **FORBIDDEN)
        entities = _entities(source)

    report = source.get_report()
    assert [e.display_name for e in entities if isinstance(e, Container)] == ["Prod"]
    assert list(report.environments_inaccessible) == ["env-b"]
    assert [w.title for w in report.warnings] == ["Environment not accessible"]


def _env_tokens(**tokens: str) -> List[Dict[str, str]]:
    return [
        {"environment_id": environment_id, "api_token": token}
        for environment_id, token in tokens.items()
    ]


def test_environment_tokens_use_their_own_token() -> None:
    source = _source(
        api_token=None,
        environment_tokens=_env_tokens(**{"env-a": "tok-a", "env-b": "tok-b"}),
    )
    with requests_mock.Mocker() as mock:
        for environment_id, name in [("env-a", "A"), ("env-b", "B")]:
            mock.get(
                f"{BASE_URL}/environments/{environment_id}",
                json=_environment(environment_id, name),
            )
            mock.get(f"{BASE_URL}/environments/{environment_id}/rivers", json=_page([]))
        entities = _entities(source)
        auth = {r.path: r.headers["Authorization"] for r in mock.request_history}

    assert sorted(
        e.display_name or "" for e in entities if isinstance(e, Container)
    ) == ["A", "B"]
    assert auth == {
        "/v1/accounts/acct-0001/environments/env-a": "Bearer tok-a",
        "/v1/accounts/acct-0001/environments/env-a/rivers": "Bearer tok-a",
        "/v1/accounts/acct-0001/environments/env-b": "Bearer tok-b",
        "/v1/accounts/acct-0001/environments/env-b/rivers": "Bearer tok-b",
    }


def test_unreadable_configured_environment_is_a_failure() -> None:
    # A failure keeps stale entity removal from deleting the environment's
    # previously ingested rivers.
    source = _source(
        api_token=None,
        environment_tokens=_env_tokens(**{"env-a": "tok-a", "env-b": "tok-b"}),
    )
    with requests_mock.Mocker() as mock:
        mock.get(f"{BASE_URL}/environments/env-a", json=_environment("env-a", "A"))
        mock.get(f"{BASE_URL}/environments/env-b", status_code=401)
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([]))
        entities = _entities(source)

    report = source.get_report()
    assert [e.display_name for e in entities if isinstance(e, Container)] == ["A"]
    assert list(report.environments_inaccessible) == ["env-b"]
    assert [f.title for f in report.failures] == [
        "Configured environment not accessible"
    ]


def test_listed_environment_server_error_is_a_failure() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers", status_code=404)
        _entities(source)

    assert [f.title for f in source.get_report().failures] == [
        "Failed to read environment"
    ]


def test_river_get_400_falls_back_to_list_fields() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(
            f"{BASE_URL}/environments/env-a/rivers",
            json=_page([_summary("r-1"), _summary("r-2", river_type="logic")]),
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-1", status_code=400)
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-2", status_code=400)
        entities = _entities(source)

    flows = [e for e in entities if isinstance(e, DataFlow)]
    jobs = [e for e in entities if isinstance(e, DataJob)]
    assert [f.description for f in flows] == ["From the list.", "From the list."]
    assert flows[0].custom_properties["schedule"] == "0 6 * * *"
    assert sorted((j.urn.job_id, j.subtype) for j in jobs) == [
        ("load", "Load"),
        ("run", "Run"),
    ]
    # No lineage is known, so no input/output aspect overwrites existing lineage.
    assert jobs[0]._get_aspect(DataJobInputOutputClass) is None

    report = source.get_report()
    assert report.rivers_without_definition == 2
    assert [w.title for w in report.warnings] == ["River definitions unavailable"]


def _ingested_flow_ids(source: BoomiDataIntegrationSource) -> List[str]:
    other_group = {**_summary("r-2"), "group_id": "g-2", "group_name": "Finance"}
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(
            f"{BASE_URL}/environments/env-a/rivers",
            json=_page([_summary("r-1"), other_group]),
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-1", status_code=400)
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-2", status_code=400)
        entities = _entities(source)
    return [e.urn.flow_id for e in entities if isinstance(e, DataFlow)]


@pytest.mark.parametrize(
    "overrides, api_url, expected",
    [
        (
            {},
            "https://api.rivery.io",
            "https://console.rivery.io/rivers/acct-0001/env-a/r-1",
        ),
        ({"region": "eu"}, "https://api.eu-west-1.rivery.io", None),
        (
            {"region": "eu", "console_url": "https://console.example.com/"},
            "https://api.eu-west-1.rivery.io",
            "https://console.example.com/rivers/acct-0001/env-a/r-1",
        ),
    ],
)
def test_river_console_url(
    overrides: Dict[str, object], api_url: str, expected: Optional[str]
) -> None:
    source = _source(**overrides)
    base_url = f"{api_url}/v1/accounts/{ACCOUNT_ID}"
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{base_url}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(
            f"{base_url}/environments/env-a/rivers",
            json=_page([_summary("r-1", river_type="logic")]),
        )
        mock.get(f"{base_url}/environments/env-a/rivers/r-1", status_code=400)
        flows = [e for e in _entities(source) if isinstance(e, DataFlow)]
    assert [f.external_url for f in flows] == [expected]


@pytest.mark.parametrize(
    "river_type", ["source_to_target", "actions", "connector_executor"]
)
def test_console_url_only_for_logic_rivers(river_type: str) -> None:
    # The console path is only confirmed for logic rivers.
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(
            f"{BASE_URL}/environments/env-a/rivers",
            json=_page([_summary("r-1", river_type=river_type)]),
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-1", status_code=400)
        flows = [e for e in _entities(source) if isinstance(e, DataFlow)]
    assert [f.external_url for f in flows] == [None]


@pytest.mark.parametrize(
    "console_url",
    [
        "http://console.example.com",
        "https://console.example.com/?tab=rivers",
        "https://console.example.com/#/home",
        "https://user:secret@console.example.com",
    ],
)
def test_console_url_rejects_unusable_base(console_url: str) -> None:
    with pytest.raises(ValidationError) as excinfo:
        BoomiDataIntegrationSourceConfig.model_validate(
            {
                "account_id": ACCOUNT_ID,
                "api_token": "token-a",
                "console_url": console_url,
            }
        )
    assert "secret" not in str(excinfo.value)


def test_river_pattern_filters_rivers() -> None:
    source = _source(river_pattern={"deny": ["River r-2"]})
    assert _ingested_flow_ids(source) == ["r-1"]
    report = source.get_report()
    assert list(report.rivers_filtered) == ["River r-2"]
    assert list(report.groups_filtered) == []


def test_group_pattern_filters_rivers() -> None:
    source = _source(group_pattern={"deny": ["Finance"]})
    assert _ingested_flow_ids(source) == ["r-1"]
    report = source.get_report()
    assert list(report.rivers_filtered) == ["River r-2"]
    assert list(report.groups_filtered) == ["Finance"]


def test_test_connection_reports_auth_failure() -> None:
    with requests_mock.Mocker() as mock:
        mock.get(f"{BASE_URL}/environments", status_code=401)
        report = BoomiDataIntegrationSource.test_connection(
            {"account_id": ACCOUNT_ID, "api_token": "wrong"}
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable


def test_test_connection_passes_when_one_environment_is_reachable() -> None:
    # The live API serves any environment record to any token of the account,
    # and only refuses the rivers of environments outside the token's scope.
    config = {
        "account_id": ACCOUNT_ID,
        "environment_tokens": _env_tokens(**{"env-a": "tok-a", "env-b": "tok-a"}),
    }
    with requests_mock.Mocker() as mock:
        mock.get(f"{BASE_URL}/environments/env-b", json=_environment("env-b", "B"))
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([]))
        mock.get(f"{BASE_URL}/environments/env-b/rivers", **FORBIDDEN)
        partial = BoomiDataIntegrationSource.test_connection(config)
        probes = [r.qs for r in mock.request_history]
        mock.get(f"{BASE_URL}/environments/env-a/rivers", **FORBIDDEN)
        none = BoomiDataIntegrationSource.test_connection(config)

    assert partial.basic_connectivity is not None
    assert partial.basic_connectivity.capable
    assert "env-b" in (partial.basic_connectivity.mitigation_message or "")
    assert "env-a" not in (partial.basic_connectivity.mitigation_message or "")
    assert all(qs["items_per_page"] == ["1"] for qs in probes)
    assert none.basic_connectivity is not None
    assert not none.basic_connectivity.capable


def test_test_connection_with_api_token_probes_listed_environments() -> None:
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments",
            json=_page([_environment("env-a", "Prod"), _environment("env-b", "Dev")]),
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([]))
        mock.get(f"{BASE_URL}/environments/env-b/rivers", **FORBIDDEN)
        report = BoomiDataIntegrationSource.test_connection(
            {"account_id": ACCOUNT_ID, "api_token": "tok-a"}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert "env-b" in (report.basic_connectivity.mitigation_message or "")


def test_test_connection_rejects_bad_config() -> None:
    report = BoomiDataIntegrationSource.test_connection({"account_id": ACCOUNT_ID})
    assert report.internal_failure


def _logic_river(river_id: str, steps: List[Dict[str, object]]) -> Dict[str, object]:
    return {
        "cross_id": river_id,
        "account_id": ACCOUNT_ID,
        "kind": "main_river",
        "type": "logic",
        "name": f"River {river_id}",
        "group_id": "g-1",
        "group_name": "Sales",
        "metadata": {"river_status": "active", "is_deleted": False},
        "properties": {"properties_type": "logic", "logic_steps": steps},
        "schedulers": [],
    }


def test_logic_steps_skip_disabled_and_invalid_steps() -> None:
    steps: List[Dict[str, object]] = [
        {"name": "Load", "is_enabled": True, "step_id": "s1", "type": "logicode"},
        {"name": "Off", "is_enabled": False, "step_id": "s2", "type": "logicode"},
        {"name": "No id", "is_enabled": True, "type": "logicode"},
        {"name": "Publish", "is_enabled": True, "step_id": "s3", "type": "logicode"},
    ]
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(
            f"{BASE_URL}/environments/env-a/rivers",
            json=_page([_summary("r-1", river_type="logic")]),
        )
        mock.get(
            f"{BASE_URL}/environments/env-a/rivers/r-1",
            json=_logic_river("r-1", steps),
        )
        entities = _entities(source)

    jobs = {e.urn.job_id: e for e in entities if isinstance(e, DataJob)}
    assert sorted(jobs) == ["s1", "s3"]
    io = jobs["s3"]._get_aspect(DataJobInputOutputClass)
    assert io is not None
    assert io.inputDatajobs == [str(jobs["s1"].urn)]
    report = source.get_report()
    assert (report.logic_steps_disabled, report.logic_steps_invalid) == (1, 1)
    assert [w.title for w in report.warnings] == ["Invalid logic steps"]


def test_river_definition_server_error_keeps_river_without_lineage() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([_summary("r-1")]))
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-1", status_code=500)
        entities = _entities(source)

    assert [e.urn.flow_id for e in entities if isinstance(e, DataFlow)] == ["r-1"]
    report = source.get_report()
    assert list(report.river_definition_failures) == ["r-1"]
    assert [w.title for w in report.warnings] == ["Failed to read river definition"]


def test_source_to_target_river_emits_load_job_with_target() -> None:
    definition = json.loads(
        (API_RESPONSES / "river_a00000000000000000000001.json").read_text()
    )
    source = _source()
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{BASE_URL}/environments", json=_page([_environment("env-a", "Prod")])
        )
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([_summary("r-1")]))
        mock.get(f"{BASE_URL}/environments/env-a/rivers/r-1", json=definition)
        entities = _entities(source)

    flow = next(e for e in entities if isinstance(e, DataFlow))
    assert flow.custom_properties["run_type"] == "predefined_report"
    assert flow.custom_properties["target"] == "postgres_rds"
    job = next(e for e in entities if isinstance(e, DataJob))
    io = job._get_aspect(DataJobInputOutputClass)
    assert io is not None
    assert io.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,crm.leads,PROD)"
    ]
