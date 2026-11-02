from typing import Dict, Iterable, List, Union

import requests_mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
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


def _source(**overrides: object) -> BoomiDataIntegrationSource:
    config: Dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "api_token": "token-a",
        "max_requests_per_second": 50,
    }
    config.update(overrides)
    return BoomiDataIntegrationSource.create(
        config, PipelineContext(run_id="boomi-di-unit")
    )


def _environment(cross_id: str, name: str) -> Dict[str, object]:
    return {"cross_id": cross_id, "environment_name": name, "variables": {}}


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
    }


def _page(items: List[Dict[str, object]]) -> Dict[str, object]:
    return {"items": items, "next_page": None, "current_page_size": len(items)}


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
        mock.get(f"{BASE_URL}/environments/env-b/rivers", status_code=403)
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
        api_token=None, environment_tokens=_env_tokens(**{"env-a": "tok-a"})
    )
    with requests_mock.Mocker() as mock:
        mock.get(f"{BASE_URL}/environments/env-a", json=_environment("env-a", "A"))
        mock.get(f"{BASE_URL}/environments/env-a/rivers", json=_page([]))
        _entities(source)
        auth = {r.path: r.headers["Authorization"] for r in mock.request_history}

    assert auth["/v1/accounts/acct-0001/environments/env-a"] == "Bearer tok-a"
    assert auth["/v1/accounts/acct-0001/environments/env-a/rivers"] == "Bearer tok-a"


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
    io = jobs[0]._get_aspect(DataJobInputOutputClass)
    assert io is not None and io.outputDatasets == []

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
    config = {
        "account_id": ACCOUNT_ID,
        "environment_tokens": _env_tokens(**{"env-a": "tok-a", "env-b": "tok-b"}),
    }
    with requests_mock.Mocker() as mock:
        mock.get(f"{BASE_URL}/environments/env-a", json=_environment("env-a", "A"))
        mock.get(f"{BASE_URL}/environments/env-b", status_code=403)
        partial = BoomiDataIntegrationSource.test_connection(config)
        mock.get(f"{BASE_URL}/environments/env-a", status_code=403)
        none = BoomiDataIntegrationSource.test_connection(config)

    assert partial.basic_connectivity is not None
    assert partial.basic_connectivity.capable
    assert "env-b" in (partial.basic_connectivity.mitigation_message or "")
    assert "env-a" not in (partial.basic_connectivity.mitigation_message or "")
    assert none.basic_connectivity is not None
    assert not none.basic_connectivity.capable


def test_test_connection_rejects_bad_config() -> None:
    report = BoomiDataIntegrationSource.test_connection({"account_id": ACCOUNT_ID})
    assert report.internal_failure
