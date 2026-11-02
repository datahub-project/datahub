from datetime import datetime, timezone
from typing import Dict, List, Optional

import pytest
import requests_mock
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.models import ComponentMetadata
from datahub.ingestion.source.boomi.source import (
    BoomiSource,
    dedupe_branch_copies,
    ui_host_for_api_url,
)
from datahub.sdk.dataflow import DataFlow

ACCOUNT_ID = "acme-AB1CD2"
BASE_URL = f"https://api.boomi.com/api/rest/v1/{ACCOUNT_ID}"
XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'folderFullPath="Acme/Orders"><bns:description>Syncs orders.</bns:description>'
    "</bns:Component>"
)


def _component(
    component_id: str,
    version: int = 1,
    modified: Optional[datetime] = None,
    name: str = "Orders Sync",
) -> ComponentMetadata:
    return ComponentMetadata(
        component_id=component_id,
        version=version,
        name=name,
        type="process",
        modified_date=modified,
    )


def _source(**overrides: object) -> BoomiSource:
    config: Dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "username": "svc@example.com",
        "api_token": "secret-token",
        "max_requests_per_second": 10,
    }
    config.update(overrides)
    return BoomiSource.create(config, PipelineContext(run_id="boomi-unit"))


def _process_record(component_id: str, name: str) -> Dict[str, object]:
    return {
        "componentId": component_id,
        "version": 1,
        "name": name,
        "type": "process",
        "createdBy": "svc@example.com",
        "modifiedBy": "svc@example.com",
        "folderId": "F1",
        "folderName": "Orders",
    }


def _register(
    mock: requests_mock.Mocker,
    processes: List[Dict[str, object]],
    deployed_status: int = 200,
    deployed: Optional[List[Dict[str, object]]] = None,
    folder_status: int = 200,
    environment_status: int = 200,
) -> None:
    mock.post(
        f"{BASE_URL}/Folder/query",
        status_code=folder_status,
        json={"result": [{"id": "F1", "name": "Orders", "fullPath": "Acme/Orders"}]},
    )
    mock.post(
        f"{BASE_URL}/Environment/query",
        status_code=environment_status,
        json={"result": [{"id": "env-prod", "name": "Production"}]},
    )
    mock.post(
        f"{BASE_URL}/DeployedPackage/query",
        status_code=deployed_status,
        json={"result": deployed or []},
    )
    mock.post(f"{BASE_URL}/ComponentMetadata/query", json={"result": processes})


def _flows(source: BoomiSource) -> List[DataFlow]:
    return [e for e in source.get_workunits_internal() if isinstance(e, DataFlow)]


@pytest.mark.parametrize(
    "api_url, expected",
    [
        ("https://api.boomi.com", "platform.boomi.com"),
        ("https://api.platform.gb.boomi.com", "platform.gb.boomi.com"),
        ("https://api.example.internal", None),
    ],
)
def test_ui_host_for_api_url(api_url: str, expected: Optional[str]) -> None:
    assert ui_host_for_api_url(api_url) == expected


def test_dedupe_keeps_most_recent_branch_copy() -> None:
    older = _component(
        "p1", version=5, modified=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    newer = _component(
        "p1", version=2, modified=datetime(2026, 6, 1, tzinfo=timezone.utc)
    )
    other = _component("p2")

    kept, duplicates = dedupe_branch_copies([older, newer, other])

    assert duplicates == 1
    assert {(c.component_id, c.version) for c in kept} == {("p1", 2), ("p2", 1)}


def test_dedupe_handles_naive_and_missing_dates() -> None:
    naive = _component("p1", version=1, modified=datetime(2026, 6, 1))
    missing = _component("p1", version=2)

    kept, duplicates = dedupe_branch_copies([missing, naive])

    assert duplicates == 1
    assert [c.version for c in kept] == [1]


def test_deployed_package_403_includes_all_processes() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register(
            mock,
            [_process_record("p1", "Orders Sync"), _process_record("p2", "Invoices")],
            deployed_status=403,
        )
        mock.get(f"{BASE_URL}/Component/p1", text=XML)
        mock.get(f"{BASE_URL}/Component/p2", text=XML)
        flows = _flows(source)

    report = source.get_report()
    assert sorted(f.display_name or "" for f in flows) == ["Invoices", "Orders Sync"]
    assert report.deployments_unavailable
    assert [w.title for w in report.warnings] == ["Cannot read Boomi deployments"]
    assert all("deployments" not in f.custom_properties for f in flows)


def test_undeployed_processes_skipped_by_default() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register(mock, [_process_record("p1", "Orders Sync")])
        assert _flows(source) == []

    assert list(source.get_report().processes_undeployed_skipped) == ["Orders Sync"]


def test_environment_listing_failure_includes_all_processes() -> None:
    source = _source(environment_pattern={"allow": ["^Production$"]})
    with requests_mock.Mocker() as mock:
        _register(
            mock,
            [_process_record("p1", "Orders Sync")],
            deployed=[
                {
                    "componentId": "p1",
                    "environmentId": "env-prod",
                    "packageVersion": "1",
                }
            ],
            environment_status=500,
        )
        mock.get(f"{BASE_URL}/Component/p1", text=XML)
        flows = _flows(source)

    assert [f.display_name for f in flows] == ["Orders Sync"]
    assert "deployments" not in flows[0].custom_properties
    assert source.get_report().deployments_unavailable


def test_deployments_on_other_branches_are_ignored() -> None:
    source = _source(branch_name="main")
    with requests_mock.Mocker() as mock:
        _register(
            mock,
            [_process_record("p1", "Orders Sync")],
            deployed=[
                {
                    "componentId": "p1",
                    "environmentId": "env-prod",
                    "packageVersion": "1",
                    "branchName": "feature",
                }
            ],
        )
        assert _flows(source) == []

    assert list(source.get_report().processes_undeployed_skipped) == ["Orders Sync"]


def test_folder_listing_failure_matches_component_folder_path() -> None:
    source = _source(
        include_undeployed_processes=True,
        folder_pattern={"allow": ["^Acme/Orders$"]},
    )
    other_xml = XML.replace("Acme/Orders", "Acme/Archive")
    with requests_mock.Mocker() as mock:
        _register(
            mock,
            [_process_record("p1", "Orders Sync"), _process_record("p2", "Old")],
            folder_status=500,
        )
        mock.get(f"{BASE_URL}/Component/p1", text=XML)
        mock.get(f"{BASE_URL}/Component/p2", text=other_xml)
        flows = _flows(source)

    report = source.get_report()
    assert [f.display_name for f in flows] == ["Orders Sync"]
    assert list(report.processes_filtered) == ["Old"]
    assert [f.title for f in report.failures] == ["Failed to list Boomi folders"]


def test_unknown_folder_without_path_fails_closed() -> None:
    source = _source(
        include_undeployed_processes=True,
        folder_pattern={"allow": ["^Acme/Orders$"]},
    )
    with requests_mock.Mocker() as mock:
        _register(mock, [_process_record("p1", "Orders Sync")], folder_status=500)
        mock.get(f"{BASE_URL}/Component/p1", text="<not-closed")
        assert _flows(source) == []

    assert list(source.get_report().processes_filtered) == ["Orders Sync"]


def test_xml_failure_still_emits_process() -> None:
    source = _source(include_undeployed_processes=True, extract_owners=False)
    with requests_mock.Mocker() as mock:
        _register(mock, [_process_record("p1", "Orders Sync")])
        mock.get(f"{BASE_URL}/Component/p1", text="<not-closed")
        flows = _flows(source)

    assert len(flows) == 1
    assert flows[0].description is None
    assert flows[0].owners is None
    assert list(source.get_report().process_xml_failures) == ["p1"]


@pytest.mark.parametrize(
    "api_url",
    [
        "api.boomi.com",
        "http://api.boomi.com",
        "https://api.boomi.com?region=us",
        "https://api.boomi.com#build",
    ],
)
def test_config_rejects_invalid_api_url(api_url: str) -> None:
    with pytest.raises(ValidationError):
        BoomiSourceConfig.model_validate(
            {
                "account_id": ACCOUNT_ID,
                "username": "svc@example.com",
                "api_token": "secret-token",
                "api_url": api_url,
            }
        )


@pytest.mark.parametrize(
    "api_url", ["http://localhost:8080", "http://127.0.0.1:9000", "http://[::1]:80"]
)
def test_config_allows_http_for_loopback(api_url: str) -> None:
    config = BoomiSourceConfig.model_validate(
        {
            "account_id": ACCOUNT_ID,
            "username": "svc@example.com",
            "api_token": "secret-token",
            "api_url": api_url,
        }
    )
    assert config.api_url == api_url


def test_config_rejects_rate_above_boomi_limit() -> None:
    with pytest.raises(ValidationError):
        BoomiSourceConfig.model_validate(
            {
                "account_id": ACCOUNT_ID,
                "username": "svc@example.com",
                "api_token": "secret-token",
                "max_requests_per_second": 20,
            }
        )


def test_config_strips_trailing_slash() -> None:
    config = BoomiSourceConfig.model_validate(
        {
            "account_id": ACCOUNT_ID,
            "username": "svc@example.com",
            "api_token": "secret-token",
            "api_url": "https://api.platform.gb.boomi.com/",
        }
    )
    assert config.api_url == "https://api.platform.gb.boomi.com"


def test_test_connection_reports_auth_failure() -> None:
    with requests_mock.Mocker() as mock:
        mock.post(f"{BASE_URL}/ComponentMetadata/query", status_code=401)
        report = BoomiSource.test_connection(
            {
                "account_id": ACCOUNT_ID,
                "username": "svc@example.com",
                "api_token": "wrong",
            }
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
