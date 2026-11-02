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
# Shaped like a live GET Component/{id} response for a process.
XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:bns="http://api.platform.boomi.com/" '
    'folderFullPath="Test Account/Orders" componentId="p1" version="1" '
    'name="Orders Sync" type="process" createdDate="2026-11-02T19:17:12Z" '
    'createdBy="svc@example.com" modifiedDate="2026-11-02T19:17:12Z" '
    'modifiedBy="svc@example.com" deleted="false" currentVersion="true" '
    'folderName="Orders" folderId="Rjo4OTQzNDEx" branchName="main" '
    'branchId="Qjo2MDk0Mzg"><bns:encryptedValues/>'
    "<bns:description>Syncs orders.</bns:description><bns:object>"
    '<process xmlns="" allowSimultaneous="false" workload="general"><shapes>'
    '<shape image="start" name="shape1" shapetype="start" userlabel="">'
    "<configuration><noaction/></configuration><dragpoints/></shape>"
    "</shapes></process></bns:object><bns:processOverrides/></bns:Component>"
)
ORDERS_FOLDER = "Rjo4OTQzNDEx"
PROD_ENVIRONMENT = "56c4f2be-9126-4e8a-bfb2-111c009e83d8"


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
        "@type": "ComponentMetadata",
        "componentId": component_id,
        "version": 1,
        "name": name,
        "type": "process",
        "createdDate": "2026-11-02T19:17:12Z",
        "createdBy": "svc@example.com",
        "modifiedDate": "2026-11-02T19:17:12Z",
        "modifiedBy": "svc@example.com",
        "deleted": False,
        "currentVersion": True,
        "folderName": "Orders",
        "folderId": ORDERS_FOLDER,
        "branchName": "main",
        "branchId": "Qjo2MDk0Mzg",
    }


def _deployment(component_id: str, branch_name: str = "main") -> Dict[str, object]:
    return {
        "@type": "DeployedPackage",
        "deploymentId": "dda2f5b2-37d6-4ccb-9349-98b6c77e1c0c",
        "version": 1,
        "packageId": "ab44fd92-6b7d-4d91-94a1-e5f46cd129bd",
        "packageVersion": "1.0",
        "environmentId": PROD_ENVIRONMENT,
        "componentId": component_id,
        "componentVersion": 1,
        "componentType": "process",
        "deployedDate": "2026-11-02T19:18:22Z",
        "deployedBy": "svc@example.com",
        "notes": "Initial deploy",
        "active": True,
        "branchName": branch_name,
    }


def _page(records: List[Dict[str, object]]) -> Dict[str, object]:
    return {
        "@type": "QueryResult",
        "result": records,
        "numberOfResults": len(records),
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
        json=_page(
            [
                {
                    "@type": "Folder",
                    "id": "Rjo4OTQzMzg4",
                    "name": "Test Account",
                    "fullPath": "Test Account",
                    "deleted": False,
                    "parentId": "",
                    "parentName": "",
                },
                {
                    "@type": "Folder",
                    "id": ORDERS_FOLDER,
                    "name": "Orders",
                    "fullPath": "Test Account/Orders",
                    "deleted": False,
                    "parentId": "Rjo4OTQzMzg4",
                    "parentName": "Test Account",
                },
            ]
        ),
    )
    mock.post(
        f"{BASE_URL}/Environment/query",
        status_code=environment_status,
        json=_page(
            [
                {
                    "@type": "Environment",
                    "id": PROD_ENVIRONMENT,
                    "name": "Production",
                    "classification": "PROD",
                }
            ]
        ),
    )
    mock.post(
        f"{BASE_URL}/DeployedPackage/query",
        status_code=deployed_status,
        json=_page(deployed or []),
    )
    mock.post(f"{BASE_URL}/ComponentMetadata/query", json=_page(processes))


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
            deployed=[_deployment("p1")],
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
            deployed=[_deployment("p1", branch_name="feature")],
        )
        assert _flows(source) == []

    assert list(source.get_report().processes_undeployed_skipped) == ["Orders Sync"]


def test_folder_listing_failure_matches_component_folder_path() -> None:
    source = _source(
        include_undeployed_processes=True,
        folder_pattern={"allow": ["^Test Account/Orders$"]},
    )
    other_xml = XML.replace("Test Account/Orders", "Test Account/Archive")
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
        folder_pattern={"allow": ["^Test Account/Orders$"]},
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
