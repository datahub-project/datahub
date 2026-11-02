import json
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2026-10-01 12:00:00"
ACCOUNT_ID = "acme-AB1CD2"
BASE_URL = f"https://api.boomi.com/api/rest/v1/{ACCOUNT_ID}"
GOLDEN_DIR = Path(__file__).parent / "golden"

FOLDERS: List[Dict[str, object]] = [
    {"id": "F0", "name": "Acme", "fullPath": "Acme", "deleted": False},
    {
        "id": "F1",
        "name": "Production",
        "parentId": "F0",
        "fullPath": "Acme/Production",
        "deleted": False,
    },
    {
        "id": "F2",
        "name": "Orders",
        "parentId": "F1",
        "fullPath": "Acme/Production/Orders",
        "deleted": False,
    },
    {
        "id": "F3",
        "name": "Finance",
        "parentId": "F1",
        "fullPath": "Acme/Production/Finance",
        "deleted": False,
    },
    {
        "id": "F4",
        "name": "Sandbox",
        "parentId": "F0",
        "fullPath": "Acme/Sandbox",
        "deleted": False,
    },
    {
        "id": "F5",
        "name": "Archive",
        "parentId": "F0",
        "fullPath": "Acme/Archive",
        "deleted": True,
    },
]

ENVIRONMENTS: List[Dict[str, object]] = [
    {"id": "env-prod", "name": "Production", "classification": "PROD"},
    {"id": "env-test", "name": "Test", "classification": "TEST"},
    {"id": "env-dev", "name": "Dev", "classification": "TEST"},
]


def _process(
    component_id: str,
    name: str,
    folder_id: str,
    folder_name: str,
    version: object = 1,
    created_by: str = "svc@example.com",
    modified_by: str = "svc@example.com",
    modified_date: str = "2026-09-01T10:00:00Z",
    branch_name: Optional[str] = "main",
) -> Dict[str, object]:
    record: Dict[str, object] = {
        "@type": "ComponentMetadata",
        "componentId": component_id,
        "version": version,
        "name": name,
        "type": "process",
        "subType": "",
        "createdBy": created_by,
        "createdDate": "2026-01-15T08:30:00Z",
        "modifiedBy": modified_by,
        "modifiedDate": modified_date,
        "deleted": False,
        "currentVersion": True,
        "folderName": folder_name,
        "folderId": folder_id,
    }
    if branch_name:
        record["branchName"] = branch_name
        record["branchId"] = f"branch-{branch_name}"
    return record


PROCESSES_PAGE_1 = [
    _process(
        "p-orders-sync",
        "Orders Sync",
        "F2",
        "Orders",
        version=["Long", 7],
        modified_by="dev@example.com",
    ),
    _process("p-invoice-export", "Invoice Export", "F3", "Finance", version=3),
    _process(
        "p-customer-upsert",
        "Customer Upsert",
        "F2",
        "Orders",
        version=2,
        modified_date="2026-08-01T10:00:00Z",
    ),
]
PROCESSES_PAGE_2 = [
    # Same component returned twice; the newer copy must win.
    _process(
        "p-customer-upsert",
        "Customer Upsert",
        "F2",
        "Orders",
        version=4,
        modified_by="ops@example.com",
        modified_date="2026-09-20T10:00:00Z",
    ),
    _process("p-legacy-cleanup", "Legacy Cleanup", "F1", "Production"),
    _process("p-orders-sync-test", "Orders Sync Test", "F2", "Orders"),
    _process("p-sandbox-probe", "Sandbox Probe", "F4", "Sandbox"),
    _process("p-shipment-notify", "Shipment Notify", "F3", "Finance"),
]

DEPLOYMENTS: List[Dict[str, object]] = [
    {
        "componentId": "p-orders-sync",
        "componentType": "process",
        "environmentId": "env-prod",
        "packageVersion": "3.0",
        "active": True,
    },
    {
        "componentId": "p-orders-sync",
        "componentType": "process",
        "environmentId": "env-test",
        "packageVersion": "4.0",
        "active": True,
    },
    {
        "componentId": "p-invoice-export",
        "componentType": "process",
        "environmentId": "env-prod",
        "packageVersion": "1.2",
        "active": True,
    },
    {
        "componentId": "p-customer-upsert",
        "componentType": "process",
        "environmentId": "env-test",
        "packageVersion": "2.0",
        "active": True,
    },
    {
        "componentId": "p-orders-sync-test",
        "componentType": "process",
        "environmentId": "env-test",
        "packageVersion": "1.0",
        "active": True,
    },
    {
        "componentId": "p-sandbox-probe",
        "componentType": "process",
        "environmentId": "env-prod",
        "packageVersion": "1.0",
        "active": True,
    },
    # Only deployed to an environment denied by environment_pattern.
    {
        "componentId": "p-shipment-notify",
        "componentType": "process",
        "environmentId": "env-dev",
        "packageVersion": "1.0",
        "active": True,
    },
]

COMPONENT_XML_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<bns:Component xmlns:bns="http://api.platform.boomi.com/" componentId="{id}" name="{name}" type="process" folderFullPath="{path}" createdBy="svc@example.com" branchName="main">
  <bns:encryptedValues/>
  <bns:description>{description}</bns:description>
  <bns:object>
    <process xmlns="" allowSimultaneous="false">
      <shapes>
        <shape shapetype="start" name="shape1"/>
        <shape shapetype="stop" name="shape2"/>
      </shapes>
    </process>
  </bns:object>
</bns:Component>
"""

COMPONENT_XML: Dict[str, str] = {
    "p-orders-sync": COMPONENT_XML_TEMPLATE.format(
        id="p-orders-sync",
        name="Orders Sync",
        path="Acme/Production/Orders",
        description="Copies new orders from the web shop into the ERP.",
    ),
    "p-invoice-export": COMPONENT_XML_TEMPLATE.format(
        id="p-invoice-export",
        name="Invoice Export",
        path="Acme/Production/Finance",
        description="Exports posted invoices to the finance warehouse.",
    ),
    "p-customer-upsert": COMPONENT_XML_TEMPLATE.format(
        id="p-customer-upsert",
        name="Customer Upsert",
        path="Acme/Production/Orders",
        description="",
    ),
}


def _page(
    records: List[Dict[str, object]], token: Optional[str] = None
) -> Dict[str, object]:
    page: Dict[str, object] = {
        "@type": "QueryResult",
        "result": records,
        "numberOfResults": len(records),
    }
    if token:
        page["queryToken"] = token
    return page


def _register_mocks(
    mock: requests_mock.Mocker, deployments: List[Dict[str, object]] = DEPLOYMENTS
) -> None:
    mock.post(f"{BASE_URL}/Folder/query", json=_page(FOLDERS))
    mock.post(f"{BASE_URL}/Environment/query", json=_page(ENVIRONMENTS))
    mock.post(f"{BASE_URL}/DeployedPackage/query", json=_page(deployments))
    mock.post(
        f"{BASE_URL}/ComponentMetadata/query",
        json=_page(PROCESSES_PAGE_1, token="page-2-token"),
    )
    mock.post(
        f"{BASE_URL}/ComponentMetadata/queryMore",
        json=_page(PROCESSES_PAGE_2),
    )
    for component_id, xml in COMPONENT_XML.items():
        mock.get(f"{BASE_URL}/Component/{component_id}", text=xml)


def _recipe(output_path: Path) -> Dict[str, object]:
    return {
        "run_id": "boomi-test",
        "source": {
            "type": "boomi",
            "config": {
                "account_id": ACCOUNT_ID,
                "username": "svc@example.com",
                "api_token": "dummy-token",
                "branch_name": "main",
                "folder_pattern": {"deny": ["^Acme/Sandbox.*"]},
                "process_pattern": {"deny": [".* Test$"]},
                "environment_pattern": {"deny": ["^Dev$"]},
                "max_requests_per_second": 10,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_ingestion(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    output_path = tmp_path / "boomi_mces.json"
    with requests_mock.Mocker() as mock:
        _register_mocks(mock)
        pipeline = Pipeline.create(_recipe(output_path))
        pipeline.run()
        pipeline.raise_from_status()

        query_more = [r for r in mock.request_history if r.path.endswith("/querymore")]
        assert len(query_more) == 1
        assert query_more[0].text == "page-2-token"
        assert query_more[0].headers["Content-Type"] == "text/plain"

    report = pipeline.source.get_report()
    assert isinstance(report, BoomiSourceReport)
    assert report.processes_emitted == 3
    assert report.processes_duplicate_branch_copies == 1
    assert sorted(report.processes_undeployed_skipped) == [
        "Legacy Cleanup",
        "Shipment Notify",
    ]
    assert sorted(report.processes_filtered) == ["Orders Sync Test", "Sandbox Probe"]

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_DIR / "boomi_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_stateful_removes_undeployed_process(
    tmp_path: Path, mock_datahub_graph: MagicMock
) -> None:
    recipe = _recipe(tmp_path / "run1.json")
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
    recipe["pipeline_name"] = "boomi_stateful_test"

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

        # The invoice export loses its only deployment between runs.
        _register_mocks(
            mock,
            deployments=[
                d for d in DEPLOYMENTS if d["componentId"] != "p-invoice-export"
            ],
        )
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
        "urn:li:dataFlow:(boomi,p-invoice-export,PROD)",
        "urn:li:dataJob:(urn:li:dataFlow:(boomi,p-invoice-export,PROD),process)",
    }
    soft_deleted = {
        mcp["entityUrn"]
        for mcp in json.loads((tmp_path / "run2.json").read_text())
        if mcp.get("aspectName") == "status" and mcp["aspect"]["json"]["removed"]
    }
    assert soft_deleted == removed
