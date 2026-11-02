import copy
import json
import re
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

FROZEN_TIME = "2026-11-02 20:00:00"
ACCOUNT_ID = "acme-AB1CD2"
BASE_URL = f"https://api.boomi.com/api/rest/v1/{ACCOUNT_ID}"
GOLDEN_DIR = Path(__file__).parent / "golden"
# Responses captured from a Boomi trial account and anonymised: the account id
# and user emails are replaced and encrypted passwords zeroed.
FIXTURES_DIR = Path(__file__).parent / "fixtures"

ORDERS_EXTRACT = "e1b56fb6-0dae-4c33-9a9d-e5af9b8aedbc"
CATALOG_SYNC = "6363d3f8-93fa-4c4d-a399-45bde722ee63"

# The ids below are not in the captures. Their records are copied from
# captured ones, so the shape stays real, and cover cases the trial account
# does not have.
INVOICE_EXPORT = "5b1e0c2a-6f0d-4c1e-9a43-000000000001"
SHIPMENT_NOTIFY = "5b1e0c2a-6f0d-4c1e-9a43-000000000002"
SANDBOX_PROBE = "5b1e0c2a-6f0d-4c1e-9a43-000000000003"
ORDERS_EXTRACT_TEST = "5b1e0c2a-6f0d-4c1e-9a43-000000000004"
SANDBOX_FOLDER = "Rjo4OTQzNDk5"
DEV_ENVIRONMENT = "9c0a7e51-2d4b-4f3e-8a61-000000000001"

START_STOP_SHAPES = (
    '<shape image="start" name="shape1" shapetype="start" userlabel="" x="48.0" y="48.0">'
    "<configuration><noaction/></configuration><dragpoints>"
    '<dragpoint name="shape1.dragpoint1" toShape="shape2" x="257.0" y="56.0"/>'
    "</dragpoints></shape>"
    '<shape image="stop_icon" name="shape2" shapetype="stop" userlabel="" x="273.0" y="48.0">'
    '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
)


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


def _records(name: str) -> List[Dict[str, object]]:
    return json.loads(_fixture(name))["result"]


def _copy(record: Dict[str, object], **overrides: object) -> Dict[str, object]:
    copied = copy.deepcopy(record)
    copied.update(overrides)
    return copied


def _process_xml(
    component_id: str, name: str, description: str, shapes: str = START_STOP_SHAPES
) -> str:
    xml = _fixture(f"components/{ORDERS_EXTRACT}.xml")
    xml = xml.replace(ORDERS_EXTRACT, component_id)
    xml = xml.replace('name="DH Test - Orders Extract"', f'name="{name}"')
    xml = re.sub(
        "<bns:description>.*?</bns:description>",
        f"<bns:description>{description}</bns:description>",
        xml,
    )
    return re.sub("<shapes>.*</shapes>", f"<shapes>{shapes}</shapes>", xml, flags=re.S)


FOLDERS = _records("folder_query.json")
FOLDERS.extend(
    [
        _copy(
            FOLDERS[-1],
            id=SANDBOX_FOLDER,
            name="Sandbox",
            fullPath="Test Account/DataHub Test/Sandbox",
        ),
        _copy(
            FOLDERS[-1],
            id="Rjo4OTQzNDk4",
            name="Archive",
            fullPath="Test Account/DataHub Test/Archive",
            deleted=True,
        ),
    ]
)

ENVIRONMENTS = _records("environment_query.json")
ENVIRONMENTS.append(_copy(ENVIRONMENTS[0], id=DEV_ENVIRONMENT, name="DH Test Dev Env"))

DEPLOYMENTS = _records("deployed_package_query.json")
DEPLOYMENTS.extend(
    [
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000001",
            componentId=INVOICE_EXPORT,
            packageVersion="2.0",
        ),
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000002",
            componentId=SANDBOX_PROBE,
        ),
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000003",
            componentId=ORDERS_EXTRACT_TEST,
        ),
        # Only deployed to an environment denied by environment_pattern.
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000004",
            componentId=SHIPMENT_NOTIFY,
            environmentId=DEV_ENVIRONMENT,
        ),
    ]
)

PROCESSES_PAGE_1 = _records("component_metadata_query.json")
_PROCESS_TEMPLATE = PROCESSES_PAGE_1[0]
PROCESSES_PAGE_2 = [
    _copy(
        _PROCESS_TEMPLATE,
        componentId=INVOICE_EXPORT,
        name="DH Test - Invoice Export",
        version=["Long", 3],
        modifiedBy="dev@example.com",
        modifiedDate="2026-11-02T19:30:00Z",
    ),
    # A stale branch copy of Orders Extract; the newer copy on page 1 wins.
    _copy(
        _PROCESS_TEMPLATE,
        version=4,
        modifiedBy="dev@example.com",
        modifiedDate="2026-10-20T10:00:00Z",
    ),
    _copy(
        _PROCESS_TEMPLATE,
        componentId=SHIPMENT_NOTIFY,
        name="DH Test - Shipment Notify",
    ),
    _copy(
        _PROCESS_TEMPLATE,
        componentId=SANDBOX_PROBE,
        name="DH Test - Sandbox Probe",
        folderName="Sandbox",
        folderId=SANDBOX_FOLDER,
    ),
    _copy(
        _PROCESS_TEMPLATE,
        componentId=ORDERS_EXTRACT_TEST,
        name="DH Test - Orders Extract Test",
    ),
]

COMPONENT_XML: Dict[str, str] = {
    path.stem: path.read_text()
    for path in sorted((FIXTURES_DIR / "components").glob("*.xml"))
}
COMPONENT_XML[INVOICE_EXPORT] = _process_xml(
    INVOICE_EXPORT,
    "DH Test - Invoice Export",
    "Exports posted invoices to the finance warehouse.",
)
COMPONENT_XML[SHIPMENT_NOTIFY] = _process_xml(
    SHIPMENT_NOTIFY, "DH Test - Shipment Notify", ""
)


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
                "folder_pattern": {"deny": [".*/Sandbox$"]},
                "process_pattern": {"deny": [".* Test$"]},
                "environment_pattern": {"deny": [".* Dev Env$"]},
                "include_undeployed_processes": True,
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
    assert report.folders_emitted == 5
    assert report.processes_emitted == 7
    assert report.processes_duplicate_branch_copies == 1
    assert list(report.processes_undeployed_skipped) == []
    assert list(report.environments_filtered) == ["DH Test Dev Env"]
    assert sorted(report.processes_filtered) == [
        "DH Test - Orders Extract Test",
        "DH Test - Sandbox Probe",
    ]
    assert not report.warnings

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
    source_config["include_undeployed_processes"] = False
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

        # Catalog Sync loses its only deployment between runs.
        _register_mocks(
            mock,
            deployments=[d for d in DEPLOYMENTS if d["componentId"] != CATALOG_SYNC],
        )
        recipe["sink"] = {
            "type": "file",
            "config": {"filename": str(tmp_path / "run2.json")},
        }
        pipeline_run2 = run_and_get_pipeline(recipe)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

    report = pipeline_run1.source.get_report()
    assert isinstance(report, BoomiSourceReport)
    # The trial account's two never-deployed processes, plus one deployed only
    # to a denied environment.
    assert sorted(report.processes_undeployed_skipped) == [
        "DH Test - SFTP Pickup",
        "DH Test - Salesforce Accounts",
        "DH Test - Shipment Notify",
    ]
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
        f"urn:li:dataFlow:(boomi,{CATALOG_SYNC},PROD)",
        f"urn:li:dataJob:(urn:li:dataFlow:(boomi,{CATALOG_SYNC},PROD),process)",
    }
    soft_deleted = {
        mcp["entityUrn"]
        for mcp in json.loads((tmp_path / "run2.json").read_text())
        if mcp.get("aspectName") == "status" and mcp["aspect"]["json"]["removed"]
    }
    assert soft_deleted == removed
