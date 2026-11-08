"""Power BI ingestion with ``metadata_api_mode: workspace_apis``.

Semantic-model and report metadata come from mocked Fabric ``getDefinition``
responses built from the TMDL / PBIR fixtures under
``mock_data/fabric_definitions`` instead of the admin Scanner API.
"""

import base64
import datetime
import json
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport
from datahub.testing import mce_helpers
from tests.integration.powerbi.powerbi_mocks import (
    default_source_config,
    mock_msal_cca,
    register_mock_api,
)

pytestmark = pytest.mark.integration_batch_2
FROZEN_TIME = "2022-02-03 07:00:00"

WORKSPACE_ID = "64ED5CAD-7C10-4684-8180-826122881108"
IMPORT_MODEL_ID = "05169CD2-E713-41E6-9600-1D8066D95445"
DIRECTLAKE_MODEL_ID = "7A1B2C3D-4E5F-4A6B-8C7D-9E0F1A2B3C4D"
LAKEHOUSE_ID = "3C4D5E6F-7A8B-4C9D-8E0F-1A2B3C4D5E6F"
SQL_ENDPOINT_ID = "6F2E7D8C-9B0A-4C1D-8E2F-3A4B5C6D7E8F"
REPORT_ID = "584cf13a-1485-41c2-a514-b1bb66fff163"

PBI = "https://api.powerbi.com/v1.0/myorg"
FABRIC = "https://api.fabric.microsoft.com/v1"
LRO_LOCATION = f"{FABRIC}/operations/0D1E2F3A-4B5C-4D6E-8F70-8192A3B4C5D6"


@pytest.fixture
def mock_time():
    return datetime.datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S")


def definition_response(fixture_dir: Path, definition_format: str) -> Dict[str, Any]:
    parts: List[Dict[str, str]] = []
    for path in sorted(fixture_dir.rglob("*")):
        if path.is_file():
            parts.append(
                {
                    "path": path.relative_to(fixture_dir).as_posix(),
                    "payload": base64.b64encode(path.read_bytes()).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
            )
    return {"definition": {"format": definition_format, "parts": parts}}


def workspace_apis_mock_data(fixtures: Path) -> Dict[str, Dict[str, Any]]:
    return {
        f"{PBI}/groups/{WORKSPACE_ID}/datasets": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": IMPORT_MODEL_ID,
                        "name": "Insurance Model",
                        "description": "Import model over the insurance warehouse",
                        "configuredBy": "User1@foo.com",
                        "webUrl": f"https://app.powerbi.com/groups/{WORKSPACE_ID}/datasets/{IMPORT_MODEL_ID}",
                    },
                    {
                        "id": DIRECTLAKE_MODEL_ID,
                        "name": "Claims DirectLake Model",
                        "configuredBy": "User1@foo.com",
                    },
                ]
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/datasets/{DIRECTLAKE_MODEL_ID}/parameters": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        f"{PBI}/groups/{WORKSPACE_ID}/datasets/{IMPORT_MODEL_ID}/refreshes": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "requestId": "0000000d-1111-4222-8333-00000000000d",
                        "id": 14998776,
                        "refreshType": "Scheduled",
                        "startTime": "2022-02-01T20:56:55.927Z",
                        "endTime": "2022-02-01T20:57:05.053Z",
                        "status": "Completed",
                    }
                ]
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/datasets/{DIRECTLAKE_MODEL_ID}/refreshes": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "requestId": "00000002-1111-4222-8333-000000000002",
                        "refreshType": "Scheduled",
                        "startTime": "2022-02-02T08:00:01.217Z",
                        "endTime": "2022-02-02T08:00:01.217Z",
                        "status": "Disabled",
                    }
                ]
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/users": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "groupUserAccessRight": "Admin",
                        "displayName": "user1",
                        "emailAddress": "User1@foo.com",
                        "identifier": "User1@foo.com",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3REE1",
                        "principalType": "User",
                    },
                    {
                        "groupUserAccessRight": "Viewer",
                        "displayName": "user2",
                        "emailAddress": "User2@foo.com",
                        "identifier": "User2@foo.com",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3REE2",
                        "principalType": "User",
                    },
                    {
                        "groupUserAccessRight": "Member",
                        "displayName": "ingestion-principal",
                        "identifier": "00000018-1111-4222-8333-000000000018",
                        "graphId": "00000018-1111-4222-8333-000000000018",
                        "principalType": "App",
                    },
                ]
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/reports": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": REPORT_ID,
                        "reportType": "PowerBIReport",
                        "name": "Loss Report",
                        "webUrl": f"https://app.powerbi.com/groups/{WORKSPACE_ID}/reports/{REPORT_ID}",
                        "embedUrl": f"https://app.powerbi.com/reportEmbed?reportId={REPORT_ID}",
                        "datasetId": IMPORT_MODEL_ID,
                        "description": "Loss ratio by state",
                    }
                ]
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/reports/{REPORT_ID}/pages": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "displayName": "Loss Overview",
                        "name": "a1b2c3d4e5f6a7b8c9d0",
                        "order": "0",
                    }
                ]
            },
        },
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/items": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": LAKEHOUSE_ID,
                        "type": "Lakehouse",
                        "displayName": "Claims_Lakehouse",
                        "workspaceId": WORKSPACE_ID,
                    },
                    {
                        "id": SQL_ENDPOINT_ID,
                        "type": "SQLEndpoint",
                        "displayName": "Claims_Lakehouse",
                        "workspaceId": WORKSPACE_ID,
                    },
                    {
                        "id": IMPORT_MODEL_ID,
                        "type": "SemanticModel",
                        "displayName": "Insurance Model",
                        "workspaceId": WORKSPACE_ID,
                    },
                    {
                        "id": DIRECTLAKE_MODEL_ID,
                        "type": "SemanticModel",
                        "displayName": "Claims DirectLake Model",
                        "workspaceId": WORKSPACE_ID,
                    },
                    {
                        "id": REPORT_ID,
                        "type": "Report",
                        "displayName": "Loss Report",
                        "workspaceId": WORKSPACE_ID,
                    },
                ]
            },
        },
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": LAKEHOUSE_ID,
                "type": "Lakehouse",
                "displayName": "Claims_Lakehouse",
                "properties": {"sqlEndpointProperties": {"id": SQL_ENDPOINT_ID}},
            },
        },
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{IMPORT_MODEL_ID}/getDefinition": {
            "method": "POST",
            "status_code": 200,
            "json": definition_response(fixtures / "insurance_model", "TMDL"),
        },
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition": {
            "method": "POST",
            "status_code": 200,
            "json": definition_response(fixtures / "loss_report", "PBIR"),
        },
        # The long-running-operation result for the DirectLake model; the 202
        # and the status poll are registered in register_lro_mocks because
        # they need response headers.
        f"{LRO_LOCATION}/result": {
            "method": "GET",
            "status_code": 200,
            "json": definition_response(fixtures / "directlake_model", "TMDL"),
        },
    }


def register_lro_mocks(requests_mock: Any) -> None:
    requests_mock.register_uri(
        "POST",
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{DIRECTLAKE_MODEL_ID}/getDefinition",
        status_code=202,
        headers={"Location": LRO_LOCATION, "Retry-After": "0"},
        text="",
    )
    requests_mock.register_uri(
        "GET",
        LRO_LOCATION,
        [
            {"json": {"status": "Running"}, "status_code": 200},
            {"json": {"status": "Succeeded"}, "status_code": 200},
        ],
    )


def workspace_apis_source_config(**overrides: Any) -> Dict[str, Any]:
    config = default_source_config()
    # The admin-scan test fixtures narrow dataset_type_mapping; this mode
    # should resolve Snowflake and Fabric OneLake upstreams.
    del config["dataset_type_mapping"]
    config.update(
        {
            "metadata_api_mode": "workspace_apis",
            "extract_reports": True,
            "extract_lineage": True,
            "extract_column_level_lineage": True,
            "enable_advance_lineage_sql_construct": True,
            "native_query_parsing": True,
            "extract_independent_datasets": True,
            "convert_lineage_urns_to_lowercase": True,
        }
    )
    config.update(overrides)
    return config


def register_workspace_apis_mocks(
    pytestconfig: pytest.Config, requests_mock: Any
) -> None:
    fixtures = (
        pytestconfig.rootpath / "tests/integration/powerbi/mock_data/fabric_definitions"
    )
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data=workspace_apis_mock_data(fixtures),
    )
    register_lro_mocks(requests_mock)


def run_pipeline(
    pytestconfig: pytest.Config,
    requests_mock: Any,
    tmp_path: str,
    output_name: str,
    source_config: Dict[str, Any],
) -> Pipeline:
    register_workspace_apis_mocks(pytestconfig, requests_mock)

    output_path = f"{tmp_path}/{output_name}"
    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {"type": "powerbi", "config": source_config},
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()
    return pipeline


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_workspace_apis_ingest_with_enrichment(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    pipeline = run_pipeline(
        pytestconfig,
        requests_mock,
        tmp_path,
        "powerbi_workspace_apis_mces.json",
        workspace_apis_source_config(
            extract_model_relationships=True,
            extract_report_field_usage=True,
            extract_refresh_history=True,
        ),
    )
    pipeline.raise_from_status()
    output_path = f"{tmp_path}/powerbi_workspace_apis_mces.json"

    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.fabric_definitions_fetched == 3  # two models + one report
    assert report.fabric_definitions_failed == 0
    assert report.tmdl_tables_parsed == 5
    assert report.relationships_parsed == 2
    assert report.report_definitions_parsed == 1
    # Four visual fields resolve; the fixture's `NotInModel[Ghost]` does not.
    assert report.report_field_references_resolved == 4
    assert report.report_field_references_unresolved == 1
    assert report.refresh_history_fetched == 2

    # The admin Scanner API must never be called in this mode.
    assert not any(
        "/admin/" in request.url for request in requests_mock.request_history
    )
    # Workspace users are fetched once and reused for every report/dashboard.
    assert (
        sum(
            request.url.endswith(f"/groups/{WORKSPACE_ID}/users")
            for request in requests_mock.request_history
        )
        == 1
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/golden_test_workspace_apis_ingest.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_workspace_apis_enrichment_flags_off(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    pipeline = run_pipeline(
        pytestconfig,
        requests_mock,
        tmp_path,
        "powerbi_workspace_apis_flags_off_mces.json",
        workspace_apis_source_config(),
    )
    pipeline.raise_from_status()
    output_path = f"{tmp_path}/powerbi_workspace_apis_flags_off_mces.json"

    with open(output_path) as f:
        aspects = {mcp["aspectName"] for mcp in json.load(f) if "aspectName" in mcp}
    assert "inputFields" not in aspects
    assert "operation" not in aspects
    with open(output_path) as f:
        schemas = [
            mcp["aspect"]["json"]
            for mcp in json.load(f)
            if mcp.get("aspectName") == "schemaMetadata"
        ]
    assert schemas
    assert all(not schema.get("foreignKeys") for schema in schemas)
    # Without the report definition no report getDefinition call is made.
    assert not any(
        request.url.endswith(f"/reports/{REPORT_ID}/getDefinition")
        for request in requests_mock.request_history
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_forbidden_model_fails_run_but_keeps_other_models(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    register_workspace_apis_mocks(pytestconfig, requests_mock)
    # Registered last so it overrides the LRO 202 for this model.
    requests_mock.register_uri(
        "POST",
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{DIRECTLAKE_MODEL_ID}/getDefinition",
        status_code=403,
        json={"errorCode": "Unauthorized"},
    )
    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {"type": "powerbi", "config": workspace_apis_source_config()},
            "sink": {
                "type": "file",
                "config": {"filename": f"{tmp_path}/powerbi_forbidden_mces.json"},
            },
        }
    )
    pipeline.run()

    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.fabric_definitions_failed == 1
    assert report.fabric_definitions_fetched == 1
    # A 403 zeroes out that model's tables, so the run is red, not green.
    assert [f.title for f in report.failures] == ["Fabric API Permission Denied"]

    with open(f"{tmp_path}/powerbi_forbidden_mces.json") as f:
        urns = {mcp["entityUrn"] for mcp in json.load(f)}
    assert any("Insurance_Model.Policies" in urn for urn in urns)
    assert not any("Claims_DirectLake_Model" in urn for urn in urns)
