"""Power BI ingestion with ``metadata_api_mode: workspace_apis``.

Semantic-model and report metadata come from mocked Fabric ``getDefinition``
responses built from the TMDL / PBIR fixtures under
``mock_data/fabric_definitions`` instead of the admin Scanner API.
"""

import base64
import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceReport,
)
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
# Second workspace of the default group listing; it hosts a composite model
# whose remote tables read the first workspace's Insurance Model.
SECOND_WORKSPACE_ID = "64ED5CAD-7C22-4684-8180-826122881108"
COMPOSITE_MODEL_ID = "8B2C3D4E-5F6A-4B7C-9D8E-0F1A2B3C4D5E"
# Lower-cased, as the Fabric item listing returns it; the TMDL fixture's
# PowerPlatform.Dataflows reference carries it upper-cased to exercise the
# case-insensitive lookup.
DATAFLOW_ID = "8b9c0d1e-2f3a-4b4c-8d5e-6f7a8b9c0d1e"

PBI = "https://api.powerbi.com/v1.0/myorg"
FABRIC = "https://api.fabric.microsoft.com/v1"
LRO_LOCATION = f"{FABRIC}/operations/0D1E2F3A-4B5C-4D6E-8F70-8192A3B4C5D6"


@pytest.fixture
def mock_time():
    return datetime.datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S")


def definition_response(
    fixture_dir: Path, definition_format: Optional[str]
) -> Dict[str, Any]:
    """``definition_format`` is None for item kinds whose getDefinition has
    a single, unnamed format (dataflows)."""
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
    definition: Dict[str, Any] = {"parts": parts}
    if definition_format is not None:
        definition["format"] = definition_format
    return {"definition": definition}


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
                        "requestId": "0000000e-1111-4222-8333-00000000000e",
                        "id": 14998777,
                        "refreshType": "Scheduled",
                        "startTime": "2022-02-02T20:56:55.927Z",
                        "endTime": "2022-02-02T20:57:05.053Z",
                        "status": "Failed",
                        "serviceExceptionJson": '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
                    },
                    {
                        "requestId": "0000000d-1111-4222-8333-00000000000d",
                        "id": 14998776,
                        "refreshType": "Scheduled",
                        "startTime": "2022-02-01T20:56:55.927Z",
                        "endTime": "2022-02-01T20:57:05.053Z",
                        "status": "Completed",
                    },
                ]
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/datasets/{IMPORT_MODEL_ID}/refreshSchedule": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "days": ["Monday", "Wednesday", "Friday"],
                "times": ["07:00", "16:30"],
                "enabled": True,
                "localTimeZoneId": "UTC",
                "notifyOption": "MailOnFailure",
            },
        },
        f"{PBI}/groups/{WORKSPACE_ID}/datasets/{DIRECTLAKE_MODEL_ID}/refreshSchedule": {
            "method": "GET",
            "status_code": 404,
            "json": {"error": {"code": "ItemNotFound"}},
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
                    {
                        "id": DATAFLOW_ID,
                        "type": "Dataflow",
                        "displayName": "Policies Dataflow",
                        "description": "Policies curated for reporting",
                        "workspaceId": WORKSPACE_ID,
                    },
                ]
            },
        },
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/dataflows/{DATAFLOW_ID}/getDefinition": {
            "method": "POST",
            "status_code": 200,
            "json": definition_response(fixtures / "policies_dataflow", None),
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
        **second_workspace_mock_data(fixtures),
    }


def second_workspace_mock_data(fixtures: Path) -> Dict[str, Dict[str, Any]]:
    return {
        f"{PBI}/groups/{SECOND_WORKSPACE_ID}/datasets": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": COMPOSITE_MODEL_ID,
                        "name": "Exec Composite Model",
                        "description": "Composite model over the Insurance Model",
                        "configuredBy": "User2@foo.com",
                    }
                ]
            },
        },
        f"{PBI}/groups/{SECOND_WORKSPACE_ID}/datasets/{COMPOSITE_MODEL_ID}/parameters": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        f"{PBI}/groups/{SECOND_WORKSPACE_ID}/datasets/{COMPOSITE_MODEL_ID}/refreshes": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        f"{PBI}/groups/{SECOND_WORKSPACE_ID}/datasets/{COMPOSITE_MODEL_ID}/refreshSchedule": {
            "method": "GET",
            "status_code": 404,
            "json": {"error": {"code": "ItemNotFound"}},
        },
        f"{PBI}/groups/{SECOND_WORKSPACE_ID}/users": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "groupUserAccessRight": "Admin",
                        "displayName": "user2",
                        "emailAddress": "User2@foo.com",
                        "identifier": "User2@foo.com",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3REE2",
                        "principalType": "User",
                    }
                ]
            },
        },
        f"{PBI}/groups/{SECOND_WORKSPACE_ID}/reports": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        f"{FABRIC}/workspaces/{SECOND_WORKSPACE_ID}/items": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": COMPOSITE_MODEL_ID,
                        "type": "SemanticModel",
                        "displayName": "Exec Composite Model",
                        "workspaceId": SECOND_WORKSPACE_ID,
                    }
                ]
            },
        },
        f"{FABRIC}/workspaces/{SECOND_WORKSPACE_ID}/semanticModels/{COMPOSITE_MODEL_ID}/getDefinition": {
            "method": "POST",
            "status_code": 200,
            "json": definition_response(fixtures / "exec_composite_model", "TMDL"),
        },
    }


def _upstream_lineages(output_path: str) -> Dict[str, Dict[str, Any]]:
    with open(output_path) as f:
        return {
            mcp["entityUrn"]: mcp["aspect"]["json"]
            for mcp in json.load(f)
            if mcp.get("aspectName") == "upstreamLineage"
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
            "workspace_id_pattern": {"allow": [WORKSPACE_ID, SECOND_WORKSPACE_ID]},
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
            extract_report_visuals=True,
            extract_refresh_history=True,
            extract_dax_reference_lineage=True,
            extract_model_properties=True,
            extract_composite_model_lineage=True,
            extract_dataflows=True,
        ),
    )
    pipeline.raise_from_status()
    output_path = f"{tmp_path}/powerbi_workspace_apis_mces.json"

    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.fabric_definitions_fetched == 4  # three models + one report
    assert report.fabric_definitions_failed == 0
    assert report.tmdl_tables_parsed == 9
    assert report.relationships_parsed == 2
    assert report.dataflow_definitions_fetched == 1
    assert report.dataflows_fetched == 1
    assert report.dataflow_entities_parsed == 2
    assert report.dataflow_queries_unparseable == 0
    # The Insurance model's "Dataflow Policies" table reads the dataflow;
    # counted once even though the model is mapped twice (report + tile).
    assert report.dataflow_references_resolved == 1
    assert report.dataflow_references_not_ingested == 0
    # One role file in the insurance model; the DirectLake model has none.
    assert report.rls_roles_parsed == 1
    assert report.tmdl_roles_failed == 0
    # The insurance model's two parameters; the DirectLake model has none.
    assert report.dataset_parameters_emitted == 2
    # Fields with at least one non-default property; `summarizeBy: none` and
    # bare calculated columns do not count. 15 in the insurance model plus 5 in
    # the composite executive model.
    assert report.model_fields_with_properties == 20
    assert report.rls_filters_unresolved == 0
    with open(output_path) as f:
        mcps = json.load(f)
    properties = {
        mcp["entityUrn"]: mcp["aspect"]["json"]["customProperties"]
        for mcp in mcps
        if mcp.get("aspectName") == "datasetProperties"
        and mcp.get("changeType") == "UPSERT"
    }
    policies = properties[
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Insurance_Model.Policies,DEV)"
    ]
    assert json.loads(policies["rlsRoles"]) == [
        {"role": "Regional Managers", "filter": "'Policies'[State] = \"CA\""}
    ]
    assert policies["storageMode"] == "Import"
    assert policies["parameter.Parameter - Source"] == "my-test-project"
    claims = properties[
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Insurance_Model.Claims,DEV)"
    ]
    assert claims["incrementalRefresh"] == "true"
    assert "rlsRoles" not in claims
    # The role file's member is an identity and must never reach the sink.
    assert "regional-managers" not in json.dumps(mcps)
    assert report.report_definitions_parsed == 1
    # The page resolves six distinct fields (visuals, visual filters and the
    # report/page filter targets, deduplicated); the fixture's
    # `NotInModel[Ghost]` does not.
    assert report.report_field_references_resolved == 6
    assert report.report_field_references_unresolved == 1
    # The visual charts resolve their own fields again (1 + 3 + 1 + 3) and
    # fail on `NotInModel[Ghost]` once more.
    assert report.report_visual_field_references_resolved == 8
    assert report.report_visual_field_references_unresolved == 1
    assert report.refresh_history_fetched == 3
    # Only the fixture's `'Budget'[Amount]` points outside the model; it is
    # counted once although its dataset is mapped twice (workspace, report).
    assert report.dax_references_unresolved == 1
    assert Constant.DAX_REFERENCE_NOT_RESOLVED in [w.title for w in report.warnings]
    # The composite model's Policies resolves to the Insurance Model across
    # workspaces; its Regions has no such table there and gets no edge.
    assert report.composite_model_sources_resolved == 1
    assert report.composite_model_sources_not_ingested == 0
    assert report.composite_model_source_tables_not_found == 1
    assert report.composite_model_tables_linked == 1
    assert report.composite_model_columns_unresolved == 0
    assert Constant.COMPOSITE_MODEL_SOURCE_TABLE_NOT_FOUND in [
        w.title for w in report.warnings
    ]
    lineages = _upstream_lineages(output_path)
    remote_policies = (
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Exec_Composite_Model.Policies,DEV)"
    )
    source_policies = (
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Insurance_Model.Policies,DEV)"
    )
    assert [u["dataset"] for u in lineages[remote_policies]["upstreams"]] == [
        source_policies
    ]
    # The mirrored columns map one-to-one onto the source table's columns; the
    # calculated PremiumInThousands only has its DAX edge within this table.
    assert {
        (edge["downstreams"][0], edge["upstreams"][0])
        for edge in lineages[remote_policies]["fineGrainedLineages"]
        if source_policies in edge["upstreams"][0]
    } == {
        (
            make_schema_field_urn(remote_policies, column),
            make_schema_field_urn(source_policies, column),
        )
        for column in ("PolicyId", "State", "WrittenPremium")
    }
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Exec_Composite_Model.Regions,DEV)"
        not in lineages
    )
    assert report.datasets_not_refreshable == 0
    assert report.refresh_schedule_fetched == 1
    assert report.refresh_schedule_not_supported == 0
    # The DirectLake and the composite model have no schedule.
    assert report.refresh_schedule_not_configured == 2
    assert report.refresh_schedule_failed == 0
    # One report-level, one page-level and two visual-level filters.
    assert report.report_filters_parsed == 4
    assert report.report_filters_failed == 0
    assert report.report_visual_filters_failed == 0
    assert report.report_visuals_emitted == 4
    assert report.report_visuals_without_fields == 0
    assert report.report_visuals_fields_unresolved == 0
    # The fixture's visual group is not a chart.
    assert report.report_visual_groups_skipped == 1

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
def test_workspace_apis_field_usage_without_visuals(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    """With `extract_report_field_usage` on and `extract_report_visuals` left
    at its default, the output is what the connector emitted before visual
    charts existed: the golden was produced from that code."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"
    output_name = "powerbi_workspace_apis_field_usage_mces.json"
    pipeline = run_pipeline(
        pytestconfig,
        requests_mock,
        tmp_path,
        output_name,
        workspace_apis_source_config(
            extract_model_relationships=True,
            extract_report_field_usage=True,
        ),
    )
    pipeline.raise_from_status()
    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.report_field_references_resolved == 6
    assert report.report_field_references_unresolved == 1
    assert report.report_filters_parsed == 2
    assert report.report_filters_failed == 0

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{output_name}",
        golden_path=f"{test_resources_dir}/golden_test_workspace_apis_field_usage.json",
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
        mcps = json.load(f)
    aspects = {mcp["aspectName"] for mcp in mcps if "aspectName" in mcp}
    assert "inputFields" not in aspects
    assert "operation" not in aspects
    # Without extract_report_visuals only page charts exist, no visual charts.
    chart_urns = {mcp["entityUrn"] for mcp in mcps if mcp.get("entityType") == "chart"}
    assert chart_urns
    assert not any("visuals." in urn for urn in chart_urns)
    with open(output_path) as f:
        schemas = [
            mcp["aspect"]["json"]
            for mcp in json.load(f)
            if mcp.get("aspectName") == "schemaMetadata"
        ]
    assert schemas
    assert all(not schema.get("foreignKeys") for schema in schemas)
    # Without extract_model_properties no field carries TMDL properties and
    # the tables' custom properties stay as they were.
    assert all(
        not field.get("jsonProps") and not field.get("isPartOfKey")
        for schema in schemas
        for field in schema["fields"]
    )
    with open(output_path) as f:
        custom_properties = [
            mcp["aspect"]["json"]["customProperties"]
            for mcp in json.load(f)
            if mcp.get("aspectName") == "datasetProperties"
            and mcp.get("changeType") == "UPSERT"
        ]
    assert custom_properties
    assert all(set(props) == {"datasetId"} for props in custom_properties)
    # Without extract_dax_reference_lineage no DAX-derived column edges exist:
    # none between fields of one dataset and none into a measure from a measure.
    with open(output_path) as f:
        lineages = [
            (mcp["entityUrn"], mcp["aspect"]["json"])
            for mcp in json.load(f)
            if mcp.get("aspectName") == "upstreamLineage"
        ]
    assert lineages
    loss_ratio = make_schema_field_urn(
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Insurance_Model.Insurance_KPIs,DEV)",
        "Loss Ratio",
    )
    for entity_urn, lineage in lineages:
        for edge in lineage.get("fineGrainedLineages") or []:
            assert not any(entity_urn in upstream for upstream in edge["upstreams"])
            assert loss_ratio not in edge["downstreams"]
    # Without the report definition no report getDefinition call is made, and
    # without extract_dataflows no dataflow definition is fetched either.
    assert not any(
        request.url.endswith(f"/reports/{REPORT_ID}/getDefinition")
        or request.url.endswith(f"/dataflows/{DATAFLOW_ID}/getDefinition")
        for request in requests_mock.request_history
    )
    # Without extract_composite_model_lineage the composite model's remote
    # tables are plain tables: no edge to the Insurance Model, no warning.
    lineage_urns = {urn for urn, _ in lineages}
    assert not any("Exec_Composite_Model.Policies" in urn for urn in lineage_urns)
    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.composite_model_sources_not_ingested == 0
    assert Constant.COMPOSITE_MODEL_SOURCE_NOT_INGESTED not in [
        w.title for w in report.warnings
    ]
    # The model table that reads the dataflow gets no edge (no phantom
    # lineage), only a warning.
    assert report.dataflow_references_not_ingested == 1  # counted once
    assert "Dataflow Not Ingested" in {w.title for w in report.warnings}
    with open(output_path) as f:
        assert not any("dataflows." in mcp["entityUrn"] for mcp in json.load(f))


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
    assert report.fabric_definitions_fetched == 2
    # A 403 zeroes out that model's tables, so the run is red, not green.
    assert [f.title for f in report.failures] == ["Fabric API Permission Denied"]

    with open(f"{tmp_path}/powerbi_forbidden_mces.json") as f:
        urns = {mcp["entityUrn"] for mcp in json.load(f)}
    assert any("Insurance_Model.Policies" in urn for urn in urns)
    assert not any("Claims_DirectLake_Model" in urn for urn in urns)


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_composite_model_over_a_forbidden_source_model_gets_no_edge(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    # The Insurance Model is listed (so its name is in the index) but its
    # definition is refused: the composite model must not get an edge to a
    # table nothing emits.
    register_workspace_apis_mocks(pytestconfig, requests_mock)
    requests_mock.register_uri(
        "POST",
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{IMPORT_MODEL_ID}/getDefinition",
        status_code=403,
        json={"errorCode": "Unauthorized"},
    )
    output_path = f"{tmp_path}/powerbi_forbidden_source_mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": workspace_apis_source_config(
                    extract_composite_model_lineage=True
                ),
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()

    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.composite_model_sources_not_ingested == 1
    assert report.composite_model_sources_resolved == 0
    assert report.composite_model_tables_linked == 0
    assert Constant.COMPOSITE_MODEL_SOURCE_NOT_INGESTED in [
        w.title for w in report.warnings
    ]
    lineages = _upstream_lineages(output_path)
    assert not any("Exec_Composite_Model.Policies" in urn for urn in lineages)
