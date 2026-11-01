"""Power BI admin-scan ingestion with ``enrich_from_item_definitions``.

The Admin Scanner API (the default mock response, with columns added to two
tables) builds the datasets; the mocked Fabric ``getDefinition`` responses
built from ``mock_data/fabric_definitions/library_dataset`` overlay
sourceColumn names, descriptions, relationships and model properties.
"""

import copy
import datetime
import json
from typing import Any, Dict, List
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceReport,
)
from datahub.testing import mce_helpers
from tests.integration.powerbi.powerbi_mocks import (
    default_source_config,
    mock_msal_cca,
    read_mock_data,
    register_mock_api,
)
from tests.integration.powerbi.test_workspace_apis_mode import definition_response

pytestmark = pytest.mark.integration_batch_4
FROZEN_TIME = "2022-02-03 07:00:00"

WORKSPACE_ID = "64ED5CAD-7C10-4684-8180-826122881108"
LIBRARY_DATASET_ID = "05169CD2-E713-41E6-9600-1D8066D95445"
HR_DATASET_ID = "ba0130a1-5b03-40de-9535-b34e778ea6ed"
EMPLOYEE_DATASET_ID = "91580e0e-1680-4b1c-bbf9-4f6764d7a5ff"
SCAN_RESULT_URL = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/4674efd1-603c-4129-8d82-03cf2be05aff"
FABRIC = "https://api.fabric.microsoft.com/v1"

SNOWFLAKE_TABLE_COLUMNS: List[Dict[str, Any]] = [
    {"name": "ID", "dataType": "Int64", "isHidden": False, "columnType": "Data"},
    {"name": "NAME", "dataType": "String", "isHidden": False, "columnType": "Data"},
    {"name": "PRICE", "dataType": "Double", "isHidden": False, "columnType": "Data"},
    {
        "name": "IS_PREMIUM",
        "dataType": "String",
        "isHidden": False,
        "columnType": "Calculated",
        "expression": 'IF(\'SNOWFLAKE_TESTTABLE\'[PRICE] > 100, "Yes", "No")',
    },
]
SNOWFLAKE_TABLE_MEASURES: List[Dict[str, Any]] = [
    {
        "name": "Total Price",
        "expression": "SUM('SNOWFLAKE_TESTTABLE'[PRICE])",
        "isHidden": False,
    }
]
POSTGRES_TABLE_COLUMNS: List[Dict[str, Any]] = [
    {"name": "id", "dataType": "Int64", "isHidden": False, "columnType": "Data"},
    {
        "name": "snowflake_id",
        "dataType": "Int64",
        "isHidden": False,
        "columnType": "Data",
    },
    {"name": "note", "dataType": "String", "isHidden": False, "columnType": "Data"},
]


@pytest.fixture
def mock_time():
    return datetime.datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S")


def scan_result_with_columns(pytestconfig: pytest.Config) -> Dict[str, Any]:
    """The default scan response with columns and a measure on two tables of
    the library dataset, so the definition has something to overlay."""
    default_mock = read_mock_data(
        pytestconfig.rootpath
        / "tests/integration/powerbi/mock_data/default_mock_response.json"
    )
    scan_entry = copy.deepcopy(default_mock[SCAN_RESULT_URL])
    tables_by_name: Dict[str, Dict[str, Any]] = {}
    for workspace in scan_entry["json"]["workspaces"]:
        for dataset in workspace.get("datasets") or []:
            if dataset["id"] == LIBRARY_DATASET_ID:
                for table in dataset["tables"]:
                    tables_by_name[table["name"]] = table
    tables_by_name["SNOWFLAKE_TESTTABLE"]["columns"] = SNOWFLAKE_TABLE_COLUMNS
    tables_by_name["SNOWFLAKE_TESTTABLE"]["measures"] = SNOWFLAKE_TABLE_MEASURES
    tables_by_name["postgres_test_table"]["columns"] = POSTGRES_TABLE_COLUMNS
    return scan_entry


def enrichment_mock_data(pytestconfig: pytest.Config) -> Dict[str, Dict[str, Any]]:
    fixtures = (
        pytestconfig.rootpath / "tests/integration/powerbi/mock_data/fabric_definitions"
    )
    return {
        SCAN_RESULT_URL: scan_result_with_columns(pytestconfig),
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{LIBRARY_DATASET_ID}/getDefinition": {
            "method": "POST",
            "status_code": 200,
            "json": definition_response(fixtures / "library_dataset", "TMDL"),
        },
        # The principal is not a member of these models' workspace / the
        # operation fails: both only lose the enrichment.
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{HR_DATASET_ID}/getDefinition": {
            "method": "POST",
            "status_code": 403,
            "json": {"errorCode": "Unauthorized"},
        },
        f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{EMPLOYEE_DATASET_ID}/getDefinition": {
            "method": "POST",
            "status_code": 500,
            "json": {"errorCode": "InternalError"},
        },
    }


def enrichment_source_config(**overrides: Any) -> Dict[str, Any]:
    config = default_source_config()
    config.update(
        {
            "enrich_from_item_definitions": True,
            "extract_model_relationships": True,
            "extract_model_properties": True,
        }
    )
    config.update(overrides)
    return config


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_admin_scan_enriched_from_item_definitions(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data=enrichment_mock_data(pytestconfig),
    )
    output_path = f"{tmp_path}/powerbi_admin_scan_enrichment_mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {"type": "powerbi", "config": enrichment_source_config()},
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = pipeline.source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert report.fabric_definitions_fetched == 1
    assert report.fabric_definitions_failed == 2
    assert report.scan_datasets_enriched == 1
    assert report.scan_datasets_not_enriched == 2
    # Calendar is only in the definition; five scanned tables have no part.
    assert report.definition_tables_not_in_scan == 1
    assert report.scan_tables_not_in_definition == 5
    assert report.relationships_parsed == 2
    assert report.rls_roles_parsed == 1
    assert report.failures == []
    warning_titles = {w.title for w in report.warnings}
    assert Constant.ITEM_DEFINITION_NOT_FETCHED in warning_titles
    assert Constant.DEFINITION_TABLE_NOT_IN_SCAN in warning_titles
    assert Constant.SCAN_TABLE_NOT_IN_DEFINITION in warning_titles
    # The admin scan still ran; it is what built the datasets.
    assert any("/admin/" in request.url for request in requests_mock.request_history)
    # The definition only ever overlays; no table exists because of it.
    with open(output_path) as f:
        urns = {mcp["entityUrn"] for mcp in json.load(f) if "entityUrn" in mcp}
    assert not any("Calendar" in urn for urn in urns)
    # The role file's member is an identity and must never reach the sink.
    with open(output_path) as f:
        assert "analysts@example.com" not in f.read()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/golden_test_admin_scan_enrichment.json",
    )
