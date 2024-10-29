from datetime import datetime
from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.schema_classes import AuditStampClass, OwnerClass, OwnershipClass
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-03 07:00:00"


def mock_existing_users(*args, **kwargs):
    return OwnershipClass(
        owners=[OwnerClass(owner="urn:li:corpuser:TEST_USER", type="TECHNICAL_OWNER")],
        lastModified=AuditStampClass(time=0, actor="urn:li:corpuser:unknown"),
    )


def mock_user_to_add(*args, **kwargs):
    return None


def register_mock_api(request_mock, override_mock_data={}):
    api_vs_response = {
        "https://host_port/Reports/api/v2.0/Reports": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "Id": "ee56dc21-248a-4138-a446-ee5ab1fc938a",
                        "Name": "Testa",
                        "Description": None,
                        "Path": "/path/to/Testa",
                        "Type": "Report",
                        "Hidden": False,
                        "Size": 1010101,
                        "ModifiedBy": "TEST_USER",
                        "ModifiedDate": str(datetime.now()),
                        "CreatedBy": "TEST_USER",
                        "CreatedDate": str(datetime.now()),
                        "ParentFolderId": "47495172-89ab-455f-a446-fffd3cf239ca",
                        "IsFavorite": False,
                        "ContentType": None,
                        "Content": "",
                        "HasDataSources": True,
                        "Roles": [],
                        "HasSharedDataSets": True,
                        "HasParameters": True,
                    },
                ]
            },
        },
        "https://host_port/Reports/api/v2.0/LinkedReports": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "Id": "ee56dc21-248a-4138-a446-ee5ab1fc938c",
                        "Name": "Testc",
                        "Description": None,
                        "Path": "/path/to/Testc",
                        "Type": "LinkedReport",
                        "Hidden": False,
                        "Size": 1010101,
                        "ModifiedBy": "TEST_USER",
                        "ModifiedDate": str(datetime.now()),
                        "CreatedBy": "TEST_USER",
                        "CreatedDate": str(datetime.now()),
                        "ParentFolderId": "47495172-89ab-455f-a446-fffd3cf239cc",
                        "IsFavorite": False,
                        "ContentType": None,
                        "Content": "",
                        "HasDataSources": True,
                        "Roles": [],
                        "HasParameters": True,
                        "Link": "sjfgnk-7134-1234-abcd-ee5axvcv938b",
                    },
                ]
            },
        },
        "https://host_port/Reports/api/v2.0/PowerBIReports": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "Id": "ee56dc21-248a-4138-a446-ee5ab1fc938d",
                        "Name": "Testd",
                        "Description": None,
                        "Path": "/path/to/Testd",
                        "Type": "PowerBIReport",
                        "Hidden": False,
                        "Size": 1010101,
                        "ModifiedBy": "TEST_USER",
                        "ModifiedDate": str(datetime.now()),
                        "CreatedBy": "TEST_USER",
                        "CreatedDate": str(datetime.now()),
                        "ParentFolderId": "47495172-89ab-455f-a446-fffd3cf239cd",
                        "IsFavorite": False,
                        "ContentType": None,
                        "Content": "",
                        "HasDataSources": True,
                        "Roles": [],
                    },
                ]
            },
        },
    }

    api_vs_response.update(override_mock_data)

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def default_source_config():
    return {
        "username": "foo",
        "password": "bar",
        "workstation_name": "workstation",
        "host_port": "host_port",
        "server_alias": "server_alias",
        "graphql_url": None,
        "report_virtual_directory_name": "Reports",
        "report_server_virtual_directory_name": "ReportServer",
        "env": "DEV",
    }


def get_default_recipe(output_path: str) -> dict:
    return {
        "run_id": "powerbi-report-server-test",
        "source": {
            "type": "powerbi-report-server",
            "config": {
                **default_source_config(),
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": output_path},  # ,
        },
    }


def add_mock_method_in_pipeline(pipeline: Pipeline) -> None:
    pipeline.ctx.graph = mock.MagicMock()
    pipeline.ctx.graph.get_ownership = mock.MagicMock()
    pipeline.ctx.graph.get_ownership.side_effect = mock_existing_users
    pipeline.ctx.graph.get_aspect_v2 = mock.MagicMock()
    pipeline.ctx.graph.get_aspect_v2.side_effect = mock_user_to_add


@freeze_time(FROZEN_TIME)
@mock.patch("requests_ntlm.HttpNtlmAuth")
def test_powerbi_ingest(mock_msal, pytestconfig, tmp_path, mock_time, requests_mock):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/powerbi_report_server"
    )

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        get_default_recipe(output_path=f"{tmp_path}/powerbi_report_server_mces.json")
    )

    add_mock_method_in_pipeline(pipeline=pipeline)

    pipeline.run()
    pipeline.raise_from_status()

    golden_file = "golden_test_ingest.json"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_report_server_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("requests_ntlm.HttpNtlmAuth")
def test_powerbi_ingest_with_failure(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/powerbi_report_server"
    )

    register_mock_api(
        request_mock=requests_mock,
        override_mock_data={
            "https://host_port/Reports/api/v2.0/LinkedReports": {
                "method": "GET",
                "status_code": 404,
                "json": {"error": "Request Failed"},
            }
        },
    )

    pipeline = Pipeline.create(
        get_default_recipe(output_path=f"{tmp_path}/powerbi_report_server_mces.json")
    )

    add_mock_method_in_pipeline(pipeline=pipeline)

    pipeline.run()
    pipeline.raise_from_status()

    golden_file = "golden_test_fail_api_ingest.json"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_report_server_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )
