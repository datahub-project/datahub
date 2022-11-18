from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-03 07:00:00"

call_number = 1


def mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def acquire_token_for_client(self, *args, **kwargs):
            return {
                "access_token": "dummy",
            }

    return MsalClient()


def scan_init_response(_request, _context):
    global call_number
    if call_number == 1:
        call_number += 1
        return {"id": "4674efd1-603c-4129-8d82-03cf2be05aff"}
    return {"id": "a674efd1-603c-4129-8d82-03cf2be05aff"}


def register_mock_api(request_mock):
    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/groups": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "64ED5CAD-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Workspace 1",
                        "type": "Workspace",
                    },
                    {
                        "id": "64ED5CAD-7C22-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Workspace 2",
                        "type": "Workspace",
                    },
                    {
                        "id": "64ED5CAD-7322-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "Workspace 2",
                        "type": "Workspace",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "7D668CAD-7FFC-4505-9215-655BCA5BEBAE",
                        "isReadOnly": True,
                        "displayName": "test_dashboard",
                        "embedUrl": "https://localhost/dashboards/embed/1",
                        "webUrl": "https://localhost/dashboards/web/1",
                    }
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C22-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "7D668CAD-8FFC-4505-9215-655BCA5BEBAE",
                        "isReadOnly": True,
                        "displayName": "test_dashboard2",
                        "embedUrl": "https://localhost/dashboards/embed/1",
                        "webUrl": "https://localhost/dashboards/web/1",
                    }
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/reports/5b218778-e7a5-4d73-8187-f10824047715/users": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "identifier": "User1@foo.com",
                        "displayName": "user1",
                        "emailAddress": "User1@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3CD46",
                        "principalType": "User",
                    },
                    {
                        "identifier": "User2@foo.com",
                        "displayName": "user2",
                        "emailAddress": "User2@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A5REWS",
                        "principalType": "User",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/users": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "identifier": "User1@foo.com",
                        "displayName": "user1",
                        "emailAddress": "User1@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3CD46",
                        "principalType": "User",
                    },
                    {
                        "identifier": "User2@foo.com",
                        "displayName": "user2",
                        "emailAddress": "User2@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A5REWS",
                        "principalType": "User",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "B8E293DC-0C83-4AA0-9BB9-0A8738DF24A0",
                        "title": "test_tile",
                        "embedUrl": "https://localhost/tiles/embed/1",
                        "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C22-4684-8180-826122881108/dashboards/7D668CAD-8FFC-4505-9215-655BCA5BEBAE/tiles": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                "name": "library-dataset",
                "webUrl": "http://localhost/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445",
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C22-4684-8180-826122881108/datasets/05169CD2-E713-41E6-96AA-1D8066D95445": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "05169CD2-E713-41E6-96AA-1D8066D95445",
                "name": "library-dataset",
                "webUrl": "http://localhost/groups/64ED5CAD-7C22-4684-8180-826122881108/datasets/05169CD2-E713-41E6-96AA-1D8066D95445",
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445/datasources": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "datasourceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                        "datasourceType": "PostgreSql",
                        "connectionDetails": {
                            "database": "library_db",
                            "server": "foo",
                        },
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C22-4684-8180-826122881108/datasets/05169CD2-E713-41E6-96AA-1D8066D95445/datasources": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "datasourceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                        "datasourceType": "PostgreSql",
                        "connectionDetails": {
                            "database": "library_db",
                            "server": "foo",
                        },
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/4674efd1-603c-4129-8d82-03cf2be05aff": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "status": "SUCCEEDED",
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/a674efd1-603c-4129-8d82-03cf2be05aff": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "status": "SUCCEEDED",
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/4674efd1-603c-4129-8d82-03cf2be05aff": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "workspaces": [
                    {
                        "id": "64ED5CAD-7C10-4684-8180-826122881108",
                        "name": "demo-workspace",
                        "state": "Active",
                        "datasets": [
                            {
                                "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                                "tables": [
                                    {
                                        "name": "public issue_history",
                                        "source": [
                                            {
                                                "expression": "dummy",
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/a674efd1-603c-4129-8d82-03cf2be05aff": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "workspaces": [
                    {
                        "id": "64ED5CAD-7C22-4684-8180-826122881108",
                        "name": "second-demo-workspace",
                        "state": "Active",
                        "datasets": [
                            {
                                "id": "05169CD2-E713-41E6-96AA-1D8066D95445",
                                "tables": [
                                    {
                                        "name": "public articles",
                                        "source": [
                                            {
                                                "expression": "dummy",
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 200,
            "json": scan_init_response,
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                        "id": "5b218778-e7a5-4d73-8187-f10824047715",
                        "name": "SalesMarketing",
                        "description": "Acryl sales marketing report",
                        "webUrl": "https://app.powerbi.com/groups/f089354e-8366-4e18-aea3-4cb4a3a50b48/reports/5b218778-e7a5-4d73-8187-f10824047715",
                        "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=5b218778-e7a5-4d73-8187-f10824047715&groupId=f089354e-8366-4e18-aea3-4cb4a3a50b48",
                    }
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/5b218778-e7a5-4d73-8187-f10824047715/pages": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "displayName": "Regional Sales Analysis",
                        "name": "ReportSection",
                        "order": "0",
                    },
                    {
                        "displayName": "Geographic Analysis",
                        "name": "ReportSection1",
                        "order": "1",
                    },
                ]
            },
        },
    }

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def default_source_config():
    return {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "workspace_id_pattern": {"allow": ["64ED5CAD-7C10-4684-8180-826122881108"]},
        "dataset_type_mapping": {
            "PostgreSql": "postgres",
            "Oracle": "oracle",
        },
        "env": "DEV",
    }


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_powerbi_ingest(mock_msal, pytestconfig, tmp_path, mock_time, requests_mock):
    global call_number
    call_number = 1

    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_reports": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_mces.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_override_ownership(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    global call_number
    call_number = 1

    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_ownership": False,
                    "extract_reports": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mces_disabled_ownership.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_disabled_ownership.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_mces_disabled_ownership.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_scan_all_workspaces(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    global call_number
    call_number = 1

    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_reports": False,
                    "extract_ownership": False,
                    "workspace_id_pattern": {
                        "deny": ["64ED5CAD-7322-4684-8180-826122881108"],
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mces_scan_all_workspaces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    mce_out_file = "golden_test_scan_all_workspaces.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_mces_scan_all_workspaces.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_extract_reports(mock_msal, pytestconfig, tmp_path, mock_time, requests_mock):
    global call_number
    call_number = 1

    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_report_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_report.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_report_mces.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )
