from typing import Any, Dict
from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-23 07:00:00"


def scan_init_response(request, context):
    # Request mock is passing POST input in the form of workspaces=<workspace_id>
    workspace_id = request.text.split("=")[1]

    w_id_vs_response: Dict[str, Any] = {
        "64ED5CAD-7C10-4684-8180-826122881108": {
            "id": "4674efd1-603c-4129-8d82-03cf2be05aff"
        }
    }

    return w_id_vs_response[workspace_id]


def admin_datasets_response(request, context):
    return {
        "value": [
            {
                "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                "name": "library-dataset",
                "webUrl": "http://localhost/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445",
            }
        ]
    }


def execute_queries_response(request, context):
    query = request.json()["queries"][0]["query"]
    if "unique_count" in query:
        return {
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {
                                    "[min]": 3,
                                    "[max]": 34333,
                                    "[unique_count]": 15,
                                },
                            ]
                        }
                    ]
                }
            ],
        }
    elif "COUNTROWS" in query:
        return {
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {
                                    "[count]": 542300,
                                },
                            ]
                        }
                    ]
                }
            ],
        }
    elif "TOPN" in query:
        return {
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {
                                    "[link]": "http://example.org",
                                    "[description]": "this is a sample",
                                    "[topic]": "urgent matters",
                                    "[view_count]": 123455,
                                },
                                {
                                    "[link]": "http://example.org/111/22/foo",
                                    "[description]": "this describes content",
                                    "[topic]": "urgent matters",
                                    "[view_count]": 123455,
                                },
                                {
                                    "[link]": "http://example.org/111/22",
                                    "[description]": "sample, this is",
                                    "[topic]": "normal matters",
                                    "[view_count]": 123455,
                                },
                            ]
                        }
                    ]
                }
            ],
        }


def register_mock_admin_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets": {
            "method": "GET",
            "status_code": 200,
            "json": admin_datasets_response,
        },
        "https://api.powerbi.com/v1.0/myorg/groups?%24skip=0&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "id": "64ED5CAD-7C10-4684-8180-826122881108",
                        "isReadOnly": True,
                        "name": "demo-workspace",
                        "type": "Workspace",
                        "state": "Active",
                    }
                ],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups?%24skip=1000&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
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
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445/executeQueries": {
            "method": "POST",
            "status_code": 200,
            "json": execute_queries_response,
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
                        "type": "Workspace",
                        "datasets": [
                            {
                                "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                                "endorsementDetails": {"endorsement": "Promoted"},
                                "name": "test_sf_pbi_test",
                                "tables": [
                                    {
                                        "name": "articles",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = PostgreSQL.Database("localhost"  ,   "mics"      ),\n  public_order_date =    Source{[Schema="public",Item="order_date"]}[Data] \n in \n public_order_date',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                        "columns": [
                                            {
                                                "name": "link",
                                                "description": "column description",
                                                "dataType": "String",
                                                "columnType": "DATA",
                                                "isHidden": False,
                                            },
                                            {
                                                "name": "description",
                                                "description": "column description",
                                                "dataType": "String",
                                                "columnType": "DATA",
                                                "isHidden": False,
                                            },
                                            {
                                                "name": "topic",
                                                "description": "column description",
                                                "dataType": "String",
                                                "columnType": "DATA",
                                                "isHidden": False,
                                            },
                                        ],
                                        "measures": [
                                            {
                                                "name": "view_count",
                                                "description": "column description",
                                                "expression": "let\n x",
                                                "isHidden": False,
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                        "dashboards": [],
                        "reports": [],
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 200,
            "json": scan_init_response,
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                "name": "library-dataset",
                "description": "Library Dataset",
                "webUrl": "http://localhost/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445",
            },
        },
    }

    api_vs_response.update(override_data)

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def acquire_token_for_client(self, *args, **kwargs):
            return {
                "access_token": "dummy",
            }

    return MsalClient()


def default_source_config():
    return {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "workspace_id": "64ED5CAD-7C10-4684-8180-826122881108",
        "extract_lineage": True,
        "extract_reports": False,
        "admin_apis_only": False,
        "extract_ownership": True,
        "convert_lineage_urns_to_lowercase": False,
        "extract_independent_datasets": True,
        "workspace_id_pattern": {"allow": ["64ED5CAD-7C10-4684-8180-826122881108"]},
        "extract_workspaces_to_containers": False,
        "profiling": {
            "enabled": True,
        },
        "profile_pattern": {"allow": [".*"]},
    }


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_profiling(mock_msal, pytestconfig, tmp_path, mock_time, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_admin_api(request_mock=requests_mock)

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
                    "filename": f"{tmp_path}/powerbi_profiling.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_profiling.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_profiling.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )
