from typing import Any, Dict
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2022-02-03 07:00:00"


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
    if "05169cd2-e713-41e6-9600-1d8066d95445" in request.query:
        return {
            "value": [
                {
                    "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                    "name": "library-dataset",
                    "webUrl": "http://localhost/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445",
                }
            ]
        }

    if "ba0130a1-5b03-40de-9535-b34e778ea6ed" in request.query:
        return {
            "value": [
                {
                    "id": "ba0130a1-5b03-40de-9535-b34e778ea6ed",
                    "name": "hr_pbi_test",
                    "webUrl": "http://localhost/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/ba0130a1-5b03-40de-9535-b34e778ea6ed",
                }
            ]
        }


def register_mock_admin_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/admin/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets": {
            "method": "GET",
            "status_code": 200,
            "json": admin_datasets_response,
        },
        "https://api.powerbi.com/v1.0/myorg/admin/groups?%24skip=0&%24top=1000": {
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
        "https://api.powerbi.com/v1.0/myorg/admin/groups?%24skip=1000&%24top=1000": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [],
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards": {
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
                        "reportUserAccessRight": "Read",
                    },
                    {
                        "identifier": "User2@foo.com",
                        "displayName": "user2",
                        "emailAddress": "User2@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A5REWS",
                        "principalType": "User",
                        "reportUserAccessRight": "Owner",
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
        "https://api.powerbi.com/v1.0/myorg/admin/dashboards/7D668CAD-8FFC-4505-9215-655BCA5BEBAE/users": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "identifier": "User3@foo.com",
                        "displayName": "user3",
                        "emailAddress": "User3@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A3CD46",
                        "principalType": "User",
                    },
                    {
                        "identifier": "User4@foo.com",
                        "displayName": "user4",
                        "emailAddress": "User4@foo.com",
                        "datasetUserAccessRight": "ReadWrite",
                        "graphId": "C9EE53F2-88EA-4711-A173-AF0515A5REWS",
                        "principalType": "User",
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/dashboards/7D668CAD-7FFC-4505-9215-655BCA5BEBAE/tiles": {
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
                    {
                        "id": "23212598-23b5-4980-87cc-5fc0ecd84385",
                        "title": "yearly_sales",
                        "embedUrl": "https://localhost/tiles/embed/2",
                        "datasetId": "ba0130a1-5b03-40de-9535-b34e778ea6ed",
                    },
                ]
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
                        "type": "Workspace",
                        "state": "Active",
                        "datasets": [
                            {
                                "id": "05169CD2-E713-41E6-9600-1D8066D95445",
                                "endorsementDetails": {"endorsement": "Promoted"},
                                "name": "test_sf_pbi_test",
                                "tables": [
                                    {
                                        "name": "public issue_history",
                                        "source": [
                                            {
                                                "expression": "dummy",
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "SNOWFLAKE_TESTTABLE",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Snowflake.Databases("hp123rt5.ap-southeast-2.fakecomputing.com","PBI_TEST_WAREHOUSE_PROD",[Role="PBI_TEST_MEMBER"]),\n    PBI_TEST_Database = Source{[Name="PBI_TEST",Kind="Database"]}[Data],\n    TEST_Schema = PBI_TEST_Database{[Name="TEST",Kind="Schema"]}[Data],\n    TESTTABLE_Table = TEST_Schema{[Name="TESTTABLE",Kind="Table"]}[Data]\nin\n    TESTTABLE_Table',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "snowflake native-query",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Value.NativeQuery(Snowflake.Databases("bu20658.ap-southeast-2.snowflakecomputing.com","operations_analytics_warehouse_prod",[Role="OPERATIONS_ANALYTICS_MEMBER"]){[Name="OPERATIONS_ANALYTICS"]}[Data], "SELECT#(lf)concat((UPPER(REPLACE(SELLER,\'-\',\'\'))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4", null, [EnableFolding=true]),\n    #"Added Conditional Column" = Table.AddColumn(Source, "SME Units ENT", each if [DEAL_TYPE] = "SME Unit" then [UNIT] else 0),\n    #"Added Conditional Column1" = Table.AddColumn(#"Added Conditional Column", "Banklink Units", each if [DEAL_TYPE] = "Banklink" then [UNIT] else 0),\n    #"Removed Columns" = Table.RemoveColumns(#"Added Conditional Column1",{"Banklink Units"}),\n    #"Added Custom" = Table.AddColumn(#"Removed Columns", "Banklink Units", each if [DEAL_TYPE] = "Banklink" and [SALES_TYPE] = "3 - Upsell"\nthen [UNIT]\n\nelse if [SALES_TYPE] = "Adjusted BL Migration"\nthen [UNIT]\n\nelse 0),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom", "SME Units in $ (*$361)", each if [DEAL_TYPE] = "SME Unit" \nand [SALES_TYPE] <> "4 - Renewal"\n    then [UNIT] * 361\nelse 0),\n    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "Banklink in $ (*$148)", each [Banklink Units] * 148)\nin\n    #"Added Custom2"',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "snowflake native-query-with-join",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Value.NativeQuery(Snowflake.Databases("xaa48144.snowflakecomputing.com","GSL_TEST_WH",[Role="ACCOUNTADMIN"]){[Name="GSL_TEST_DB"]}[Data], "select A.name from GSL_TEST_DB.PUBLIC.SALES_ANALYST as A inner join GSL_TEST_DB.PUBLIC.SALES_FORECAST as B on A.name = B.name where startswith(A.name, \'mo\')", null, [EnableFolding=true])\nin\n    Source',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "job-history",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Oracle.Database("localhost:1521/salesdb.domain.com", [HierarchicalNavigation=true]), HR = Source{[Schema="HR"]}[Data], EMPLOYEES1 = HR{[Name="EMPLOYEES"]}[Data] \n in EMPLOYEES1',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "postgres_test_table",
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
                                    },
                                ],
                            },
                            {
                                "id": "ba0130a1-5b03-40de-9535-b34e778ea6ed",
                                "endorsementDetails": {"endorsement": "Certified"},
                                "configuredBy": "sindri@something.com",
                                "name": "hr_pbi_test",
                                "tables": [
                                    {
                                        "name": "dbo_book_issue",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Sql.Database("localhost", "library"),\n dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]\n in dbo_book_issue',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "ms_sql_native_table",
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Sql.Database("AUPRDWHDB", "COMMOPSDB", [Query="select *,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,\'-\',\'\'))), MONTH_WID) as CD_AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_MANAGER_CLOSING_MONTH,\'-\',\'\'))), MONTH_WID) as AGENT_KEY#(lf)#(lf)from V_PS_CD_RETENTION", CommandTimeout=#duration(0, 1, 30, 0)]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"mth_date", type date}}),\n    #"Added Custom" = Table.AddColumn(#"Changed Type", "Month", each Date.Month([mth_date])),\n    #"Added Custom1" = Table.AddColumn(#"Added Custom", "TPV Opening", each if [Month] = 1 then [TPV_AMV_OPENING]\nelse if [Month] = 2 then 0\nelse if [Month] = 3 then 0\nelse if [Month] = 4 then [TPV_AMV_OPENING]\nelse if [Month] = 5 then 0\nelse if [Month] = 6 then 0\nelse if [Month] = 7 then [TPV_AMV_OPENING]\nelse if [Month] = 8 then 0\nelse if [Month] = 9 then 0\nelse if [Month] = 10 then [TPV_AMV_OPENING]\nelse if [Month] = 11 then 0\nelse if [Month] = 12 then 0\n\nelse 0)\nin\n    #"Added Custom1"',
                                            }
                                        ],
                                        "datasourceUsages": [
                                            {
                                                "datasourceInstanceId": "DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "revenue",
                                        "columns": [
                                            {
                                                "name": "op_item_id",
                                                "dataType": "String",
                                                "description": "op_item_id column description",
                                                "isHidden": False,
                                                "columnType": "Data",
                                            },
                                            {
                                                "name": "op_id",
                                                "dataType": "String",
                                                "description": "op_id description",
                                                "isHidden": False,
                                                "columnType": "Data",
                                            },
                                            {
                                                "name": "op_product_name",
                                                "dataType": "String",
                                                "isHidden": False,
                                                "columnType": "Data",
                                            },
                                        ],
                                        "measures": [
                                            {
                                                "name": "event_name",
                                                "description": "column description",
                                                "expression": "let\n x",
                                                "isHidden": False,
                                            }
                                        ],
                                        "isHidden": False,
                                        "source": [
                                            {
                                                "expression": 'let\n    Source = Sql.Database("database.sql.net", "analytics", [Query="select * from analytics.sales_revenue"])\nin\n    Source'
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                        "dashboards": [
                            {
                                "id": "7D668CAD-7FFC-4505-9215-655BCA5BEBAE",
                                "isReadOnly": True,
                            }
                        ],
                        "reports": [
                            {
                                "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                                "id": "5b218778-e7a5-4d73-8187-f10824047715",
                                "reportType": "PowerBIReport",
                                "name": "SalesMarketing",
                                "description": "Acryl sales marketing report",
                            }
                        ],
                    },
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified": {
            "method": "GET",
            "status_code": 200,
            "json": [
                {"id": "64ED5CAD-7C10-4684-8180-826122881108"},
                {"id": "64ED5CAD-7C22-4684-8180-826122881108"},
                {"id": "64ED5CAD-7322-4684-8180-826122881108"},
            ],
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 200,
            "json": scan_init_response,
        },
        "https://api.powerbi.com/v1.0/myorg/admin/groups/64ED5CAD-7C10-4684-8180-826122881108/reports": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                    {
                        "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                        "id": "5b218778-e7a5-4d73-8187-f10824047715",
                        "name": "SalesMarketing",
                        "reportType": "PowerBIReport",
                        "description": "Acryl sales marketing report",
                        "webUrl": "https://app.powerbi.com/groups/f089354e-8366-4e18-aea3-4cb4a3a50b48/reports/5b218778-e7a5-4d73-8187-f10824047715",
                        "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=5b218778-e7a5-4d73-8187-f10824047715&groupId=f089354e-8366-4e18-aea3-4cb4a3a50b48",
                    }
                ]
            },
        },
        "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/5b218778-e7a5-4d73-8187-f10824047715": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                "id": "5b218778-e7a5-4d73-8187-f10824047715",
                "name": "SalesMarketing",
                "reportType": "PowerBIReport",
                "description": "Acryl sales marketing report",
                "webUrl": "https://app.powerbi.com/groups/f089354e-8366-4e18-aea3-4cb4a3a50b48/reports/5b218778-e7a5-4d73-8187-f10824047715",
                "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=5b218778-e7a5-4d73-8187-f10824047715&groupId=f089354e-8366-4e18-aea3-4cb4a3a50b48",
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
        "extract_reports": True,
        "admin_apis_only": True,
        "extract_ownership": True,
        "convert_lineage_urns_to_lowercase": False,
        "workspace_id_pattern": {"allow": ["64ED5CAD-7C10-4684-8180-826122881108"]},
        "dataset_type_mapping": {
            "PostgreSql": {"platform_instance": "operational_instance"},
            "Oracle": {"platform_instance": "high_performance_production_unit"},
            "Sql": {"platform_instance": "reporting-db"},
            "Snowflake": {"platform_instance": "sn-2"},
            "Databricks": {"platform_instance": "az-databrick"},
        },
        "env": "DEV",
        "extract_workspaces_to_containers": False,
        "enable_advance_lineage_sql_construct": False,
    }


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_admin_only_apis(mock_msal, pytestconfig, tmp_path, mock_time, requests_mock):
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
                    "filename": f"{tmp_path}/powerbi_admin_only_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_admin_only.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_admin_only_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_most_config_and_modified_since(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_admin_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_workspaces_to_containers": True,
                    "workspace_id_pattern": {
                        "deny": [
                            "64ED5CAD-7C22-4684-8180-826122881108",
                            "64ED5CAD-7322-4684-8180-826122881108",
                        ],
                    },
                    "modified_since": "2023-02-10T00:00:00.0000000Z",
                    "extract_ownership": True,
                    "extract_reports": True,
                    "extract_endorsements_to_tags": True,
                    "extract_dashboards": True,
                    "extract_dataset_schema": True,
                    "admin_apis_only": True,
                    "scan_batch_size": 100,
                    "workspace_id_as_urn_part": True,
                    "ownership": {
                        "create_corp_user": False,
                        "use_powerbi_email": True,
                        "remove_email_suffix": True,
                        "dataset_configured_by_as_owner": True,
                        "owner_criteria": [
                            "Owner",
                        ],
                    },
                    "extract_datasets_to_containers": True,
                    "filter_dataset_endorsements": {"allow": ["Certified"]},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_test_most_config_and_modified_since_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_most_config_and_modified_since_admin_only.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "powerbi_test_most_config_and_modified_since_mces.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )
