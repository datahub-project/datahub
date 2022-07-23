from typing import Any, Dict
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redash import (
    RedashConfig,
    RedashSource,
    get_full_qualified_name,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.schema_classes import ChartInfoClass, DashboardInfoClass

mock_dashboard_response = {
    "tags": [],
    "is_archived": False,
    "updated_at": "2021-08-13T19:14:15.288Z",
    "is_favorite": False,
    "user": {
        "auth_type": "password",
        "is_disabled": False,
        "updated_at": "2021-08-13T19:31:44.116Z",
        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
        "is_invitation_pending": False,
        "groups": [1, 2],
        "id": 1,
        "name": "redash",
        "created_at": "2021-08-13T13:39:56.216Z",
        "disabled_at": None,
        "is_email_verified": True,
        "active_at": "2021-08-13T19:31:24Z",
        "email": "redash@example.com",
    },
    "layout": [],
    "is_draft": False,
    "id": 3,
    "can_edit": True,
    "user_id": 1,
    "name": "My Dashboard",
    "created_at": "2021-08-13T19:13:07.408Z",
    "slug": "my-dashboard",
    "version": 2,
    "widgets": [
        {
            "visualization": {
                "description": "",
                "created_at": "2021-08-13T19:09:55.779Z",
                "updated_at": "2021-08-13T19:13:42.544Z",
                "id": 10,
                "query": {
                    "user": {
                        "auth_type": "password",
                        "is_disabled": False,
                        "updated_at": "2021-08-13T19:31:44.116Z",
                        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
                        "is_invitation_pending": False,
                        "groups": [1, 2],
                        "id": 1,
                        "name": "redash",
                        "created_at": "2021-08-13T13:39:56.216Z",
                        "disabled_at": None,
                        "is_email_verified": True,
                        "active_at": "2021-08-13T19:31:24Z",
                        "email": "redash@example.com",
                    },
                    "created_at": "2021-08-13T18:57:33.074Z",
                    "latest_query_data_id": 20,
                    "schedule": None,
                    "description": None,
                    "tags": [],
                    "updated_at": "2021-08-13T19:10:04.396Z",
                    "last_modified_by": {
                        "auth_type": "password",
                        "is_disabled": False,
                        "updated_at": "2021-08-13T19:31:44.116Z",
                        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
                        "is_invitation_pending": False,
                        "groups": [1, 2],
                        "id": 1,
                        "name": "redash",
                        "created_at": "2021-08-13T13:39:56.216Z",
                        "disabled_at": None,
                        "is_email_verified": True,
                        "active_at": "2021-08-13T19:31:24Z",
                        "email": "redash@example.com",
                    },
                    "options": {"parameters": []},
                    "is_safe": True,
                    "version": 1,
                    "query_hash": "f709ca3a345e6fa2b7d00e005c8c3185",
                    "is_archived": False,
                    "query": "SELECT\nmarried AS stage1, pet as stage2, happy as stage3, freq as value\nFROM (\nSELECT 'Yes' AS married,'Yes' AS pet,'Yes' AS happy,5 AS freq\nUNION ALL SELECT 'Yes' AS married,'Yes' AS pet,'Yes' AS happy,4 AS freq\nUNION ALL SELECT 'Yes' AS married,'No' AS pet,'Yes' AS happy,3 AS freq\nUNION ALL SELECT 'No' AS married,'Yes' AS pet,'Yes' AS happy,2 AS freq\nUNION ALL SELECT 'No' AS married,'No' AS pet,'No' AS happy,1 AS freq\n) t",
                    "api_key": "3MJOZjtshCa2mt3O4x6pzWNKMWcrLIOq5O0u6AVU",
                    "is_draft": False,
                    "id": 4,
                    "data_source_id": 2,
                    "name": "My Query",
                },
                "type": "CHART",
                "options": {
                    "showDataLabels": True,
                    "direction": {"type": "counterclockwise"},
                    "missingValuesAsZero": True,
                    "error_y": {"visible": True, "type": "data"},
                    "numberFormat": "0,0[.]00000",
                    "yAxis": [{"type": "linear"}, {"type": "linear", "opposite": True}],
                    "series": {
                        "stacking": None,
                        "error_y": {"visible": True, "type": "data"},
                    },
                    "globalSeriesType": "pie",
                    "percentFormat": "0[.]00%",
                    "sortX": True,
                    "seriesOptions": {
                        "value": {"zIndex": 0, "index": 0, "type": "pie", "yAxis": 0}
                    },
                    "valuesOptions": {"Yes": {}, "No": {}},
                    "xAxis": {"labels": {"enabled": True}, "type": "-"},
                    "dateTimeFormat": "DD/MM/YY HH:mm",
                    "columnMapping": {"stage1": "x", "value": "y"},
                    "textFormat": "",
                    "customCode": "// Available variables are x, ys, element, and Plotly\n// Type console.log(x, ys); for more info about x and ys\n// To plot your graph call Plotly.plot(element, ...)\n// Plotly examples and docs: https://plot.ly/javascript/",
                    "legend": {"enabled": True},
                },
                "name": "Chart",
            },
            "text": "",
            "created_at": "2021-08-13T19:13:42.544Z",
            "updated_at": "2021-08-13T19:14:11.171Z",
            "options": {
                "parameterMappings": {},
                "isHidden": False,
                "position": {
                    "autoHeight": False,
                    "sizeX": 3,
                    "sizeY": 14,
                    "maxSizeY": 1000,
                    "maxSizeX": 6,
                    "minSizeY": 5,
                    "minSizeX": 1,
                    "col": 3,
                    "row": 3,
                },
            },
            "dashboard_id": 3,
            "width": 1,
            "id": 11,
        },
        {
            "text": "My description",
            "created_at": "2021-08-13T19:13:17.453Z",
            "updated_at": "2021-08-13T19:13:22.165Z",
            "options": {
                "position": {
                    "autoHeight": False,
                    "sizeX": 6,
                    "sizeY": 3,
                    "maxSizeY": 1000,
                    "maxSizeX": 6,
                    "minSizeY": 1,
                    "minSizeX": 1,
                    "col": 0,
                    "row": 0,
                },
                "isHidden": False,
                "parameterMappings": {},
            },
            "dashboard_id": 3,
            "width": 1,
            "id": 9,
        },
        {
            "visualization": {
                "description": "",
                "created_at": "2021-08-13T19:09:11.445Z",
                "updated_at": "2021-08-13T19:13:29.571Z",
                "id": 9,
                "query": {
                    "user": {
                        "auth_type": "password",
                        "is_disabled": False,
                        "updated_at": "2021-08-13T19:31:44.116Z",
                        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
                        "is_invitation_pending": False,
                        "groups": [1, 2],
                        "id": 1,
                        "name": "redash",
                        "created_at": "2021-08-13T13:39:56.216Z",
                        "disabled_at": None,
                        "is_email_verified": True,
                        "active_at": "2021-08-13T19:31:24Z",
                        "email": "redash@example.com",
                    },
                    "created_at": "2021-08-13T18:57:33.074Z",
                    "latest_query_data_id": 20,
                    "schedule": None,
                    "description": None,
                    "tags": [],
                    "updated_at": "2021-08-13T19:10:04.396Z",
                    "last_modified_by": {
                        "auth_type": "password",
                        "is_disabled": False,
                        "updated_at": "2021-08-13T19:31:44.116Z",
                        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
                        "is_invitation_pending": False,
                        "groups": [1, 2],
                        "id": 1,
                        "name": "redash",
                        "created_at": "2021-08-13T13:39:56.216Z",
                        "disabled_at": None,
                        "is_email_verified": True,
                        "active_at": "2021-08-13T19:31:24Z",
                        "email": "redash@example.com",
                    },
                    "options": {"parameters": []},
                    "is_safe": True,
                    "version": 1,
                    "query_hash": "f709ca3a345e6fa2b7d00e005c8c3185",
                    "is_archived": False,
                    "query": "SELECT\nmarried AS stage1, pet as stage2, happy as stage3, freq as value\nFROM (\nSELECT 'Yes' AS married,'Yes' AS pet,'Yes' AS happy,5 AS freq\nUNION ALL SELECT 'Yes' AS married,'Yes' AS pet,'Yes' AS happy,4 AS freq\nUNION ALL SELECT 'Yes' AS married,'No' AS pet,'Yes' AS happy,3 AS freq\nUNION ALL SELECT 'No' AS married,'Yes' AS pet,'Yes' AS happy,2 AS freq\nUNION ALL SELECT 'No' AS married,'No' AS pet,'No' AS happy,1 AS freq\n) t",
                    "api_key": "3MJOZjtshCa2mt3O4x6pzWNKMWcrLIOq5O0u6AVU",
                    "is_draft": False,
                    "id": 4,
                    "data_source_id": 2,
                    "name": "My Query",
                },
                "type": "SANKEY",
                "options": {},
                "name": "Sankey",
            },
            "text": "",
            "created_at": "2021-08-13T19:13:29.571Z",
            "updated_at": "2021-08-13T19:13:29.665Z",
            "options": {
                "parameterMappings": {},
                "isHidden": False,
                "position": {
                    "autoHeight": False,
                    "sizeX": 3,
                    "sizeY": 7,
                    "maxSizeY": 1000,
                    "maxSizeX": 6,
                    "minSizeY": 1,
                    "minSizeX": 1,
                    "col": 0,
                    "row": 3,
                },
            },
            "dashboard_id": 3,
            "width": 1,
            "id": 10,
        },
        {
            "visualization": {
                "description": "",
                "created_at": "2021-08-13T18:57:33.074Z",
                "updated_at": "2021-08-13T19:13:51.175Z",
                "id": 8,
                "query": {
                    "user": {
                        "auth_type": "password",
                        "is_disabled": False,
                        "updated_at": "2021-08-13T19:31:44.116Z",
                        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
                        "is_invitation_pending": False,
                        "groups": [1, 2],
                        "id": 1,
                        "name": "redash",
                        "created_at": "2021-08-13T13:39:56.216Z",
                        "disabled_at": None,
                        "is_email_verified": True,
                        "active_at": "2021-08-13T19:31:24Z",
                        "email": "redash@example.com",
                    },
                    "created_at": "2021-08-13T18:57:33.074Z",
                    "latest_query_data_id": 20,
                    "schedule": None,
                    "description": None,
                    "tags": [],
                    "updated_at": "2021-08-13T19:10:04.396Z",
                    "last_modified_by": {
                        "auth_type": "password",
                        "is_disabled": False,
                        "updated_at": "2021-08-13T19:31:44.116Z",
                        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
                        "is_invitation_pending": False,
                        "groups": [1, 2],
                        "id": 1,
                        "name": "redash",
                        "created_at": "2021-08-13T13:39:56.216Z",
                        "disabled_at": None,
                        "is_email_verified": True,
                        "active_at": "2021-08-13T19:31:24Z",
                        "email": "redash@example.com",
                    },
                    "options": {"parameters": []},
                    "is_safe": True,
                    "version": 1,
                    "query_hash": "f709ca3a345e6fa2b7d00e005c8c3185",
                    "is_archived": False,
                    "query": "SELECT\nmarried AS stage1, pet as stage2, happy as stage3, freq as value\nFROM (\nSELECT 'Yes' AS married,'Yes' AS pet,'Yes' AS happy,5 AS freq\nUNION ALL SELECT 'Yes' AS married,'Yes' AS pet,'Yes' AS happy,4 AS freq\nUNION ALL SELECT 'Yes' AS married,'No' AS pet,'Yes' AS happy,3 AS freq\nUNION ALL SELECT 'No' AS married,'Yes' AS pet,'Yes' AS happy,2 AS freq\nUNION ALL SELECT 'No' AS married,'No' AS pet,'No' AS happy,1 AS freq\n) t",
                    "api_key": "3MJOZjtshCa2mt3O4x6pzWNKMWcrLIOq5O0u6AVU",
                    "is_draft": False,
                    "id": 4,
                    "data_source_id": 2,
                    "name": "My Query",
                },
                "type": "TABLE",
                "options": {},
                "name": "Table",
            },
            "text": "",
            "created_at": "2021-08-13T19:13:51.175Z",
            "updated_at": "2021-08-13T19:14:58.898Z",
            "options": {
                "parameterMappings": {},
                "isHidden": False,
                "position": {
                    "autoHeight": False,
                    "sizeX": 3,
                    "sizeY": 7,
                    "maxSizeY": 1000,
                    "maxSizeX": 6,
                    "minSizeY": 1,
                    "minSizeX": 2,
                    "col": 0,
                    "row": 10,
                },
            },
            "dashboard_id": 3,
            "width": 1,
            "id": 12,
        },
    ],
    "dashboard_filters_enabled": False,
}
mock_mysql_data_source_response = {
    "scheduled_queue_name": "scheduled_queries",
    "name": "mysql-rfam-public.ebi.ac.uk",
    "pause_reason": None,
    "queue_name": "queries",
    "syntax": "sql",
    "paused": 0,
    "options": {
        "passwd": "--------",
        "host": "mysql-rfam-public.ebi.ac.uk",
        "db": "Rfam",
        "port": 4497,
        "user": "rfamro",
    },
    "groups": {"2": False},
    "type": "mysql",
    "id": 2,
}
mock_chart_response: Dict[str, Any] = {
    "is_archived": False,
    "updated_at": "2021-08-13T19:10:04.396Z",
    "is_favorite": True,
    "query": "SELECT\n name,\n SUM(quantity * list_price * (1 - discount)) AS total,\n YEAR(order_date) as order_year\n FROM\n `orders` o\n INNER JOIN `order_items` i ON i.order_id = o.order_id\nINNER JOIN `staffs` s ON s.staff_id = o.staff_id\nGROUP BY\nname,\nyear(order_date)",
    "id": 4,
    "description": None,
    "tags": [],
    "version": 1,
    "query_hash": "f709ca3a345e6fa2b7d00e005c8c3185",
    "api_key": "3MJOZjtshCa2mt3O4x6pzWNKMWcrLIOq5O0u6AVU",
    "data_source_id": 2,
    "is_safe": True,
    "latest_query_data_id": 20,
    "schedule": None,
    "user": {
        "auth_type": "password",
        "is_disabled": False,
        "updated_at": "2021-08-13T19:53:44.365Z",
        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
        "is_invitation_pending": False,
        "groups": [1, 2],
        "id": 1,
        "name": "redash",
        "created_at": "2021-08-13T13:39:56.216Z",
        "disabled_at": None,
        "is_email_verified": True,
        "active_at": "2021-08-13T19:53:33Z",
        "email": "redash@example.com",
    },
    "is_draft": False,
    "can_edit": True,
    "name": "My Query",
    "created_at": "2021-08-13T18:57:33.074Z",
    "last_modified_by": {
        "auth_type": "password",
        "is_disabled": False,
        "updated_at": "2021-08-13T19:53:44.365Z",
        "profile_image_url": "https://www.gravatar.com/avatar/db00ae5315ea20071d35b08e959b328e?s=40&d=identicon",
        "is_invitation_pending": False,
        "groups": [1, 2],
        "id": 1,
        "name": "redash",
        "created_at": "2021-08-13T13:39:56.216Z",
        "disabled_at": None,
        "is_email_verified": True,
        "active_at": "2021-08-13T19:53:33Z",
        "email": "redash@example.com",
    },
    "visualizations": [
        {
            "description": "",
            "created_at": "2021-08-13T18:57:33.074Z",
            "updated_at": "2021-08-13T19:13:51.175Z",
            "id": 8,
            "type": "TABLE",
            "options": {},
            "name": "Table",
        },
        {
            "description": "",
            "created_at": "2021-08-13T19:09:11.445Z",
            "updated_at": "2021-08-13T19:13:29.571Z",
            "id": 9,
            "type": "SANKEY",
            "options": {},
            "name": "Sankey",
        },
        {
            "description": "",
            "created_at": "2021-08-13T19:09:55.779Z",
            "updated_at": "2021-08-13T19:13:42.544Z",
            "id": 10,
            "type": "CHART",
            "options": {
                "showDataLabels": True,
                "direction": {"type": "counterclockwise"},
                "missingValuesAsZero": True,
                "error_y": {"visible": True, "type": "data"},
                "numberFormat": "0,0[.]00000",
                "yAxis": [{"type": "linear"}, {"type": "linear", "opposite": True}],
                "series": {
                    "stacking": None,
                    "error_y": {"visible": True, "type": "data"},
                },
                "globalSeriesType": "pie",
                "percentFormat": "0[.]00%",
                "sortX": True,
                "seriesOptions": {
                    "value": {"zIndex": 0, "index": 0, "type": "pie", "yAxis": 0}
                },
                "valuesOptions": {"Yes": {}, "No": {}},
                "xAxis": {"labels": {"enabled": True}, "type": "-"},
                "dateTimeFormat": "DD/MM/YY HH:mm",
                "columnMapping": {"stage1": "x", "value": "y"},
                "textFormat": "",
                "customCode": "// Available variables are x, ys, element, and Plotly\n// Type console.log(x, ys); for more info about x and ys\n// To plot your graph call Plotly.plot(element, ...)\n// Plotly examples and docs: https://plot.ly/javascript/",
                "legend": {"enabled": True},
            },
            "name": "Chart",
        },
    ],
    "options": {"parameters": []},
}


def redash_source() -> RedashSource:
    return RedashSource(
        ctx=PipelineContext(run_id="redash-source-test"),
        config=RedashConfig(
            connect_uri="http://localhost:5000",
            api_key="REDASH_API_KEY",
            parse_table_names_from_sql=False,
        ),
    )


def test_get_dashboard_snapshot():
    expected = DashboardSnapshot(
        urn="urn:li:dashboard:(redash,3)",
        aspects=[
            DashboardInfoClass(
                description="My description",
                title="My Dashboard",
                charts=[
                    "urn:li:chart:(redash,10)",
                    "urn:li:chart:(redash,9)",
                    "urn:li:chart:(redash,8)",
                ],
                datasets=[],
                lastModified=ChangeAuditStamps(
                    created=AuditStamp(
                        time=1628882055288, actor="urn:li:corpuser:unknown"
                    ),
                    lastModified=AuditStamp(
                        time=1628882055288, actor="urn:li:corpuser:unknown"
                    ),
                ),
                dashboardUrl="http://localhost:5000/dashboard/my-dashboard",
                customProperties={},
            )
        ],
    )
    result = redash_source()._get_dashboard_snapshot(mock_dashboard_response)
    assert result == expected


@patch("datahub.ingestion.source.redash.RedashSource._get_chart_data_source")
def test_get_known_viz_chart_snapshot(mocked_data_source):
    mocked_data_source.return_value = mock_mysql_data_source_response
    expected = ChartSnapshot(
        urn="urn:li:chart:(redash,10)",
        aspects=[
            ChartInfoClass(
                customProperties={},
                externalUrl=None,
                title="My Query Chart",
                description="",
                lastModified=ChangeAuditStamps(
                    created=AuditStamp(
                        time=1628882022544, actor="urn:li:corpuser:unknown"
                    ),
                    lastModified=AuditStamp(
                        time=1628882022544, actor="urn:li:corpuser:unknown"
                    ),
                ),
                chartUrl="http://localhost:5000/queries/4#10",
                inputs=["urn:li:dataset:(urn:li:dataPlatform:mysql,Rfam,PROD)"],
                type="PIE",
            )
        ],
    )
    viz_data = mock_chart_response.get("visualizations", [])[2]
    result = redash_source()._get_chart_snapshot(mock_chart_response, viz_data)
    assert result == expected


@patch("datahub.ingestion.source.redash.RedashSource._get_chart_data_source")
def test_get_unknown_viz_chart_snapshot(mocked_data_source):
    """
    Testing with unmapped visualization type SANKEY
    """
    mocked_data_source.return_value = mock_mysql_data_source_response
    expected = ChartSnapshot(
        urn="urn:li:chart:(redash,9)",
        aspects=[
            ChartInfoClass(
                customProperties={},
                externalUrl=None,
                title="My Query Sankey",
                description="",
                lastModified=ChangeAuditStamps(
                    created=AuditStamp(
                        time=1628882009571, actor="urn:li:corpuser:unknown"
                    ),
                    lastModified=AuditStamp(
                        time=1628882009571, actor="urn:li:corpuser:unknown"
                    ),
                ),
                chartUrl="http://localhost:5000/queries/4#9",
                inputs=["urn:li:dataset:(urn:li:dataPlatform:mysql,Rfam,PROD)"],
                type="TABLE",
            )
        ],
    )
    viz_data = mock_chart_response.get("visualizations", [])[1]
    result = redash_source()._get_chart_snapshot(mock_chart_response, viz_data)
    assert result == expected


# TODO: Getting table lineage from SQL parsing test


def test_get_full_qualified_name():
    test_sql_table_names = [
        {
            "platform": "postgres",
            "database_name": "postgres_db",
            "table_name": "orders",
            "full_qualified_table_name": "postgres_db.public.orders",
        },
        {
            "platform": "postgres",
            "database_name": "postgres_db",
            "table_name": "schema.orders",
            "full_qualified_table_name": "postgres_db.schema.orders",
        },
        {
            "platform": "postgres",
            "database_name": "postgres_db",
            "table_name": "other_db.schema.orders",
            "full_qualified_table_name": "other_db.schema.orders",
        },
        {
            "platform": "mysql",
            "database_name": "mysql_db",
            "table_name": "orders",
            "full_qualified_table_name": "mysql_db.orders",
        },
        {
            "platform": "mysql",
            "database_name": "mysql_db",
            "table_name": "other_schema.orders",
            "full_qualified_table_name": "other_schema.orders",
        },
        {
            "platform": "bigquery",
            "database_name": "projectId",
            "table_name": "dataset.table",
            "full_qualified_table_name": "projectId.dataset.table",
        },
        {
            "platform": "bigquery",
            "database_name": "projectId",
            "table_name": "projectIdOther.dataset2.table",
            "full_qualified_table_name": "projectIdOther.dataset2.table",
        },
        {
            "platform": "mssql",
            "database_name": "AdventureWork",
            "table_name": "dbo.Sale Order",
            "full_qualified_table_name": "AdventureWork.dbo.Sale Order",
        },
        {
            "platform": "mssql",
            "database_name": "AdventureWork",
            "table_name": "SaleOrder",
            "full_qualified_table_name": "AdventureWork.dbo.SaleOrder",
        },
        {
            "platform": "mssql",
            "database_name": "AdventureWork",
            "table_name": "OtherDB.dbo.Sale Order",
            "full_qualified_table_name": "OtherDB.dbo.Sale Order",
        },
    ]

    expected = list()
    result = list()

    for sql_table_name in test_sql_table_names:
        platform = sql_table_name["platform"]
        database_name = sql_table_name["database_name"]
        table_name = sql_table_name["table_name"]

        expected.append(sql_table_name["full_qualified_table_name"])

        result.append(
            get_full_qualified_name(
                platform=platform, database_name=database_name, table_name=table_name
            )
        )

    assert expected == result


def redash_source_parse_table_names_from_sql() -> RedashSource:
    return RedashSource(
        ctx=PipelineContext(run_id="redash-source-test"),
        config=RedashConfig(
            connect_uri="http://localhost:5000",
            api_key="REDASH_API_KEY",
            parse_table_names_from_sql=True,
        ),
    )


@patch("datahub.ingestion.source.redash.RedashSource._get_chart_data_source")
def test_get_chart_snapshot_parse_table_names_from_sql(mocked_data_source):
    mocked_data_source.return_value = mock_mysql_data_source_response
    expected = ChartSnapshot(
        urn="urn:li:chart:(redash,10)",
        aspects=[
            ChartInfoClass(
                customProperties={},
                externalUrl=None,
                title="My Query Chart",
                description="",
                lastModified=ChangeAuditStamps(
                    created=AuditStamp(
                        time=1628882022544, actor="urn:li:corpuser:unknown"
                    ),
                    lastModified=AuditStamp(
                        time=1628882022544, actor="urn:li:corpuser:unknown"
                    ),
                ),
                chartUrl="http://localhost:5000/queries/4#10",
                inputs=[
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,Rfam.order_items,PROD)",
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,Rfam.orders,PROD)",
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,Rfam.staffs,PROD)",
                ],
                type="PIE",
            )
        ],
    )
    viz_data = mock_chart_response.get("visualizations", [])[2]
    result = redash_source_parse_table_names_from_sql()._get_chart_snapshot(
        mock_chart_response, viz_data
    )

    assert result == expected
