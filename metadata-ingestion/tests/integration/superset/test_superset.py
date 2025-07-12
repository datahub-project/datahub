from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def register_mock_api(request_mock: Any, override_data: Optional[dict] = None) -> None:
    if override_data is None:
        override_data = {}
    api_vs_response = {
        "mock://mock-domain.superset.com/api/v1/security/login": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "access_token": "test_token",
            },
        },
        "mock://mock-domain.superset.com/api/v1/dashboard/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 2,
                "result": [
                    {
                        "id": 1,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owners1",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "dashboard_title": "test_dashboard_title_1",
                        "url": "/dashboard/test_dashboard_url_1",
                        "position_json": '{"CHART-test-1": {"meta": { "chartId": "10" }}, "CHART-test-2": {"meta": { "chartId": "11" }}}',
                        "status": "published",
                        "published": True,
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 1,
                                "last_name": "Owner1",
                            },
                            {
                                "first_name": "Test",
                                "id": 2,
                                "last_name": "Owner2",
                            },
                        ],
                        "certified_by": "Certification team",
                        "certification_details": "Approved",
                    },
                    {
                        "id": 2,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "Owners2",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "dashboard_title": "test_dashboard_title_2",
                        "url": "/dashboard/test_dashboard_url_2",
                        "position_json": '{"CHART-test-3": {"meta": { "chartId": "12" }}, "CHART-test-4": {"meta": { "chartId": "13" }}}',
                        "status": "draft",
                        "published": False,
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 4,
                                "last_name": "Owner4",
                            }
                        ],
                        "certified_by": "",
                        "certification_details": "",
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/chart/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 4,
                "result": [
                    {
                        "id": 10,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owners1",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_1",
                        "viz_type": "box_plot",
                        "url": "/explore/test_chart_url_10",
                        "datasource_id": 1,
                        "params": '{"metrics": [], "adhoc_filters": []}',
                        "form_data": {
                            "all_columns": [
                                {
                                    "expressionType": "SQL",
                                    "label": "test_label",
                                    "sqlExpression": "",
                                },
                                "test_column1",
                                "test_column2",
                            ],
                        },
                    },
                    {
                        "id": 11,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owners1",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_2",
                        "viz_type": "pie",
                        "url": "/explore/test_chart_url_11",
                        "datasource_id": 2,
                        "params": '{"metrics": [], "adhoc_filters": []}',
                        "form_data": {
                            "all_columns": [
                                "test_column3",
                                "test_column4",
                            ],
                        },
                    },
                    {
                        "id": 12,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "Owners2",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_3",
                        "viz_type": "treemap",
                        "url": "/explore/test_chart_url_12",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                    {
                        "id": 13,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "Owners2",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_4",
                        "viz_type": "histogram",
                        "url": "/explore/test_chart_url_13",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 215,
                "description_columns": {},
                "ids": [1, 2, 3],
                "result": [
                    {
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "User1",
                        },
                        "changed_by_name": "test_username_1",
                        "changed_on_delta_humanized": "10 months ago",
                        "changed_on_utc": "2024-01-05T21:10:15.650819+0000",
                        "database": {"database_name": "test_database1", "id": 1},
                        "datasource_type": "table",
                        "default_endpoint": None,
                        "description": None,
                        "explore_url": "/explore/?datasource_type=table&datasource_id=1",
                        "extra": None,
                        "id": 1,
                        "kind": "virtual",
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 1,
                                "last_name": "Owner1",
                            }
                        ],
                        "schema": "test_schema1",
                        "table_name": "Test Table 1",
                    },
                    {
                        "changed_by": {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "User2",
                        },
                        "changed_by_name": "test_username_2",
                        "changed_on_delta_humanized": "9 months ago",
                        "changed_on_utc": "2024-02-10T15:30:20.123456+0000",
                        "database": {"database_name": "test_database2", "id": 2},
                        "datasource_type": "table",
                        "default_endpoint": None,
                        "description": "Sample description for dataset 2",
                        "explore_url": "/explore/?datasource_type=table&datasource_id=2",
                        "extra": None,
                        "id": 2,
                        "kind": "virtual",
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 2,
                                "last_name": "Owner2",
                            }
                        ],
                        "schema": "test_schema2",
                        "table_name": "Test Table 2",
                    },
                    {
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "User1",
                        },
                        "changed_by_name": "test_username_1",
                        "changed_on_delta_humanized": "9 months ago",
                        "columns": [
                            {
                                "created_on": "2024-01-05T21:10:15.650819+0000",
                                "changed_on": "2024-01-05T21:10:15.650819+0000",
                                "column_name": "id",
                                "type": "INT",
                                "id": 1,
                                "verbose_name": "null",
                            },
                            {
                                "created_on": "2024-01-05T21:10:15.650819+0000",
                                "changed_on": "2024-01-05T21:10:15.650819+0000",
                                "column_name": "name",
                                "type": "STRING",
                                "id": 2,
                                "verbose_name": "null",
                            },
                        ],
                        "changed_on_utc": "2024-02-10T15:30:20.123456+0000",
                        "database": {"database_name": "test_database1", "id": 1},
                        "datasource_type": "table",
                        "default_endpoint": None,
                        "description": "Sample description for dataset 3",
                        "explore_url": "/explore/?datasource_type=table&datasource_id=3",
                        "extra": None,
                        "id": 3,
                        "kind": "physical",
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 2,
                                "last_name": "Owner2",
                            }
                        ],
                        "schema": "test_schema3",
                        "select_star": "SELECT * FROM test_schema3.test_table3 LIMIT 100",
                        "table_name": "Test Table 3",
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/1": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": 1,
                "result": {
                    "always_filter_main_dttm": False,
                    "cache_timeout": None,
                    "changed_by": {
                        "first_name": "Test",
                        "id": 1,
                        "last_name": "Owners1",
                    },
                    "changed_on": "2024-01-05T21:10:15.650819+0000",
                    "changed_on_humanized": "10 months ago",
                    "created_by": {"first_name": "Test", "last_name": "User1"},
                    "created_on": "2024-01-05T21:10:15.650819+0000",
                    "created_on_humanized": "10 months ago",
                    "currency_formats": {},
                    "database": {
                        "backend": "postgresql",
                        "database_name": "test_database1",
                        "id": 1,
                    },
                    "columns": [
                        {
                            "column_name": "test_column1",
                            "description": "some description 1",
                            "type": "INT",
                        },
                        {
                            "column_name": "test_column2",
                            "description": "some description 2",
                            "type": "STRING",
                        },
                    ],
                    "datasource_name": "Test Table 1",
                    "datasource_type": "table",
                    "default_endpoint": None,
                    "description": None,
                    "extra": None,
                    "fetch_values_predicate": None,
                    "filter_select_enabled": True,
                    "granularity_sqla": [
                        ["created_at", "created_at"],
                        ["updated_at", "updated_at"],
                    ],
                    "id": 1,
                    "is_managed_externally": False,
                    "is_sqllab_view": False,
                    "kind": "virtual",
                    "main_dttm_col": None,
                    "metrics": [
                        {
                            "changed_on": "2024-01-05T21:10:15.650819+0000",
                            "created_on": "2024-01-05T21:10:15.650819+0000",
                            "currency": None,
                            "d3format": None,
                            "description": None,
                            "expression": "count(*)",
                            "extra": None,
                            "id": 1,
                            "metric_name": "count",
                            "metric_type": None,
                            "rendered_expression": "count(*)",
                            "verbose_name": None,
                            "warning_text": None,
                        },
                        {
                            "changed_on": "2025-06-27T15:30:20.123456+0000",
                            "created_on": "2025-06-27T15:30:20.123456+0000",
                            "currency": None,
                            "d3format": None,
                            "description": "Total count of rows",
                            "expression": "count(1)",
                            "extra": None,
                            "id": 3,
                            "metric_name": "total_count",
                            "metric_type": "NUMERIC",
                            "rendered_expression": "count(1)",
                            "verbose_name": "Total Count",
                            "warning_text": None,
                        },
                    ],
                    "name": "Test Table 1",
                    "normalize_columns": True,
                    "offset": 0,
                    "owners": [
                        {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owner1",
                        }
                    ],
                    "schema": "test_schema1",
                    "select_star": "SELECT * FROM test_schema1.test_table1 LIMIT 100",
                    "table_name": "Test Table 1",
                    "uid": "1__table",
                    "url": "/tablemodelview/edit/1",
                    "verbose_map": {
                        "__timestamp": "Time",
                        "id": "ID",
                        "name": "Name",
                        "created_at": "Created At",
                        "updated_at": "Updated At",
                    },
                },
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/2": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": 2,
                "result": {
                    "always_filter_main_dttm": False,
                    "cache_timeout": None,
                    "changed_by": {
                        "first_name": "Test",
                        "id": 2,
                        "last_name": "Owners2",
                    },
                    "changed_on": "2024-02-10T15:30:20.123456+0000",
                    "changed_on_humanized": "9 months ago",
                    "created_by": {"first_name": "Test", "last_name": "User2"},
                    "created_on": "2024-02-10T15:30:20.123456+0000",
                    "created_on_humanized": "9 months ago",
                    "currency_formats": {},
                    "database": {
                        "backend": "postgresql",
                        "database_name": "test_database1",
                        "id": 1,
                    },
                    "columns": [
                        {
                            "column_name": "test_column3",
                            "description": "some description 3",
                            "type": "FLOAT",
                        },
                        {
                            "column_name": "test_column4",
                            "description": "some description 4",
                            "type": "DATETIME",
                        },
                    ],
                    "datasource_name": "Test Table 2",
                    "datasource_type": "table",
                    "default_endpoint": None,
                    "description": "Sample description for dataset 2",
                    "extra": None,
                    "fetch_values_predicate": None,
                    "filter_select_enabled": True,
                    "granularity_sqla": [["date_column", "date_column"]],
                    "id": 2,
                    "is_managed_externally": False,
                    "is_sqllab_view": True,
                    "kind": "virtual",
                    "main_dttm_col": "date_column",
                    "metrics": [
                        {
                            "changed_on": "2024-02-10T15:30:20.123456+0000",
                            "created_on": "2024-02-10T15:30:20.123456+0000",
                            "currency": None,
                            "d3format": None,
                            "description": None,
                            "expression": "sum(value)",
                            "extra": None,
                            "id": 2,
                            "metric_name": "total_value",
                            "metric_type": "NUMERIC",
                            "rendered_expression": "sum(value)",
                            "verbose_name": "Total Value",
                            "warning_text": None,
                        },
                        {
                            "changed_on": "2025-06-27T15:30:20.123456+0000",
                            "created_on": "2025-06-27T15:30:20.123456+0000",
                            "currency": None,
                            "d3format": None,
                            "description": "Count of rows",
                            "expression": "count(*)",
                            "extra": None,
                            "id": 3,
                            "metric_name": "count",
                            "metric_type": None,
                            "rendered_expression": "count(*)",
                            "verbose_name": "Count",
                            "warning_text": None,
                        },
                    ],
                    "name": "Test Table 2",
                    "normalize_columns": True,
                    "offset": 0,
                    "owners": [
                        {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "Owner2",
                        }
                    ],
                    "rendered_sql": """
                    SELECT tt2.id, tt2.name, tt2.description, db.database_name 
                    FROM test_table2 tt2
                    JOIN databases db ON tt2.database_id = db.id
                    WHERE tt2.kind = 'virtual'
                    ORDER BY tt2.id DESC;
                    """,
                    "schema": "test_schema2",
                    "table_name": "Test Table 2",
                    "uid": "2__table",
                    "url": "/tablemodelview/edit/2",
                    "verbose_map": {
                        "__timestamp": "Time",
                        "id": "ID",
                        "name": "Name",
                        "value": "Value",
                        "date_column": "Date",
                    },
                },
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/3": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": 3,
                "result": {
                    "always_filter_main_dttm": False,
                    "cache_timeout": None,
                    "changed_by": {
                        "first_name": "Test",
                        "id": 1,
                        "last_name": "Owners1",
                    },
                    "changed_on": "2024-01-05T21:10:15.650819+0000",
                    "changed_on_humanized": "10 months ago",
                    "columns": [
                        {
                            "created_on": "2024-01-05T21:10:15.650819+0000",
                            "changed_on": "2024-01-05T21:10:15.650819+0000",
                            "column_name": "id",
                            "type": "INT",
                            "id": 1,
                            "verbose_name": "null",
                        },
                        {
                            "created_on": "2024-01-05T21:10:15.650819+0000",
                            "changed_on": "2024-01-05T21:10:15.650819+0000",
                            "column_name": "name",
                            "type": "STRING",
                            "id": 2,
                            "verbose_name": "null",
                        },
                    ],
                    "created_by": {"first_name": "Test", "last_name": "User1"},
                    "created_on": "2024-01-05T21:10:15.650819+0000",
                    "created_on_humanized": "10 months ago",
                    "currency_formats": {},
                    "database": {
                        "backend": "postgresql",
                        "database_name": "test_database1",
                        "id": 1,
                    },
                    "datasource_name": "Test Table 1",
                    "datasource_type": "table",
                    "default_endpoint": None,
                    "description": None,
                    "extra": None,
                    "fetch_values_predicate": None,
                    "filter_select_enabled": True,
                    "granularity_sqla": [
                        ["created_at", "created_at"],
                        ["updated_at", "updated_at"],
                    ],
                    "id": 3,
                    "is_managed_externally": False,
                    "is_sqllab_view": False,
                    "kind": "virtual",
                    "main_dttm_col": None,
                    "metrics": [
                        {
                            "changed_on": "2024-01-05T21:10:15.650819+0000",
                            "created_on": "2024-01-05T21:10:15.650819+0000",
                            "currency": None,
                            "d3format": None,
                            "description": None,
                            "expression": "count(*)",
                            "extra": None,
                            "id": 1,
                            "metric_name": "count",
                            "metric_type": None,
                            "rendered_expression": "count(*)",
                            "verbose_name": None,
                            "warning_text": None,
                        },
                        {
                            "changed_on": "2025-06-27T15:30:20.123456+0000",
                            "created_on": "2025-06-27T15:30:20.123456+0000",
                            "currency": None,
                            "d3format": None,
                            "description": "Total rows",
                            "expression": "count(1)",
                            "extra": None,
                            "id": 3,
                            "metric_name": "total_rows",
                            "metric_type": "NUMERIC",
                            "rendered_expression": "count(1)",
                            "verbose_name": "Total Rows",
                            "warning_text": None,
                        },
                    ],
                    "name": "Test Table 3",
                    "normalize_columns": True,
                    "offset": 0,
                    "owners": [
                        {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owner1",
                        }
                    ],
                    "schema": "test_schema3",
                    "select_star": "SELECT * FROM test_schema3.test_table3 LIMIT 100",
                    "table_name": "Test Table 3",
                    "uid": "1__table",
                    "url": "/tablemodelview/edit/3",
                    "verbose_map": {
                        "__timestamp": "Time",
                        "id": "ID",
                        "name": "Name",
                        "created_at": "Created At",
                        "updated_at": "Updated At",
                    },
                },
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/20": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "result": {
                    "schema": "test_schema_name",
                    "table_name": "test_table_name",
                    "database": {
                        "id": 30,
                        "database_name": "test_database_name",
                    },
                },
            },
        },
        "mock://mock-domain.superset.com/api/v1/database/1": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": 1,
                "result": {
                    "configuration_method": "sqlalchemy_form",
                    "database_name": "test_database1",
                    "id": 1,
                    "sqlalchemy_uri": "postgresql://user:password@host:port/test_database1",
                },
            },
        },
        "mock://mock-domain.superset.com/api/v1/database/30": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "result": {
                    "sqlalchemy_uri": "test_sqlalchemy_uri",
                },
            },
        },
        "mock://mock-domain.superset.com/api/v1/dashboard/related/owners": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 2,
                "result": [
                    {
                        "extra": {"active": True, "email": "test_owner1@example.com"},
                        "text": "test_owner1",
                        "value": 1,
                    },
                    {
                        "extra": {"active": True, "email": "test_owner2@example.com"},
                        "text": "test_owner2",
                        "value": 2,
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/related/owners": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 2,
                "result": [
                    {
                        "extra": {"active": True, "email": "test_owner3@example.com"},
                        "text": "test_owner3",
                        "value": 3,
                    },
                    {
                        "extra": {"active": True, "email": "test_owner4@example.com"},
                        "text": "test_owner4",
                        "value": 4,
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/chart/related/owners": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 2,
                "result": [
                    {
                        "extra": {"active": True, "email": "test_owner5@example.com"},
                        "text": "test_owner5",
                        "value": 5,
                    },
                    {
                        "extra": {"active": True, "email": "test_owner6@example.com"},
                        "text": "test_owner6",
                        "value": 6,
                    },
                ],
            },
        },
    }

    api_vs_response.update(override_data)

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_superset_ingest(
    pytestconfig: pytest.Config, tmp_path: Path, mock_time: None, requests_mock: Any
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/superset"

    register_mock_api(request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "superset-test",
            "source": {
                "type": "superset",
                "config": {
                    "connect_uri": "mock://mock-domain.superset.com/",
                    "username": "test_username",
                    "password": "test_password",
                    "provider": "db",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/superset_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "superset_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_superset_stateful_ingest(
    pytestconfig: pytest.Config,
    tmp_path: Path,
    mock_time: None,
    requests_mock: Any,
    mock_datahub_graph: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/superset"

    register_mock_api(request_mock=requests_mock)

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "superset",
            "config": {
                "connect_uri": "mock://mock-domain.superset.com/",
                "username": "test_username",
                "password": "test_password",
                "provider": "db",
                # enable dataset ingestion
                "ingest_datasets": True,
                # enable timeout for api calls, this is not required
                # but just for coverage
                "timeout": 10,
                # set max_threads to 10
                "max_threads": 10,
                # enable stateful ingestion
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100.0,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            },
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "test_pipeline",
    }

    asset_override = {
        "mock://mock-domain.superset.com/api/v1/dashboard/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 1,
                "result": [
                    {
                        "id": 1,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owners1",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "dashboard_title": "test_dashboard_title_1",
                        "url": "/dashboard/test_dashboard_url_1",
                        "position_json": '{"CHART-test-1": {"meta": { "chartId": "10" }}, "CHART-test-2": {"meta": { "chartId": "11" }}}',
                        "status": "published",
                        "published": True,
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 1,
                                "last_name": "Owner1",
                            },
                            {
                                "first_name": "Test",
                                "id": 2,
                                "last_name": "Owner2",
                            },
                        ],
                        "certified_by": "Certification team",
                        "certification_details": "Approved",
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/chart/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 3,
                "result": [
                    {
                        "id": 10,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owners1",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_1",
                        "viz_type": "box_plot",
                        "url": "/explore/test_chart_url_10",
                        "datasource_id": 1,
                        "params": '{"metrics": [], "adhoc_filters": []}',
                        "form_data": {
                            "all_columns": [
                                {
                                    "expressionType": "SQL",
                                    "label": "test_label",
                                    "sqlExpression": "",
                                },
                                "test_column1",
                                "test_column2",
                            ],
                        },
                    },
                    {
                        "id": 11,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "Owners1",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_2",
                        "viz_type": "pie",
                        "url": "/explore/test_chart_url_11",
                        "datasource_id": 2,
                        "params": '{"metrics": [], "adhoc_filters": []}',
                        "form_data": {
                            "all_columns": [
                                "test_column3",
                                "test_column4",
                            ],
                        },
                    },
                    {
                        "id": 12,
                        "changed_by": {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "Owners2",
                        },
                        "changed_on_utc": "2020-04-14T07:00:00.000000+0000",
                        "slice_name": "test_chart_title_3",
                        "viz_type": "treemap",
                        "url": "/explore/test_chart_url_12",
                        "datasource_id": "20",
                        "params": '{"metrics": [], "adhoc_filters": []}',
                    },
                ],
            },
        },
        "mock://mock-domain.superset.com/api/v1/dataset/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "count": 214,
                "description_columns": {},
                "ids": [1, 2, 3],
                "result": [
                    {
                        "changed_by": {
                            "first_name": "Test",
                            "id": 2,
                            "last_name": "User2",
                        },
                        "changed_by_name": "test_username_2",
                        "changed_on_delta_humanized": "9 months ago",
                        "changed_on_utc": "2024-02-10T15:30:20.123456+0000",
                        "database": {"database_name": "test_database1", "id": 1},
                        "datasource_type": "table",
                        "default_endpoint": None,
                        "description": "Sample description for dataset 2",
                        "explore_url": "/explore/?datasource_type=table&datasource_id=2",
                        "extra": None,
                        "id": 2,
                        "kind": "virtual",
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 2,
                                "last_name": "Owner2",
                            }
                        ],
                        "schema": "test_schema2",
                        "rendered_sql": """
                        SELECT tt2.id, tt2.name, tt2.description, db.database_name 
                        FROM test_table2 tt2
                        JOIN databases db ON tt2.database_id = db.id
                        WHERE tt2.kind = 'virtual'
                        ORDER BY tt2.id DESC;
                        """,
                        "table_name": "Test Table 2",
                    },
                    {
                        "changed_by": {
                            "first_name": "Test",
                            "id": 1,
                            "last_name": "User1",
                        },
                        "changed_by_name": "test_username_1",
                        "changed_on_delta_humanized": "9 months ago",
                        "columns": [
                            {
                                "created_on": "2024-01-05T21:10:15.650819+0000",
                                "changed_on": "2024-01-05T21:10:15.650819+0000",
                                "column_name": "id",
                                "type": "INT",
                                "id": 1,
                                "verbose_name": "null",
                            },
                            {
                                "created_on": "2024-01-05T21:10:15.650819+0000",
                                "changed_on": "2024-01-05T21:10:15.650819+0000",
                                "column_name": "name",
                                "type": "STRING",
                                "id": 2,
                                "verbose_name": "null",
                            },
                        ],
                        "changed_on_utc": "2024-02-10T15:30:20.123456+0000",
                        "database": {"database_name": "test_database1", "id": 1},
                        "datasource_type": "table",
                        "default_endpoint": None,
                        "description": "Sample description for dataset 3",
                        "explore_url": "/explore/?datasource_type=table&datasource_id=3",
                        "extra": None,
                        "id": 3,
                        "kind": "physical",
                        "owners": [
                            {
                                "first_name": "Test",
                                "id": 2,
                                "last_name": "Owner2",
                            }
                        ],
                        "schema": "test_schema3",
                        "select_star": "SELECT * FROM test_schema3.test_table3 LIMIT 100",
                        "table_name": "Test Table 3",
                    },
                ],
            },
        },
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        # Both checkpoint and reporting will use the same mocked graph instance.
        mock_checkpoint.return_value = mock_datahub_graph

        # Do the first run of the pipeline and get the default job's checkpoint.
        pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Remove one dashboard, chart, dataset from the superset config.
        register_mock_api(request_mock=requests_mock, override_data=asset_override)

        # Capture MCEs of second run to validate Status(removed=true)
        deleted_mces_path = f"{tmp_path}/superset_deleted_mces.json"
        pipeline_config_dict["sink"]["type"] = "file"
        pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}

        # Do the second run of the pipeline.
        pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        # Perform all assertions on the states. The deleted dashboard should not be
        # part of the second state
        state1 = checkpoint1.state
        state2 = checkpoint2.state
        dashboard_difference_urns = list(
            state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
        )
        chart_difference_urns = list(
            state1.get_urns_not_in(type="chart", other_checkpoint_state=state2)
        )
        dataset_difference_urns = list(
            state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
        )

        assert len(dashboard_difference_urns) == 1
        assert len(chart_difference_urns) == 1
        assert len(dataset_difference_urns) == 1

        urn1 = "urn:li:dashboard:(superset,2)"
        urn2 = "urn:li:chart:(superset,13)"
        urn3 = "urn:li:dataset:(urn:li:dataPlatform:superset,test_database1.test_schema1.Test Table 1,PROD)"

        assert urn1 in dashboard_difference_urns
        assert urn2 in chart_difference_urns
        assert urn3 in dataset_difference_urns

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=deleted_mces_path,
            golden_path=test_resources_dir / "golden_test_stateful_ingest.json",
        )
