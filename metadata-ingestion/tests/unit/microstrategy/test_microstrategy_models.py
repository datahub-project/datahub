from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    DatasourceConnection,
    ReportDefinition,
)


def test_dashboard_definition_extracts_datasets_and_visualizations() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "result": {
                "definition": {
                    "datasets": [{"id": "ds-1", "name": "Sales Cube"}],
                    "chapters": [
                        {
                            "pages": [
                                {
                                    "visualizations": [
                                        {
                                            "key": "viz-1",
                                            "name": "Revenue Trend",
                                            "datasets": [{"id": "ds-1"}],
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                }
            }
        },
    )

    assert [dataset.id for dataset in definition.datasets] == ["ds-1"]
    assert [visualization.key for visualization in definition.visualizations] == [
        "viz-1"
    ]
    assert definition.visualizations[0].datasets == ["ds-1"]
    assert definition.visualizations[0].chapter_key is None


def test_dataset_available_objects_list_is_grouped_by_type() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "datasets": [
                {
                    "id": "ds-1",
                    "name": "Sales Cube",
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"},
                        {
                            "id": "attribute-1",
                            "name": "Region",
                            "type": "attribute",
                        },
                    ],
                }
            ]
        },
    )

    available_objects = definition.datasets[0].available_objects

    assert available_objects["metrics"][0]["id"] == "metric-1"
    assert available_objects["attributes"][0]["id"] == "attribute-1"


def test_dashboard_definition_preserves_visualization_chapter_key() -> None:
    definition = DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Executive Sales",
        response={
            "chapters": [
                {
                    "key": "chapter-1",
                    "pages": [
                        {
                            "key": "page-1",
                            "visualizations": [
                                {
                                    "key": "viz-1",
                                    "name": "Revenue Trend",
                                }
                            ],
                        }
                    ],
                }
            ]
        },
    )

    visualization = definition.visualizations[0]

    assert visualization.chapter_key == "chapter-1"
    assert visualization.page_key == "page-1"


def test_datasource_extracts_source_type_and_connection() -> None:
    datasource = Datasource.model_validate(
        {
            "id": "source-1",
            "name": "Enterprise Warehouse",
            "datasourceType": "normal",
            "database": {
                "type": "snow_flake",
                "version": "snowflake_1x",
                "connection": {
                    "id": "conn-1",
                    "name": "Snowflake Connection",
                    "embedded": False,
                },
            },
            "dbms": {"name": "Snowflake"},
        }
    )

    assert datasource.database_type == "snow_flake"
    assert datasource.database_version == "snowflake_1x"
    assert datasource.dbms_name == "Snowflake"
    assert datasource.connection_id == "conn-1"
    assert datasource.connection_name == "Snowflake Connection"
    assert datasource.connection_embedded is False


def test_datasource_connection_drops_raw_connection_string_but_keeps_context() -> None:
    connection = DatasourceConnection.model_validate(
        {
            "id": "conn-1",
            "name": "Sales Warehouse Connection",
            "database": {"type": "snow_flake"},
            "connectionString": "DATABASE=SALES_DB;SCHEMA=ORDERS;UID=metadata_reader",
        }
    )

    assert connection.database_type == "snow_flake"
    assert connection.database_name == "SALES_DB"
    assert connection.schema_name == "ORDERS"
    assert connection.connection_string_present is True
    assert "connectionString" not in connection.model_dump()


def test_dataset_preserves_source_warehouse_reference_when_present() -> None:
    dataset = DatasetObject.model_validate(
        {
            "id": "ds-1",
            "name": "Sales Cube",
            "sourceWarehouse": {
                "id": "source-1",
                "name": "Enterprise Warehouse",
                "database": {"type": "snow_flake"},
            },
        }
    )

    assert dataset.source_warehouse is not None
    assert dataset.source_warehouse.id == "source-1"
    assert dataset.source_warehouse.database_type == "snow_flake"


def test_report_definition_extracts_source_and_available_objects() -> None:
    definition = ReportDefinition.from_api_response(
        object_id="report-1",
        object_name="Sales Report",
        response={
            "result": {
                "definition": {
                    "dataSource": {"id": "cube-1", "name": "Sales Cube"},
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"},
                        {
                            "id": "attr-1",
                            "name": "Region",
                            "type": "attribute",
                        },
                    ],
                    "prompts": [{"id": "prompt-1"}],
                    "filter": {"id": "filter-1"},
                }
            }
        },
    )

    assert definition.source_id == "cube-1"
    assert definition.source_name == "Sales Cube"
    assert definition.available_objects["metrics"][0]["id"] == "metric-1"
    assert definition.available_objects["attributes"][0]["id"] == "attr-1"
    assert definition.object_ids == ["attr-1", "metric-1"]
    assert definition.prompt_count == 1
    assert definition.has_filter is True
