from pathlib import Path
from typing import Any, Dict, Iterable, List
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.microstrategy.client import MicroStrategyClient
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MSTRObject,
    Project,
)
from datahub.testing import mce_helpers


def _pipeline_config(output_path: Path) -> Dict[str, Any]:
    return {
        "run_id": "microstrategy-source-test",
        "source": {
            "type": "microstrategy",
            "config": {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "platform_instance": "prod",
                "dashboard_pattern": {"allow": ["^Sales Performance$"]},
                "extract_warehouse_lineage": False,
                "stateful_ingestion": {"enabled": False},
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": str(output_path),
            },
        },
    }


def _projects(_client: MicroStrategyClient) -> List[Project]:
    return [
        Project.model_validate(
            {
                "id": "project-1",
                "name": "Sales Analytics",
                "description": "Sales project",
            }
        )
    ]


def _source_warehouses(
    _client: MicroStrategyClient,
    _project_id: str,
) -> List[Datasource]:
    return [
        Datasource.model_validate(
            {
                "id": "source-1",
                "name": "Sales Warehouse",
                "database": {
                    "type": "snow_flake",
                    "version": "snowflake_1x",
                    "name": "SALES_DB",
                    "schema": "ORDERS",
                },
                "dbms": {"name": "Snowflake"},
                "connection": {"id": "connection-1", "name": "Sales Connection"},
            }
        )
    ]


def _source_connection(
    _client: MicroStrategyClient,
    _connection_id: str,
    project_id: str | None = None,
) -> DatasourceConnection:
    assert project_id == "project-1"
    return DatasourceConnection.model_validate(
        {
            "id": "connection-1",
            "name": "Sales Connection",
            "driverType": "odbc",
            "database": {"type": "snow_flake"},
            "connectionString": "DATABASE=SALES_DB;SCHEMA=ORDERS;UID=reader",
        }
    )


def _dashboards(
    _client: MicroStrategyClient,
    _project_id: str,
) -> Iterable[MSTRObject]:
    return [
        MSTRObject.model_validate(
            {
                "id": "dash-1",
                "name": "Sales Performance",
                "type": "55",
                "subtype": "14081",
                "description": "Sales performance dashboard",
                "owner": {"username": "sales_owner"},
                # Real quick-search responses (getAncestors=true) carry the
                # folder path as a top-down ancestors array.
                "ancestors": [
                    {"id": "folder-1", "name": "Shared Reports"},
                    {"id": "folder-2", "name": "Finance"},
                ],
            }
        )
    ]


def _dashboard_dependencies(
    _client: MicroStrategyClient,
    project_id: str,
    object_id: str,
    object_type: str,
) -> List[MSTRObject]:
    assert project_id == "project-1"
    assert object_id == "dash-1"
    assert object_type == "55"
    return [
        MSTRObject.model_validate(
            {"id": "metric-1", "name": "Revenue", "type": "4", "subtype": "1024"}
        ),
        MSTRObject.model_validate(
            {"id": "attr-1", "name": "Order Date", "type": "12", "subtype": "3072"}
        ),
    ]


def _dossier_definition(
    _client: MicroStrategyClient,
    _project_id: str,
    _dossier_id: str,
) -> Dict[str, Any]:
    return {
        "result": {
            "definition": {
                "datasets": [
                    {
                        "id": "ds-1",
                        "name": "Sales Cube",
                        "description": "Embedded sales cube",
                        "sourceWarehouse": {
                            "id": "source-1",
                            "name": "Sales Warehouse",
                            "database": {
                                "type": "snow_flake",
                                "version": "snowflake_1x",
                                "name": "SALES_DB",
                                "schema": "ORDERS",
                            },
                            "dbms": {"name": "Snowflake"},
                            "connection": {
                                "id": "connection-1",
                                "name": "Sales Connection",
                            },
                        },
                        "availableObjects": {
                            "metrics": [
                                {
                                    "id": "metric-1",
                                    "name": "Revenue",
                                    "dataType": "double",
                                }
                            ],
                            "attributes": [
                                {
                                    "id": "attr-1",
                                    "name": "Order Date",
                                    "forms": [
                                        {
                                            "id": "form-1",
                                            "name": "ID",
                                            "dataType": "date",
                                            "baseFormCategory": "DATE",
                                        }
                                    ],
                                }
                            ],
                        },
                    }
                ],
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
                                        "type": "line",
                                        "datasets": ["ds-1"],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        }
    }


def _visualization_definition(
    _client: MicroStrategyClient,
    project_id: str,
    dossier_id: str,
    instance_id: str,
    chapter_key: str,
    visualization_key: str,
) -> Dict[str, Any]:
    assert project_id == "project-1"
    assert dossier_id == "dash-1"
    assert instance_id == "i-1"
    assert chapter_key == "chapter-1"
    assert visualization_key == "viz-1"
    return {
        "key": "viz-1",
        "name": "Revenue Trend",
        "datasets": ["ds-1"],
        "template": {
            "metrics": [{"id": "metric-1", "name": "Revenue"}],
            "attributes": [{"id": "attr-1", "name": "Order Date"}],
        },
    }


def _metric_model(
    _client: MicroStrategyClient,
    _project_id: str,
    _metric_id: str,
) -> Dict[str, Any]:
    # Shape matches what /api/model/metrics/{id}?showExpressionAs=tokens returns:
    # object references are nested under each token's "target".
    return {
        "expression": {
            "text": "Sum(Revenue Fact)",
            "tokens": [
                {
                    "type": "object_reference",
                    "target": {
                        "objectId": "fact-1",
                        "subType": "fact",
                        "name": "NET_SALES_AMT",
                    },
                }
            ],
        }
    }


def _model_tables(
    _client: MicroStrategyClient,
    _project_id: str,
    limit: int = 1,
    offset: int = 0,
    fields: str | None = None,
) -> Dict[str, Any]:
    assert fields == "physicalTable,attributes,facts"
    if offset > 0:
        return {"tables": [], "total": 1}
    return {
        "tables": [
            {
                "physicalTable": {
                    "namespace": "SALES_DB",
                    "tablePrefix": "ORDERS",
                    "tableName": "fact_orders",
                },
                "facts": [
                    {
                        "information": {"objectId": "fact-1"},
                        "expression": {"text": "net_sales_amt"},
                    }
                ],
                "attributes": [
                    {
                        "information": {"objectId": "attr-1"},
                        "forms": [
                            {
                                "name": "ID",
                                "expression": {"text": "order_date_id"},
                            }
                        ],
                    }
                ],
            }
        ],
        "total": 1,
    }


def _dataset_sql_view(
    _client: MicroStrategyClient,
    project_id: str,
    dossier_id: str,
    instance_id: str,
) -> List[Dict[str, Any]]:
    assert project_id == "project-1"
    assert dossier_id == "dash-1"
    assert instance_id == "i-1"
    return [
        {
            "id": "ds-1",
            "name": "Sales Cube",
            "sqlStatement": (
                "select net_sales_amt, order_date_id from SALES_DB.ORDERS.fact_orders"
            ),
        }
    ]


def test_microstrategy_ingestion(pytestconfig: Any, tmp_path: Path) -> None:
    output_path = tmp_path / "microstrategy_mcps.json"
    test_resources_dir = pytestconfig.rootpath / "tests/integration/microstrategy"

    with (
        patch.object(MicroStrategyClient, "login", return_value=None),
        patch.object(MicroStrategyClient, "close", return_value=None),
        patch.object(MicroStrategyClient, "list_projects", _projects),
        patch.object(
            MicroStrategyClient,
            "list_project_datasources",
            _source_warehouses,
        ),
        patch.object(
            MicroStrategyClient,
            "get_datasource_connection",
            _source_connection,
        ),
        patch.object(MicroStrategyClient, "search_dashboards", _dashboards),
        patch.object(
            MicroStrategyClient,
            "get_dossier_definition",
            _dossier_definition,
        ),
        patch.object(
            MicroStrategyClient,
            "get_dossier_visualization",
            _visualization_definition,
        ),
        patch.object(
            MicroStrategyClient,
            "get_object_dependencies",
            _dashboard_dependencies,
        ),
        patch.object(MicroStrategyClient, "get_metric_model", _metric_model),
        patch.object(MicroStrategyClient, "list_model_tables", _model_tables),
        patch.object(
            MicroStrategyClient,
            "create_dossier_instance",
            return_value="i-1",
        ),
        patch.object(MicroStrategyClient, "delete_dossier_instance", return_value=True),
        patch.object(
            MicroStrategyClient,
            "create_document_instance",
            return_value="i-1",
        ),
        patch.object(
            MicroStrategyClient,
            "delete_document_instance",
            return_value=True,
        ),
        patch.object(
            MicroStrategyClient,
            "get_dossier_datasets_sql",
            _dataset_sql_view,
        ),
    ):
        pipeline = Pipeline.create(_pipeline_config(output_path))
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "microstrategy_mcps_golden.json",
    )
