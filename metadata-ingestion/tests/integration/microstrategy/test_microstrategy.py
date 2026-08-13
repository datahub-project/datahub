import json
from pathlib import Path
from typing import Any, Dict, Iterable, List
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.microstrategy.client import MicroStrategyClient
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_FOLDER_TYPE_SHARED_REPORTS,
    MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MicroStrategyObject,
    ModelTablesResponse,
    PredefinedFolder,
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
) -> Iterable[MicroStrategyObject]:
    return [
        MicroStrategyObject.model_validate(
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
) -> List[MicroStrategyObject]:
    assert project_id == "project-1"
    assert object_id == "dash-1"
    assert object_type == "55"
    return [
        MicroStrategyObject.model_validate(
            {"id": "metric-1", "name": "Revenue", "type": "4", "subtype": "1024"}
        ),
        MicroStrategyObject.model_validate(
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
                                },
                                {
                                    "id": "metric-ly",
                                    "name": "Revenue LY",
                                    "dataType": "double",
                                },
                                {
                                    "id": "metric-growth",
                                    "name": "Revenue Growth",
                                    "dataType": "double",
                                },
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
                    },
                    {
                        "id": "ds-2",
                        "name": "Cost Cube",
                        "description": "Embedded cost cube",
                        "availableObjects": {
                            "metrics": [
                                {
                                    "id": "metric-2",
                                    "name": "Cost",
                                    "dataType": "double",
                                }
                            ],
                            "attributes": [],
                        },
                    },
                ],
                "chapters": [
                    {
                        "key": "chapter-1",
                        "pages": [
                            {
                                "key": "page-1",
                                "name": "TREND",
                                "visualizations": [
                                    {
                                        "key": "viz-1",
                                        "name": "Revenue Trend",
                                        "type": "line",
                                        "datasets": ["ds-1"],
                                    }
                                ],
                            },
                            {
                                "key": "page-2",
                                "name": "SUMMARY",
                                "visualizations": [
                                    {
                                        "key": "viz-2",
                                        "name": "Sales vs Cost",
                                        "visualizationType": "compound_grid",
                                        "columnSets": [
                                            {"key": "cs-sales", "name": "SALES"},
                                            {"key": "cs-cost", "name": "COST"},
                                        ],
                                    }
                                ],
                            },
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
    if visualization_key == "viz-2":
        # Compound-grid runtime payload: column groups with member metrics,
        # including visualization-local derived metrics.
        return {
            "key": "viz-2",
            "name": "Sales vs Cost",
            "visualizationType": "compound_grid",
            "definition": {
                "grid": {
                    "columnSets": [
                        {
                            "key": "cs-sales",
                            "name": "SALES",
                            "columns": [
                                {
                                    "type": "templateMetrics",
                                    "elements": [
                                        {
                                            "type": "metric",
                                            "id": "metric-1",
                                            "name": "Revenue",
                                        },
                                        {
                                            "type": "metric",
                                            "id": "derived-1",
                                            "name": "Revenue Pct",
                                            "derived": True,
                                            "dataType": "double",
                                        },
                                    ],
                                }
                            ],
                        },
                        {
                            "key": "cs-cost",
                            "name": "COST",
                            "columns": [
                                {
                                    "type": "templateMetrics",
                                    "elements": [
                                        {
                                            "type": "metric",
                                            "id": "metric-2",
                                            "name": "Cost",
                                        },
                                        {
                                            "type": "metric",
                                            "id": "derived-2",
                                            "name": "Cost Pct",
                                            "derived": True,
                                            "dataType": "double",
                                        },
                                    ],
                                }
                            ],
                        },
                    ]
                }
            },
        }
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
    if _metric_id == "metric-growth":
        return {
            "expression": {
                "text": "({Revenue} - {Revenue LY}) / Abs({Revenue LY})",
                "tokens": [],
            }
        }
    if _metric_id == "metric-ly":
        return {"expression": {"text": "Sum(NET_SALES_AMT_LY)", "tokens": []}}
    if _metric_id == "metric-2":
        return {"expression": {"text": "Sum(NET_COST_AMT)", "tokens": []}}
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
) -> ModelTablesResponse:
    assert fields == "physicalTable,attributes,facts"
    if offset > 0:
        return ModelTablesResponse(tables=[], total=1)
    return ModelTablesResponse.model_validate(
        {
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
    )


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


def _no_predefined_folders(
    _client: MicroStrategyClient,
    _project_id: str,
    _folder_types: List[int],
) -> List[PredefinedFolder]:
    return []


def _dashboards_under_reports_folder(
    _client: MicroStrategyClient,
    _project_id: str,
) -> Iterable[MicroStrategyObject]:
    return [
        MicroStrategyObject.model_validate(
            {
                "id": "dash-1",
                "name": "Sales Performance",
                "type": "55",
                "subtype": "14081",
                "description": "Sales performance dashboard",
                "owner": {"username": "sales_owner"},
                # Real getAncestors shape from a live MicroStrategy instance:
                # project root folder (named after the project) -> "Public
                # Objects" -> "Reports" (shown by Strategy Web as "Shared
                # Reports") -> user folders.
                "ancestors": [
                    {"id": "project-root-id", "name": "Sales Analytics"},
                    {"id": "public-objects-id", "name": "Public Objects"},
                    {"id": "reports-folder-id", "name": "Reports"},
                    {"id": "finance-folder-id", "name": "Finance"},
                ],
            }
        )
    ]


def _shared_reports_predefined_folder(
    _client: MicroStrategyClient,
    _project_id: str,
    folder_types: List[int],
) -> List[PredefinedFolder]:
    assert folder_types == sorted(
        {MSTR_FOLDER_TYPE_SHARED_REPORTS} | MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES
    )
    return [
        PredefinedFolder.model_validate(
            {"id": "project-root-id", "name": "Sales Analytics", "folderType": 39}
        ),
        PredefinedFolder.model_validate(
            {"id": "public-objects-id", "name": "Public Objects", "folderType": 1}
        ),
        PredefinedFolder.model_validate(
            {"id": "reports-folder-id", "name": "Reports", "folderType": 7}
        ),
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
            MicroStrategyClient, "get_predefined_folders", _no_predefined_folders
        ),
        patch.object(MicroStrategyClient, "get_model_document", return_value={}),
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


def _empty_model_tables(
    _client: MicroStrategyClient,
    _project_id: str,
    limit: int = 1,
    offset: int = 0,
    fields: str | None = None,
) -> ModelTablesResponse:
    return ModelTablesResponse(tables=[], total=0)


def _temp_table_dataset_sql_view(
    _client: MicroStrategyClient,
    project_id: str,
    dossier_id: str,
    instance_id: str,
) -> List[Dict[str, Any]]:
    # Multi-pass SQL: base table -> volatile temp table -> final SELECT. Column
    # lineage must flow through the temp table to fact_orders' real columns, and
    # the final projection's aliases match the dataset's fields.
    return [
        {
            "id": "ds-1",
            "name": "Sales Cube",
            "sqlStatement": (
                'CREATE VOLATILE TABLE T0SP000 AS\nselect net_sales_amt as "Revenue", '
                'order_date_id as "Order Date" from SALES_DB.ORDERS.fact_orders\n\n'
                'select "Revenue", "Order Date" from T0SP000\n'
            ),
        }
    ]


def test_microstrategy_sql_view_temp_table_column_lineage(
    pytestconfig: Any, tmp_path: Path
) -> None:
    output_path = tmp_path / "microstrategy_temp_table_mcps.json"
    test_resources_dir = pytestconfig.rootpath / "tests/integration/microstrategy"

    config = _pipeline_config(output_path)
    config["source"]["config"]["extract_warehouse_lineage"] = True

    with (
        patch.object(MicroStrategyClient, "login", return_value=None),
        patch.object(MicroStrategyClient, "close", return_value=None),
        patch.object(MicroStrategyClient, "list_projects", _projects),
        patch.object(
            MicroStrategyClient, "list_project_datasources", _source_warehouses
        ),
        patch.object(
            MicroStrategyClient, "get_datasource_connection", _source_connection
        ),
        patch.object(MicroStrategyClient, "search_dashboards", _dashboards),
        patch.object(
            MicroStrategyClient, "get_dossier_definition", _dossier_definition
        ),
        patch.object(
            MicroStrategyClient, "get_dossier_visualization", _visualization_definition
        ),
        patch.object(
            MicroStrategyClient, "get_object_dependencies", _dashboard_dependencies
        ),
        patch.object(MicroStrategyClient, "get_metric_model", _metric_model),
        # No model tables, so column lineage can only come from the SQL view.
        patch.object(MicroStrategyClient, "list_model_tables", _empty_model_tables),
        patch.object(
            MicroStrategyClient, "get_predefined_folders", _no_predefined_folders
        ),
        patch.object(MicroStrategyClient, "get_model_document", return_value={}),
        patch.object(
            MicroStrategyClient, "create_dossier_instance", return_value="i-1"
        ),
        patch.object(MicroStrategyClient, "delete_dossier_instance", return_value=True),
        patch.object(
            MicroStrategyClient, "create_document_instance", return_value="i-1"
        ),
        patch.object(
            MicroStrategyClient, "delete_document_instance", return_value=True
        ),
        patch.object(
            MicroStrategyClient,
            "get_dossier_datasets_sql",
            _temp_table_dataset_sql_view,
        ),
    ):
        pipeline = Pipeline.create(config)
        pipeline.run()
        pipeline.raise_from_status()

    mcps = json.loads(output_path.read_text())
    fine_grained = [
        fine
        for mcp in mcps
        if mcp.get("aspectName") == "upstreamLineage"
        for fine in (mcp["aspect"]["json"].get("fineGrainedLineages") or [])
    ]
    upstreams = {urn for fine in fine_grained for urn in fine["upstreams"]}

    base = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "sales_db.orders.fact_orders,PROD)"
    )
    assert f"urn:li:schemaField:({base},net_sales_amt)" in upstreams
    assert f"urn:li:schemaField:({base},order_date_id)" in upstreams

    # Golden file pins the full end-to-end lineage shape for the
    # extract_warehouse_lineage=True path (targeted asserts above document intent).
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "microstrategy_warehouse_lineage_golden.json",
    )


def _run_predefined_folder_pipeline(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    with (
        patch.object(MicroStrategyClient, "login", return_value=None),
        patch.object(MicroStrategyClient, "close", return_value=None),
        patch.object(MicroStrategyClient, "list_projects", _projects),
        patch.object(
            MicroStrategyClient, "list_project_datasources", _source_warehouses
        ),
        patch.object(
            MicroStrategyClient, "get_datasource_connection", _source_connection
        ),
        patch.object(
            MicroStrategyClient, "search_dashboards", _dashboards_under_reports_folder
        ),
        patch.object(
            MicroStrategyClient, "get_dossier_definition", _dossier_definition
        ),
        patch.object(
            MicroStrategyClient, "get_dossier_visualization", _visualization_definition
        ),
        patch.object(
            MicroStrategyClient, "get_object_dependencies", _dashboard_dependencies
        ),
        patch.object(MicroStrategyClient, "get_metric_model", _metric_model),
        patch.object(MicroStrategyClient, "list_model_tables", _model_tables),
        patch.object(
            MicroStrategyClient,
            "get_predefined_folders",
            _shared_reports_predefined_folder,
        ),
        patch.object(MicroStrategyClient, "get_model_document", return_value={}),
        patch.object(
            MicroStrategyClient, "create_dossier_instance", return_value="i-1"
        ),
        patch.object(MicroStrategyClient, "delete_dossier_instance", return_value=True),
        patch.object(
            MicroStrategyClient, "create_document_instance", return_value="i-1"
        ),
        patch.object(
            MicroStrategyClient, "delete_document_instance", return_value=True
        ),
        patch.object(
            MicroStrategyClient, "get_dossier_datasets_sql", _dataset_sql_view
        ),
    ):
        pipeline = Pipeline.create(config)
        pipeline.run()
        pipeline.raise_from_status()

    output_path = Path(config["sink"]["config"]["filename"])
    return json.loads(output_path.read_text())


def _container_names(mcps: List[Dict[str, Any]]) -> set[str]:
    return {
        mcp["aspect"]["json"]["name"]
        for mcp in mcps
        if mcp.get("aspectName") == "containerProperties"
    }


def test_microstrategy_resolves_shared_reports_predefined_folder(
    tmp_path: Path,
) -> None:
    # The "Reports" folder's real MicroStrategy-assigned label ("Shared Reports")
    # comes from GET /api/folders/preDefined, not the raw ancestor name -- this
    # is the default (use_predefined_folder_names defaults to True).
    output_path = tmp_path / "microstrategy_predefined_folder_mcps.json"
    mcps = _run_predefined_folder_pipeline(_pipeline_config(output_path))

    names = _container_names(mcps)
    assert "Shared Reports" in names
    assert "Reports" not in names
    assert "Finance" in names
    # System containers Strategy Web never shows: "Public Objects" and the
    # project root folder, whose name would duplicate the project container's.
    assert "Public Objects" not in names
    project_name_containers = [
        mcp
        for mcp in mcps
        if mcp.get("aspectName") == "containerProperties"
        and mcp["aspect"]["json"].get("name") == "Sales Analytics"
    ]
    assert len(project_name_containers) == 1


def test_microstrategy_keeps_raw_folder_name_when_predefined_lookup_disabled(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "microstrategy_raw_folder_mcps.json"
    config = _pipeline_config(output_path)
    config["source"]["config"]["use_predefined_folder_names"] = False

    mcps = _run_predefined_folder_pipeline(config)

    names = _container_names(mcps)
    assert "Reports" in names
    assert "Shared Reports" not in names
    # Without the predefined lookup nothing can be hidden either; the raw
    # ancestor chain keeps its system containers.
    assert "Public Objects" in names


def test_microstrategy_metric_formula_lineage_flag_emits_field_edges(
    tmp_path: Path,
) -> None:
    # Opt-in flag: catalog metric formulas that reference sibling metrics as
    # `{Name}` tokens become same-dataset field-to-field lineage.
    output_path = tmp_path / "microstrategy_formula_lineage_mcps.json"
    config = _pipeline_config(output_path)
    config["source"]["config"]["extract_metric_formula_lineage"] = True

    mcps = _run_predefined_folder_pipeline(config)

    # Collect edges across all datasets; the growth metric's edge must exist.
    all_fine_grained = [
        lineage
        for mcp in mcps
        if mcp.get("aspectName") == "upstreamLineage"
        for lineage in (mcp["aspect"]["json"].get("fineGrainedLineages") or [])
    ]
    growth_edges = [
        lineage
        for lineage in all_fine_grained
        if any("Revenue Growth" in downstream for downstream in lineage["downstreams"])
    ]
    assert len(growth_edges) == 1
    upstream_fields = {urn.split(",")[-1][:-1] for urn in growth_edges[0]["upstreams"]}
    assert upstream_fields == {"Revenue", "Revenue LY"}
    # Same-dataset edges: the upstream fields live on the downstream's dataset.
    downstream_parent = growth_edges[0]["downstreams"][0].rsplit(",", 1)[0]
    assert all(
        urn.startswith(downstream_parent) for urn in growth_edges[0]["upstreams"]
    )
