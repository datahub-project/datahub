from typing import Any, Dict, Iterator, List
from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.microstrategy.client import MicroStrategyAPIError
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import WarehouseLineageContext
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    MSTRObject,
    ReportDefinition,
)
from datahub.ingestion.source.microstrategy.source import (
    MicroStrategySource,
    _LazyProjectLineage,
)


def _source(extra_config: dict | None = None) -> MicroStrategySource:
    config_dict = {
        "base_url": "https://mstr.example.com/MicroStrategyLibrary",
        "extract_warehouse_lineage": True,
    }
    if extra_config:
        config_dict.update(extra_config)
    config = MicroStrategyConfig.model_validate(config_dict)
    return MicroStrategySource(config, PipelineContext(run_id="test"))


def _dashboard(source_warehouse: dict | None = None) -> DashboardDefinition:
    dataset = {
        "id": "ds-1",
        "name": "Sales Cube",
        "availableObjects": {
            "metrics": [
                {
                    "id": "metric-1",
                    "name": "Revenue",
                    "dataType": "double",
                }
            ]
        },
    }
    if source_warehouse:
        dataset["sourceWarehouse"] = source_warehouse

    return DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Sales Dashboard",
        response={"result": {"definition": {"datasets": [dataset]}}},
    )


def test_attach_dataset_warehouse_upstreams_uses_dataset_source_warehouse() -> None:
    source = _source()
    dashboard = _dashboard(
        {
            "id": "source-1",
            "name": "Sales Warehouse",
            "database": {
                "type": "snow_flake",
                "name": "SALES_DB",
                "schema": "SALES",
            },
        }
    )

    source._attach_dataset_warehouse_upstreams(
        [
            {
                "id": "ds-1",
                "sqlStatement": "select * from fact_sales",
            }
        ],
        dashboard,
        context=None,
    )

    assert dashboard.datasets[0].warehouse_upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]


def test_attach_dataset_warehouse_upstreams_skips_without_context() -> None:
    source = _source()
    dashboard = _dashboard()

    source._attach_dataset_warehouse_upstreams(
        [
            {
                "id": "ds-1",
                "sqlStatement": "select * from fact_sales",
            }
        ],
        dashboard,
        context=None,
    )

    assert dashboard.datasets[0].warehouse_upstream_urns == []


def test_report_source_dataset_uses_report_definition_source_and_fields() -> None:
    report_object = MSTRObject.model_validate(
        {"id": "report-1", "name": "Sales Report", "type": "3"}
    )
    report_definition = ReportDefinition.from_api_response(
        object_id="report-1",
        object_name="Sales Report",
        response={
            "result": {
                "definition": {
                    "dataSource": {"id": "cube-1", "name": "Sales Cube"},
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"}
                    ],
                }
            }
        },
    )

    dataset = MicroStrategySource._report_source_dataset(
        report_object,
        report_definition,
    )

    assert dataset is not None
    assert dataset.id == "cube-1"
    assert dataset.name == "Sales Cube"
    assert dataset.available_objects["metrics"][0]["id"] == "metric-1"


def test_dashboard_report_chart_edges_are_filtered_by_report_pattern() -> None:
    source = _source(
        {
            "extract_reports": True,
            "report_pattern": {"allow": ["^Sales Report$"]},
        }
    )
    dashboard = _dashboard()
    dashboard.dependencies = [
        MSTRObject.model_validate(
            {"id": "report-1", "name": "Sales Report", "type": "3"}
        ),
        MSTRObject.model_validate(
            {"id": "report-2", "name": "Operations Report", "type": "3"}
        ),
        MSTRObject.model_validate(
            {"id": "dash-2", "name": "Other Dashboard", "type": "55"}
        ),
    ]

    assert source._dashboard_report_chart_urns("project-1", dashboard) == [
        source.mapper.report_urn("project-1", "report-1")
    ]


def test_report_sql_lineage_attaches_upstreams_to_report_source_dataset() -> None:
    source = _source({"extract_report_sql_lineage": True})
    report_object = MSTRObject.model_validate(
        {"id": "report-1", "name": "Sales Report", "type": "3"}
    )
    dataset = DatasetObject.model_validate({"id": "cube-1", "name": "Sales Cube"})

    class FakeClient:
        deleted = False

        def create_report_instance(self, project_id: str, report_id: str) -> str:
            return "instance-1"

        def get_report_sql_view(
            self,
            project_id: str,
            report_id: str,
            instance_id: str,
        ) -> dict:
            return {"sqlStatement": "select * from fact_sales"}

        def delete_report_instance(
            self,
            project_id: str,
            report_id: str,
            instance_id: str,
        ) -> bool:
            self.deleted = True
            return True

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    source._enrich_report_sql_lineage(
        project_id="project-1",
        report_object=report_object,
        dataset=dataset,
        context=WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        ),
    )

    assert fake_client.deleted is True
    assert dataset.warehouse_upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]


def test_test_connection_reports_capable_when_login_and_projects_succeed() -> None:
    with mock.patch(
        "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
    ):
        report = MicroStrategySource.test_connection(
            {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_test_connection_reports_login_failure() -> None:
    with mock.patch(
        "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
    ) as client_cls:
        client_cls.return_value.login.side_effect = MicroStrategyAPIError(
            "invalid credentials"
        )
        report = MicroStrategySource.test_connection(
            {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason


def test_test_connection_handles_invalid_config_without_raising() -> None:
    report = MicroStrategySource.test_connection({})

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason


def test_get_project_source_warehouses_falls_back_to_datasource_inventory() -> None:
    source = _source()
    datasource = Datasource.model_validate(
        {
            "id": "source-1",
            "name": "Sales Warehouse",
            "database": {"type": "snow_flake"},
        }
    )

    class FakeClient:
        def list_project_datasources(self, project_id: str) -> List[Datasource]:
            raise MicroStrategyAPIError("forbidden")

        def list_datasources(self, project_id: str) -> List[Datasource]:
            return [datasource]

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_project_source_warehouses("project-1") == [datasource]
    assert len(source.report.warnings) == 1


def test_get_project_source_warehouses_returns_empty_when_both_apis_fail() -> None:
    source = _source()

    class FakeClient:
        def list_project_datasources(self, project_id: str) -> List[Datasource]:
            raise MicroStrategyAPIError("forbidden")

        def list_datasources(self, project_id: str) -> List[Datasource]:
            raise MicroStrategyAPIError("also forbidden")

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_project_source_warehouses("project-1") == []
    assert source.report.source_warehouse_api_failures == 1
    assert len(source.report.warnings) == 1


def test_get_dashboard_definition_falls_back_to_document_definition() -> None:
    source = _source(
        {
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )
    dashboard_object = MSTRObject.model_validate(
        {"id": "dash-1", "name": "Sales Dashboard"}
    )

    class FakeClient:
        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a dossier")

        def get_document_definition(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            return {
                "result": {
                    "definition": {"datasets": [{"id": "ds-1", "name": "Sales Cube"}]}
                }
            }

    source.client = FakeClient()  # type: ignore[assignment]

    dashboard = source._get_dashboard_definition("project-1", dashboard_object)

    assert dashboard is not None
    assert [dataset.id for dataset in dashboard.datasets] == ["ds-1"]


def test_get_dashboard_definition_returns_none_when_both_apis_fail() -> None:
    source = _source(
        {
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )
    dashboard_object = MSTRObject.model_validate(
        {"id": "dash-1", "name": "Sales Dashboard"}
    )

    class FakeClient:
        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a dossier")

        def get_document_definition(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a document either")

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_dashboard_definition("project-1", dashboard_object) is None
    assert len(source.report.warnings) == 1


def test_metric_model_fact_ids_terminates_on_metric_cycles() -> None:
    source = _source()
    models: Dict[str, Dict[str, Any]] = {
        "METRIC-A": {
            "expression": {
                "tokens": [
                    {"target": {"objectId": "fact-1", "subType": "fact"}},
                    {"target": {"objectId": "metric-b", "subType": "metric"}},
                ]
            }
        },
        "METRIC-B": {
            "expression": {
                "tokens": [
                    {"target": {"objectId": "fact-2", "subType": "fact"}},
                    {"target": {"objectId": "metric-a", "subType": "metric"}},
                ]
            }
        },
    }

    class FakeClient:
        def get_metric_model(self, project_id: str, metric_id: str) -> Dict[str, Any]:
            return models[metric_id.upper()]

    source.client = FakeClient()  # type: ignore[assignment]

    fact_ids = source._metric_model_fact_ids(
        "project-1",
        models["METRIC-A"],
        visited={"METRIC-A"},
    )

    assert fact_ids == ["FACT-1", "FACT-2"]


def test_per_dashboard_error_boundary_continues_with_next_dashboard() -> None:
    source = _source(
        {
            "extract_warehouse_lineage": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
            "extract_model_lineage": False,
        }
    )
    dashboards = [
        MSTRObject.model_validate({"id": "dash-1", "name": "Broken Dashboard"}),
        MSTRObject.model_validate({"id": "dash-2", "name": "Healthy Dashboard"}),
    ]

    class FakeClient:
        def search_dashboards(self, project_id: str) -> Iterator[MSTRObject]:
            return iter(dashboards)

        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            if dossier_id == "dash-1":
                raise ValueError("unexpected definition failure")
            return {"result": {"definition": {"datasets": [], "chapters": []}}}

    source.client = FakeClient()  # type: ignore[assignment]

    workunits = list(
        source._process_project_dashboards(
            "project-1",
            _LazyProjectLineage(source, "project-1", []),
        )
    )
    urns = {workunit.get_urn() for workunit in workunits}

    assert source.mapper.dashboard_urn("project-1", "dash-2") in urns
    assert source.mapper.dashboard_urn("project-1", "dash-1") not in urns
    assert len(source.report.warnings) == 1


def test_resolve_visualization_bindings_uses_dataset_scoped_derived_objects() -> None:
    source = _source()
    derived_id = "A" * 32
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Salon Sales To Plan",
            "datasets": [
                {"id": "ds-retail", "name": "Retail WTD"},
                {"id": "ds-service", "name": "Service WTD"},
            ],
            "visualizations": [
                {
                    "key": "viz-1",
                    "name": "Salon Sales WTD",
                    "metrics": [{"id": derived_id, "type": "metric"}],
                },
                # Already explicitly bound: must not be touched.
                {
                    "key": "viz-2",
                    "name": "Bound Viz",
                    "datasets": ["ds-service"],
                    "metrics": [{"id": derived_id, "type": "metric"}],
                },
            ],
        }
    )

    class FakeClient:
        def get_model_document(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            return {
                "datasets": [
                    {
                        "information": {"objectId": derived_id[:-1] + "0"},
                        "derivedMetrics": [],
                    },
                    {
                        "information": {"objectId": "ds-retail"},
                        "derivedMetrics": [{"information": {"objectId": derived_id}}],
                    },
                ]
            }

    source.client = FakeClient()  # type: ignore[assignment]

    source._resolve_visualization_bindings("project-1", dashboard)

    assert dashboard.visualizations[0].datasets == ["ds-retail"]
    assert dashboard.visualizations[1].datasets == ["ds-service"]
    assert source.report.visualizations_bound_by_derived_objects == 1


def test_resolve_visualization_bindings_survives_modeling_api_failure() -> None:
    source = _source()
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Dash",
            "datasets": [
                {"id": "ds-1", "name": "A"},
                {"id": "ds-2", "name": "B"},
            ],
            "visualizations": [
                {
                    "key": "viz-1",
                    "name": "Viz",
                    "metrics": [{"id": "B" * 32, "type": "metric"}],
                }
            ],
        }
    )

    class FakeClient:
        def get_model_document(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("403 modeling access denied")

    source.client = FakeClient()  # type: ignore[assignment]

    source._resolve_visualization_bindings("project-1", dashboard)

    assert dashboard.visualizations[0].datasets == []
    assert source.report.visualizations_bound_by_derived_objects == 0


def test_resolve_visualization_bindings_ignores_owners_outside_dashboard() -> None:
    source = _source()
    derived_id = "A" * 32
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Dash",
            "datasets": [
                {"id": "ds-1", "name": "A"},
                {"id": "ds-2", "name": "B"},
            ],
            "visualizations": [
                {
                    "key": "viz-1",
                    "name": "Viz",
                    "metrics": [{"id": derived_id, "type": "metric"}],
                }
            ],
        }
    )

    class FakeClient:
        def get_model_document(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            # The derived object's owner is NOT one of the dashboard datasets.
            return {
                "datasets": [
                    {
                        "information": {"objectId": "ds-elsewhere"},
                        "derivedMetrics": [{"information": {"objectId": derived_id}}],
                    }
                ]
            }

    source.client = FakeClient()  # type: ignore[assignment]

    source._resolve_visualization_bindings("project-1", dashboard)

    assert dashboard.visualizations[0].datasets == []
    assert source.report.visualizations_bound_by_derived_objects == 0


def test_resolve_visualization_bindings_memoizes_modeling_failure() -> None:
    source = _source()

    def _dashboard_with_two_datasets(dash_id: str) -> DashboardDefinition:
        return DashboardDefinition.model_validate(
            {
                "id": dash_id,
                "name": dash_id,
                "datasets": [
                    {"id": "ds-1", "name": "A"},
                    {"id": "ds-2", "name": "B"},
                ],
                "visualizations": [
                    {
                        "key": "viz-1",
                        "name": "Viz",
                        "metrics": [{"id": "B" * 32, "type": "metric"}],
                    }
                ],
            }
        )

    class FakeClient:
        calls = 0

        def get_model_document(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            self.calls += 1
            raise MicroStrategyAPIError("403 modeling access denied")

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    source._resolve_visualization_bindings(
        "project-1", _dashboard_with_two_datasets("dash-1")
    )
    source._resolve_visualization_bindings(
        "project-1", _dashboard_with_two_datasets("dash-2")
    )

    # The project-scoped failure is remembered; no per-dashboard re-attempts.
    assert fake_client.calls == 1
    assert len(source.report.infos) == 1


def test_lazy_project_lineage_resolves_once_and_caches_failures() -> None:
    source = _source({"extract_model_lineage": True})

    class FakeClient:
        model_table_calls = 0

        def list_model_tables(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            self.model_table_calls += 1
            raise ValueError("unexpected model tables failure")

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    datasource = Datasource.model_validate(
        {"id": "source-1", "name": "Warehouse", "database": {"type": "snowflake"}}
    )
    lineage_context = _LazyProjectLineage(source, "project-1", [datasource])

    with pytest.raises(ValueError):
        lineage_context.model_lineage_index  # noqa: B018

    # The failed resolution is cached: subsequent accesses return None
    # without re-calling the API.
    assert lineage_context.model_lineage_index is None
    assert fake_client.model_table_calls == 1


def test_project_lineage_apis_skipped_when_all_dashboards_filtered() -> None:
    source = _source(
        {
            "dashboard_pattern": {"allow": ["^Allowed Only$"]},
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )

    class FakeClient:
        def __init__(self) -> None:
            self.model_table_calls = 0
            self.connection_calls = 0

        def search_dashboards(self, project_id: str) -> Iterator[MSTRObject]:
            return iter(
                [MSTRObject.model_validate({"id": "dash-1", "name": "Filtered Out"})]
            )

        def list_model_tables(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            self.model_table_calls += 1
            return {"tables": [], "total": 0}

        def get_datasource_connection(self, *args: Any, **kwargs: Any) -> Any:
            self.connection_calls += 1
            raise AssertionError("should not be called for filtered projects")

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    # A resolvable warehouse context ensures model-table pagination WOULD
    # run if the lazy context were resolved.
    datasource = Datasource.model_validate(
        {"id": "source-1", "name": "Warehouse", "database": {"type": "snowflake"}}
    )
    lineage_context = _LazyProjectLineage(source, "project-1", [datasource])
    workunits = list(source._process_project_dashboards("project-1", lineage_context))

    assert workunits == []
    assert list(source.report.filtered_dashboards) == ["Filtered Out"]
    # Model-table and connection APIs must not be touched when every
    # dashboard was pattern-filtered.
    assert not lineage_context._index_resolved
    assert not lineage_context._context_resolved
    assert fake_client.model_table_calls == 0
    assert fake_client.connection_calls == 0
