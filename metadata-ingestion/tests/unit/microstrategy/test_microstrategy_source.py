from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import WarehouseLineageContext
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    MSTRObject,
    ReportDefinition,
)
from datahub.ingestion.source.microstrategy.source import MicroStrategySource


def _source(extra_config: dict | None = None) -> MicroStrategySource:
    config_dict = {
        "base_url": "https://mstr.example.com/MicroStrategyLibrary",
        "extract_warehouse_lineage": True,
    }
    if extra_config:
        config_dict.update(extra_config)
    config = MicroStrategyConfig.model_validate(
        config_dict
    )
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
