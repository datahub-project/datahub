from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.models import DashboardDefinition
from datahub.ingestion.source.microstrategy.source import MicroStrategySource


def _source() -> MicroStrategySource:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "extract_warehouse_lineage": True,
        }
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
