from typing import Any, Dict
from unittest import mock

from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import (
    MicroStrategyLineageExtractor,
    WarehouseLineageContext,
    datahub_platform_for_source_type,
    extract_field_names_from_expression,
    metric_fact_ids_from_model,
    metric_metric_ids_from_model,
    qualify_table_name,
    warehouse_context_from_datasource,
    warehouse_context_from_datasources,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceReference,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport


def _extractor() -> MicroStrategyLineageExtractor:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    return MicroStrategyLineageExtractor(config, MicroStrategyReport())


def test_maps_microstrategy_source_type_to_datahub_platform() -> None:
    assert datahub_platform_for_source_type("snow_flake") == "snowflake"
    assert datahub_platform_for_source_type("SQL Server") == "mssql"
    assert datahub_platform_for_source_type("postgre_sql") == "postgres"


def test_warehouse_context_from_datasource_reference() -> None:
    datasource = DatasourceReference.model_validate(
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

    assert warehouse_context_from_datasource(datasource, "PROD") == (
        WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        )
    )


def test_warehouse_context_from_datasource_applies_platform_instance_map() -> None:
    datasource = DatasourceReference.model_validate(
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

    context = warehouse_context_from_datasource(
        datasource,
        "PROD",
        platform_instance_map={"snowflake": "prod_wh"},
    )

    assert context is not None
    assert context.platform_instance == "prod_wh"


def test_warehouse_context_from_datasources_requires_unique_context() -> None:
    datasources = [
        Datasource.model_validate(
            {
                "id": "source-1",
                "name": "Sales Warehouse",
                "database": {
                    "type": "snow_flake",
                    "name": "SALES_DB",
                    "schema": "SALES",
                },
            }
        ),
        Datasource.model_validate(
            {
                "id": "source-2",
                "name": "Orders Warehouse",
                "database": {
                    "type": "snow_flake",
                    "name": "ORDERS_DB",
                    "schema": "ORDERS",
                },
            }
        ),
    ]

    assert warehouse_context_from_datasources(datasources, "PROD") is None


def test_qualify_table_name_uses_connection_database_and_schema() -> None:
    assert (
        qualify_table_name("fact_sales", database="SALES_DB", schema="SALES")
        == "SALES_DB.SALES.fact_sales"
    )
    assert (
        qualify_table_name("ORDERS.fact_orders", database="SALES_DB", schema="SALES")
        == "SALES_DB.ORDERS.fact_orders"
    )
    assert (
        qualify_table_name(
            "SALES_DB.SALES.fact_sales", database="OTHER_DB", schema="ORDERS"
        )
        == "SALES_DB.SALES.fact_sales"
    )


def test_warehouse_upstream_urns_from_sql() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    upstreams = extractor.warehouse_upstream_urns_from_sql(
        "select * from fact_sales join ORDERS.fact_orders on 1 = 1",
        context,
    )

    assert upstreams == [
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)",
    ]


def test_warehouse_upstream_urns_from_sql_reports_parse_failures() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    with mock.patch(
        "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result",
        side_effect=RuntimeError("parser exploded"),
    ):
        upstreams = extractor.warehouse_upstream_urns_from_sql(
            "select * from fact_sales",
            context,
        )

    assert upstreams == []
    assert len(extractor.report.sql_parse_failures) == 1


def test_extract_field_names_from_model_expression() -> None:
    assert extract_field_names_from_expression("net_sales_amt") == ["net_sales_amt"]
    assert extract_field_names_from_expression(
        "(net_sales_amt + tax_amt) - discount_amt"
    ) == ["discount_amt", "net_sales_amt", "tax_amt"]


def test_extract_field_names_excludes_function_names() -> None:
    assert extract_field_names_from_expression("SUM(QTY_SOLD * UNIT_PRICE)") == [
        "QTY_SOLD",
        "UNIT_PRICE",
    ]


def test_metric_fact_ids_from_model_accepts_nested_and_top_level_tokens() -> None:
    nested_model: Dict[str, Any] = {
        "expression": {
            "tokens": [{"target": {"objectId": "fact-1", "subType": "fact"}}]
        }
    }
    top_level_model: Dict[str, Any] = {
        "expression": {"tokens": [{"objectId": "fact-1", "subType": "fact"}]}
    }

    assert metric_fact_ids_from_model(nested_model) == ["FACT-1"]
    assert metric_fact_ids_from_model(top_level_model) == ["FACT-1"]


def test_metric_metric_ids_from_model_accepts_nested_and_top_level_tokens() -> None:
    nested_model: Dict[str, Any] = {
        "expression": {
            "tokens": [{"target": {"objectId": "metric-2", "subType": "metric"}}]
        }
    }
    top_level_model: Dict[str, Any] = {
        "expression": {"tokens": [{"objectId": "metric-2", "subType": "metric"}]}
    }

    assert metric_metric_ids_from_model(nested_model) == ["METRIC-2"]
    assert metric_metric_ids_from_model(top_level_model) == ["METRIC-2"]


def test_model_lineage_index_maps_facts_and_attribute_forms_to_fields() -> None:
    extractor = _extractor()
    context = WarehouseLineageContext(
        platform="snowflake",
        env="PROD",
        database="SALES_DB",
        schema="SALES",
    )

    index = extractor.model_lineage_index_from_tables(
        [
            {
                "physicalTable": {
                    "namespace": "SALES_DB",
                    "tablePrefix": "ORDERS.",
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
        context,
    )

    upstream_dataset_urn = (
        "urn:li:dataset:"
        "(urn:li:dataPlatform:snowflake,sales_db.orders.fact_orders,PROD)"
    )
    assert index.fact_field_urns(["fact-1"]) == [
        f"urn:li:schemaField:({upstream_dataset_urn},net_sales_amt)"
    ]
    assert index.attribute_field_urns("attr-1", "ID") == [
        f"urn:li:schemaField:({upstream_dataset_urn},order_date_id)"
    ]
