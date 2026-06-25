from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import (
    WarehouseLineageContext,
    datahub_platform_for_source_type,
    extract_field_names_from_expression,
    extract_tables_from_sql,
    qualify_table_name,
)
from datahub.ingestion.source.microstrategy.lineage import MicroStrategyLineageExtractor


def test_maps_microstrategy_source_type_to_datahub_platform() -> None:
    assert datahub_platform_for_source_type("snow_flake") == "snowflake"
    assert datahub_platform_for_source_type("SQL Server") == "mssql"
    assert datahub_platform_for_source_type("postgre_sql") == "postgres"


def test_extract_tables_from_sql_handles_qualified_names_and_ctes() -> None:
    sql = """
    WITH recent_orders AS (
      SELECT * FROM SALES_DB.ORDERS.fact_orders
    )
    SELECT *
    FROM SALES_DB.SALES.fact_sales sales
    JOIN "SALES_DB"."SALES"."dim_customer" customer
      ON sales.customer_id = customer.customer_id
    JOIN recent_orders ro
      ON sales.order_id = ro.order_id
    """

    assert extract_tables_from_sql(sql) == [
        "SALES_DB.ORDERS.fact_orders",
        "SALES_DB.SALES.dim_customer",
        "SALES_DB.SALES.fact_sales",
    ]


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
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    extractor = MicroStrategyLineageExtractor(config)
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
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES_DB.ORDERS.fact_orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES_DB.SALES.fact_sales,PROD)",
    ]


def test_extract_field_names_from_model_expression() -> None:
    assert extract_field_names_from_expression("net_sales_amt") == ["net_sales_amt"]
    assert extract_field_names_from_expression(
        "(net_sales_amt + tax_amt) - discount_amt"
    ) == ["discount_amt", "net_sales_amt", "tax_amt"]


def test_model_lineage_index_maps_facts_and_attribute_forms_to_fields() -> None:
    config = MicroStrategyConfig.model_validate(
        {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
    )
    extractor = MicroStrategyLineageExtractor(config)
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
        "(urn:li:dataPlatform:snowflake,SALES_DB.ORDERS.fact_orders,PROD)"
    )
    assert index.table_count == 1
    assert index.fact_field_urns(["fact-1"]) == [
        f"urn:li:schemaField:({upstream_dataset_urn},net_sales_amt)"
    ]
    assert index.attribute_field_urns("attr-1", "ID") == [
        f"urn:li:schemaField:({upstream_dataset_urn},order_date_id)"
    ]
