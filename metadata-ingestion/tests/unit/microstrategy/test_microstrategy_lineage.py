from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import (
    WarehouseLineageContext,
    datahub_platform_for_source_type,
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
