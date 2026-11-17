from datahub.ingestion.source.looker.urn_functions import (
    get_qualified_table_name,
    get_table_name,
)


def test_platform_instance_is_dropped_from_qualified_name() -> None:
    urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,instance.db.schema.orders,PROD)"
    )
    assert get_qualified_table_name(urn) == "db.schema.orders"
    assert get_table_name(urn) == "orders"


def test_short_names_are_returned_unchanged() -> None:
    urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders.sql_table_name,PROD)"
    assert get_qualified_table_name(urn) == "db.orders.sql_table_name"
    assert get_table_name(urn) == "sql_table_name"
