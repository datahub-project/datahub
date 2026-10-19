from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import SchemaResolver

MIXED = (
    "urn:li:dataset:(urn:li:dataPlatform:vertica,Vmart.Public.Customer_Dimension,PROD)"
)


def test_resolve_table_matches_registered_casing_on_case_insensitive_platform() -> None:
    # The connector registered the schema under the casing it ingested, while
    # the parser hands over a lowercased name. The registered URN must win over
    # a lowercased phantom.
    resolver = SchemaResolver(platform="vertica", env="PROD")
    resolver.add_raw_schema_info(MIXED, {"customer_key": "int"})

    urn, schema_info = resolver.resolve_table(
        _TableName(database="vmart", db_schema="public", table="customer_dimension")
    )

    assert urn == MIXED
    assert schema_info == {"customer_key": "int"}


def test_resolve_table_without_registration_stays_lowercase() -> None:
    resolver = SchemaResolver(platform="vertica", env="PROD")

    urn, schema_info = resolver.resolve_table(
        _TableName(database="Vmart", db_schema="Public", table="Orders")
    )

    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.public.orders,PROD)"
    )
    assert schema_info is None


def test_resolve_table_case_folding_skipped_on_case_sensitive_platform() -> None:
    mixed = "urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.DataSet.Table,PROD)"
    resolver = SchemaResolver(platform="bigquery", env="PROD")
    resolver.add_raw_schema_info(mixed, {"a": "int"})

    urn, schema_info = resolver.resolve_table(
        _TableName(database="proj", db_schema="dataset", table="table")
    )

    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.dataset.table,PROD)"
    )
    assert schema_info is None
