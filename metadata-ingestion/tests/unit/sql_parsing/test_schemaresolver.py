from datahub.sql_parsing.schema_resolver import (
    SchemaInfo,
    SchemaResolver,
    _TableName,
    match_columns_to_schema,
)


def create_default_schema_resolver(urn: str) -> SchemaResolver:
    schema_resolver = SchemaResolver(
        platform="redshift",
        env="PROD",
        graph=None,
    )

    schema_resolver.add_raw_schema_info(
        urn=urn,
        schema_info={"name": "STRING"},
    )

    return schema_resolver


def test_basic_schema_resolver():
    input_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.public.test_table,PROD)"
    )

    schema_resolver = create_default_schema_resolver(urn=input_urn)

    urn, schema = schema_resolver.resolve_table(
        _TableName(database="my_db", db_schema="public", table="test_table")
    )

    assert urn == input_urn

    assert schema

    assert schema["name"]

    assert schema_resolver.schema_count() == 1


def test_resolve_urn():
    input_urn: str = (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.public.test_table,PROD)"
    )

    schema_resolver = create_default_schema_resolver(urn=input_urn)

    schema_resolver.add_raw_schema_info(
        urn=input_urn,
        schema_info={"name": "STRING"},
    )

    urn, schema = schema_resolver.resolve_urn(urn=input_urn)

    assert urn == input_urn

    assert schema

    assert schema["name"]

    assert schema_resolver.schema_count() == 1


def test_get_urn_for_table_lowercase():
    schema_resolver = SchemaResolver(
        platform="mssql",
        platform_instance="Uppercased-Instance",
        env="PROD",
        graph=None,
    )

    table = _TableName(database="Database", db_schema="DataSet", table="Table")

    assert (
        schema_resolver.get_urn_for_table(table=table, lower=True)
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,uppercased-instance.database.dataset.table,PROD)"
    )

    assert (
        schema_resolver.get_urn_for_table(table=table, lower=True, mixed=True)
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,Uppercased-Instance.database.dataset.table,PROD)"
    )


def test_get_urn_for_table_not_lower_should_keep_capital_letters():
    schema_resolver = SchemaResolver(
        platform="mssql",
        platform_instance="Uppercased-Instance",
        env="PROD",
        graph=None,
    )

    table = _TableName(database="Database", db_schema="DataSet", table="Table")

    assert (
        schema_resolver.get_urn_for_table(table=table, lower=False)
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,Uppercased-Instance.Database.DataSet.Table,PROD)"
    )
    assert schema_resolver.schema_count() == 0


def test_match_columns_to_schema():
    schema_info: SchemaInfo = {"id": "string", "Name": "string", "Address": "string"}

    output_columns = match_columns_to_schema(
        schema_info, input_columns=["Id", "name", "address", "weight"]
    )

    assert output_columns == ["id", "Name", "Address", "weight"]
