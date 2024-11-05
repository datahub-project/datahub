from datahub.sql_parsing.schema_resolver import SchemaResolver, _TableName


def test_basic_schema_resolver():
    schema_resolver = SchemaResolver(
        platform="redshift",
        env="PROD",
        graph=None,
    )

    schema_resolver.add_raw_schema_info(
        urn="urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.public.test_table,PROD)",
        schema_info={"name": "STRING"},
    )

    urn, schema = schema_resolver.resolve_table(
        _TableName(database="my_db", db_schema="public", table="test_table")
    )
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.public.test_table,PROD)"
    )
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
