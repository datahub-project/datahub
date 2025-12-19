import sqlglot
from sqlglot import parse_one

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


class TestTableNameParts:
    """Test _TableName.parts field for multi-part table names."""

    def test_multi_part_table_name_5_parts(self):
        """Test 5-part table name (e.g., Dremio CE format)."""
        sql = 'SELECT * FROM "Performance"."source"."schema"."folder"."table"'
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _TableName.from_sqlglot_table(table)

        assert table_name.parts is not None
        assert len(table_name.parts) == 5
        assert table_name.parts == (
            "Performance",
            "source",
            "schema",
            "folder",
            "table",
        )

    def test_multi_part_table_name_4_parts(self):
        """Test 4-part table name."""
        sql = 'SELECT * FROM "catalog"."database"."schema"."table"'
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _TableName.from_sqlglot_table(table)

        assert table_name.parts is not None
        assert len(table_name.parts) == 4
        assert table_name.parts == ("catalog", "database", "schema", "table")

    def test_parts_are_strings_not_objects(self):
        """Verify that parts contain strings, not SQLGlot objects."""
        sql = "SELECT * FROM db.schema.table"
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _TableName.from_sqlglot_table(table)

        assert table_name.parts is not None
        for part in table_name.parts:
            assert isinstance(part, str), f"Expected string, got {type(part)}"

    def test_hashability_with_parts(self):
        """Test that _TableName with parts is hashable."""
        sql = "SELECT * FROM db.schema.table"
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _TableName.from_sqlglot_table(table)

        # Should be able to hash it
        h = hash(table_name)
        assert isinstance(h, int)

        # Should work in a set
        s = {table_name}
        assert len(s) == 1

    def test_equality_excludes_parts(self):
        """Test that equality is based on database/schema/table only, not parts."""
        # Two tables with same database/schema/table but different parts should be equal
        tn1 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("db", "schema", "table"),
        )
        tn2 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("different", "parts"),
        )

        assert tn1 == tn2
        assert hash(tn1) == hash(tn2)

    def test_create_without_parts(self):
        """Test that _TableName can be created without parts field."""
        table_name = _TableName(database="db", db_schema="schema", table="table")

        assert table_name.database == "db"
        assert table_name.db_schema == "schema"
        assert table_name.table == "table"
        assert table_name.parts is None

    def test_parts_in_set_operations(self):
        """Test that parts don't affect deduplication in set operations."""
        sql1 = "SELECT * FROM db.schema.table1"
        sql2 = "SELECT * FROM db.schema.table2"
        sql3 = "SELECT * FROM db.schema.table1"  # duplicate of sql1

        parsed1 = parse_one(sql1)
        parsed2 = parse_one(sql2)
        parsed3 = parse_one(sql3)

        tn1 = _TableName.from_sqlglot_table(
            list(parsed1.find_all(sqlglot.exp.Table))[0]
        )
        tn2 = _TableName.from_sqlglot_table(
            list(parsed2.find_all(sqlglot.exp.Table))[0]
        )
        tn3 = _TableName.from_sqlglot_table(
            list(parsed3.find_all(sqlglot.exp.Table))[0]
        )

        # Set should deduplicate tn1 and tn3
        table_set = {tn1, tn2, tn3}
        assert len(table_set) == 2
