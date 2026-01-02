import sqlglot
from sqlglot import parse_one
from sqlglot.expressions import Table

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

    def test_parts_stored_but_excluded_from_equality_when_3_or_less(self):
        """Test that parts with <=3 elements don't affect equality (backward compatible)."""
        table1 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("source", "folder1", "table"),
        )
        table2 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("source", "folder2", "table"),
        )

        assert table1 == table2, (
            "3-part tables with different parts should be equal (backward compatible)"
        )
        assert hash(table1) == hash(table2)

        table_set = {table1, table2}
        assert len(table_set) == 1, "Should deduplicate based on db/schema/table only"

        # But parts are still accessible
        assert table1.parts == ("source", "folder1", "table")
        assert table2.parts == ("source", "folder2", "table")

    def test_parts_included_in_equality_when_more_than_3(self):
        """Test that parts with >3 elements DO affect equality (multi-part tables are different)."""
        table1 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("source", "folder1", "subfolder", "table"),
        )
        table2 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("source", "folder2", "subfolder", "table"),
        )

        assert table1 != table2, (
            "Multi-part tables (>3) with different paths should NOT be equal"
        )
        assert hash(table1) != hash(table2)

        table_set = {table1, table2}
        assert len(table_set) == 2, "Both tables should be in the set"

    def test_urn_consistency_across_operations(self):
        """Test that same table always generates same URN."""
        from datahub.sql_parsing.schema_resolver import SchemaResolver

        resolver = SchemaResolver(platform="test", env="PROD", graph=None)

        table1 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("a", "b", "c", "table"),
        )
        table2 = _TableName(
            database="db",
            db_schema="schema",
            table="table",
            parts=("a", "b", "c", "table"),
        )

        urn1 = resolver.get_urn_for_table(table1)
        urn2 = resolver.get_urn_for_table(table2)

        assert urn1 == urn2, "Same table should always generate same URN"
        assert table1 == table2, "Same table should be equal"

    def test_deep_hierarchy_6_plus_parts(self):
        """Test tables with 6+ parts (deep hierarchies)."""
        sql = 'SELECT * FROM "a"."b"."c"."d"."e"."f"."table"'
        parsed = parse_one(sql)
        table = list(parsed.find_all(Table))[0]

        table_name = _TableName.from_sqlglot_table(table)

        assert table_name.parts is not None
        assert len(table_name.parts) == 7
        assert table_name.parts == ("a", "b", "c", "d", "e", "f", "table")

    def test_special_characters_in_parts(self):
        """Test that special characters in table names are preserved."""
        sql = 'SELECT * FROM "my-source"."folder_with_underscore"."table.with.dots"'
        parsed = parse_one(sql)
        table = list(parsed.find_all(Table))[0]

        table_name = _TableName.from_sqlglot_table(table)

        assert table_name.parts is not None
        assert "my-source" in table_name.parts
        assert "folder_with_underscore" in table_name.parts
        assert "table.with.dots" in table_name.parts

    def test_three_part_with_parts_field(self):
        """Test that 3-part names with parts field are handled correctly."""
        table_with_parts = _TableName(
            database="source",
            db_schema="schema",
            table="table",
            parts=("source", "schema", "table"),
        )
        table_without_parts = _TableName(
            database="source", db_schema="schema", table="table", parts=None
        )

        assert table_with_parts == table_without_parts, "Equality ignores parts field"
        assert table_with_parts.parts == ("source", "schema", "table")
        assert table_without_parts.parts is None
