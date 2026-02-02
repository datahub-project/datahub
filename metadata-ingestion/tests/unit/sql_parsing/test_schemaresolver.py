from unittest.mock import MagicMock

import pytest
from sqlglot import parse_one
from sqlglot.expressions import Table

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.sql_parsing.schema_resolver import (
    SchemaInfo,
    SchemaResolver,
    _TableName,
    match_columns_to_schema,
)


def create_default_schema_resolver(urn: str) -> SchemaResolver:
    schema_resolver = SchemaResolver(platform="redshift", env="PROD", graph=None)
    schema_resolver.add_raw_schema_info(urn=urn, schema_info={"name": "STRING"})
    return schema_resolver


def create_mock_schema(columns: list[tuple[str, str]]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=MagicMock(),
        fields=[
            SchemaFieldClass(
                fieldPath=col_name,
                nativeDataType=col_type,
                type=MagicMock(),
            )
            for col_name, col_type in columns
        ],
    )


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
    input_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.public.test_table,PROD)"
    )
    schema_resolver = create_default_schema_resolver(urn=input_urn)

    urn, schema = schema_resolver.resolve_urn(urn=input_urn)

    assert urn == input_urn
    assert schema
    assert schema["name"]
    assert schema_resolver.schema_count() == 1


@pytest.mark.parametrize(
    "lower,mixed,expected",
    [
        (
            False,
            False,
            "urn:li:dataset:(urn:li:dataPlatform:mssql,Uppercased-Instance.Database.DataSet.Table,PROD)",
        ),
        (
            True,
            False,
            "urn:li:dataset:(urn:li:dataPlatform:mssql,uppercased-instance.database.dataset.table,PROD)",
        ),
        (
            True,
            True,
            "urn:li:dataset:(urn:li:dataPlatform:mssql,Uppercased-Instance.database.dataset.table,PROD)",
        ),
    ],
)
def test_get_urn_for_table(lower, mixed, expected):
    schema_resolver = SchemaResolver(
        platform="mssql",
        platform_instance="Uppercased-Instance",
        env="PROD",
        graph=None,
    )
    table = _TableName(database="Database", db_schema="DataSet", table="Table")

    assert (
        schema_resolver.get_urn_for_table(table=table, lower=lower, mixed=mixed)
        == expected
    )


def test_match_columns_to_schema():
    schema_info: SchemaInfo = {"id": "string", "Name": "string", "Address": "string"}
    output_columns = match_columns_to_schema(
        schema_info, input_columns=["Id", "name", "address", "weight"]
    )
    assert output_columns == ["id", "Name", "Address", "weight"]


class TestResolveTableBatching:
    @pytest.fixture
    def mock_graph(self):
        return MagicMock(spec=DataHubGraph)

    @pytest.fixture
    def schema_resolver(self, mock_graph):
        return SchemaResolver(
            platform="snowflake",
            platform_instance="prod",
            env="PROD",
            graph=mock_graph,
        )

    def test_batches_all_urn_variations(self, schema_resolver, mock_graph):
        mock_graph.get_entities.return_value = {
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)": {
                "schemaMetadata": (
                    create_mock_schema([("id", "int"), ("name", "string")]),
                    {},
                )
            }
        }

        table = _TableName(database="DB", db_schema="SCHEMA", table="TABLE")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is not None
        assert "id" in schema_info
        assert mock_graph.get_entities.call_count == 1
        urns_tried = mock_graph.get_entities.call_args[1]["urns"]
        assert len(urns_tried) >= 2
        assert len(urns_tried) == len(set(urns_tried))

    def test_uses_cache_first(self, schema_resolver, mock_graph):
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        schema_resolver.add_schema_metadata(
            urn, create_mock_schema([("cached_col", "string")])
        )

        table = _TableName(database="db", db_schema="schema", table="table")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is not None
        assert "cached_col" in schema_info
        mock_graph.get_entities.assert_not_called()

    def test_respects_ingestion_cache_precedence(self, schema_resolver, mock_graph):
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        schema_resolver.add_schema_metadata(
            urn, create_mock_schema([("fresh_col", "string")])
        )

        table = _TableName(database="db", db_schema="schema", table="table")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is not None
        assert "fresh_col" in schema_info
        mock_graph.get_entities.assert_not_called()

    def test_fallback_on_batch_error(self, schema_resolver, mock_graph):
        mock_graph.get_entities.side_effect = ConnectionError("Network error")
        mock_graph.get_aspect.return_value = create_mock_schema([("col1", "string")])

        table = _TableName(database="db", db_schema="schema", table="table")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is not None
        assert "col1" in schema_info
        assert mock_graph.get_entities.call_count == 1
        assert mock_graph.get_aspect.call_count >= 1

    def test_unexpected_errors_propagate(self, schema_resolver, mock_graph):
        mock_graph.get_entities.side_effect = ValueError("Unexpected bug")

        table = _TableName(database="db", db_schema="schema", table="table")

        with pytest.raises(ValueError, match="Unexpected bug"):
            schema_resolver.resolve_table(table)

    def test_handles_not_found(self, schema_resolver, mock_graph):
        mock_graph.get_entities.return_value = {}

        table = _TableName(database="db", db_schema="schema", table="table")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is None
        assert "prod.db.schema.table" in resolved_urn.lower()
        assert mock_graph.get_entities.call_count == 1

    def test_caches_results(self, schema_resolver, mock_graph):
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        mock_graph.get_entities.return_value = {
            urn: {"schemaMetadata": (create_mock_schema([("id", "int")]), {})}
        }

        table = _TableName(database="db", db_schema="schema", table="table")

        resolved_urn1, schema_info1 = schema_resolver.resolve_table(table)
        assert schema_info1 is not None
        assert mock_graph.get_entities.call_count == 1

        resolved_urn2, schema_info2 = schema_resolver.resolve_table(table)
        assert schema_info2 == schema_info1
        assert mock_graph.get_entities.call_count == 1


class TestTableNameParts:
    @pytest.mark.parametrize(
        "sql,expected_parts",
        [
            (
                'SELECT * FROM "catalog"."database"."schema"."table"',
                ("catalog", "database", "schema", "table"),
            ),
            (
                'SELECT * FROM "Performance"."source"."schema"."folder"."table"',
                ("Performance", "source", "schema", "folder", "table"),
            ),
            (
                'SELECT * FROM "a"."b"."c"."d"."e"."f"."table"',
                ("a", "b", "c", "d", "e", "f", "table"),
            ),
        ],
    )
    def test_multi_part_table_names(self, sql, expected_parts):
        table_name = _TableName.from_sqlglot_table(
            list(parse_one(sql).find_all(Table))[0]
        )
        assert table_name.parts == expected_parts
        assert all(isinstance(part, str) for part in table_name.parts)

    def test_parts_hashability_and_equality(self):
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
        assert {tn1} == {tn2}

    def test_parts_optional(self):
        table_name = _TableName(database="db", db_schema="schema", table="table")
        assert table_name.parts is None

    def test_deduplication_in_sets(self):
        tn1 = _TableName.from_sqlglot_table(
            list(parse_one("SELECT * FROM db.schema.table1").find_all(Table))[0]
        )
        tn2 = _TableName.from_sqlglot_table(
            list(parse_one("SELECT * FROM db.schema.table2").find_all(Table))[0]
        )
        tn3 = _TableName.from_sqlglot_table(
            list(parse_one("SELECT * FROM db.schema.table1").find_all(Table))[0]
        )

        assert {tn1, tn2, tn3} == {tn1, tn2}

    @pytest.mark.parametrize(
        "parts1,parts2,should_be_equal",
        [
            (("a", "b", "c"), ("x", "y", "z"), True),  # 3 parts: equality ignores parts
            (
                ("a", "b", "c", "d"),
                ("a", "b", "x", "d"),
                False,
            ),  # 4+ parts: parts matter
        ],
    )
    def test_equality_rules_by_part_count(self, parts1, parts2, should_be_equal):
        table1 = _TableName(
            database="db", db_schema="schema", table="table", parts=parts1
        )
        table2 = _TableName(
            database="db", db_schema="schema", table="table", parts=parts2
        )

        if should_be_equal:
            assert table1 == table2
            assert {table1, table2} == {table1}
        else:
            assert table1 != table2
            assert {table1, table2} == {table1, table2}

    def test_urn_consistency(self):
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

        assert resolver.get_urn_for_table(table1) == resolver.get_urn_for_table(table2)
        assert table1 == table2

    def test_special_characters(self):
        sql = 'SELECT * FROM "my-source"."folder_with_underscore"."table.with.dots"'
        table_name = _TableName.from_sqlglot_table(
            list(parse_one(sql).find_all(Table))[0]
        )

        assert table_name.parts is not None
        assert "my-source" in table_name.parts
        assert "folder_with_underscore" in table_name.parts
        assert "table.with.dots" in table_name.parts
