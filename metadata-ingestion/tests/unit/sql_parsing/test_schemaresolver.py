import json
import pathlib
from typing import List
from unittest.mock import MagicMock

import pytest
import sqlglot
from sqlglot import parse_one
from sqlglot.expressions import Table

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.sql_parsing.schema_resolver import (
    GraphQLSchemaMetadata,
    SchemaInfo,
    SchemaResolver,
    _SchemaResolverWithExtras,
    _TableName,
    match_columns_to_schema,
)
from datahub.sql_parsing.sqlglot_lineage import _table_name_from_sqlglot_table
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path


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


@pytest.mark.parametrize(
    "keep_platform_instance_case, expected_name",
    [
        pytest.param(False, "prodws.main.sales.orders", id="default_lowercases"),
        pytest.param(True, "ProdWS.main.sales.orders", id="opt_in_keeps_case"),
    ],
)
def test_resolve_table_miss_platform_instance_case(
    keep_platform_instance_case: bool, expected_name: str
) -> None:
    schema_resolver = SchemaResolver(
        platform="databricks",
        platform_instance="ProdWS",
        env="PROD",
        keep_platform_instance_case=keep_platform_instance_case,
    )

    urn, schema_info = schema_resolver.resolve_table(
        _TableName(database="Main", db_schema="Sales", table="Orders")
    )

    assert schema_info is None
    assert (
        urn == f"urn:li:dataset:(urn:li:dataPlatform:databricks,{expected_name},PROD)"
    )


@pytest.mark.parametrize("keep_platform_instance_case", [False, True])
def test_temp_table_registered_under_miss_urn_resolves(
    keep_platform_instance_case: bool,
) -> None:
    # The aggregator registers a temp table under the URN the base resolver returned
    # for it, so later lookups must build that same URN.
    base = SchemaResolver(
        platform="databricks",
        platform_instance="ProdWS",
        env="PROD",
        keep_platform_instance_case=keep_platform_instance_case,
    )
    resolver = _SchemaResolverWithExtras(base_resolver=base, extra_schemas={})
    temp_view = _TableName(database="main", db_schema="sales", table="tmp_v")
    temp_urn, _ = base.resolve_table(temp_view)

    resolver.add_temp_tables(
        {
            temp_urn: [
                SchemaFieldClass(fieldPath="id", nativeDataType="int", type=MagicMock())
            ]
        }
    )

    assert resolver.resolve_table(temp_view) == (temp_urn, {"id": "int"})


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

    def test_batch_fetch_keeps_original_field_paths(self, schema_resolver, mock_graph):
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        mock_graph.get_entities.return_value = {
            urn: {
                "schemaMetadata": (
                    create_mock_schema([("[version=2.0].[type=long].col_a", "int")]),
                    {},
                )
            }
        }

        resolved_urn, schema_info = schema_resolver.resolve_table(
            _TableName(database="DB", db_schema="SCHEMA", table="TABLE")
        )

        assert resolved_urn == urn
        assert schema_info == {"col_a": "int"}
        assert schema_resolver.get_original_field_paths(urn) == {
            "col_a": "[version=2.0].[type=long].col_a"
        }

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

    def test_batch_error_caches_none_and_no_fallback(self, schema_resolver, mock_graph):
        """Test that batch errors cache None for all URNs without falling back to individual fetches."""
        mock_graph.get_entities.side_effect = ConnectionError("Network error")

        table = _TableName(database="db", db_schema="schema", table="table")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is None
        assert mock_graph.get_entities.call_count == 1
        mock_graph.get_aspect.assert_not_called()

        # Verify URNs are cached as None to prevent repeated lookups
        resolved_urn2, schema_info2 = schema_resolver.resolve_table(table)
        assert schema_info2 is None
        assert mock_graph.get_entities.call_count == 1

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

    def test_partial_urn_match_from_batch(self, schema_resolver, mock_graph):
        """Test when batch fetch returns schemas for SOME URN variations but not others."""
        urn_lower = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        )
        mock_graph.get_entities.return_value = {
            urn_lower: {
                "schemaMetadata": (create_mock_schema([("col1", "string")]), {})
            }
        }

        table = _TableName(database="DB", db_schema="SCHEMA", table="TABLE")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is not None
        assert "col1" in schema_info
        assert resolved_urn == urn_lower
        assert mock_graph.get_entities.call_count == 1

    def test_mixed_cached_none_and_fetch(self, schema_resolver, mock_graph):
        """Test when some URN variations are cached as None and others need fetching."""
        urn_upper = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.DB.SCHEMA.TABLE,PROD)"
        )
        urn_lower = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.table,PROD)"
        )

        schema_resolver.add_schema_metadata_from_fetch(urn_upper, None)

        mock_graph.get_entities.return_value = {
            urn_lower: {
                "schemaMetadata": (create_mock_schema([("found_col", "int")]), {})
            }
        }

        table = _TableName(database="DB", db_schema="SCHEMA", table="TABLE")
        resolved_urn, schema_info = schema_resolver.resolve_table(table)

        assert schema_info is not None
        assert "found_col" in schema_info
        assert resolved_urn == urn_lower
        assert mock_graph.get_entities.call_count == 1

    def test_caches_none_to_prevent_repeated_lookups(self, schema_resolver, mock_graph):
        """Test that None results are cached to prevent repeated API calls."""
        mock_graph.get_entities.return_value = {}

        table = _TableName(database="db", db_schema="schema", table="missing_table")

        resolved_urn1, schema_info1 = schema_resolver.resolve_table(table)
        assert schema_info1 is None
        assert mock_graph.get_entities.call_count == 1

        resolved_urn2, schema_info2 = schema_resolver.resolve_table(table)
        assert schema_info2 is None
        assert mock_graph.get_entities.call_count == 1


class TestTableNameParts:
    """Test _TableName.parts field for multi-part table names."""

    def test_multi_part_table_name_5_parts(self):
        """Test 5-part table name (e.g., Dremio CE format)."""
        sql = 'SELECT * FROM "Performance"."source"."schema"."folder"."table"'
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _table_name_from_sqlglot_table(table, None)

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

        table_name = _table_name_from_sqlglot_table(table, None)

        assert table_name.parts is not None
        assert len(table_name.parts) == 4
        assert table_name.parts == ("catalog", "database", "schema", "table")

    def test_parts_are_strings_not_objects(self):
        """Verify that parts contain strings, not SQLGlot objects."""
        sql = "SELECT * FROM db.schema.table"
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _table_name_from_sqlglot_table(table, None)

        assert table_name.parts is not None
        for part in table_name.parts:
            assert isinstance(part, str), f"Expected string, got {type(part)}"

    def test_hashability_with_parts(self):
        """Test that _TableName with parts is hashable."""
        sql = "SELECT * FROM db.schema.table"
        parsed = parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _table_name_from_sqlglot_table(table, None)

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

        tn1 = _table_name_from_sqlglot_table(
            list(parsed1.find_all(sqlglot.exp.Table))[0], None
        )
        tn2 = _table_name_from_sqlglot_table(
            list(parsed2.find_all(sqlglot.exp.Table))[0], None
        )
        tn3 = _table_name_from_sqlglot_table(
            list(parsed3.find_all(sqlglot.exp.Table))[0], None
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

        table_name = _table_name_from_sqlglot_table(table, None)

        assert table_name.parts is not None
        assert len(table_name.parts) == 7
        assert table_name.parts == ("a", "b", "c", "d", "e", "f", "table")

    def test_special_characters_in_parts(self):
        """Test that special characters in table names are preserved."""
        sql = 'SELECT * FROM "my-source"."folder_with_underscore"."table.with.dots"'
        parsed = parse_one(sql)
        table = list(parsed.find_all(Table))[0]

        table_name = _table_name_from_sqlglot_table(table, None)

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


def test_closed_reflects_the_underlying_cache():
    """`closed` is the accessor callers use instead of reaching through
    `_schema_cache`, so the delegation needs a test of its own rather than being
    covered only where it happens to be used.
    """
    resolver = SchemaResolver(platform="snowflake")

    assert not resolver.closed
    assert not resolver._schema_cache.closed

    resolver.close()

    assert resolver.closed
    assert resolver._schema_cache.closed


_V2_URN = "urn:li:dataset:(urn:li:dataPlatform:glue,my_db.my_table,PROD)"
_V2_FIELDS = [
    ("[version=2.0].[type=long].col_a", "bigint"),
    ("[version=2.0].[type=string].Col_B", "string"),
    # Nested fields are dropped from the schema info, so they get no mapping either.
    ("[version=2.0].[type=struct].col_c.[type=string].inner", "string"),
]


def _v2_graphql_schema() -> GraphQLSchemaMetadata:
    return {
        "fields": [
            {"fieldPath": path, "nativeDataType": native_type}
            for path, native_type in _V2_FIELDS
        ]
    }


@pytest.mark.parametrize(
    "add_schema",
    [
        pytest.param(
            lambda r: r.add_schema_metadata(_V2_URN, create_mock_schema(_V2_FIELDS)),
            id="add_schema_metadata",
        ),
        pytest.param(
            lambda r: r.add_schema_metadata_from_fetch(
                _V2_URN, create_mock_schema(_V2_FIELDS)
            ),
            id="add_schema_metadata_from_fetch",
        ),
        pytest.param(
            lambda r: r.add_graphql_schema_metadata(_V2_URN, _v2_graphql_schema()),
            id="add_graphql_schema_metadata",
        ),
    ],
)
def test_original_field_paths_kept_for_v2_schemas(add_schema):
    resolver = SchemaResolver(platform="glue", env="PROD", graph=None)
    add_schema(resolver)

    # resolve_urn keeps returning the simplified paths the SQL parser matches on.
    assert resolver.resolve_urn(_V2_URN)[1] == {"col_a": "bigint", "Col_B": "string"}
    assert resolver.get_original_field_paths(_V2_URN) == {
        "col_a": "[version=2.0].[type=long].col_a",
        "Col_B": "[version=2.0].[type=string].Col_B",
    }


def test_original_field_paths_kept_when_fetched_from_graph():
    graph = MagicMock(spec=DataHubGraph)
    graph.get_aspect.return_value = create_mock_schema(_V2_FIELDS)
    resolver = SchemaResolver(platform="glue", env="PROD", graph=graph)

    resolver.resolve_urn(_V2_URN)

    assert resolver.get_original_field_paths(_V2_URN)["col_a"] == (
        "[version=2.0].[type=long].col_a"
    )


def test_original_field_paths_empty_for_v1_and_unknown_schemas():
    resolver = SchemaResolver(platform="glue", env="PROD", graph=None)
    resolver.add_schema_metadata(_V2_URN, create_mock_schema([("col_a", "bigint")]))

    assert resolver.get_original_field_paths(_V2_URN) == {}
    assert resolver.get_original_field_paths("urn:li:dataset:unknown") == {}


@pytest.mark.parametrize(
    "replace",
    [
        pytest.param(
            lambda r: r.add_raw_schema_info(_V2_URN, {"col_a": "bigint"}), id="raw"
        ),
        pytest.param(
            lambda r: r.add_schema_metadata(
                _V2_URN, create_mock_schema([("col_a", "bigint")])
            ),
            id="v1_schema",
        ),
        pytest.param(
            lambda r: r.add_schema_metadata_from_fetch(_V2_URN, None), id="fetch_miss"
        ),
    ],
)
def test_original_field_paths_cleared_when_schema_replaced(replace):
    resolver = SchemaResolver(platform="glue", env="PROD", graph=None)
    resolver.add_schema_metadata(_V2_URN, create_mock_schema(_V2_FIELDS))
    replace(resolver)

    assert resolver.get_original_field_paths(_V2_URN) == {}


def test_original_field_paths_for_mixed_v1_and_v2_schema():
    resolver = SchemaResolver(platform="glue", env="PROD", graph=None)
    resolver.add_schema_metadata(
        _V2_URN,
        create_mock_schema(
            [("col_a", "bigint"), ("[version=2.0].[type=string].col_b", "string")]
        ),
    )

    assert resolver.resolve_urn(_V2_URN)[1] == {"col_a": "bigint", "col_b": "string"}
    assert resolver.get_original_field_paths(_V2_URN) == {
        "col_b": "[version=2.0].[type=string].col_b"
    }


def _hive_paths(name: str, hive_type: str) -> List[str]:
    return [f.fieldPath for f in get_schema_fields_for_hive_column(name, hive_type)]


def _avro_paths(avro_type: object) -> List[str]:
    record = {
        "type": "record",
        "name": "R",
        "fields": [{"name": "u", "type": avro_type}],
    }
    return [f.fieldPath for f in avro_schema_to_mce_fields(json.dumps(record))]


@pytest.mark.parametrize(
    "paths, expected",
    [
        # Real converter output; the column root is what a schemaField URN targets.
        (
            _hive_paths("s", "struct<a:int,b:string>"),
            "[version=2.0].[type=struct].[type=struct].s",
        ),
        (
            _hive_paths("arr", "array<int>"),
            "[version=2.0].[type=struct].[type=array].[type=int].arr",
        ),
        (
            _hive_paths("m", "map<string,int>"),
            "[version=2.0].[type=struct].[type=map].[type=int].m",
        ),
        (
            _hive_paths("u", "uniontype<int,string>"),
            "[version=2.0].[type=struct].[type=union].u",
        ),
        # A nullable Avro union collapses to the member type: there is no wrapper.
        (_avro_paths(["null", "long"]), "[version=2.0].[type=R].[type=long].u"),
        (
            _avro_paths(["null", "long", "string"]),
            "[version=2.0].[type=R].[type=union].u",
        ),
    ],
    ids=[
        "struct",
        "array",
        "map",
        "hive_union",
        "avro_nullable_union",
        "avro_multi_union",
    ],
)
@pytest.mark.parametrize(
    "order",
    [list, sorted, lambda p: list(reversed(p))],
    ids=["emitted", "sorted", "reversed"],
)
def test_original_field_path_is_the_column_root(paths, expected, order):
    # Union members simplify to the same path as their column. The column must win
    # whatever the field order: some sources sort fields by fieldPath, which puts
    # members before the column.
    resolver = SchemaResolver(platform="glue", env="PROD", graph=None)
    resolver.add_schema_metadata(
        _V2_URN, create_mock_schema([(p, "t") for p in order(paths)])
    )

    simple = get_simple_field_path_from_v2_field_path(expected)
    assert resolver.get_original_field_paths(_V2_URN) == {simple: expected}


@pytest.mark.parametrize(
    "order",
    [list, sorted, lambda p: list(reversed(p))],
    ids=["emitted", "sorted", "reversed"],
)
def test_original_field_path_tie_break_is_order_independent(order):
    # A Kafka-style key and value field share a name and a [type=...] count; the
    # value field wins regardless of field order.
    value_path = "[version=2.0].[type=V].[type=long].id"
    key_path = "[version=2.0].[key=True].[type=K].[type=long].id"
    other_value_path = "[version=2.0].[type=W].[type=long].id"
    resolver = SchemaResolver(platform="kafka", env="PROD", graph=None)
    resolver.add_schema_metadata(
        _V2_URN,
        create_mock_schema(
            [(p, "t") for p in order([key_path, value_path, other_value_path])]
        ),
    )

    # Ties between value fields fall back to lexicographic order.
    assert resolver.get_original_field_paths(_V2_URN) == {"id": value_path}


def test_close_releases_the_shared_connection():
    resolver = SchemaResolver(platform="glue", env="PROD", graph=None)
    resolver.add_schema_metadata(_V2_URN, create_mock_schema(_V2_FIELDS))
    temp_directory = resolver._conn._temp_directory
    assert temp_directory is not None and pathlib.Path(temp_directory).exists()

    resolver.close()

    assert resolver.closed
    assert resolver._original_field_paths.closed
    assert not pathlib.Path(temp_directory).exists()


def test_reopened_cache_file_restores_schemas_and_original_field_paths(tmp_path):
    cache_file = tmp_path / "schema_cache.db"
    resolver = SchemaResolver(
        platform="glue", env="PROD", graph=None, _cache_filename=cache_file
    )
    resolver.add_schema_metadata(_V2_URN, create_mock_schema(_V2_FIELDS))
    # Indexes on the schema cache's extra column are created lazily by queries.
    assert resolver.schema_count() == 1
    resolver.close()

    reopened = SchemaResolver(
        platform="glue", env="PROD", graph=None, _cache_filename=cache_file
    )

    assert reopened.schema_count() == 1
    assert reopened.resolve_urn(_V2_URN)[1] == {"col_a": "bigint", "Col_B": "string"}
    assert reopened.get_original_field_paths(_V2_URN) == {
        "col_a": "[version=2.0].[type=long].col_a",
        "Col_B": "[version=2.0].[type=string].Col_B",
    }
