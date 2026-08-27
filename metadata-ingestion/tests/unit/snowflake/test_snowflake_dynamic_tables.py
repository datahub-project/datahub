import json
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.constants import (
    SnowflakeObjectDomain,
    SnowflakeShowKind,
)
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
    SnowflakeDynamicTableInput,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)


@pytest.fixture
def mock_snowflake_data_dictionary() -> SnowflakeDataDictionary:
    connection = cast(SnowflakeConnection, MagicMock())
    report = cast(SnowflakeV2Report, MagicMock())
    data_dict = SnowflakeDataDictionary(connection, report)
    return data_dict


def test_get_dynamic_table_graph_info(mock_snowflake_data_dictionary):
    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            # DYNAMIC_TABLE_GRAPH_HISTORY() reports NAME unqualified, with SCHEMA_NAME and
            # DATABASE_NAME as separate columns - verified against a live account. The
            # earlier fixture put a qualified name in NAME, which made a lookup keyed on
            # the fully qualified name appear to work when it never did.
            "NAME": "DYNAMIC_TABLE1",
            "SCHEMA_NAME": "PUBLIC",
            "DATABASE_NAME": "TEST_DB",
            "INPUTS": [{"name": "TEST_DB.PUBLIC.SOURCE_TABLE", "kind": "TABLE"}],
            "TARGET_LAG_TYPE": "INTERVAL",
            "TARGET_LAG_SEC": 60,
            "SCHEDULING_STATE": "ACTIVE",
            "ALTER_TRIGGER": "AUTO",
        }
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    result = mock_snowflake_data_dictionary.get_dynamic_table_graph_info("TEST_DB")

    assert len(result) == 1
    table_info = result.get("TEST_DB.PUBLIC.DYNAMIC_TABLE1")
    assert table_info is not None
    assert table_info["target_lag_type"] == "INTERVAL"
    assert table_info["target_lag_sec"] == 60
    assert table_info["inputs"] == [
        {"name": "TEST_DB.PUBLIC.SOURCE_TABLE", "kind": "TABLE"}
    ]


def test_get_dynamic_tables_with_definitions(mock_snowflake_data_dictionary):
    mock_snowflake_data_dictionary.get_dynamic_table_graph_info = MagicMock(
        return_value={
            "TEST_DB.PUBLIC.DYNAMIC_TABLE1": {
                "inputs": [{"name": "TEST_DB.PUBLIC.SOURCE_TABLE", "kind": "TABLE"}],
                "target_lag_type": "INTERVAL",
                "target_lag_sec": 60,
                "scheduling_state": "ACTIVE",
                "alter_trigger": "AUTO",
            }
        }
    )

    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "name": "DYNAMIC_TABLE1",
            "schema_name": "PUBLIC",
            "database_name": "TEST_DB",
            "owner": "TEST_USER",
            "comment": "Test dynamic table",
            "created_on": "2024-01-01 00:00:00",
            "text": "SELECT * FROM source_table",
            "target_lag": "1 minute",
            "warehouse": "TEST_WH",
            "bytes": 1000,
            "rows": 100,
        }
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    result = mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions(
        "TEST_DB"
    )

    assert "PUBLIC" in result
    dt = result["PUBLIC"][0]
    assert isinstance(dt, SnowflakeDynamicTable)
    assert dt.name == "DYNAMIC_TABLE1"
    assert dt.definition == "SELECT * FROM source_table"
    assert dt.target_lag == "1 minute"
    assert dt.upstream_tables == [
        SnowflakeDynamicTableInput("TEST_DB.PUBLIC.SOURCE_TABLE", "TABLE")
    ]


@pytest.mark.parametrize(
    "graph_info,expected_missing,expected_upstreams",
    [
        (
            {
                "TEST_DB.PUBLIC.DYNAMIC_TABLE1": {
                    "inputs": [{"name": "TEST_DB.PUBLIC.SOURCE_TABLE", "kind": "TABLE"}]
                }
            },
            0,
            [SnowflakeDynamicTableInput("TEST_DB.PUBLIC.SOURCE_TABLE", "TABLE")],
        ),
        ({}, 1, []),
    ],
    ids=["matched", "no_graph_row"],
)
def test_a_dynamic_table_without_a_graph_row_is_counted(
    graph_info, expected_missing, expected_upstreams
):
    """Losing the graph row loses INPUTS upstreams and the target_lag fallback silently -
    which is exactly how the keying bug went unnoticed. The counter is the signal: it
    equalling the dynamic-table count means every lookup missed, whether from a bad key or
    from the graph-history query's filters excluding everything on some edition."""
    report = SnowflakeV2Report()
    data_dictionary = SnowflakeDataDictionary(
        cast(SnowflakeConnection, MagicMock()), report
    )
    data_dictionary.get_dynamic_table_graph_info = MagicMock(return_value=graph_info)  # type: ignore[method-assign]

    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "name": "DYNAMIC_TABLE1",
            "schema_name": "PUBLIC",
            "created_on": "2024-01-01 00:00:00",
            "text": "SELECT * FROM source_table",
            "target_lag": "1 minute",
            "bytes": 1000,
            "rows": 100,
            "comment": None,
        }
    ]
    data_dictionary.connection.query.return_value = mock_cursor  # type: ignore[attr-defined]

    result = data_dictionary.get_dynamic_tables_with_definitions("TEST_DB")

    assert result is not None
    assert result["PUBLIC"][0].upstream_tables == expected_upstreams
    assert report.num_dynamic_tables_missing_graph_info == expected_missing


def test_get_dynamic_tables_with_definitions_inputs_as_json_string(
    mock_snowflake_data_dictionary,
):
    """INPUTS returned as a JSON string (as some Snowflake driver versions do) is parsed correctly."""
    mock_snowflake_data_dictionary.get_dynamic_table_graph_info = MagicMock(
        return_value={
            "TEST_DB.PUBLIC.DYNAMIC_TABLE1": {
                "inputs": json.dumps(
                    [{"name": "TEST_DB.PUBLIC.SOURCE_TABLE", "kind": "TABLE"}]
                ),
                "target_lag_type": None,
                "target_lag_sec": None,
            }
        }
    )

    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "name": "DYNAMIC_TABLE1",
            "schema_name": "PUBLIC",
            "database_name": "TEST_DB",
            "created_on": "2024-01-01 00:00:00",
            "text": "SELECT * FROM source_table",
            "target_lag": "1 minute",
            "bytes": 0,
            "rows": 0,
            "comment": None,
        }
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    result = mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions(
        "TEST_DB"
    )

    dt = result["PUBLIC"][0]
    assert dt.upstream_tables == [
        SnowflakeDynamicTableInput("TEST_DB.PUBLIC.SOURCE_TABLE", "TABLE")
    ]


def test_get_dynamic_tables_with_definitions_malformed_inputs_json(
    mock_snowflake_data_dictionary,
):
    """A malformed INPUTS JSON value for one table doesn't skip remaining dynamic tables."""
    mock_snowflake_data_dictionary.get_dynamic_table_graph_info = MagicMock(
        return_value={
            "TEST_DB.PUBLIC.BAD_TABLE": {
                "inputs": "{not valid json",
                "target_lag_type": None,
                "target_lag_sec": None,
            },
            "TEST_DB.PUBLIC.GOOD_TABLE": {
                "inputs": [{"name": "TEST_DB.PUBLIC.SOURCE_TABLE", "kind": "TABLE"}],
                "target_lag_type": None,
                "target_lag_sec": None,
            },
        }
    )

    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "name": "BAD_TABLE",
            "schema_name": "PUBLIC",
            "created_on": "2024-01-01 00:00:00",
            "text": None,
            "target_lag": None,
            "bytes": 0,
            "rows": 0,
            "comment": None,
        },
        {
            "name": "GOOD_TABLE",
            "schema_name": "PUBLIC",
            "created_on": "2024-01-01 00:00:00",
            "text": None,
            "target_lag": None,
            "bytes": 0,
            "rows": 0,
            "comment": None,
        },
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    result = mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions(
        "TEST_DB"
    )

    assert len(result["PUBLIC"]) == 2
    bad_dt = next(t for t in result["PUBLIC"] if t.name == "BAD_TABLE")
    good_dt = next(t for t in result["PUBLIC"] if t.name == "GOOD_TABLE")
    assert bad_dt.upstream_tables == []
    assert good_dt.upstream_tables == [
        SnowflakeDynamicTableInput("TEST_DB.PUBLIC.SOURCE_TABLE", "TABLE")
    ]


@pytest.mark.parametrize(
    "raw_inputs",
    [
        pytest.param("null", id="json-null"),
        pytest.param('{"name": "TEST_DB.PUBLIC.X", "kind": "TABLE"}', id="single-dict"),
    ],
)
def test_get_dynamic_tables_with_definitions_inputs_non_list_json(
    mock_snowflake_data_dictionary, raw_inputs
):
    """INPUTS that parses to a non-list value (null, single object) doesn't crash
    ingestion — the table is kept with empty upstream_tables."""
    mock_snowflake_data_dictionary.get_dynamic_table_graph_info = MagicMock(
        return_value={
            "TEST_DB.PUBLIC.WEIRD_TABLE": {
                "inputs": raw_inputs,
                "target_lag_type": None,
                "target_lag_sec": None,
            },
        }
    )

    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "name": "WEIRD_TABLE",
            "schema_name": "PUBLIC",
            "created_on": "2024-01-01 00:00:00",
            "text": None,
            "target_lag": None,
            "bytes": 0,
            "rows": 0,
            "comment": None,
        },
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    result = mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions(
        "TEST_DB"
    )

    assert len(result["PUBLIC"]) == 1
    assert result["PUBLIC"][0].upstream_tables == []


@pytest.mark.parametrize(
    "kind,expected_domain",
    [
        ("TABLE", SnowflakeObjectDomain.TABLE),
        ("MATERIALIZED_VIEW", SnowflakeObjectDomain.MATERIALIZED_VIEW),
        ("EXTERNAL_TABLE", SnowflakeObjectDomain.EXTERNAL_TABLE),
        ("DYNAMIC_TABLE", SnowflakeObjectDomain.DYNAMIC_TABLE),
        ("SOMETHING_NEW", SnowflakeObjectDomain.TABLE),
    ],
)
def test_resolve_input_kind_normalization(kind, expected_domain):
    assert SnowflakeSchemaGenerator._resolve_input_kind(kind) == expected_domain


def test_populate_dynamic_table_definitions(mock_snowflake_data_dictionary):
    mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions = MagicMock(
        return_value={
            "PUBLIC": [
                SnowflakeDynamicTable(
                    name="DYNAMIC_TABLE1",
                    created=None,
                    last_altered=None,
                    size_in_bytes=1000,
                    rows_count=100,
                    comment="Test dynamic table",
                    definition="SELECT * FROM source_table",
                    target_lag="1 minute",
                    upstream_tables=[
                        SnowflakeDynamicTableInput(
                            "TEST_DB.PUBLIC.SOURCE_TABLE", "Table"
                        )
                    ],
                    is_dynamic=True,
                    type="DYNAMIC TABLE",
                )
            ]
        }
    )

    tables = {
        "PUBLIC": [
            SnowflakeDynamicTable(
                name="DYNAMIC_TABLE1",
                created=None,
                last_altered=None,
                size_in_bytes=0,
                rows_count=0,
                comment="Test dynamic table",
                is_dynamic=True,
                type="DYNAMIC TABLE",
            )
        ]
    }

    mock_snowflake_data_dictionary.populate_dynamic_table_definitions(tables, "TEST_DB")

    dt = tables["PUBLIC"][0]
    assert dt.definition == "SELECT * FROM source_table"
    assert dt.target_lag == "1 minute"
    assert dt.upstream_tables == [
        SnowflakeDynamicTableInput("TEST_DB.PUBLIC.SOURCE_TABLE", "Table")
    ]


def test_dynamic_table_subtype():
    # Test that dynamic tables are correctly identified as having DYNAMIC_TABLE subtype
    dt = SnowflakeDynamicTable(
        name="test",
        created=None,
        last_altered=None,
        size_in_bytes=0,
        rows_count=0,
        comment="Test dynamic table",
        is_dynamic=True,
        type="DYNAMIC TABLE",
    )

    assert dt.get_subtype() == DatasetSubTypes.DYNAMIC_TABLE


def test_dynamic_table_pagination():
    # Pagination is only sound at schema scope, where the output is ordered by name
    # alone and so matches what the `FROM '<name>'` cursor compares against. The
    # database-wide variant is ordered by (schema, name) and therefore takes no marker.
    query = SnowflakeQuery.show_objects_for_schema(
        SnowflakeShowKind.DYNAMIC_TABLES,
        db_name="TEST_DB",
        schema_name="TEST_SCHEMA",
        marker="LAST_TABLE",
    )

    assert 'IN SCHEMA "TEST_DB"."TEST_SCHEMA"' in query
    assert "FROM 'LAST_TABLE'" in query


def test_dynamic_table_graph_history_query():
    # Test the dynamic table graph history query generation
    query = SnowflakeQuery.get_dynamic_table_graph_history("TEST_DB")

    # Verify the query references the correct view
    assert "DYNAMIC_TABLE_GRAPH_HISTORY()" in query
    assert "TEST_DB" in query

    # Both filters carry correctness, so assert them rather than just the table function.
    # The function is account-scoped, so without the database_name predicate this returns
    # every database's dynamic tables; and it reports history, so without valid_to a
    # superseded row can win the key.
    assert "database_name = 'TEST_DB'" in query
    assert "valid_to IS NULL" in query
    # The row's own database is needed to key the result; the caller's cannot be assumed.
    assert "database_name," in query


@patch(
    "datahub.ingestion.source.snowflake.snowflake_lineage_v2.SnowflakeLineageExtractor"
)
def test_dynamic_table_lineage_extraction(mock_extractor_class):
    # Mock the extractor instance
    mock_extractor = mock_extractor_class.return_value
    mock_connection = MagicMock()
    mock_extractor.connection = mock_connection

    # Mock the query response for dynamic table definition
    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "DOWNSTREAM_TABLE_NAME": "TEST_DB.PUBLIC.DYNAMIC_TABLE1",
            "DOWNSTREAM_TABLE_DOMAIN": "Dynamic Table",
            "UPSTREAM_TABLES": [
                {
                    "upstream_object_domain": "Table",
                    "upstream_object_name": "TEST_DB.PUBLIC.SOURCE_TABLE",
                    "query_id": "123",
                }
            ],
            "UPSTREAM_COLUMNS": [],
            "QUERIES": [],
        }
    ]
    mock_connection.query.return_value = mock_cursor

    # Test processing the lineage
    from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
        UpstreamLineageEdge,
    )

    result = UpstreamLineageEdge.model_validate(mock_cursor.__iter__.return_value[0])

    # Verify the lineage information
    assert result.DOWNSTREAM_TABLE_NAME == "TEST_DB.PUBLIC.DYNAMIC_TABLE1"
    assert result.DOWNSTREAM_TABLE_DOMAIN == "Dynamic Table"
    assert result.UPSTREAM_TABLES is not None  # Check for None before accessing
    assert len(result.UPSTREAM_TABLES) == 1
    upstream = result.UPSTREAM_TABLES[0]  # Safe to access after length check
    assert upstream.upstream_object_domain == "Table"
    assert upstream.upstream_object_name == "TEST_DB.PUBLIC.SOURCE_TABLE"
    assert upstream.query_id == "123"


def test_dynamic_table_error_handling(mock_snowflake_data_dictionary):
    # Mock an error response from the graph history query
    mock_cursor = MagicMock()
    mock_cursor.__iter__.side_effect = Exception("Failed to fetch dynamic table info")
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    # Test error handling in get_dynamic_table_graph_info
    result = mock_snowflake_data_dictionary.get_dynamic_table_graph_info("TEST_DB")

    # Verify empty result is returned on error
    assert result == {}


def test_populate_dynamic_table_definitions_missing_definition(
    mock_snowflake_data_dictionary,
):
    """When SHOW DYNAMIC TABLES returns text=None (MONITOR not granted), definition
    stays None but upstream_tables is still populated from DYNAMIC_TABLE_GRAPH_HISTORY."""
    mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions = MagicMock(
        return_value={
            "PUBLIC": [
                SnowflakeDynamicTable(
                    name="DYNAMIC_TABLE1",
                    created=None,
                    last_altered=None,
                    size_in_bytes=0,
                    rows_count=0,
                    comment=None,
                    definition=None,
                    target_lag=None,
                    upstream_tables=[
                        SnowflakeDynamicTableInput(
                            "TEST_DB.PUBLIC.SOURCE_TABLE", "Table"
                        )
                    ],
                    is_dynamic=True,
                    type="DYNAMIC TABLE",
                )
            ]
        }
    )

    tables = {
        "PUBLIC": [
            SnowflakeDynamicTable(
                name="DYNAMIC_TABLE1",
                created=None,
                last_altered=None,
                size_in_bytes=0,
                rows_count=0,
                comment=None,
                is_dynamic=True,
                type="DYNAMIC TABLE",
            )
        ]
    }

    mock_snowflake_data_dictionary.populate_dynamic_table_definitions(tables, "TEST_DB")

    dt = tables["PUBLIC"][0]
    assert dt.definition is None
    assert dt.upstream_tables == [
        SnowflakeDynamicTableInput("TEST_DB.PUBLIC.SOURCE_TABLE", "Table")
    ]


def test_dynamic_table_definition_error_handling(mock_snowflake_data_dictionary):
    # Mock an error in get_dynamic_tables_with_definitions
    mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions = MagicMock()
    mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions.side_effect = (
        Exception("Failed to get definitions")
    )

    # Create test tables dictionary
    tables = {
        "PUBLIC": [
            SnowflakeDynamicTable(
                name="DYNAMIC_TABLE1",
                created=None,
                last_altered=None,
                size_in_bytes=0,
                rows_count=0,
                comment="Test dynamic table",
                is_dynamic=True,
                type="DYNAMIC TABLE",
            )
        ]
    }

    # Test error handling in populate_dynamic_table_definitions
    mock_snowflake_data_dictionary.populate_dynamic_table_definitions(tables, "TEST_DB")

    # Verify tables remain unchanged
    assert len(tables["PUBLIC"]) == 1
    dt = tables["PUBLIC"][0]
    assert isinstance(dt, SnowflakeDynamicTable)
    assert dt.name == "DYNAMIC_TABLE1"


def test_dynamic_table_invalid_response_handling(mock_snowflake_data_dictionary):
    # Mock an invalid response missing required fields
    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "NAME": "DYNAMIC_TABLE1",
            "SCHEMA_NAME": "PUBLIC",
            "DATABASE_NAME": "TEST_DB",
            # Missing other required fields
        }
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    # Test handling of invalid response
    result = mock_snowflake_data_dictionary.get_dynamic_table_graph_info("TEST_DB")

    # Verify partial result is handled gracefully
    assert len(result) == 1
    table_info = result.get("TEST_DB.PUBLIC.DYNAMIC_TABLE1", {})
    assert table_info.get("target_lag_type") is None
    assert table_info.get("scheduling_state") is None
