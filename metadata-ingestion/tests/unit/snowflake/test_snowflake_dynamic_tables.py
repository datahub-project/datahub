from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
)


@pytest.fixture
def mock_snowflake_data_dictionary() -> SnowflakeDataDictionary:
    connection = cast(SnowflakeConnection, MagicMock())
    data_dict = SnowflakeDataDictionary(connection)
    return data_dict


def test_get_dynamic_table_graph_info(mock_snowflake_data_dictionary):
    # Mock the query response for dynamic table graph history
    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {
            "NAME": "TEST_DB.PUBLIC.DYNAMIC_TABLE1",
            "INPUTS": "source_table",
            "TARGET_LAG_TYPE": "INTERVAL",
            "TARGET_LAG_SEC": 60,
            "SCHEDULING_STATE": "ACTIVE",
            "ALTER_TRIGGER": "AUTO",
        }
    ]
    mock_snowflake_data_dictionary.connection.query.return_value = mock_cursor

    # Test getting dynamic table graph info
    result = mock_snowflake_data_dictionary.get_dynamic_table_graph_info("TEST_DB")

    # Verify the results
    assert len(result) == 1
    table_info = result.get("TEST_DB.PUBLIC.DYNAMIC_TABLE1")
    assert table_info is not None
    assert table_info["target_lag_type"] == "INTERVAL"
    assert table_info["target_lag_sec"] == 60
    assert table_info["scheduling_state"] == "ACTIVE"
    assert table_info["alter_trigger"] == "AUTO"


def test_get_dynamic_tables_with_definitions(mock_snowflake_data_dictionary):
    # Mock the graph info response
    mock_snowflake_data_dictionary.get_dynamic_table_graph_info = MagicMock(
        return_value={
            "TEST_DB.PUBLIC.DYNAMIC_TABLE1": {
                "target_lag_type": "INTERVAL",
                "target_lag_sec": 60,
                "scheduling_state": "ACTIVE",
                "alter_trigger": "AUTO",
            }
        }
    )

    # Mock the show dynamic tables response
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

    # Test getting dynamic tables with definitions
    result = mock_snowflake_data_dictionary.get_dynamic_tables_with_definitions(
        "TEST_DB"
    )

    # Verify the results
    assert len(result) == 1
    assert "PUBLIC" in result
    dynamic_tables = result["PUBLIC"]
    assert len(dynamic_tables) == 1
    dt = dynamic_tables[0]
    assert isinstance(dt, SnowflakeDynamicTable)
    assert dt.name == "DYNAMIC_TABLE1"
    assert dt.definition == "SELECT * FROM source_table"
    assert dt.target_lag == "1 minute"
    assert dt.is_dynamic is True


def test_populate_dynamic_table_definitions(mock_snowflake_data_dictionary):
    # Mock get_dynamic_tables_with_definitions response
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
                    is_dynamic=True,
                    type="DYNAMIC TABLE",
                )
            ]
        }
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

    # Test populating dynamic table definitions
    mock_snowflake_data_dictionary.populate_dynamic_table_definitions(tables, "TEST_DB")

    # Verify the results
    assert len(tables["PUBLIC"]) == 1
    dt = tables["PUBLIC"][0]
    assert isinstance(dt, SnowflakeDynamicTable)
    assert dt.name == "DYNAMIC_TABLE1"
    assert dt.definition == "SELECT * FROM source_table"
    assert dt.target_lag == "1 minute"


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
    # Test the pagination marker handling in show_dynamic_tables_for_database
    query = SnowflakeQuery.show_dynamic_tables_for_database(
        db_name="TEST_DB", dynamic_table_pagination_marker="LAST_TABLE"
    )

    # Verify the pagination marker is included in the query
    assert "FROM 'LAST_TABLE'" in query


def test_dynamic_table_graph_history_query():
    # Test the dynamic table graph history query generation
    query = SnowflakeQuery.get_dynamic_table_graph_history("TEST_DB")

    # Verify the query references the correct view
    assert "DYNAMIC_TABLE_GRAPH_HISTORY()" in query
    assert "TEST_DB" in query


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

    result = UpstreamLineageEdge.parse_obj(mock_cursor.__iter__.return_value[0])

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
            "NAME": "TEST_DB.PUBLIC.DYNAMIC_TABLE1",
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
