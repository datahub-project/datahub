import datetime
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
)


def test_snowflake_config_semantic_view_pattern():
    """Test that SnowflakeV2Config includes semantic view pattern filtering."""
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
            "semantic_view_pattern": AllowDenyPattern(
                allow=["TEST_DB\\.PUBLIC\\..*"],
                deny=[".*_INTERNAL"],
            ),
        }
    )

    assert config.semantic_view_pattern is not None
    assert config.semantic_view_pattern.allowed("TEST_DB.PUBLIC.SALES_SV")
    assert not config.semantic_view_pattern.allowed(
        "TEST_DB.PUBLIC.INTERNAL_SV_INTERNAL"
    )


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_get_semantic_views_for_database(mock_connection):
    """Test fetching semantic views for a database."""
    # Mock the connection query method
    mock_cursor = MagicMock()
    mock_cursor.__iter__ = MagicMock(
        return_value=iter(
            [
                {
                    "name": "test_semantic_view_1",
                    "schema_name": "PUBLIC",
                    "created_on": datetime.datetime.now(),
                    "comment": "Test semantic view 1",
                    "text": "yaml: definition 1",
                },
                {
                    "name": "test_semantic_view_2",
                    "schema_name": "PUBLIC",
                    "created_on": datetime.datetime.now(),
                    "comment": "Test semantic view 2",
                    "text": "yaml: definition 2",
                },
            ]
        )
    )

    mock_connection_instance = MagicMock()
    mock_connection_instance.query.return_value = mock_cursor

    report = SnowflakeV2Report()
    data_dict = SnowflakeDataDictionary(
        connection=mock_connection_instance,
        report=report,
    )

    semantic_views = data_dict.get_semantic_views_for_database("TEST_DB")

    assert semantic_views is not None
    assert "PUBLIC" in semantic_views
    assert len(semantic_views["PUBLIC"]) == 2
    assert semantic_views["PUBLIC"][0].name == "test_semantic_view_1"
    assert semantic_views["PUBLIC"][1].name == "test_semantic_view_2"


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_populate_semantic_view_base_tables(mock_connection):
    """Test populating base tables for semantic views from INFORMATION_SCHEMA.SEMANTIC_TABLES."""
    # Mock the connection for semantic views query
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "name": "test_semantic_view",
            "schema_name": "PUBLIC",
            "created_on": datetime.datetime.now(),
            "comment": "Test semantic view",
        },
    ]

    # Mock the connection for semantic tables query
    mock_semantic_tables_cursor = MagicMock()
    mock_semantic_tables_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_semantic_view",
            "SEMANTIC_TABLE_NAME": "customers_table",
            "BASE_TABLE_CATALOG": "TEST_DB",
            "BASE_TABLE_SCHEMA": "PUBLIC",
            "BASE_TABLE_NAME": "CUSTOMERS",
            "PRIMARY_KEYS": None,
            "UNIQUE_KEYS": None,
            "COMMENT": None,
            "SYNONYMS": None,
        },
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_semantic_view",
            "SEMANTIC_TABLE_NAME": "orders_table",
            "BASE_TABLE_CATALOG": "TEST_DB",
            "BASE_TABLE_SCHEMA": "PUBLIC",
            "BASE_TABLE_NAME": "ORDERS",
            "PRIMARY_KEYS": None,
            "UNIQUE_KEYS": None,
            "COMMENT": None,
            "SYNONYMS": None,
        },
    ]

    # Mock DDL query to return empty
    mock_ddl_cursor = MagicMock()
    mock_ddl_cursor.__iter__.return_value = []

    # Mock dimensions query to return empty
    mock_dimensions_cursor = MagicMock()
    mock_dimensions_cursor.__iter__.return_value = []

    # Mock facts query to return empty
    mock_facts_cursor = MagicMock()
    mock_facts_cursor.__iter__.return_value = []

    # Mock metrics query to return empty
    mock_metrics_cursor = MagicMock()
    mock_metrics_cursor.__iter__.return_value = []

    # Mock relationships query to return empty
    mock_relationships_cursor = MagicMock()
    mock_relationships_cursor.__iter__.return_value = []

    mock_connection_instance = MagicMock()

    # Set up query to return different cursors based on the query
    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "semantic_tables" in query_lower:
            return mock_semantic_tables_cursor
        elif "semantic_dimensions" in query_lower:
            return mock_dimensions_cursor
        elif "semantic_facts" in query_lower:
            return mock_facts_cursor
        elif "semantic_metrics" in query_lower:
            return mock_metrics_cursor
        elif "semantic_relationships" in query_lower:
            return mock_relationships_cursor
        elif "get_ddl" in query_lower:
            return mock_ddl_cursor
        else:
            return mock_semantic_views_cursor

    mock_connection_instance.query.side_effect = query_side_effect

    report = SnowflakeV2Report()
    data_dict = SnowflakeDataDictionary(
        connection=mock_connection_instance,
        report=report,
    )

    semantic_views = data_dict.get_semantic_views_for_database("TEST_DB")

    assert semantic_views is not None
    assert "PUBLIC" in semantic_views
    assert len(semantic_views["PUBLIC"]) == 1

    # Check that base tables were populated
    semantic_view = semantic_views["PUBLIC"][0]
    assert len(semantic_view.base_tables) == 2
    assert ("TEST_DB", "PUBLIC", "CUSTOMERS") in semantic_view.base_tables
    assert ("TEST_DB", "PUBLIC", "ORDERS") in semantic_view.base_tables


if __name__ == "__main__":
    pytest.main([__file__])
