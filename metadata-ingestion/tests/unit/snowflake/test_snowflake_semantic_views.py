import datetime
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDataDictionary,
    SnowflakeSemanticView,
)


def test_snowflake_semantic_view_subtype():
    """Test that SnowflakeSemanticView returns the correct subtype."""
    semantic_view = SnowflakeSemanticView(
        name="test_semantic_view",
        created=datetime.datetime.now(),
        comment="Test semantic view",
        view_definition="yaml: definition",
        last_altered=datetime.datetime.now(),
        semantic_definition="yaml: definition",
    )

    assert semantic_view.get_subtype() == DatasetSubTypes.SEMANTIC_VIEW


def test_snowflake_semantic_view_columns():
    """Test that SnowflakeSemanticView can have columns."""
    semantic_view = SnowflakeSemanticView(
        name="test_semantic_view",
        created=datetime.datetime.now(),
        comment="Test semantic view",
        view_definition="yaml: definition",
        last_altered=datetime.datetime.now(),
        semantic_definition="yaml: definition",
        columns=[
            SnowflakeColumn(
                name="metric_1",
                ordinal_position=1,
                is_nullable=False,
                data_type="NUMBER",
                comment="First metric",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=0,
            ),
            SnowflakeColumn(
                name="dimension_1",
                ordinal_position=2,
                is_nullable=True,
                data_type="VARCHAR",
                comment="First dimension",
                character_maximum_length=255,
                numeric_precision=None,
                numeric_scale=None,
            ),
        ],
    )

    assert len(semantic_view.columns) == 2
    assert semantic_view.columns[0].name == "metric_1"
    assert semantic_view.columns[0].data_type == "NUMBER"
    assert semantic_view.columns[1].name == "dimension_1"
    assert semantic_view.columns[1].data_type == "VARCHAR"


def test_snowflake_semantic_view_with_tags():
    """Test that SnowflakeSemanticView can have tags."""
    from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag

    semantic_view = SnowflakeSemanticView(
        name="test_semantic_view",
        created=datetime.datetime.now(),
        comment="Test semantic view",
        view_definition="yaml: definition",
        last_altered=datetime.datetime.now(),
        semantic_definition="yaml: definition",
        tags=[
            SnowflakeTag(
                database="TEST_DB",
                schema="TEST_SCHEMA",
                name="PII",
                value="true",
            )
        ],
    )

    assert semantic_view.tags is not None
    assert len(semantic_view.tags) == 1
    assert semantic_view.tags[0].name == "PII"
    assert semantic_view.tags[0].value == "true"


def test_snowflake_config_includes_semantic_views():
    """Test that SnowflakeV2Config includes semantic view configuration."""
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
            "include_semantic_views": True,
        }
    )

    assert config.include_semantic_views is True


def test_snowflake_config_semantic_view_pattern():
    """Test that SnowflakeV2Config includes semantic view pattern filtering."""
    from datahub.configuration.common import AllowDenyPattern

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


def test_snowflake_semantic_view_domain():
    """Test that SEMANTIC_VIEW is in SnowflakeObjectDomain."""
    from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain

    assert hasattr(SnowflakeObjectDomain, "SEMANTIC_VIEW")
    assert SnowflakeObjectDomain.SEMANTIC_VIEW == "semantic view"


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


def test_snowflake_semantic_view_base_tables():
    """Test that SnowflakeSemanticView can store base table lineage."""
    semantic_view = SnowflakeSemanticView(
        name="test_semantic_view",
        created=datetime.datetime.now(),
        comment="Test semantic view with lineage",
        view_definition="yaml: definition",
        last_altered=datetime.datetime.now(),
        semantic_definition="yaml: definition",
        base_tables=[
            ("TEST_DB", "PUBLIC", "CUSTOMERS"),
            ("TEST_DB", "PUBLIC", "ORDERS"),
            ("TEST_DB", "SALES", "REVENUE"),
        ],
    )

    assert len(semantic_view.base_tables) == 3
    assert ("TEST_DB", "PUBLIC", "CUSTOMERS") in semantic_view.base_tables
    assert ("TEST_DB", "PUBLIC", "ORDERS") in semantic_view.base_tables
    assert ("TEST_DB", "SALES", "REVENUE") in semantic_view.base_tables


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


def test_snowflake_semantic_view_columns_with_expressions():
    """Test that SnowflakeColumn can store SQL expressions for derived metrics/facts."""
    semantic_view = SnowflakeSemanticView(
        name="test_semantic_view",
        created=datetime.datetime.now(),
        comment="Test semantic view",
        view_definition="yaml: definition",
        last_altered=datetime.datetime.now(),
        semantic_definition="yaml: definition",
        columns=[
            SnowflakeColumn(
                name="order_total_metric",
                ordinal_position=1,
                is_nullable=False,
                data_type="NUMBER",
                comment="Sum of order totals",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
                expression="SUM(ORDER_TOTAL)",
            ),
            SnowflakeColumn(
                name="derived_metric",
                ordinal_position=2,
                is_nullable=False,
                data_type="NUMBER",
                comment="Calculated from other metrics",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
                expression="ORDERS.ORDER_TOTAL_METRIC + TRANSACTIONS.TRANSACTION_AMOUNT_METRIC",
            ),
        ],
    )

    assert len(semantic_view.columns) == 2
    assert semantic_view.columns[0].expression == "SUM(ORDER_TOTAL)"
    assert (
        semantic_view.columns[1].expression
        == "ORDERS.ORDER_TOTAL_METRIC + TRANSACTIONS.TRANSACTION_AMOUNT_METRIC"
    )


def test_snowflake_semantic_view_logical_to_physical_mapping():
    """Test that SnowflakeSemanticView stores logical to physical table mappings."""
    semantic_view = SnowflakeSemanticView(
        name="test_semantic_view",
        created=datetime.datetime.now(),
        comment="Test semantic view",
        view_definition="yaml: definition",
        last_altered=datetime.datetime.now(),
        semantic_definition="yaml: definition",
        logical_to_physical_table={
            "ORDERS": ("TEST_DB", "PUBLIC", "ORDERS"),
            "TRANSACTIONS": ("TEST_DB", "PUBLIC", "TRANSACTIONS"),
            "CUSTOMERS": ("TEST_DB", "SALES", "CUSTOMER_DATA"),
        },
    )

    assert len(semantic_view.logical_to_physical_table) == 3
    assert semantic_view.logical_to_physical_table["ORDERS"] == (
        "TEST_DB",
        "PUBLIC",
        "ORDERS",
    )
    assert semantic_view.logical_to_physical_table["TRANSACTIONS"] == (
        "TEST_DB",
        "PUBLIC",
        "TRANSACTIONS",
    )
    assert semantic_view.logical_to_physical_table["CUSTOMERS"] == (
        "TEST_DB",
        "SALES",
        "CUSTOMER_DATA",
    )


if __name__ == "__main__":
    pytest.main([__file__])
