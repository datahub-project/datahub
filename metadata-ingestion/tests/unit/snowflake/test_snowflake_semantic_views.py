import datetime
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeDataDictionary


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_populate_semantic_view_base_tables(mock_connection):
    """Test populating base tables for semantic views from INFORMATION_SCHEMA.SEMANTIC_TABLES."""
    # Mock the connection for semantic views query (INFORMATION_SCHEMA format)
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_semantic_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Test semantic view",
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

    # Check base tables (they are SnowflakeTableIdentifier objects)
    base_table_strs = [str(bt) for bt in semantic_view.base_tables]
    assert "TEST_DB.PUBLIC.CUSTOMERS" in base_table_strs
    assert "TEST_DB.PUBLIC.ORDERS" in base_table_strs


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_populate_semantic_view_columns_with_dimensions(mock_connection):
    """Test populating semantic view columns with dimensions."""
    # Mock semantic views (INFORMATION_SCHEMA format)
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "sales_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Sales semantic view",
        },
    ]

    # Mock dimensions
    mock_dimensions_cursor = MagicMock()
    mock_dimensions_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "sales_view",
            "NAME": "customer_name",
            "DATA_TYPE": "VARCHAR",
            "COMMENT": "Customer name",
            "TABLE_NAME": "customers",
            "SYNONYMS": '["client_name", "buyer_name"]',
            "EXPRESSION": None,
        },
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "sales_view",
            "NAME": "product_category",
            "DATA_TYPE": "VARCHAR",
            "COMMENT": "Product category",
            "TABLE_NAME": "products",
            "SYNONYMS": None,
            "EXPRESSION": None,
        },
    ]

    # Mock empty cursors for other queries
    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []
    mock_empty_cursor.fetchone.return_value = None

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "semantic_dimensions" in query_lower:
            return mock_dimensions_cursor
        elif (
            "semantic_facts" in query_lower
            or "semantic_metrics" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
            or "get_ddl" in query_lower
        ):
            return mock_empty_cursor
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
    semantic_view = semantic_views["PUBLIC"][0]

    # Verify columns were populated
    assert len(semantic_view.columns) == 2
    assert semantic_view.columns[0].name == "customer_name"
    assert semantic_view.columns[0].data_type == "VARCHAR"
    assert semantic_view.columns[1].name == "product_category"

    # Verify column subtypes
    assert semantic_view.column_subtypes["CUSTOMER_NAME"] == "DIMENSION"
    assert semantic_view.column_subtypes["PRODUCT_CATEGORY"] == "DIMENSION"

    # Verify synonyms were parsed
    assert "CUSTOMER_NAME" in semantic_view.column_synonyms
    assert len(semantic_view.column_synonyms["CUSTOMER_NAME"]) == 2


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_populate_semantic_view_columns_with_duplicates(mock_connection):
    """Test handling of duplicate columns (same column appearing as dimension and fact)."""
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "sales_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Sales view",
        },
    ]

    # Mock dimension
    mock_dimensions_cursor = MagicMock()
    mock_dimensions_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "sales_view",
            "NAME": "amount",
            "DATA_TYPE": "NUMBER",
            "COMMENT": "Amount as dimension",
            "TABLE_NAME": "sales",
            "SYNONYMS": None,
            "EXPRESSION": None,
        },
    ]

    # Mock fact with same name
    mock_facts_cursor = MagicMock()
    mock_facts_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "sales_view",
            "NAME": "amount",
            "DATA_TYPE": "NUMBER",
            "COMMENT": "Amount as fact",
            "TABLE_NAME": "orders",
            "SYNONYMS": None,
            "EXPRESSION": None,
        },
    ]

    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []
    mock_empty_cursor.fetchone.return_value = None

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "semantic_dimensions" in query_lower:
            return mock_dimensions_cursor
        elif "semantic_facts" in query_lower:
            return mock_facts_cursor
        elif (
            "semantic_metrics" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
            or "get_ddl" in query_lower
        ):
            return mock_empty_cursor
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
    semantic_view = semantic_views["PUBLIC"][0]

    # Should have only 1 deduplicated column
    assert len(semantic_view.columns) == 1
    assert semantic_view.columns[0].name == "amount"

    # Should have merged subtype
    assert semantic_view.column_subtypes["AMOUNT"] == "DIMENSION,FACT"

    # Should have both table mappings
    assert len(semantic_view.column_table_mappings["AMOUNT"]) == 2
    assert "sales" in semantic_view.column_table_mappings["AMOUNT"]
    assert "orders" in semantic_view.column_table_mappings["AMOUNT"]


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_semantic_view_with_metrics(mock_connection):
    """Test semantic view with metric columns."""
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "metrics_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Metrics view",
        },
    ]

    # Mock metrics
    mock_metrics_cursor = MagicMock()
    mock_metrics_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "metrics_view",
            "NAME": "total_revenue",
            "DATA_TYPE": "NUMBER",
            "COMMENT": "Total revenue metric",
            "TABLE_NAME": None,
            "SYNONYMS": '["revenue", "sales_total"]',
            "EXPRESSION": "SUM(amount)",
        },
    ]

    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []
    mock_empty_cursor.fetchone.return_value = None

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "semantic_metrics" in query_lower:
            return mock_metrics_cursor
        elif (
            "semantic_dimensions" in query_lower
            or "semantic_facts" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
            or "get_ddl" in query_lower
        ):
            return mock_empty_cursor
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
    semantic_view = semantic_views["PUBLIC"][0]

    # Verify metric column
    assert len(semantic_view.columns) == 1
    assert semantic_view.columns[0].name == "total_revenue"
    assert semantic_view.columns[0].expression == "SUM(amount)"

    # Verify subtype
    assert semantic_view.column_subtypes["TOTAL_REVENUE"] == "METRIC"

    # Verify synonyms
    assert len(semantic_view.column_synonyms["TOTAL_REVENUE"]) == 2


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_orphaned_columns_warning(mock_connection):
    """Test that orphaned columns (columns without corresponding semantic view) trigger warnings."""
    # Mock semantic views - only one view (INFORMATION_SCHEMA format)
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "existing_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Existing view",
        },
    ]

    # Mock dimensions - includes columns for a non-existent view
    mock_dimensions_cursor = MagicMock()
    mock_dimensions_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "existing_view",
            "NAME": "valid_column",
            "DATA_TYPE": "VARCHAR",
            "COMMENT": "Valid column",
            "TABLE_NAME": "table1",
            "SYNONYMS": None,
            "EXPRESSION": None,
        },
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "missing_view",  # This view doesn't exist!
            "NAME": "orphaned_column",
            "DATA_TYPE": "VARCHAR",
            "COMMENT": "Orphaned column",
            "TABLE_NAME": "table2",
            "SYNONYMS": None,
            "EXPRESSION": None,
        },
    ]

    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []
    mock_empty_cursor.fetchone.return_value = None

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "semantic_dimensions" in query_lower:
            return mock_dimensions_cursor
        elif (
            "semantic_facts" in query_lower
            or "semantic_metrics" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
            or "get_ddl" in query_lower
        ):
            return mock_empty_cursor
        else:
            return mock_semantic_views_cursor

    mock_connection_instance.query.side_effect = query_side_effect

    report = SnowflakeV2Report()
    data_dict = SnowflakeDataDictionary(
        connection=mock_connection_instance,
        report=report,
    )

    # Should log warning but not crash
    with patch(
        "datahub.ingestion.source.snowflake.snowflake_schema.logger"
    ) as mock_logger:
        semantic_views = data_dict.get_semantic_views_for_database("TEST_DB")

        # Verify warning was logged for orphaned columns
        warning_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "missing_view" in str(call)
        ]
        assert len(warning_calls) > 0, "Expected warning for orphaned columns"

    # Verify only valid view has columns
    assert semantic_views is not None
    assert "PUBLIC" in semantic_views
    assert len(semantic_views["PUBLIC"]) == 1
    assert semantic_views["PUBLIC"][0].name == "existing_view"
    assert len(semantic_views["PUBLIC"][0].columns) == 1
    assert semantic_views["PUBLIC"][0].columns[0].name == "valid_column"


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_synonym_case_insensitive_deduplication(mock_connection):
    """Test that synonyms are deduplicated case-insensitively."""
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Test view",
        },
    ]

    # Mock dimension appearing twice with synonyms in different cases
    mock_dimensions_cursor = MagicMock()
    mock_dimensions_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_view",
            "NAME": "customer_id",
            "DATA_TYPE": "NUMBER",
            "COMMENT": "Customer ID as dimension",
            "TABLE_NAME": "customers",
            "SYNONYMS": '["client_id", "BUYER_ID", "Customer_Number"]',
            "EXPRESSION": None,
        },
    ]

    # Mock fact with same column and overlapping synonyms in different cases
    mock_facts_cursor = MagicMock()
    mock_facts_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_view",
            "NAME": "customer_id",
            "DATA_TYPE": "NUMBER",
            "COMMENT": "Customer ID as fact",
            "TABLE_NAME": "orders",
            "SYNONYMS": '["CLIENT_ID", "buyer_id", "cust_id"]',  # CLIENT_ID and buyer_id are duplicates (different case)
            "EXPRESSION": None,
        },
    ]

    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []
    mock_empty_cursor.fetchone.return_value = None

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "semantic_dimensions" in query_lower:
            return mock_dimensions_cursor
        elif "semantic_facts" in query_lower:
            return mock_facts_cursor
        elif (
            "semantic_metrics" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
            or "get_ddl" in query_lower
        ):
            return mock_empty_cursor
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
    semantic_view = semantic_views["PUBLIC"][0]

    # Verify synonyms were deduplicated case-insensitively
    synonyms = semantic_view.column_synonyms["CUSTOMER_ID"]
    # Should have: client_id, BUYER_ID, Customer_Number, cust_id (4 unique case-insensitive)
    # Not 6 (which would be without deduplication)
    assert len(synonyms) == 4, (
        f"Expected 4 deduplicated synonyms, got {len(synonyms)}: {synonyms}"
    )

    # Verify all expected synonyms are present (case-insensitive check)
    synonyms_upper = [s.upper() for s in synonyms]
    assert "CLIENT_ID" in synonyms_upper
    assert "BUYER_ID" in synonyms_upper
    assert "CUSTOMER_NUMBER" in synonyms_upper
    assert "CUST_ID" in synonyms_upper


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_semantic_views_query_failure_returns_none(mock_connection):
    """Test that query failure returns None for graceful error handling."""
    mock_connection_instance = MagicMock()
    mock_connection_instance.query.side_effect = Exception("Query failed")

    report = SnowflakeV2Report()
    data_dict = SnowflakeDataDictionary(
        connection=mock_connection_instance,
        report=report,
    )

    result = data_dict.get_semantic_views_for_database("TEST_DB")

    assert result is None, "Expected None when query fails"


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_ddl_fetch_success(mock_connection):
    """Test successful DDL fetching via GET_DDL."""
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Test view",
        },
    ]

    # Mock successful DDL fetch
    mock_ddl_cursor = MagicMock()
    mock_ddl_cursor.fetchone.return_value = {
        "DDL": "CREATE SEMANTIC VIEW test_view AS SELECT * FROM base_table"
    }

    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "get_ddl" in query_lower:
            return mock_ddl_cursor
        elif (
            "semantic_dimensions" in query_lower
            or "semantic_facts" in query_lower
            or "semantic_metrics" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
        ):
            return mock_empty_cursor
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
    semantic_view = semantic_views["PUBLIC"][0]
    assert semantic_view.view_definition is not None
    assert "CREATE SEMANTIC VIEW" in semantic_view.view_definition


@patch("datahub.ingestion.source.snowflake.snowflake_schema.SnowflakeConnection")
def test_ddl_fetch_failure(mock_connection):
    """Test graceful handling when DDL fetch fails."""
    mock_semantic_views_cursor = MagicMock()
    mock_semantic_views_cursor.__iter__.return_value = [
        {
            "SEMANTIC_VIEW_CATALOG": "TEST_DB",
            "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
            "SEMANTIC_VIEW_NAME": "test_view",
            "CREATED": datetime.datetime.now(),
            "COMMENT": "Test view",
        },
    ]

    # Mock DDL fetch that returns None
    mock_ddl_cursor = MagicMock()
    mock_ddl_cursor.fetchone.return_value = None

    mock_empty_cursor = MagicMock()
    mock_empty_cursor.__iter__.return_value = []

    mock_connection_instance = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        if "get_ddl" in query_lower:
            return mock_ddl_cursor
        elif (
            "semantic_dimensions" in query_lower
            or "semantic_facts" in query_lower
            or "semantic_metrics" in query_lower
            or "semantic_tables" in query_lower
            or "semantic_relationships" in query_lower
        ):
            return mock_empty_cursor
        else:
            return mock_semantic_views_cursor

    mock_connection_instance.query.side_effect = query_side_effect

    report = SnowflakeV2Report()
    data_dict = SnowflakeDataDictionary(
        connection=mock_connection_instance,
        report=report,
    )

    # Should log warning but not crash
    with patch(
        "datahub.ingestion.source.snowflake.snowflake_schema.logger"
    ) as mock_logger:
        semantic_views = data_dict.get_semantic_views_for_database("TEST_DB")

        # Verify warning was logged
        warning_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "get_ddl" in str(call).lower() or "ddl" in str(call).lower()
        ]
        assert len(warning_calls) > 0, "Expected warning for missing DDL"

    assert semantic_views is not None
    semantic_view = semantic_views["PUBLIC"][0]
    # view_definition should remain None
    assert semantic_view.view_definition is None
