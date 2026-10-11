"""
Comprehensive test suite for SQL expression parsing in Snowflake semantic views.

This file contains extensive tests for the expression parsing logic used to extract
column references from SQL expressions in semantic view metrics and facts.
"""

from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)


@pytest.fixture
def schema_gen():
    """Create a SnowflakeSchemaGenerator instance for testing."""
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
        }
    )
    report = SnowflakeV2Report()
    connection = MagicMock()
    filters = MagicMock()
    identifiers = MagicMock()
    identifiers.gen_dataset_urn.return_value = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
    )
    identifiers.get_dataset_identifier.return_value = "test.schema.table"
    domain_registry = None
    profiler = None
    aggregator = MagicMock()
    aggregator._schema_resolver = MagicMock()
    aggregator._schema_resolver._resolve_schema_info.return_value = None
    snowsight_url_builder = None

    gen = SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=connection,
        filters=filters,
        identifiers=identifiers,
        domain_registry=domain_registry,
        profiler=profiler,
        aggregator=aggregator,
        snowsight_url_builder=snowsight_url_builder,
    )
    return gen


class TestBasicColumnExtraction:
    """Test basic column reference extraction."""

    def test_simple_column(self, schema_gen):
        """Test extracting a simple column name."""
        expression = "AMOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "AMOUNT") in columns

    def test_qualified_column(self, schema_gen):
        """Test extracting table-qualified column."""
        expression = "ORDERS.AMOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert ("ORDERS", "AMOUNT") in columns

    def test_multiple_columns(self, schema_gen):
        """Test extracting multiple columns."""
        expression = "AMOUNT + QUANTITY - DISCOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 3
        assert (None, "AMOUNT") in columns
        assert (None, "QUANTITY") in columns
        assert (None, "DISCOUNT") in columns

    def test_mixed_qualified_unqualified(self, schema_gen):
        """Test mix of qualified and unqualified columns."""
        expression = "ORDERS.TOTAL + DISCOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert ("ORDERS", "TOTAL") in columns
        assert (None, "DISCOUNT") in columns


class TestAggregationFunctions:
    """Test column extraction from aggregation functions."""

    def test_sum(self, schema_gen):
        """Test SUM aggregation."""
        expression = "SUM(AMOUNT)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "AMOUNT") in columns

    def test_count(self, schema_gen):
        """Test COUNT aggregation."""
        expression = "COUNT(CUSTOMER_ID)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "CUSTOMER_ID") in columns

    def test_count_distinct(self, schema_gen):
        """Test COUNT DISTINCT."""
        expression = "COUNT(DISTINCT CUSTOMER_ID)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "CUSTOMER_ID") in columns

    def test_avg(self, schema_gen):
        """Test AVG aggregation."""
        expression = "AVG(ORDERS.PRICE)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert ("ORDERS", "PRICE") in columns

    def test_min_max(self, schema_gen):
        """Test MIN and MAX aggregations."""
        expression = "MIN(START_DATE) + MAX(END_DATE)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "START_DATE") in columns
        assert (None, "END_DATE") in columns

    def test_multiple_aggregations(self, schema_gen):
        """Test multiple different aggregations."""
        expression = "SUM(AMOUNT) / COUNT(DISTINCT CUSTOMER_ID)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "AMOUNT") in columns
        assert (None, "CUSTOMER_ID") in columns


class TestComplexExpressions:
    """Test extraction from complex SQL expressions."""

    def test_case_when_simple(self, schema_gen):
        """Test simple CASE WHEN expression."""
        expression = "CASE WHEN STATUS = 'ACTIVE' THEN 1 ELSE 0 END"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "STATUS") in columns

    def test_case_when_multiple_conditions(self, schema_gen):
        """Test CASE WHEN with multiple conditions."""
        expression = """
            CASE 
                WHEN ORDERS.STATUS = 'COMPLETED' THEN ORDERS.TOTAL
                WHEN ORDERS.STATUS = 'PENDING' THEN ORDERS.SUBTOTAL
                ELSE 0 
            END
        """
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 3
        assert ("ORDERS", "STATUS") in columns
        assert ("ORDERS", "TOTAL") in columns
        assert ("ORDERS", "SUBTOTAL") in columns

    def test_nested_functions(self, schema_gen):
        """Test nested function calls."""
        expression = "COALESCE(SUM(AMOUNT), 0)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "AMOUNT") in columns

    def test_deeply_nested_functions(self, schema_gen):
        """Test deeply nested functions."""
        expression = "ROUND(COALESCE(AVG(NULLIF(ORDERS.AMOUNT, 0)), 0), 2)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert ("ORDERS", "AMOUNT") in columns

    def test_arithmetic_operations(self, schema_gen):
        """Test arithmetic operations."""
        expression = "(PRICE * QUANTITY) - DISCOUNT + TAX"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 4
        assert (None, "PRICE") in columns
        assert (None, "QUANTITY") in columns
        assert (None, "DISCOUNT") in columns
        assert (None, "TAX") in columns

    def test_comparison_operators(self, schema_gen):
        """Test comparison operators."""
        expression = "AMOUNT > THRESHOLD AND STATUS != 'CANCELLED'"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 3
        assert (None, "AMOUNT") in columns
        assert (None, "THRESHOLD") in columns
        assert (None, "STATUS") in columns


class TestStringFunctions:
    """Test extraction from string manipulation functions."""

    def test_concat(self, schema_gen):
        """Test CONCAT function."""
        expression = "CONCAT(FIRST_NAME, ' ', LAST_NAME)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "FIRST_NAME") in columns
        assert (None, "LAST_NAME") in columns

    def test_substring(self, schema_gen):
        """Test SUBSTRING function."""
        expression = "SUBSTRING(CUSTOMER_NAME, 1, 10)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "CUSTOMER_NAME") in columns

    def test_upper_lower(self, schema_gen):
        """Test UPPER and LOWER functions."""
        expression = "UPPER(EMAIL) || LOWER(USERNAME)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "EMAIL") in columns
        assert (None, "USERNAME") in columns


class TestDateTimeFunctions:
    """Test extraction from date/time functions."""

    def test_date_trunc(self, schema_gen):
        """Test DATE_TRUNC function."""
        expression = "DATE_TRUNC('MONTH', ORDER_DATE)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "ORDER_DATE") in columns

    def test_datediff(self, schema_gen):
        """Test DATEDIFF function."""
        expression = "DATEDIFF('DAY', START_DATE, END_DATE)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "START_DATE") in columns
        assert (None, "END_DATE") in columns

    def test_extract(self, schema_gen):
        """Test EXTRACT function."""
        expression = "EXTRACT(YEAR FROM CREATED_AT)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "CREATED_AT") in columns


class TestWindowFunctions:
    """Test extraction from window functions."""

    def test_row_number(self, schema_gen):
        """Test ROW_NUMBER window function."""
        expression = "ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY ORDER_DATE)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "CUSTOMER_ID") in columns
        assert (None, "ORDER_DATE") in columns

    def test_rank(self, schema_gen):
        """Test RANK window function."""
        expression = "RANK() OVER (PARTITION BY CATEGORY ORDER BY SALES DESC)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "CATEGORY") in columns
        assert (None, "SALES") in columns

    def test_lag_lead(self, schema_gen):
        """Test LAG and LEAD window functions."""
        expression = (
            "LAG(AMOUNT, 1) OVER (ORDER BY DATE) - LEAD(AMOUNT, 1) OVER (ORDER BY DATE)"
        )
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "AMOUNT") in columns
        assert (None, "DATE") in columns


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_expression(self, schema_gen):
        """Test empty expression."""
        assert schema_gen._extract_columns_from_expression("") == []
        assert schema_gen._extract_columns_from_expression(None) == []

    def test_whitespace_only(self, schema_gen):
        """Test whitespace-only expression."""
        columns = schema_gen._extract_columns_from_expression("   \n\t  ")
        assert columns == []

    def test_literals_only(self, schema_gen):
        """Test expression with only literals."""
        expression = "123 + 456 * 'test'"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert columns == []

    def test_duplicate_columns(self, schema_gen):
        """Test that duplicate columns are deduplicated."""
        expression = "AMOUNT + AMOUNT + AMOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 1
        assert (None, "AMOUNT") in columns

    def test_case_sensitivity(self, schema_gen):
        """Test that column names preserve case."""
        expression = "Amount + AMOUNT + amount"
        columns = schema_gen._extract_columns_from_expression(expression)
        # Should be deduplicated to uppercase
        assert len(columns) == 1

    def test_special_characters_in_names(self, schema_gen):
        """Test columns with special characters."""
        expression = "CUSTOMER_ID + ORDER_TOTAL_$"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2

    def test_very_long_expression(self, schema_gen):
        """Test very long expression with many columns."""
        cols = [f"COL{i}" for i in range(50)]
        expression = " + ".join(cols)
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 50

    def test_malformed_sql(self, schema_gen):
        """Test that malformed SQL doesn't crash."""
        expression = "SELECT ((( INVALID {{{{ ))))"
        columns = schema_gen._extract_columns_from_expression(expression)
        # Should return empty list or partial results, not crash
        assert isinstance(columns, list)


class TestRealWorldExamples:
    """Test real-world metric expressions."""

    def test_revenue_per_customer(self, schema_gen):
        """Test revenue per customer metric."""
        expression = "SUM(ORDERS.TOTAL) / COUNT(DISTINCT CUSTOMERS.CUSTOMER_ID)"
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert ("ORDERS", "TOTAL") in columns
        assert ("CUSTOMERS", "CUSTOMER_ID") in columns

    def test_conversion_rate(self, schema_gen):
        """Test conversion rate metric."""
        expression = """
            (COUNT(DISTINCT CASE WHEN STATUS = 'CONVERTED' THEN USER_ID END) * 100.0) /
            NULLIF(COUNT(DISTINCT USER_ID), 0)
        """
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert (None, "STATUS") in columns
        assert (None, "USER_ID") in columns

    def test_average_order_value(self, schema_gen):
        """Test average order value with filters."""
        expression = """
            SUM(CASE WHEN ORDERS.STATUS = 'COMPLETED' THEN ORDERS.TOTAL ELSE 0 END) /
            NULLIF(COUNT(DISTINCT CASE WHEN ORDERS.STATUS = 'COMPLETED' THEN ORDERS.ORDER_ID END), 0)
        """
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 3
        assert ("ORDERS", "STATUS") in columns
        assert ("ORDERS", "TOTAL") in columns
        assert ("ORDERS", "ORDER_ID") in columns

    def test_month_over_month_growth(self, schema_gen):
        """Test month-over-month growth calculation."""
        expression = """
            (SUM(CURRENT_MONTH.REVENUE) - SUM(PREVIOUS_MONTH.REVENUE)) * 100.0 /
            NULLIF(SUM(PREVIOUS_MONTH.REVENUE), 0)
        """
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 2
        assert ("CURRENT_MONTH", "REVENUE") in columns
        assert ("PREVIOUS_MONTH", "REVENUE") in columns

    def test_customer_lifetime_value(self, schema_gen):
        """Test customer lifetime value metric."""
        expression = """
            SUM(ORDERS.TOTAL) * 
            AVG(DATEDIFF('DAY', CUSTOMERS.FIRST_ORDER_DATE, CUSTOMERS.LAST_ORDER_DATE)) /
            365.0
        """
        columns = schema_gen._extract_columns_from_expression(expression)
        assert len(columns) == 3
        assert ("ORDERS", "TOTAL") in columns
        assert ("CUSTOMERS", "FIRST_ORDER_DATE") in columns
        assert ("CUSTOMERS", "LAST_ORDER_DATE") in columns
