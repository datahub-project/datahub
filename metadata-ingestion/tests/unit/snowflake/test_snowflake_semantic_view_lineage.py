import datetime
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeSemanticView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.utilities.urns.tag_urn import TagUrn


def create_mock_schema_gen():
    """Create a SnowflakeSchemaGenerator instance with mocked dependencies."""
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


class TestSnowflakeSemanticViewExpressionParsing:
    """Test suite for SQL expression parsing in semantic views."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        return create_mock_schema_gen()

    def test_extract_simple_column_reference(self, schema_gen):
        """Test extracting a simple column reference without table qualifier."""
        expression = "SUM(ORDER_TOTAL)"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 1
        assert columns[0] == (None, "ORDER_TOTAL")

    def test_extract_table_qualified_column(self, schema_gen):
        """Test extracting table-qualified column references."""
        expression = "ORDERS.ORDER_TOTAL + TRANSACTIONS.AMOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert ("ORDERS", "ORDER_TOTAL") in columns
        assert ("TRANSACTIONS", "AMOUNT") in columns

    def test_extract_complex_expression(self, schema_gen):
        """Test extracting columns from complex SQL expressions."""
        expression = "SUM(ORDERS.ORDER_TOTAL) / COUNT(DISTINCT CUSTOMERS.CUSTOMER_ID)"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert ("ORDERS", "ORDER_TOTAL") in columns
        assert ("CUSTOMERS", "CUSTOMER_ID") in columns

    def test_extract_case_when_expression(self, schema_gen):
        """Test extracting columns from CASE WHEN expressions."""
        expression = (
            "CASE WHEN ORDERS.STATUS = 'COMPLETED' THEN ORDERS.TOTAL ELSE 0 END"
        )
        columns = schema_gen._extract_columns_from_expression(expression)

        # Should extract STATUS and TOTAL from ORDERS
        assert len(columns) == 2
        assert ("ORDERS", "STATUS") in columns
        assert ("ORDERS", "TOTAL") in columns

    def test_extract_multiple_aggregations(self, schema_gen):
        """Test extracting columns from multiple aggregation functions."""
        expression = "SUM(AMOUNT) + AVG(QUANTITY) - MIN(PRICE)"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 3
        assert (None, "AMOUNT") in columns
        assert (None, "QUANTITY") in columns
        assert (None, "PRICE") in columns

    def test_extract_nested_functions(self, schema_gen):
        """Test extracting columns from nested function calls."""
        expression = "COALESCE(SUM(ORDERS.AMOUNT), 0) * ROUND(AVG(ORDERS.RATE), 2)"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert ("ORDERS", "AMOUNT") in columns
        assert ("ORDERS", "RATE") in columns

    def test_extract_empty_expression(self, schema_gen):
        """Test that empty expression returns empty list."""
        columns = schema_gen._extract_columns_from_expression("")
        assert columns == []

        columns = schema_gen._extract_columns_from_expression(None)
        assert columns == []

    def test_extract_invalid_expression(self, schema_gen):
        """Test that invalid expression returns empty list with warning."""
        # This should not raise an exception but return empty list
        columns = schema_gen._extract_columns_from_expression("INVALID SQL {{{{")
        assert columns == []

    def test_extract_duplicate_columns(self, schema_gen):
        """Test that duplicate column references are deduplicated."""
        expression = "ORDERS.AMOUNT + ORDERS.AMOUNT + ORDERS.TOTAL"
        columns = schema_gen._extract_columns_from_expression(expression)

        # AMOUNT should only appear once
        assert len(columns) == 2
        assert ("ORDERS", "AMOUNT") in columns
        assert ("ORDERS", "TOTAL") in columns

    def test_extract_mixed_qualifiers(self, schema_gen):
        """Test extracting columns with mixed qualified and unqualified references."""
        expression = "ORDERS.ORDER_TOTAL + AMOUNT"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert ("ORDERS", "ORDER_TOTAL") in columns
        assert (None, "AMOUNT") in columns


class TestSnowflakeSemanticViewTags:
    """Test suite for building tags for semantic view columns."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        return create_mock_schema_gen()

    def test_build_tags_dimension(self, schema_gen):
        """Test building tags for a DIMENSION column."""
        column_subtypes = {"CUSTOMER_ID": "DIMENSION"}
        tags = schema_gen._build_semantic_view_tags(
            "CUSTOMER_ID", column_subtypes, None
        )

        assert tags is not None
        assert len(tags.tags) == 1
        assert tags.tags[0].tag == str(TagUrn("DIMENSION"))

    def test_build_tags_fact(self, schema_gen):
        """Test building tags for a FACT column."""
        column_subtypes = {"ORDER_TOTAL": "FACT"}
        tags = schema_gen._build_semantic_view_tags(
            "ORDER_TOTAL", column_subtypes, None
        )

        assert tags is not None
        assert len(tags.tags) == 1
        assert tags.tags[0].tag == str(TagUrn("FACT"))

    def test_build_tags_metric(self, schema_gen):
        """Test building tags for a METRIC column."""
        column_subtypes = {"REVENUE": "METRIC"}
        tags = schema_gen._build_semantic_view_tags("REVENUE", column_subtypes, None)

        assert tags is not None
        assert len(tags.tags) == 1
        assert tags.tags[0].tag == str(TagUrn("METRIC"))

    def test_build_tags_multiple_subtypes(self, schema_gen):
        """Test building tags for a column with multiple subtypes (DIMENSION and FACT)."""
        column_subtypes = {"ORDER_ID": "DIMENSION,FACT"}
        tags = schema_gen._build_semantic_view_tags("ORDER_ID", column_subtypes, None)

        assert tags is not None
        assert len(tags.tags) == 2
        tag_names = [tag.tag for tag in tags.tags]
        assert str(TagUrn("DIMENSION")) in tag_names
        assert str(TagUrn("FACT")) in tag_names

    def test_build_tags_with_existing_snowflake_tags(self, schema_gen):
        """Test that existing Snowflake tags are preserved when adding subtype tags."""
        existing_tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag=str(TagUrn("PII")))]
        )
        column_subtypes = {"CUSTOMER_ID": "DIMENSION"}
        tags = schema_gen._build_semantic_view_tags(
            "CUSTOMER_ID", column_subtypes, existing_tags
        )

        assert tags is not None
        assert len(tags.tags) == 2
        tag_names = [tag.tag for tag in tags.tags]
        assert str(TagUrn("PII")) in tag_names
        assert str(TagUrn("DIMENSION")) in tag_names

    def test_build_tags_no_subtype(self, schema_gen):
        """Test building tags for a column with no subtype."""
        column_subtypes = {}
        tags = schema_gen._build_semantic_view_tags("RANDOM_COL", column_subtypes, None)

        assert tags is None

    def test_build_tags_with_whitespace(self, schema_gen):
        """Test building tags with whitespace in subtype string."""
        column_subtypes = {"ORDER_ID": "DIMENSION , FACT , METRIC"}
        tags = schema_gen._build_semantic_view_tags("ORDER_ID", column_subtypes, None)

        assert tags is not None
        assert len(tags.tags) == 3
        tag_names = [tag.tag for tag in tags.tags]
        assert str(TagUrn("DIMENSION")) in tag_names
        assert str(TagUrn("FACT")) in tag_names
        assert str(TagUrn("METRIC")) in tag_names


class TestSnowflakeSemanticViewRecursiveResolution:
    """Test suite for recursive resolution of chained derived metrics."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        return create_mock_schema_gen()

    def test_extract_columns_from_derived_metric(self, schema_gen):
        """Test extracting columns from a derived metric that references other metrics."""
        expression = "ORDERS.ORDER_TOTAL_METRIC + TRANSACTIONS.AMOUNT_METRIC"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert ("ORDERS", "ORDER_TOTAL_METRIC") in columns
        assert ("TRANSACTIONS", "AMOUNT_METRIC") in columns

    def test_recursive_chained_metrics_structure(self, schema_gen):
        """Test that chained metrics are structured correctly for resolution.

        This tests the scenario:
        - METRIC_A = SUM(physical_column)
        - METRIC_B = METRIC_A * 2
        - METRIC_C = METRIC_B + METRIC_A

        Verify the semantic view structure is set up correctly.
        """
        columns = [
            SnowflakeColumn(
                name="METRIC_A",
                ordinal_position=1,
                is_nullable=False,
                data_type="NUMBER",
                comment="Base metric",
                expression="SUM(ORDER_TOTAL)",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
            SnowflakeColumn(
                name="METRIC_B",
                ordinal_position=2,
                is_nullable=False,
                data_type="NUMBER",
                comment="Derived from METRIC_A",
                expression="ORDERS.METRIC_A * 2",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
            SnowflakeColumn(
                name="METRIC_C",
                ordinal_position=3,
                is_nullable=False,
                data_type="NUMBER",
                comment="Derived from METRIC_B and METRIC_A",
                expression="ORDERS.METRIC_B + ORDERS.METRIC_A",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
        ]

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test semantic view with chained metrics",
            view_definition="yaml: definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="yaml: definition",
            columns=columns,
            logical_to_physical_table={
                "ORDERS": ("TEST_DB", "PUBLIC", "ORDERS"),
            },
        )

        # Verify that the columns and expressions are set up correctly
        assert semantic_view.columns[0].expression == "SUM(ORDER_TOTAL)"
        assert semantic_view.columns[1].expression == "ORDERS.METRIC_A * 2"
        assert (
            semantic_view.columns[2].expression == "ORDERS.METRIC_B + ORDERS.METRIC_A"
        )

        # Verify logical to physical mapping
        assert semantic_view.logical_to_physical_table["ORDERS"] == (
            "TEST_DB",
            "PUBLIC",
            "ORDERS",
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
