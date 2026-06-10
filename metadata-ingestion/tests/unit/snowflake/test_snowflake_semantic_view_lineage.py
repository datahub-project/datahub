import datetime
from typing import Dict
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeColumn,
    SnowflakeDataDictionary,
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
        column_subtypes: Dict[str, str] = {}
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


class TestSnowflakeSemanticViewColumnMerging:
    """Test suite for column metadata merging when same column appears in multiple contexts."""

    @pytest.fixture
    def data_dict(self):
        """Create a SnowflakeDataDictionary instance for testing."""
        connection = MagicMock()
        report = MagicMock()
        return SnowflakeDataDictionary(connection=connection, report=report)

    def test_merge_dimension_and_fact_with_different_descriptions(self, data_dict):
        """Test merging when same column is both DIMENSION and FACT with different descriptions."""
        occurrences = [
            SemanticViewColumnMetadata(
                name="ORDER_ID",
                data_type="NUMBER",
                comment="Order identifier for lookups",
                subtype=SemanticViewColumnSubtype.DIMENSION,
                table_name=None,
                synonyms=[],
                expression=None,
            ),
            SemanticViewColumnMetadata(
                name="ORDER_ID",
                data_type="NUMBER",
                comment="Order ID used in aggregations",
                subtype=SemanticViewColumnSubtype.FACT,
                table_name=None,
                synonyms=[],
                expression="ORDER_ID",
            ),
        ]

        data_type, merged_comment, merged_subtype = data_dict._merge_column_metadata(
            occurrences, "ORDER_ID", "TEST_VIEW"
        )

        assert data_type == "NUMBER"
        assert merged_subtype == "DIMENSION,FACT"
        # Both descriptions should appear
        assert "DIMENSION:" in merged_comment
        assert "FACT:" in merged_comment
        # FACT should include expression
        assert "[Expression: ORDER_ID]" in merged_comment

    def test_merge_single_metric_with_expression(self, data_dict):
        """Test that single METRIC occurrence includes expression in comment."""
        occurrences = [
            SemanticViewColumnMetadata(
                name="TOTAL_REVENUE",
                data_type="NUMBER",
                comment="Total revenue calculation",
                subtype=SemanticViewColumnSubtype.METRIC,
                table_name=None,
                synonyms=[],
                expression="SUM(ORDERS.ORDER_TOTAL)",
            ),
        ]

        data_type, merged_comment, merged_subtype = data_dict._merge_column_metadata(
            occurrences, "TOTAL_REVENUE", "TEST_VIEW"
        )

        assert data_type == "NUMBER"
        assert merged_subtype == "METRIC"
        assert "Total revenue calculation" in merged_comment
        assert "[Expression: SUM(ORDERS.ORDER_TOTAL)]" in merged_comment

    def test_merge_conflicting_data_types(self, data_dict):
        """Test merging when same column has conflicting data types (uses first)."""
        occurrences = [
            SemanticViewColumnMetadata(
                name="VALUE",
                data_type="NUMBER",
                comment="Numeric value",
                subtype=SemanticViewColumnSubtype.FACT,
                table_name=None,
                synonyms=[],
                expression=None,
            ),
            SemanticViewColumnMetadata(
                name="VALUE",
                data_type="VARCHAR",
                comment="String value",
                subtype=SemanticViewColumnSubtype.DIMENSION,
                table_name=None,
                synonyms=[],
                expression=None,
            ),
        ]

        data_type, _, merged_subtype = data_dict._merge_column_metadata(
            occurrences, "VALUE", "TEST_VIEW"
        )

        # Should use first type
        assert data_type == "NUMBER"
        assert merged_subtype == "DIMENSION,FACT"


class TestSnowflakeSemanticViewDerivationResolution:
    """Test suite for recursive derivation resolution with depth limits and cycle detection."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator with mocked column verification."""
        gen = create_mock_schema_gen()
        # Mock _verify_column_exists_in_table to return False (forces recursion)
        gen._verify_column_exists_in_table = MagicMock(return_value=False)
        return gen

    def test_max_depth_limit_stops_recursion(self, schema_gen):
        """Test that recursion stops when max depth is reached."""
        # Create a semantic view with deeply nested derived columns
        columns = []
        for i in range(10):
            columns.append(
                SnowflakeColumn(
                    name=f"METRIC_{i}",
                    ordinal_position=i,
                    is_nullable=False,
                    data_type="NUMBER",
                    comment=f"Metric {i}",
                    expression=f"ORDERS.METRIC_{i + 1} * 2"
                    if i < 9
                    else "SUM(BASE_COL)",
                    character_maximum_length=None,
                    numeric_precision=38,
                    numeric_scale=2,
                )
            )

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="def",
            last_altered=datetime.datetime.now(),
            columns=columns,
            logical_to_physical_table={"ORDERS": ("DB", "SCHEMA", "ORDERS")},
        )

        # Start resolution from depth 0
        # Should stop before reaching the bottom due to max depth (default 5)
        result = schema_gen._resolve_derived_column_sources(
            "ORDERS.METRIC_0 + 1", semantic_view
        )

        # Result should be empty or limited due to depth
        # The exact behavior depends on max depth, but it should not infinite loop
        assert isinstance(result, list)

    def test_cycle_detection_prevents_infinite_loop(self, schema_gen):
        """Test that cycles in derived columns are detected and broken."""
        # Create circular dependency: A -> B -> C -> A
        columns = [
            SnowflakeColumn(
                name="METRIC_A",
                ordinal_position=1,
                is_nullable=False,
                data_type="NUMBER",
                comment="Metric A",
                expression="ORDERS.METRIC_B + 1",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
            SnowflakeColumn(
                name="METRIC_B",
                ordinal_position=2,
                is_nullable=False,
                data_type="NUMBER",
                comment="Metric B",
                expression="ORDERS.METRIC_C * 2",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
            SnowflakeColumn(
                name="METRIC_C",
                ordinal_position=3,
                is_nullable=False,
                data_type="NUMBER",
                comment="Metric C - creates cycle",
                expression="ORDERS.METRIC_A / 3",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
        ]

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test with cycle",
            view_definition="def",
            last_altered=datetime.datetime.now(),
            columns=columns,
            logical_to_physical_table={"ORDERS": ("DB", "SCHEMA", "ORDERS")},
        )

        # Should not infinite loop - cycle detection should kick in
        result = schema_gen._resolve_derived_column_sources(
            "ORDERS.METRIC_A", semantic_view
        )

        # Result should be empty (no physical sources found due to cycle)
        assert isinstance(result, list)
        # Should complete without hanging
