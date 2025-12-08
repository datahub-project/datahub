import datetime
from typing import Dict, List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeSemanticView,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.utilities.urns.tag_urn import TagUrn


class TestSnowflakeSemanticViewExpressionParsing:
    """Test suite for SQL expression parsing in semantic views."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "test_user",
                "password": "test_password",
            }
        )
        report = SnowflakeV2Report()
        ctx = MagicMock(spec=PipelineContext)
        return SnowflakeSchemaGenerator(config, report, ctx)

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
        expression = "CASE WHEN STATUS = 'COMPLETED' THEN AMOUNT ELSE 0 END"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert (None, "STATUS") in columns
        assert (None, "AMOUNT") in columns

    def test_extract_multiple_aggregations(self, schema_gen):
        """Test extracting columns from multiple aggregation functions."""
        expression = "AVG(PRICE) * SUM(QUANTITY) + MAX(DISCOUNT)"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 3
        assert (None, "PRICE") in columns
        assert (None, "QUANTITY") in columns
        assert (None, "DISCOUNT") in columns

    def test_extract_nested_functions(self, schema_gen):
        """Test extracting columns from nested function calls."""
        expression = "ROUND(AVG(ORDERS.TOTAL_AMOUNT), 2)"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 1
        assert columns[0] == ("ORDERS", "TOTAL_AMOUNT")

    def test_extract_empty_expression(self, schema_gen):
        """Test handling empty or None expressions."""
        assert schema_gen._extract_columns_from_expression("") == []
        assert schema_gen._extract_columns_from_expression(None) == []

    def test_extract_invalid_expression(self, schema_gen):
        """Test handling invalid SQL expressions."""
        expression = "INVALID SQL ((("
        columns = schema_gen._extract_columns_from_expression(expression)
        # Should return empty list on parse error, not crash
        assert columns == []

    def test_extract_duplicate_columns(self, schema_gen):
        """Test that duplicate column references are deduplicated."""
        expression = "AMOUNT + AMOUNT * 2"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 1
        assert columns[0] == (None, "AMOUNT")

    def test_extract_mixed_qualifiers(self, schema_gen):
        """Test extracting columns with mixed qualified and unqualified references."""
        expression = "ORDERS.ORDER_ID + AMOUNT - TAX"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 3
        assert ("ORDERS", "ORDER_ID") in columns
        assert (None, "AMOUNT") in columns
        assert (None, "TAX") in columns


class TestSnowflakeSemanticViewTags:
    """Test suite for semantic view column tagging."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "test_user",
                "password": "test_password",
            }
        )
        report = SnowflakeV2Report()
        ctx = MagicMock(spec=PipelineContext)
        return SnowflakeSchemaGenerator(config, report, ctx)

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
        column_subtypes = {"REVENUE_SUM": "METRIC"}
        tags = schema_gen._build_semantic_view_tags(
            "REVENUE_SUM", column_subtypes, None
        )

        assert tags is not None
        assert len(tags.tags) == 1
        assert tags.tags[0].tag == str(TagUrn("METRIC"))

    def test_build_tags_multiple_subtypes(self, schema_gen):
        """Test building tags for a column with multiple subtypes."""
        column_subtypes = {"ORDER_ID": "DIMENSION,FACT"}
        tags = schema_gen._build_semantic_view_tags("ORDER_ID", column_subtypes, None)

        assert tags is not None
        assert len(tags.tags) == 2
        tag_names = [tag.tag for tag in tags.tags]
        assert str(TagUrn("DIMENSION")) in tag_names
        assert str(TagUrn("FACT")) in tag_names

    def test_build_tags_with_existing_snowflake_tags(self, schema_gen):
        """Test building tags when Snowflake object tags already exist."""
        column_subtypes = {"CUSTOMER_ID": "DIMENSION"}
        existing_tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag=str(TagUrn("PII")))]
        )

        tags = schema_gen._build_semantic_view_tags(
            "CUSTOMER_ID", column_subtypes, existing_tags
        )

        assert tags is not None
        assert len(tags.tags) == 2
        tag_names = [tag.tag for tag in tags.tags]
        assert str(TagUrn("PII")) in tag_names
        assert str(TagUrn("DIMENSION")) in tag_names

    def test_build_tags_no_subtype(self, schema_gen):
        """Test building tags when column has no subtype."""
        column_subtypes = {}
        tags = schema_gen._build_semantic_view_tags(
            "UNKNOWN_COL", column_subtypes, None
        )

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
    """Test suite for merging column metadata with expressions."""

    def test_merge_single_dimension(self):
        """Test merging a single DIMENSION column."""
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            _merge_column_metadata,
        )

        occurrences = [
            {
                "subtype": "DIMENSION",
                "comment": "Customer ID for filtering",
                "expression": None,
            }
        ]

        name, comment, subtypes = _merge_column_metadata("CUSTOMER_ID", occurrences)

        assert name == "CUSTOMER_ID"
        assert comment == "• DIMENSION: Customer ID for filtering"
        assert subtypes == ["DIMENSION"]

    def test_merge_fact_with_expression(self):
        """Test merging a FACT column with expression."""
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            _merge_column_metadata,
        )

        occurrences = [
            {
                "subtype": "FACT",
                "comment": "Order total amount",
                "expression": "ORDER_TOTAL",
            }
        ]

        name, comment, subtypes = _merge_column_metadata(
            "ORDER_TOTAL_FACT", occurrences
        )

        assert name == "ORDER_TOTAL_FACT"
        assert "• FACT: Order total amount [Expression: ORDER_TOTAL]" in comment
        assert subtypes == ["FACT"]

    def test_merge_metric_with_expression(self):
        """Test merging a METRIC column with expression."""
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            _merge_column_metadata,
        )

        occurrences = [
            {
                "subtype": "METRIC",
                "comment": "Total revenue calculation",
                "expression": "SUM(ORDER_TOTAL)",
            }
        ]

        name, comment, subtypes = _merge_column_metadata("REVENUE_METRIC", occurrences)

        assert name == "REVENUE_METRIC"
        assert (
            "• METRIC: Total revenue calculation [Expression: SUM(ORDER_TOTAL)]"
            in comment
        )
        assert subtypes == ["METRIC"]

    def test_merge_multiple_subtypes(self):
        """Test merging a column that appears as both DIMENSION and FACT."""
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            _merge_column_metadata,
        )

        occurrences = [
            {
                "subtype": "DIMENSION",
                "comment": "Order identifier for filtering",
                "expression": None,
            },
            {
                "subtype": "FACT",
                "comment": "Order count metric",
                "expression": "ORDER_ID",
            },
        ]

        name, comment, subtypes = _merge_column_metadata("ORDER_ID", occurrences)

        assert name == "ORDER_ID"
        assert "• DIMENSION: Order identifier for filtering" in comment
        assert "• FACT: Order count metric [Expression: ORDER_ID]" in comment
        assert set(subtypes) == {"DIMENSION", "FACT"}

    def test_merge_without_comments(self):
        """Test merging columns without comments."""
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            _merge_column_metadata,
        )

        occurrences = [
            {
                "subtype": "DIMENSION",
                "comment": None,
                "expression": None,
            }
        ]

        name, comment, subtypes = _merge_column_metadata("CUSTOMER_ID", occurrences)

        assert name == "CUSTOMER_ID"
        assert "• DIMENSION: (no description)" in comment
        assert subtypes == ["DIMENSION"]

    def test_merge_dimension_without_expression(self):
        """Test that DIMENSION columns don't show expression even if present."""
        from datahub.ingestion.source.snowflake.snowflake_schema import (
            _merge_column_metadata,
        )

        occurrences = [
            {
                "subtype": "DIMENSION",
                "comment": "Customer ID",
                "expression": "CUSTOMER_ID",
            }
        ]

        name, comment, subtypes = _merge_column_metadata("CUSTOMER_ID", occurrences)

        assert name == "CUSTOMER_ID"
        assert comment == "• DIMENSION: Customer ID"
        assert "[Expression:" not in comment
        assert subtypes == ["DIMENSION"]


class TestSnowflakeSemanticViewLineageGeneration:
    """Test suite for column lineage generation in semantic views."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "test_user",
                "password": "test_password",
            }
        )
        report = SnowflakeV2Report()
        ctx = MagicMock(spec=PipelineContext)
        gen = SnowflakeSchemaGenerator(config, report, ctx)
        # Mock the schema_resolver
        gen.schema_resolver = MagicMock()
        return gen

    def _create_test_table(self, name: str, columns: List[str]) -> SnowflakeTable:
        """Helper to create a test table with columns."""
        return SnowflakeTable(
            name=name,
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            columns=[
                SnowflakeColumn(
                    name=col_name,
                    ordinal_position=i + 1,
                    is_nullable=True,
                    data_type="VARCHAR",
                    comment=None,
                    character_maximum_length=255,
                    numeric_precision=None,
                    numeric_scale=None,
                )
                for i, col_name in enumerate(columns)
            ],
        )

    def _create_test_semantic_view(
        self,
        name: str,
        columns: List[SnowflakeColumn],
        logical_to_physical: Dict[str, tuple],
    ) -> SnowflakeSemanticView:
        """Helper to create a test semantic view."""
        return SnowflakeSemanticView(
            name=name,
            created=datetime.datetime.now(),
            comment="Test semantic view",
            view_definition="yaml: definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="yaml: definition",
            columns=columns,
            logical_to_physical_table=logical_to_physical,
        )

    def test_verify_column_exists_simple(self, schema_gen):
        """Test verifying a column exists in a physical table."""
        # Create a mock physical table
        physical_table = self._create_test_table(
            "ORDERS", ["ORDER_ID", "ORDER_TOTAL", "CUSTOMER_ID"]
        )

        # Store it in the schema
        schema_gen.db_tables = {("TEST_DB", "PUBLIC"): {"ORDERS": physical_table}}

        # Test verification
        result = schema_gen._verify_column_exists_in_table(
            "TEST_DB", "PUBLIC", "ORDERS", "ORDER_TOTAL"
        )

        assert result is True

    def test_verify_column_not_exists(self, schema_gen):
        """Test verifying a column that doesn't exist."""
        # Create a mock physical table
        physical_table = self._create_test_table("ORDERS", ["ORDER_ID", "ORDER_TOTAL"])

        # Store it in the schema
        schema_gen.db_tables = {("TEST_DB", "PUBLIC"): {"ORDERS": physical_table}}

        # Test verification for non-existent column
        result = schema_gen._verify_column_exists_in_table(
            "TEST_DB", "PUBLIC", "ORDERS", "NONEXISTENT_COLUMN"
        )

        assert result is False

    def test_resolve_physical_table(self, schema_gen):
        """Test resolving logical table name to physical table location."""
        semantic_view = self._create_test_semantic_view(
            "TEST_SV",
            [],
            {
                "ORDERS": ("TEST_DB", "PUBLIC", "ORDERS"),
                "CUSTOMERS": ("TEST_DB", "SALES", "CUSTOMER_DATA"),
            },
        )

        # Test resolving ORDERS
        db, schema, table = schema_gen._resolve_physical_table(semantic_view, "ORDERS")
        assert (db, schema, table) == ("TEST_DB", "PUBLIC", "ORDERS")

        # Test resolving CUSTOMERS
        db, schema, table = schema_gen._resolve_physical_table(
            semantic_view, "CUSTOMERS"
        )
        assert (db, schema, table) == ("TEST_DB", "SALES", "CUSTOMER_DATA")

    def test_resolve_physical_table_missing(self, schema_gen):
        """Test resolving a logical table that's not in the mapping."""
        semantic_view = self._create_test_semantic_view(
            "TEST_SV",
            [],
            {
                "ORDERS": ("TEST_DB", "PUBLIC", "ORDERS"),
            },
        )

        # Test resolving non-existent logical table
        result = schema_gen._resolve_physical_table(semantic_view, "NONEXISTENT_TABLE")
        assert result is None


class TestSnowflakeSemanticViewRecursiveResolution:
    """Test suite for recursive resolution of chained derived metrics."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "test_user",
                "password": "test_password",
            }
        )
        report = SnowflakeV2Report()
        ctx = MagicMock(spec=PipelineContext)
        gen = SnowflakeSchemaGenerator(config, report, ctx)
        gen.schema_resolver = MagicMock()
        return gen

    def test_extract_columns_from_derived_metric(self, schema_gen):
        """Test extracting columns from a derived metric that references other metrics."""
        # Test derived metric: ORDERS.ORDER_TOTAL_METRIC + TRANSACTIONS.AMOUNT_METRIC
        expression = "ORDERS.ORDER_TOTAL_METRIC + TRANSACTIONS.AMOUNT_METRIC"
        columns = schema_gen._extract_columns_from_expression(expression)

        assert len(columns) == 2
        assert ("ORDERS", "ORDER_TOTAL_METRIC") in columns
        assert ("TRANSACTIONS", "AMOUNT_METRIC") in columns

    def test_recursive_chained_metrics(self, schema_gen):
        """Test that chained metrics are resolved recursively to physical columns.

        This tests the scenario:
        - METRIC_A = SUM(physical_column)
        - METRIC_B = METRIC_A * 2
        - METRIC_C = METRIC_B + METRIC_A

        The lineage for METRIC_C should trace back to physical_column.
        """
        # Create a semantic view with chained metrics
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
