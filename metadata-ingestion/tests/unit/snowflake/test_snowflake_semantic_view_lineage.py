import datetime
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeSchema,
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
        column_subtypes: dict[str, str] = {}
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


class TestSnowflakeSemanticViewBaseTableLineage:
    """Test suite for base table lineage resolution in semantic views."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        return create_mock_schema_gen()

    def test_resolve_upstream_urns_from_base_tables(self, schema_gen):
        """Test resolving upstream URNs from semantic view base tables."""
        # Setup filters to allow the dataset
        schema_gen.filters.is_dataset_pattern_allowed.return_value = True
        schema_gen.identifiers.get_dataset_identifier.return_value = "db.schema.orders"
        schema_gen.identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
        )

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test semantic view",
            view_definition="definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="definition",
            columns=[],
            base_tables=[("TEST_DB", "PUBLIC", "ORDERS")],
        )

        # Simulate the upstream URN resolution logic
        upstream_urns = []
        if semantic_view.base_tables:
            for base_db, base_schema, base_table in semantic_view.base_tables:
                base_table_identifier = schema_gen.identifiers.get_dataset_identifier(
                    base_table, base_schema, base_db
                )
                is_allowed = schema_gen.filters.is_dataset_pattern_allowed(
                    base_table_identifier, "TABLE"
                )
                if is_allowed:
                    upstream_urn = schema_gen.identifiers.gen_dataset_urn(
                        base_table_identifier
                    )
                    upstream_urns.append(upstream_urn)

        semantic_view.resolved_upstream_urns = upstream_urns

        assert len(semantic_view.resolved_upstream_urns) == 1
        assert semantic_view.resolved_upstream_urns[0] == (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
        )

    def test_filter_disallowed_base_tables(self, schema_gen):
        """Test that disallowed base tables are filtered out."""
        schema_gen.filters.is_dataset_pattern_allowed.return_value = False

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test semantic view",
            view_definition="definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="definition",
            columns=[],
            base_tables=[("TEST_DB", "PUBLIC", "ORDERS")],
        )

        upstream_urns = []
        if semantic_view.base_tables:
            for base_db, base_schema, base_table in semantic_view.base_tables:
                base_table_identifier = schema_gen.identifiers.get_dataset_identifier(
                    base_table, base_schema, base_db
                )
                is_allowed = schema_gen.filters.is_dataset_pattern_allowed(
                    base_table_identifier, "TABLE"
                )
                if is_allowed:
                    upstream_urn = schema_gen.identifiers.gen_dataset_urn(
                        base_table_identifier
                    )
                    upstream_urns.append(upstream_urn)

        semantic_view.resolved_upstream_urns = upstream_urns

        assert len(semantic_view.resolved_upstream_urns) == 0


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


class TestSnowflakeSemanticViewProcessing:
    """Test suite for semantic view processing flow."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        gen = create_mock_schema_gen()
        gen.config.include_technical_schema = True
        gen.config.include_semantic_view_column_lineage = True
        return gen

    def test_process_semantic_view_with_columns(self, schema_gen):
        """Test processing a semantic view that already has columns populated."""
        columns = [
            SnowflakeColumn(
                name="ORDER_ID",
                ordinal_position=1,
                is_nullable=False,
                data_type="NUMBER",
                comment="Order identifier",
                expression=None,
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=0,
            ),
        ]

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test semantic view",
            view_definition="definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="definition",
            columns=columns,
            column_subtypes={"ORDER_ID": "DIMENSION"},
            base_tables=[("TEST_DB", "PUBLIC", "ORDERS")],
            resolved_upstream_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
            ],
        )

        snowflake_schema = SnowflakeSchema(
            name="PUBLIC",
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            comment="Test schema",
        )

        # Mock gen_dataset_workunits to return empty iterator
        schema_gen.gen_dataset_workunits = MagicMock(return_value=iter([]))
        schema_gen._emit_semantic_view_lineage = MagicMock(return_value=iter([]))
        schema_gen._generate_column_lineage_for_semantic_view = MagicMock(
            return_value=[]
        )
        schema_gen.get_columns_for_table = MagicMock(return_value=[])
        schema_gen.tag_extractor = MagicMock()
        schema_gen.tag_extractor.get_column_tags_for_table.return_value = {}
        schema_gen.tag_extractor.get_tags_on_object.return_value = None

        # Execute
        list(
            schema_gen._process_semantic_view(
                semantic_view, snowflake_schema, "TEST_DB"
            )
        )

        # Verify gen_dataset_workunits was called
        schema_gen.gen_dataset_workunits.assert_called_once()

    def test_process_semantic_view_without_columns(self, schema_gen):
        """Test processing a semantic view that needs columns fetched."""
        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test semantic view",
            view_definition="definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="definition",
            columns=[],  # Empty columns
            base_tables=[],
            resolved_upstream_urns=[],
        )

        snowflake_schema = SnowflakeSchema(
            name="PUBLIC",
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            comment="Test schema",
        )

        # Mock to return columns
        mock_columns = [
            SnowflakeColumn(
                name="COL1",
                ordinal_position=1,
                is_nullable=True,
                data_type="VARCHAR",
                comment=None,
                expression=None,
                character_maximum_length=100,
                numeric_precision=None,
                numeric_scale=None,
            )
        ]
        schema_gen.get_columns_for_table = MagicMock(return_value=mock_columns)
        schema_gen.gen_dataset_workunits = MagicMock(return_value=iter([]))
        schema_gen.tag_extractor = MagicMock()
        schema_gen.tag_extractor.get_column_tags_for_table.return_value = {}
        schema_gen.tag_extractor.get_tags_on_object.return_value = None

        # Execute
        list(
            schema_gen._process_semantic_view(
                semantic_view, snowflake_schema, "TEST_DB"
            )
        )

        # Verify columns were fetched
        schema_gen.get_columns_for_table.assert_called_once()
        assert semantic_view.columns == mock_columns

    def test_process_semantic_view_column_lineage_emission(self, schema_gen):
        """Test that column lineage is generated and emitted."""
        columns = [
            SnowflakeColumn(
                name="TOTAL_REVENUE",
                ordinal_position=1,
                is_nullable=False,
                data_type="NUMBER",
                comment="Total revenue metric",
                expression="SUM(ORDERS.ORDER_TOTAL)",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=2,
            ),
        ]

        semantic_view = SnowflakeSemanticView(
            name="TEST_SV",
            created=datetime.datetime.now(),
            comment="Test semantic view",
            view_definition="definition",
            last_altered=datetime.datetime.now(),
            semantic_definition="definition",
            columns=columns,
            column_subtypes={"TOTAL_REVENUE": "METRIC"},
            base_tables=[("TEST_DB", "PUBLIC", "ORDERS")],
            resolved_upstream_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.orders,PROD)"
            ],
            logical_to_physical_table={"ORDERS": ("TEST_DB", "PUBLIC", "ORDERS")},
        )

        snowflake_schema = SnowflakeSchema(
            name="PUBLIC",
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            comment="Test schema",
        )

        schema_gen.gen_dataset_workunits = MagicMock(return_value=iter([]))
        schema_gen._emit_semantic_view_lineage = MagicMock(return_value=iter([]))
        schema_gen.tag_extractor = MagicMock()
        schema_gen.tag_extractor.get_column_tags_for_table.return_value = {}
        schema_gen.tag_extractor.get_tags_on_object.return_value = None

        # Execute
        list(
            schema_gen._process_semantic_view(
                semantic_view, snowflake_schema, "TEST_DB"
            )
        )

        # Verify lineage emission was called
        schema_gen._emit_semantic_view_lineage.assert_called_once()


class TestSnowflakeSemanticViewFiltering:
    """Test suite for semantic view filtering in get_semantic_views_for_schema."""

    @pytest.fixture
    def schema_gen(self):
        """Create a SnowflakeSchemaGenerator instance for testing."""
        return create_mock_schema_gen()

    def test_filter_allowed_semantic_views(self, schema_gen):
        """Test that allowed semantic views pass the filter."""
        schema_gen.filters.is_semantic_view_allowed.return_value = True
        schema_gen.identifiers.get_dataset_identifier.return_value = (
            "test_db.public.test_sv"
        )

        semantic_views_from_db = [
            SnowflakeSemanticView(
                name="TEST_SV",
                created=datetime.datetime.now(),
                comment="Test",
                view_definition="def",
                last_altered=datetime.datetime.now(),
                semantic_definition="def",
                columns=[],
            )
        ]

        # Simulate filtering logic
        filtered = []
        for sv in semantic_views_from_db:
            sv_name = schema_gen.identifiers.get_dataset_identifier(
                sv.name, "PUBLIC", "TEST_DB"
            )
            schema_gen.report.report_entity_scanned(sv_name, "semantic view")
            if schema_gen.filters.is_semantic_view_allowed(sv_name):
                filtered.append(sv)
            else:
                schema_gen.report.report_dropped(sv_name)

        assert len(filtered) == 1
        assert filtered[0].name == "TEST_SV"

    def test_filter_disallowed_semantic_views(self, schema_gen):
        """Test that disallowed semantic views are filtered out."""
        schema_gen.filters.is_semantic_view_allowed.return_value = False
        schema_gen.identifiers.get_dataset_identifier.return_value = (
            "test_db.public.test_sv"
        )

        semantic_views_from_db = [
            SnowflakeSemanticView(
                name="TEST_SV",
                created=datetime.datetime.now(),
                comment="Test",
                view_definition="def",
                last_altered=datetime.datetime.now(),
                semantic_definition="def",
                columns=[],
            )
        ]

        # Simulate filtering logic
        filtered = []
        for sv in semantic_views_from_db:
            sv_name = schema_gen.identifiers.get_dataset_identifier(
                sv.name, "PUBLIC", "TEST_DB"
            )
            schema_gen.report.report_entity_scanned(sv_name, "semantic view")
            if schema_gen.filters.is_semantic_view_allowed(sv_name):
                filtered.append(sv)
            else:
                schema_gen.report.report_dropped(sv_name)

        assert len(filtered) == 0
        # Verify dropped was reported (report stores tuples with index)
        assert len(schema_gen.report.filtered) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
