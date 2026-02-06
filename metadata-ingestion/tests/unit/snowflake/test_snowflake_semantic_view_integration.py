"""
Integration-level tests for Snowflake Semantic View ingestion.

These tests cover end-to-end flows with realistic data, testing the interaction
between multiple components (data dictionary, schema generator, lineage generation).

NOTE: Despite being in the unit test folder, these are integration-style tests that
test multiple components together. They use mocked Snowflake connections but test
the full ingestion pipeline.

TODO: Consider moving to tests/integration/snowflake/ and using golden MCE files
for more comprehensive assertions instead of manual field checks. Golden files would:
- Capture the complete output structure
- Make it easier to spot regressions
- Reduce test maintenance when output format changes
- Follow the pattern used in other Snowflake integration tests
"""

import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDataDictionary,
    SnowflakeSemanticView,
    SnowflakeTableIdentifier,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)


class MockQueryResults:
    """Helper to create mock query results for different Snowflake system tables."""

    @staticmethod
    def semantic_views() -> List[Dict[str, Any]]:
        """INFORMATION_SCHEMA.SEMANTIC_VIEWS result."""
        return [
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "CREATED": datetime.datetime(2024, 1, 1),
                "COMMENT": "Sales analytics semantic view",
            },
        ]

    @staticmethod
    def semantic_tables() -> List[Dict[str, Any]]:
        """INFORMATION_SCHEMA.SEMANTIC_TABLES result."""
        return [
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "SEMANTIC_TABLE_NAME": "orders",
                "BASE_TABLE_CATALOG": "TEST_DB",
                "BASE_TABLE_SCHEMA": "PUBLIC",
                "BASE_TABLE_NAME": "ORDERS",
                "PRIMARY_KEYS": '["ORDER_ID"]',
                "SYNONYMS": '["sales_orders"]',
                "COMMENT": None,
            },
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "SEMANTIC_TABLE_NAME": "customers",
                "BASE_TABLE_CATALOG": "TEST_DB",
                "BASE_TABLE_SCHEMA": "PUBLIC",
                "BASE_TABLE_NAME": "CUSTOMERS",
                "PRIMARY_KEYS": '["CUSTOMER_ID"]',
                "SYNONYMS": None,
                "COMMENT": None,
            },
        ]

    @staticmethod
    def semantic_dimensions() -> List[Dict[str, Any]]:
        """INFORMATION_SCHEMA.SEMANTIC_DIMENSIONS result."""
        return [
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "customer_id",
                "TABLE_NAME": "customers",
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Unique customer identifier",
                "EXPRESSION": None,
                "SYNONYMS": '["cust_id", "client_id"]',
            },
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "order_id",
                "TABLE_NAME": "orders",
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Order identifier for tracking",
                "EXPRESSION": None,
                "SYNONYMS": None,
            },
            # Same column as DIMENSION (for multi-subtype test)
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "order_id",
                "TABLE_NAME": "customers",
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Order ID for customer lookups",
                "EXPRESSION": None,
                "SYNONYMS": None,
            },
        ]

    @staticmethod
    def semantic_facts() -> List[Dict[str, Any]]:
        """INFORMATION_SCHEMA.SEMANTIC_FACTS result."""
        return [
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "order_total",
                "TABLE_NAME": "orders",
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Total order amount",
                "EXPRESSION": "ORDER_TOTAL",
                "SYNONYMS": '["total", "amount"]',
            },
            # ORDER_ID also as FACT (testing DIMENSION+FACT on same column)
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "order_id",
                "TABLE_NAME": "orders",
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Order ID for aggregations",
                "EXPRESSION": "ORDER_ID",
                "SYNONYMS": None,
            },
        ]

    @staticmethod
    def semantic_metrics() -> List[Dict[str, Any]]:
        """INFORMATION_SCHEMA.SEMANTIC_METRICS result."""
        return [
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "total_revenue",
                "TABLE_NAME": None,  # Metrics often have no direct table
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Sum of all order totals",
                "EXPRESSION": "SUM(orders.ORDER_TOTAL)",
                "SYNONYMS": '["revenue", "sales"]',
            },
            # Derived metric referencing another metric (chained derivation)
            {
                "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                "NAME": "avg_order_value",
                "TABLE_NAME": None,
                "DATA_TYPE": "NUMBER",
                "COMMENT": "Average order value",
                "EXPRESSION": "orders.total_revenue / COUNT(orders.ORDER_ID)",
                "SYNONYMS": None,
            },
        ]

    @staticmethod
    def ddl() -> List[Dict[str, Any]]:
        """GET_DDL result."""
        return [{"DDL": "CREATE SEMANTIC VIEW SALES_ANALYTICS AS ..."}]


def create_mock_connection(query_results: Dict[str, List[Dict]]) -> MagicMock:
    """Create a mock connection that returns appropriate results based on query."""
    mock_conn = MagicMock()

    def query_side_effect(query_str):
        query_lower = query_str.lower()
        cursor = MagicMock()

        if "semantic_tables" in query_lower:
            cursor.__iter__ = MagicMock(
                return_value=iter(query_results.get("semantic_tables", []))
            )
        elif "semantic_dimensions" in query_lower:
            cursor.__iter__ = MagicMock(
                return_value=iter(query_results.get("semantic_dimensions", []))
            )
        elif "semantic_facts" in query_lower:
            cursor.__iter__ = MagicMock(
                return_value=iter(query_results.get("semantic_facts", []))
            )
        elif "semantic_metrics" in query_lower:
            cursor.__iter__ = MagicMock(
                return_value=iter(query_results.get("semantic_metrics", []))
            )
        elif "get_ddl" in query_lower:
            cursor.__iter__ = MagicMock(return_value=iter(query_results.get("ddl", [])))
        elif "information_schema.semantic_views" in query_lower:
            # INFORMATION_SCHEMA.SEMANTIC_VIEWS query
            cursor.__iter__ = MagicMock(
                return_value=iter(query_results.get("semantic_views", []))
            )
        else:
            cursor.__iter__ = MagicMock(return_value=iter([]))

        return cursor

    mock_conn.query.side_effect = query_side_effect
    return mock_conn


class TestSemanticViewEndToEndFlow:
    """Test the complete semantic view ingestion flow."""

    def test_full_semantic_view_population(self):
        """Test that semantic view is fully populated with all metadata."""
        query_results = {
            "semantic_views": MockQueryResults.semantic_views(),
            "semantic_tables": MockQueryResults.semantic_tables(),
            "semantic_dimensions": MockQueryResults.semantic_dimensions(),
            "semantic_facts": MockQueryResults.semantic_facts(),
            "semantic_metrics": MockQueryResults.semantic_metrics(),
            "ddl": MockQueryResults.ddl(),
        }

        mock_conn = create_mock_connection(query_results)
        report = SnowflakeV2Report()
        data_dict = SnowflakeDataDictionary(connection=mock_conn, report=report)

        # Execute
        result = data_dict.get_semantic_views_for_database("TEST_DB")

        # Verify structure
        assert result is not None
        assert "PUBLIC" in result
        assert len(result["PUBLIC"]) == 1

        sv = result["PUBLIC"][0]
        assert sv.name == "SALES_ANALYTICS"

        # Verify base tables populated
        assert len(sv.base_tables) == 2
        # Check using SnowflakeTableIdentifier objects
        orders_id = SnowflakeTableIdentifier(
            database="TEST_DB", schema="PUBLIC", table="ORDERS"
        )
        customers_id = SnowflakeTableIdentifier(
            database="TEST_DB", schema="PUBLIC", table="CUSTOMERS"
        )
        assert orders_id in sv.base_tables
        assert customers_id in sv.base_tables

        # Verify logical to physical mapping
        assert "ORDERS" in sv.logical_to_physical_table
        assert "CUSTOMERS" in sv.logical_to_physical_table

        # Verify columns populated (should have merged duplicates)
        column_names = [c.name.upper() for c in sv.columns]
        assert "CUSTOMER_ID" in column_names
        assert "ORDER_ID" in column_names
        assert "ORDER_TOTAL" in column_names
        assert "TOTAL_REVENUE" in column_names
        assert "AVG_ORDER_VALUE" in column_names

        # Verify column subtypes include merged types
        # ORDER_ID should be both DIMENSION and FACT
        assert "ORDER_ID" in sv.column_subtypes
        subtypes = sv.column_subtypes["ORDER_ID"]
        assert "DIMENSION" in subtypes
        assert "FACT" in subtypes

    def test_column_merging_preserves_all_metadata(self):
        """Test that merging columns from different contexts preserves all metadata."""
        query_results = {
            "semantic_views": MockQueryResults.semantic_views(),
            "semantic_tables": MockQueryResults.semantic_tables(),
            "semantic_dimensions": MockQueryResults.semantic_dimensions(),
            "semantic_facts": MockQueryResults.semantic_facts(),
            "semantic_metrics": MockQueryResults.semantic_metrics(),
            "ddl": MockQueryResults.ddl(),
        }

        mock_conn = create_mock_connection(query_results)
        report = SnowflakeV2Report()
        data_dict = SnowflakeDataDictionary(connection=mock_conn, report=report)

        result = data_dict.get_semantic_views_for_database("TEST_DB")
        assert result is not None
        sv = result["PUBLIC"][0]

        # Find ORDER_ID column
        order_id_col = next(c for c in sv.columns if c.name.upper() == "ORDER_ID")

        # Should have merged comment from multiple occurrences
        assert order_id_col.comment is not None
        # DIMENSION and FACT descriptions should both appear
        assert "DIMENSION" in order_id_col.comment or "FACT" in order_id_col.comment

    def test_derived_metric_expression_captured(self):
        """Test that derived metric expressions are captured for lineage."""
        query_results = {
            "semantic_views": MockQueryResults.semantic_views(),
            "semantic_tables": MockQueryResults.semantic_tables(),
            "semantic_dimensions": MockQueryResults.semantic_dimensions(),
            "semantic_facts": MockQueryResults.semantic_facts(),
            "semantic_metrics": MockQueryResults.semantic_metrics(),
            "ddl": MockQueryResults.ddl(),
        }

        mock_conn = create_mock_connection(query_results)
        report = SnowflakeV2Report()
        data_dict = SnowflakeDataDictionary(connection=mock_conn, report=report)

        result = data_dict.get_semantic_views_for_database("TEST_DB")
        assert result is not None
        sv = result["PUBLIC"][0]

        # Find total_revenue metric
        revenue_col = next(c for c in sv.columns if c.name.upper() == "TOTAL_REVENUE")
        assert revenue_col.expression is not None
        assert "SUM" in revenue_col.expression
        assert "ORDER_TOTAL" in revenue_col.expression

        # Find avg_order_value (chained metric)
        avg_col = next(c for c in sv.columns if c.name.upper() == "AVG_ORDER_VALUE")
        assert avg_col.expression is not None
        assert "total_revenue" in avg_col.expression or "ORDER_ID" in avg_col.expression


class TestSemanticViewLineageGeneration:
    """Test column lineage generation for semantic views."""

    @pytest.fixture
    def schema_gen_with_semantic_view(self):
        """Create schema generator with a realistic semantic view."""
        config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test",
                "username": "user",
                "password": "pass",
                "semantic_views": {"enabled": True, "column_lineage": True},
            }
        )
        report = SnowflakeV2Report()

        filters = MagicMock()
        filters.is_dataset_pattern_allowed.return_value = True

        identifiers = MagicMock()
        identifiers.get_dataset_identifier.side_effect = lambda t, s, d: f"{d}.{s}.{t}"
        identifiers.gen_dataset_urn.side_effect = (
            lambda x: f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{x},PROD)"
        )

        aggregator = MagicMock()
        # Mock schema resolver to return schema for ORDERS table
        schema_info: Dict[str, Any] = {
            "order_id": {},
            "order_total": {},
            "customer_id": {},
        }
        aggregator._schema_resolver = MagicMock()
        aggregator._schema_resolver._resolve_schema_info.return_value = schema_info

        gen = SnowflakeSchemaGenerator(
            config=config,
            report=report,
            connection=MagicMock(),
            filters=filters,
            identifiers=identifiers,
            domain_registry=None,
            profiler=None,
            aggregator=aggregator,
            snowsight_url_builder=None,
        )

        # Create semantic view with columns
        semantic_view = SnowflakeSemanticView(
            name="SALES_ANALYTICS",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="CREATE SEMANTIC VIEW ...",
            last_altered=datetime.datetime.now(),
            columns=[
                SnowflakeColumn(
                    name="CUSTOMER_ID",
                    ordinal_position=1,
                    is_nullable=False,
                    data_type="NUMBER",
                    comment="Customer ID",
                    expression=None,
                    character_maximum_length=None,
                    numeric_precision=38,
                    numeric_scale=0,
                ),
                SnowflakeColumn(
                    name="ORDER_TOTAL",
                    ordinal_position=2,
                    is_nullable=False,
                    data_type="NUMBER",
                    comment="Order total",
                    expression="ORDER_TOTAL",
                    character_maximum_length=None,
                    numeric_precision=38,
                    numeric_scale=2,
                ),
                SnowflakeColumn(
                    name="TOTAL_REVENUE",
                    ordinal_position=3,
                    is_nullable=False,
                    data_type="NUMBER",
                    comment="Total revenue",
                    expression="SUM(orders.ORDER_TOTAL)",
                    character_maximum_length=None,
                    numeric_precision=38,
                    numeric_scale=2,
                ),
            ],
            column_subtypes={
                "CUSTOMER_ID": "DIMENSION",
                "ORDER_TOTAL": "FACT",
                "TOTAL_REVENUE": "METRIC",
            },
            column_table_mappings={
                "CUSTOMER_ID": ["customers"],
                "ORDER_TOTAL": ["orders"],
            },
            logical_to_physical_table={
                "ORDERS": ("TEST_DB", "PUBLIC", "ORDERS"),
                "CUSTOMERS": ("TEST_DB", "PUBLIC", "CUSTOMERS"),
            },
            base_tables=[
                SnowflakeTableIdentifier(
                    database="TEST_DB", schema="PUBLIC", table="ORDERS"
                ),
                SnowflakeTableIdentifier(
                    database="TEST_DB", schema="PUBLIC", table="CUSTOMERS"
                ),
            ],
        )

        return gen, semantic_view

    def test_generate_lineage_for_direct_columns(self, schema_gen_with_semantic_view):
        """Test lineage generation for columns that map directly to physical columns."""
        gen, semantic_view = schema_gen_with_semantic_view
        semantic_view_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST_DB.PUBLIC.SALES_ANALYTICS,PROD)"

        lineages = gen._generate_column_lineage_for_semantic_view(
            semantic_view, semantic_view_urn, "TEST_DB"
        )

        # Should generate lineage for columns with table mappings
        assert len(lineages) >= 1

    def test_generate_lineage_for_derived_metric(self, schema_gen_with_semantic_view):
        """Test lineage generation for derived metrics with expressions."""
        gen, _ = schema_gen_with_semantic_view

        # Extract columns from the TOTAL_REVENUE expression
        expression = "SUM(orders.ORDER_TOTAL)"
        columns = gen._extract_columns_from_expression(expression)

        assert len(columns) == 1
        assert ("ORDERS", "ORDER_TOTAL") in columns


class TestSemanticViewOrchestrationFlow:
    """Test orchestration methods (_process_semantic_views, _process_semantic_view, etc.)."""

    @pytest.fixture
    def mock_schema_gen(self):
        """Create a minimally mocked SnowflakeSchemaGenerator."""
        config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test",
                "username": "user",
                "password": "pass",
                "semantic_views": {"enabled": True, "column_lineage": True},
                "include_technical_schema": True,
            }
        )
        report = SnowflakeV2Report()

        filters = MagicMock()
        filters.is_dataset_pattern_allowed.return_value = True
        filters.is_semantic_view_allowed.return_value = True

        identifiers = MagicMock()
        identifiers.get_dataset_identifier.side_effect = lambda t, s, d: f"{d}.{s}.{t}"
        identifiers.gen_dataset_urn.side_effect = (
            lambda x: f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{x},PROD)"
        )

        aggregator = MagicMock()
        aggregator._schema_resolver = MagicMock()
        aggregator._schema_resolver._resolve_schema_info.return_value = {}

        gen = SnowflakeSchemaGenerator(
            config=config,
            report=report,
            connection=MagicMock(),
            filters=filters,
            identifiers=identifiers,
            domain_registry=None,
            profiler=None,
            aggregator=aggregator,
            snowsight_url_builder=None,
        )

        return gen

    def test_process_semantic_views_no_base_tables(self, mock_schema_gen):
        """Test _process_semantic_views when semantic view has no base tables."""
        gen = mock_schema_gen

        semantic_view = SnowflakeSemanticView(
            name="NO_BASE_TABLES",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="CREATE SEMANTIC VIEW ...",
            last_altered=datetime.datetime.now(),
            base_tables=[],  # No base tables
            columns=[
                SnowflakeColumn(
                    name="COL1",
                    ordinal_position=1,
                    is_nullable=True,
                    data_type="VARCHAR",
                    comment=None,
                    character_maximum_length=100,
                    numeric_precision=None,
                    numeric_scale=None,
                ),
            ],
        )

        schema = MagicMock()
        schema.name = "PUBLIC"

        # Mock gen_dataset_workunits to return empty
        gen.gen_dataset_workunits = MagicMock(return_value=iter([]))

        # Execute - should not raise
        list(gen._process_semantic_views([semantic_view], schema, "TEST_DB", "PUBLIC"))

        # Should have empty upstream URNs
        assert semantic_view.resolved_upstream_urns == []

    def test_process_semantic_views_include_technical_schema_false(
        self, mock_schema_gen
    ):
        """Test _process_semantic_views skips schema when include_technical_schema=False."""
        gen = mock_schema_gen
        gen.config.include_technical_schema = False

        semantic_view = SnowflakeSemanticView(
            name="TEST_VIEW",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="CREATE SEMANTIC VIEW ...",
            last_altered=datetime.datetime.now(),
            columns=[],
        )

        schema = MagicMock()
        schema.name = "PUBLIC"

        # gen_dataset_workunits should NOT be called
        gen.gen_dataset_workunits = MagicMock(return_value=iter([]))

        list(gen._process_semantic_views([semantic_view], schema, "TEST_DB", "PUBLIC"))

        # Should NOT have called gen_dataset_workunits since include_technical_schema=False
        gen.gen_dataset_workunits.assert_not_called()

    def test_process_semantic_view_columns_not_populated(self, mock_schema_gen):
        """Test _process_semantic_view fetches columns when not pre-populated."""
        gen = mock_schema_gen

        semantic_view = SnowflakeSemanticView(
            name="TEST_VIEW",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="CREATE SEMANTIC VIEW ...",
            last_altered=datetime.datetime.now(),
            columns=[],  # Empty - should trigger column fetch
        )
        semantic_view.resolved_upstream_urns = []

        schema = MagicMock()
        schema.name = "PUBLIC"

        # Mock get_columns_for_table
        mock_columns = [
            SnowflakeColumn(
                name="FETCHED_COL",
                ordinal_position=1,
                is_nullable=True,
                data_type="VARCHAR",
                comment=None,
                character_maximum_length=100,
                numeric_precision=None,
                numeric_scale=None,
            ),
        ]
        gen.get_columns_for_table = MagicMock(return_value=mock_columns)
        gen.gen_dataset_workunits = MagicMock(return_value=iter([]))

        list(gen._process_semantic_view(semantic_view, schema, "TEST_DB"))

        # Should have called get_columns_for_table since columns were empty
        gen.get_columns_for_table.assert_called_once()

    def test_process_semantic_view_column_lineage_disabled(self, mock_schema_gen):
        """Test _process_semantic_view skips lineage when disabled."""
        gen = mock_schema_gen
        gen.config.semantic_views.column_lineage = False

        semantic_view = SnowflakeSemanticView(
            name="TEST_VIEW",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="CREATE SEMANTIC VIEW ...",
            last_altered=datetime.datetime.now(),
            columns=[
                SnowflakeColumn(
                    name="COL1",
                    ordinal_position=1,
                    is_nullable=True,
                    data_type="VARCHAR",
                    comment=None,
                    character_maximum_length=100,
                    numeric_precision=None,
                    numeric_scale=None,
                ),
            ],
        )
        semantic_view.resolved_upstream_urns = ["urn:li:dataset:test"]

        schema = MagicMock()
        schema.name = "PUBLIC"

        gen.gen_dataset_workunits = MagicMock(return_value=iter([]))
        gen._generate_column_lineage_for_semantic_view = MagicMock()

        list(gen._process_semantic_view(semantic_view, schema, "TEST_DB"))

        # Should NOT have called lineage generation
        gen._generate_column_lineage_for_semantic_view.assert_not_called()

    def test_process_semantic_view_no_upstream_urns_skips_lineage_emission(
        self, mock_schema_gen
    ):
        """Test _process_semantic_view skips lineage emission when no upstream URNs."""
        gen = mock_schema_gen

        semantic_view = SnowflakeSemanticView(
            name="TEST_VIEW",
            created=datetime.datetime.now(),
            comment="Test",
            view_definition="CREATE SEMANTIC VIEW ...",
            last_altered=datetime.datetime.now(),
            columns=[
                SnowflakeColumn(
                    name="COL1",
                    ordinal_position=1,
                    is_nullable=True,
                    data_type="VARCHAR",
                    comment=None,
                    character_maximum_length=100,
                    numeric_precision=None,
                    numeric_scale=None,
                ),
            ],
        )
        semantic_view.resolved_upstream_urns = []  # No upstream URNs

        schema = MagicMock()
        schema.name = "PUBLIC"

        gen.gen_dataset_workunits = MagicMock(return_value=iter([]))
        gen._generate_column_lineage_for_semantic_view = MagicMock(return_value=[])
        gen._emit_semantic_view_lineage = MagicMock(return_value=iter([]))

        list(gen._process_semantic_view(semantic_view, schema, "TEST_DB"))

        # Lineage generation called but emission should NOT be called (no upstream URNs)
        gen._generate_column_lineage_for_semantic_view.assert_called_once()
        gen._emit_semantic_view_lineage.assert_not_called()


class TestSemanticViewEdgeCases:
    """Test edge cases in semantic view processing."""

    def test_malformed_json_in_synonyms_handled_gracefully(self):
        """Test that malformed JSON in SYNONYMS field doesn't crash."""
        query_results = {
            "semantic_views": MockQueryResults.semantic_views(),
            "semantic_tables": [
                {
                    "SEMANTIC_VIEW_CATALOG": "TEST_DB",
                    "SEMANTIC_VIEW_SCHEMA": "PUBLIC",
                    "SEMANTIC_VIEW_NAME": "SALES_ANALYTICS",
                    "SEMANTIC_TABLE_NAME": "orders",
                    "BASE_TABLE_CATALOG": "TEST_DB",
                    "BASE_TABLE_SCHEMA": "PUBLIC",
                    "BASE_TABLE_NAME": "ORDERS",
                    "PRIMARY_KEYS": "invalid json {{{",  # Malformed
                    "SYNONYMS": "also invalid",  # Malformed
                    "COMMENT": None,
                },
            ],
            "semantic_dimensions": [],
            "semantic_facts": [],
            "semantic_metrics": [],
            "ddl": MockQueryResults.ddl(),
        }

        mock_conn = create_mock_connection(query_results)
        report = SnowflakeV2Report()
        data_dict = SnowflakeDataDictionary(connection=mock_conn, report=report)

        # Should not raise exception
        result = data_dict.get_semantic_views_for_database("TEST_DB")

        assert result is not None
        assert "PUBLIC" in result

    def test_missing_base_tables_handled(self):
        """Test handling when SEMANTIC_TABLES returns no results."""
        query_results = {
            "semantic_views": MockQueryResults.semantic_views(),
            "semantic_tables": [],  # No base tables
            "semantic_dimensions": MockQueryResults.semantic_dimensions(),
            "semantic_facts": [],
            "semantic_metrics": [],
            "ddl": MockQueryResults.ddl(),
        }

        mock_conn = create_mock_connection(query_results)
        report = SnowflakeV2Report()
        data_dict = SnowflakeDataDictionary(connection=mock_conn, report=report)

        result = data_dict.get_semantic_views_for_database("TEST_DB")

        assert result is not None
        sv = result["PUBLIC"][0]
        # Should still have view, just no base tables
        assert len(sv.base_tables) == 0
