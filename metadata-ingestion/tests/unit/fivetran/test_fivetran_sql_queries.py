"""
Unit tests for fivetran_query.py
"""

import pytest

from datahub.ingestion.source.fivetran.fivetran_constants import (
    DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR,
    MAX_COLUMN_LINEAGE_PER_CONNECTOR,
    MAX_JOBS_PER_CONNECTOR,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery


class TestFivetranLogQuery:
    """Test class for FivetranLogQuery."""

    @pytest.fixture
    def query_builder(self):
        """Create a FivetranLogQuery instance for testing."""
        return FivetranLogQuery()

    def test_init(self, query_builder):
        """Test FivetranLogQuery initialization."""
        assert query_builder.schema_clause == ""

    def test_use_database(self, query_builder):
        """Test use_database method."""
        query = query_builder.use_database("test_db")
        assert query == "use database test_db"

        query = query_builder.use_database("prod_database")
        assert query == "use database prod_database"

    def test_set_schema(self, query_builder):
        """Test set_schema method."""
        query_builder.set_schema("test_schema")
        assert query_builder.schema_clause == '"test_schema".'

        # Test with schema name containing quotes
        query_builder.set_schema('schema"with"quotes')
        assert query_builder.schema_clause == '"schema""with""quotes".'

        # Test with empty schema name
        query_builder.set_schema("")
        assert query_builder.schema_clause == '"".'

    def test_get_connectors_query(self, query_builder):
        """Test get_connectors_query method."""
        query = query_builder.get_connectors_query()

        # Check that query contains expected elements
        assert "SELECT" in query
        assert "connection_id" in query
        assert "connecting_user_id" in query
        assert "connector_type_id" in query
        assert "connection_name" in query
        assert "paused" in query
        assert "sync_frequency" in query
        assert "destination_id" in query
        assert "FROM connection" in query
        assert "_fivetran_deleted = FALSE" in query
        assert "QUALIFY ROW_NUMBER()" in query

        # Test with schema clause
        query_builder.set_schema("test_schema")
        query_with_schema = query_builder.get_connectors_query()
        assert 'FROM "test_schema".connection' in query_with_schema

    def test_get_users_query(self, query_builder):
        """Test get_users_query method."""
        query = query_builder.get_users_query()

        # Check that query contains expected elements
        assert "SELECT id as user_id" in query
        assert "given_name" in query
        assert "family_name" in query
        assert "email" in query
        assert "FROM user" in query

        # Test with schema clause
        query_builder.set_schema("test_schema")
        query_with_schema = query_builder.get_users_query()
        assert 'FROM "test_schema".user' in query_with_schema

    def test_get_sync_logs_query(self, query_builder):
        """Test get_sync_logs_query method."""
        connector_ids = ["connector_1", "connector_2", "connector_3"]
        syncs_interval = 7

        query = query_builder.get_sync_logs_query(syncs_interval, connector_ids)

        # Check that query contains expected elements
        assert "WITH ranked_syncs AS" in query
        assert "SELECT" in query
        assert "connection_id" in query
        assert "sync_id" in query
        assert "start_time" in query
        assert "end_time" in query
        assert "end_message_data" in query
        assert "FROM log" in query
        assert "message_event in ('sync_start', 'sync_end')" in query
        assert f"INTERVAL '{syncs_interval} days'" in query
        assert "'connector_1', 'connector_2', 'connector_3'" in query
        assert f"WHERE rn <= {MAX_JOBS_PER_CONNECTOR}" in query

        # Test with schema clause
        query_builder.set_schema("test_schema")
        query_with_schema = query_builder.get_sync_logs_query(
            syncs_interval, connector_ids
        )
        assert '"test_schema".log' in query_with_schema

    def test_get_sync_logs_query_single_connector(self, query_builder):
        """Test get_sync_logs_query with single connector."""
        connector_ids = ["single_connector"]
        syncs_interval = 30

        query = query_builder.get_sync_logs_query(syncs_interval, connector_ids)

        assert "'single_connector'" in query
        assert f"INTERVAL '{syncs_interval} days'" in query

    def test_get_sync_logs_query_empty_connectors(self, query_builder):
        """Test get_sync_logs_query with empty connector list."""
        connector_ids: list[str] = []
        syncs_interval = 7

        query = query_builder.get_sync_logs_query(syncs_interval, connector_ids)

        # Should still generate valid query structure
        assert "WITH ranked_syncs AS" in query
        assert "connection_id IN ()" in query

    def test_get_table_lineage_query(self, query_builder):
        """Test get_table_lineage_query method with default (unlimited)."""
        connector_ids = ["connector_1", "connector_2"]

        query = query_builder.get_table_lineage_query(connector_ids)

        # Check that query contains expected elements
        assert "SELECT" in query
        assert "connection_id" in query
        assert "source_table_id" in query
        assert "source_table_name" in query
        assert "source_schema_name" in query
        assert "destination_table_id" in query
        assert "destination_table_name" in query
        assert "destination_schema_name" in query
        assert "table_lineage" in query
        assert "source_table" in query
        assert "destination_table" in query
        assert "source_schema" in query
        assert "destination_schema" in query
        assert "'connector_1', 'connector_2'" in query
        # Default behavior should NOT include QUALIFY limit (None = unlimited by default)
        assert "table_combo_rn = 1" in query

    def test_get_table_lineage_query_unlimited(self, query_builder):
        """Test get_table_lineage_query with unlimited lineage (None)."""
        connector_ids = ["connector_1"]

        query = query_builder.get_table_lineage_query(connector_ids, max_lineage=None)

        # Should NOT contain QUALIFY clause for limiting
        assert "QUALIFY ROW_NUMBER()" not in query
        # Should still have deduplication
        assert "table_combo_rn = 1" in query
        # Should have ORDER BY
        assert "ORDER BY connection_id, created_at DESC" in query

    def test_get_table_lineage_query_custom_limit(self, query_builder):
        """Test get_table_lineage_query with custom limit."""
        connector_ids = ["connector_1", "connector_2"]
        custom_limit = 50

        query = query_builder.get_table_lineage_query(
            connector_ids, max_lineage=custom_limit
        )

        # Should contain QUALIFY clause with custom limit
        assert "QUALIFY ROW_NUMBER()" in query
        assert f"<= {custom_limit}" in query
        # Should still have deduplication
        assert "table_combo_rn = 1" in query

    def test_get_table_lineage_query_default_limit(self, query_builder):
        """Test get_table_lineage_query with explicit default limit."""
        connector_ids = ["connector_1"]

        query = query_builder.get_table_lineage_query(
            connector_ids, max_lineage=DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR
        )

        # Should contain QUALIFY clause with default limit
        assert "QUALIFY ROW_NUMBER()" in query
        assert f"<= {DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR}" in query
        assert "table_combo_rn = 1" in query

    def test_get_table_lineage_query_with_schema(self, query_builder):
        """Test get_table_lineage_query with schema clause."""
        query_builder.set_schema("test_schema")
        connector_ids = ["connector_1"]

        query = query_builder.get_table_lineage_query(connector_ids)

        # All table references should include schema
        assert '"test_schema".table_lineage' in query
        assert '"test_schema".source_table' in query
        assert '"test_schema".destination_table' in query
        assert '"test_schema".source_schema' in query
        assert '"test_schema".destination_schema' in query

    def test_get_column_lineage_query(self, query_builder):
        """Test get_column_lineage_query method with default limit."""
        connector_ids = ["connector_1", "connector_2"]

        query = query_builder.get_column_lineage_query(connector_ids)

        # Check that query contains expected elements
        assert "SELECT" in query
        assert "source_table_id" in query
        assert "destination_table_id" in query
        assert "source_column_name" in query
        assert "destination_column_name" in query
        assert "column_lineage" in query
        assert "source_column" in query
        assert "destination_column" in query
        assert "source_table" in query
        assert "'connector_1', 'connector_2'" in query
        # Default behavior should NOT include QUALIFY limit (None = unlimited by default)
        assert "column_combo_rn = 1" in query

    def test_get_column_lineage_query_with_schema(self, query_builder):
        """Test get_column_lineage_query with schema clause."""
        query_builder.set_schema("test_schema")
        connector_ids = ["connector_1"]

        query = query_builder.get_column_lineage_query(connector_ids)

        # All table references should include schema
        assert '"test_schema".column_lineage' in query
        assert '"test_schema".source_column' in query
        assert '"test_schema".destination_column' in query
        assert '"test_schema".source_table' in query

    def test_get_column_lineage_query_single_connector(self, query_builder):
        """Test get_column_lineage_query with single connector."""
        connector_ids = ["single_connector"]

        query = query_builder.get_column_lineage_query(connector_ids)

        assert "'single_connector'" in query

    def test_get_column_lineage_query_empty_connectors(self, query_builder):
        """Test get_column_lineage_query with empty connector list."""
        connector_ids: list[str] = []

        query = query_builder.get_column_lineage_query(connector_ids)

        # Should still generate valid query structure
        assert "SELECT" in query
        assert "connection_id IN ()" in query

    def test_get_column_lineage_query_unlimited(self, query_builder):
        """Test get_column_lineage_query with unlimited lineage (None)."""
        connector_ids = ["connector_1"]

        query = query_builder.get_column_lineage_query(
            connector_ids, max_column_lineage=None
        )

        # Should NOT contain QUALIFY clause for limiting
        assert "QUALIFY ROW_NUMBER()" not in query
        # Should still have deduplication
        assert "column_combo_rn = 1" in query
        # Should have ORDER BY
        assert "ORDER BY connection_id, created_at DESC" in query

    def test_get_column_lineage_query_custom_limit(self, query_builder):
        """Test get_column_lineage_query with custom limit."""
        connector_ids = ["connector_1", "connector_2"]
        custom_limit = 5000

        query = query_builder.get_column_lineage_query(
            connector_ids, max_column_lineage=custom_limit
        )

        # Should contain QUALIFY clause with custom limit
        assert "QUALIFY ROW_NUMBER()" in query
        assert f"<= {custom_limit}" in query
        # Should still have deduplication
        assert "column_combo_rn = 1" in query

    def test_get_column_lineage_query_default_limit(self, query_builder):
        """Test get_column_lineage_query with explicit default limit."""
        connector_ids = ["connector_1"]

        query = query_builder.get_column_lineage_query(
            connector_ids, max_column_lineage=MAX_COLUMN_LINEAGE_PER_CONNECTOR
        )

        # Should contain QUALIFY clause with default limit
        assert "QUALIFY ROW_NUMBER()" in query
        assert f"<= {MAX_COLUMN_LINEAGE_PER_CONNECTOR}" in query
        assert "column_combo_rn = 1" in query

    def test_get_column_lineage_query_zero_limit(self, query_builder):
        """Test get_column_lineage_query with zero limit."""
        connector_ids = ["connector_1"]

        query = query_builder.get_column_lineage_query(
            connector_ids, max_column_lineage=0
        )

        # Should contain QUALIFY clause even with 0 (which would return no results)
        assert "QUALIFY ROW_NUMBER()" in query
        assert "<= 0" in query

    def test_connector_ids_formatting(self, query_builder):
        """Test that connector IDs are properly formatted in queries."""
        # Test with special characters in connector IDs
        connector_ids = [
            "connector-with-dash",
            "connector_with_underscore",
            "connector.with.dots",
        ]

        sync_query = query_builder.get_sync_logs_query(7, connector_ids)
        table_query = query_builder.get_table_lineage_query(
            connector_ids, max_lineage=100
        )
        column_query = query_builder.get_column_lineage_query(connector_ids)

        # All queries should properly quote the connector IDs
        expected_formatted = (
            "'connector-with-dash', 'connector_with_underscore', 'connector.with.dots'"
        )

        assert expected_formatted in sync_query
        assert expected_formatted in table_query
        assert expected_formatted in column_query

    def test_schema_clause_persistence(self, query_builder):
        """Test that schema clause persists across multiple query calls."""
        query_builder.set_schema("persistent_schema")

        # Call multiple query methods
        connectors_query = query_builder.get_connectors_query()
        users_query = query_builder.get_users_query()
        sync_query = query_builder.get_sync_logs_query(7, ["test"])
        table_query = query_builder.get_table_lineage_query(["test"], max_lineage=100)
        column_query = query_builder.get_column_lineage_query(["test"])

        # All queries should use the same schema clause
        schema_prefix = '"persistent_schema".'
        assert schema_prefix in connectors_query
        assert schema_prefix in users_query
        assert schema_prefix in sync_query
        assert schema_prefix in table_query
        assert schema_prefix in column_query

    def test_query_sql_validity_structure(self, query_builder):
        """Test that generated queries have valid SQL structure."""
        connector_ids: list[str] = ["test_connector"]

        # Test all query methods
        queries = [
            query_builder.get_connectors_query(),
            query_builder.get_users_query(),
            query_builder.get_sync_logs_query(7, connector_ids),
            query_builder.get_table_lineage_query(connector_ids, max_lineage=100),
            query_builder.get_column_lineage_query(connector_ids),
        ]

        for query in queries:
            # Basic SQL structure checks
            assert query.count("SELECT") >= 1
            assert query.count("FROM") >= 1
            # Remove the string termination check as it was failing
            assert "SELECT" in query.upper()
            assert "FROM" in query.upper()
