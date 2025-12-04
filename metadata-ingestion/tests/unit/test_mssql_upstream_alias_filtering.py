"""
Unit tests for MSSQL upstream alias filtering in stored procedures.

Tests the filtering of spurious TSQL aliases that appear in UPDATE/DELETE statements
like: UPDATE t SET col = val FROM schema.table t
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource


@pytest.fixture
def mssql_source():
    """Create a mock MSSQL source for testing."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        username="test",
        password="test",
        database="db1",
        platform_instance="test_instance",
        env="PROD",
        include_descriptions=False,
    )

    # Mock the parent class's __init__ to avoid DB connections
    with patch("datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__"):
        source = SQLServerSource(config, MagicMock())

        # Set platform attribute (required by is_temp_table)
        source.platform = "mssql"

        # Mock discovered_datasets (with platform_instance prefix to match URN names)
        source.discovered_datasets = {
            "test_instance.db1.dbo.real_table",
            "test_instance.db1.dbo.another_real_table",
            "test_instance.db2.dbo.cross_db_table",
            # Also add without prefix for some tests
            "db1.dbo.real_table",
            "db1.dbo.another_real_table",
            "db2.dbo.cross_db_table",
        }

        # Mock schema_resolver
        schema_resolver = MagicMock()
        schema_resolver.platform_instance = "test_instance"

        def has_urn_side_effect(urn):
            """Mock schema_resolver.has_urn() - returns True for tables with schemas."""
            # Tables with loaded schemas
            schema_tables = [
                "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db2.dbo.cross_db_table,PROD)",
            ]
            return urn in schema_tables

        schema_resolver.has_urn = MagicMock(side_effect=has_urn_side_effect)
        source.get_schema_resolver = MagicMock(return_value=schema_resolver)

        # Mock aggregator
        source.aggregator = MagicMock()

        # Mock report
        source.report = MagicMock()
        source.ctx = MagicMock()

        return source


class TestUpstreamAliasFiltering:
    """Test upstream alias filtering logic."""

    def test_filter_upstream_aliases_basic(self, mssql_source):
        """Test basic filtering of TSQL aliases."""
        upstream_urns = [
            # Real table in schema_resolver
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            # Alias (not in schema_resolver or discovered_datasets)
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.t,PROD)",
            # Another alias
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.src,PROD)",
        ]

        # Implement _filter_upstream_aliases method
        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep only the real table
        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_keeps_real_tables_in_schema_resolver(self, mssql_source):
        """Test that tables in schema_resolver are kept."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep all real tables
        assert len(filtered) == 2

    def test_filter_keeps_real_tables_in_discovered_datasets(self, mssql_source):
        """Test that discovered tables without schemas are kept."""
        # Table in discovered_datasets but not in schema_resolver
        # (This can happen if schema loading failed but table was discovered)
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
        ]

        # Clear schema_resolver to simulate missing schema
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should still keep it because it's in discovered_datasets
        assert len(filtered) == 1

    def test_filter_keeps_cross_database_references(self, mssql_source):
        """Test that cross-database references are kept."""
        upstream_urns = [
            # Cross-DB table (different database)
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db2.dbo.cross_db_table,PROD)",
            # Same-DB alias (should be filtered)
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep cross-DB table, filter same-DB alias
        assert len(filtered) == 1
        assert "db2" in filtered[0]

    def test_filter_removes_common_aliases(self, mssql_source):
        """Test filtering of common TSQL alias patterns."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            # Common single-letter aliases
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.t,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.s,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.d,PROD)",
            # Common word aliases
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.src,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.dst,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.temp,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep only the real table
        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_handles_temp_tables_with_hash(self, mssql_source):
        """Test that actual MSSQL temp tables (#temp) are filtered."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            # MSSQL temp table
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.#temp_table,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should filter temp table
        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_empty_list(self, mssql_source):
        """Test filtering with empty upstream list."""
        filtered = mssql_source._filter_upstream_aliases([])

        assert filtered == []

    def test_filter_preserves_order(self, mssql_source):
        """Test that filtering preserves the order of kept tables."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias2,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        assert len(filtered) == 2
        assert "real_table" in filtered[0]
        assert "another_real_table" in filtered[1]


class TestIsTempTableForAliases:
    """Test is_temp_table() method for alias detection."""

    def test_is_temp_table_real_table_in_schema_resolver(self, mssql_source):
        """Test that real tables with schemas are not marked as temp."""
        assert not mssql_source.is_temp_table("db1.dbo.real_table")

    def test_is_temp_table_real_table_in_discovered_datasets(self, mssql_source):
        """Test that discovered real tables are not marked as temp."""
        # Clear schema_resolver
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        assert not mssql_source.is_temp_table("db1.dbo.real_table")

    def test_is_temp_table_undiscovered_same_db(self, mssql_source):
        """Test that undiscovered same-DB tables are marked as temp (likely aliases)."""
        # Clear schema_resolver
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        assert mssql_source.is_temp_table("db1.dbo.unknown_alias")

    def test_is_temp_table_hash_prefix(self, mssql_source):
        """Test that tables with # prefix are marked as temp."""
        assert mssql_source.is_temp_table("db1.dbo.#temp_table")

    def test_is_temp_table_cross_db_undiscovered(self, mssql_source):
        """Test that cross-DB undiscovered tables are NOT marked as temp."""
        # Clear schema_resolver
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        # Cross-DB table not in discovered_datasets
        # Current implementation doesn't distinguish - this test documents current behavior
        result = mssql_source.is_temp_table("other_db.dbo.unknown_table")

        # This will be True with schema_resolver approach (treats all undiscovered as temp)
        # This is the known limitation we're addressing with upstream filtering
        assert result


class TestUpstreamFilteringIntegration:
    """Integration tests for upstream filtering in lineage generation."""

    def test_procedure_with_update_alias(self, mssql_source):
        """Test filtering aliases from UPDATE statement."""
        # Simulates: UPDATE t SET col = val FROM db1.dbo.users t
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.users,PROD)",  # Real table
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.t,PROD)",  # Alias
        ]

        # Mock users table as discovered
        mssql_source.discovered_datasets.add("db1.dbo.users")
        schema_resolver = mssql_source.get_schema_resolver()
        original_has_urn = schema_resolver.has_urn.side_effect

        def new_has_urn(urn):
            if "users" in urn:
                return True
            return original_has_urn(urn) if callable(original_has_urn) else False

        schema_resolver.has_urn = MagicMock(side_effect=new_has_urn)

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep only users table
        assert len(filtered) == 1
        assert "users" in filtered[0]

    def test_procedure_with_delete_alias(self, mssql_source):
        """Test filtering aliases from DELETE statement."""
        # Simulates: DELETE d FROM db1.dbo.logs d WHERE ...
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.logs,PROD)",  # Real table
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.d,PROD)",  # Alias
        ]

        # Mock logs table as discovered
        mssql_source.discovered_datasets.add("db1.dbo.logs")
        schema_resolver = mssql_source.get_schema_resolver()
        original_has_urn = schema_resolver.has_urn.side_effect

        def new_has_urn(urn):
            if "logs" in urn:
                return True
            return original_has_urn(urn) if callable(original_has_urn) else False

        schema_resolver.has_urn = MagicMock(side_effect=new_has_urn)

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep only logs table
        assert len(filtered) == 1
        assert "logs" in filtered[0]

    def test_procedure_with_cross_db_and_alias(self, mssql_source):
        """Test mixed case: real tables from multiple DBs plus aliases."""
        upstream_urns = [
            # Real table in db1
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.orders,PROD)",
            # Cross-DB real table
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db2.dbo.customers,PROD)",
            # Alias in db1
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.o,PROD)",
            # Another alias
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.temp,PROD)",
        ]

        # Mock tables
        mssql_source.discovered_datasets.add("db1.dbo.orders")
        mssql_source.discovered_datasets.add("db2.dbo.customers")

        schema_resolver = mssql_source.get_schema_resolver()

        def has_urn_for_test(urn):
            return "orders" in urn or "customers" in urn

        schema_resolver.has_urn = MagicMock(side_effect=has_urn_for_test)

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep only real tables
        assert len(filtered) == 2
        assert any("orders" in urn for urn in filtered)
        assert any("customers" in urn for urn in filtered)
