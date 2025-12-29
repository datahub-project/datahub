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

        # Mock get_schema_resolver method
        def mock_get_schema_resolver() -> MagicMock:
            return schema_resolver

        source.get_schema_resolver = mock_get_schema_resolver  # type: ignore[method-assign,assignment]

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
        """Test that cross-DB undiscovered tables ARE marked as temp (filtered)."""
        # Clear schema_resolver
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        # Cross-DB table not in discovered_datasets and not in schema_resolver
        # No evidence it's a real table - filter it out
        result = mssql_source.is_temp_table("other_db.dbo.unknown_table")

        # Tables not in schema_resolver and not matching patterns are filtered
        # We don't assume cross-DB references are real without evidence
        assert result


class TestPlatformInstancePrefixHandling:
    """Test platform_instance prefix stripping logic."""

    def test_filter_with_platform_instance_prefix_in_urn(self, mssql_source):
        """Test that platform_instance prefix is correctly stripped from URN names."""
        # URNs include platform_instance prefix, but discovered_datasets doesn't
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias,PROD)",
        ]

        # discovered_datasets has entries WITHOUT platform_instance prefix
        assert "db1.dbo.real_table" in mssql_source.discovered_datasets

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep real_table (found after stripping prefix), filter alias
        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_handles_mixed_prefix_formats(self, mssql_source):
        """Test filtering when some tables have prefix and some don't."""
        upstream_urns = [
            # With platform_instance prefix
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            # Without platform_instance prefix (shouldn't happen but handle gracefully)
            "urn:li:dataset:(urn:li:dataPlatform:mssql,db1.dbo.another_real_table,PROD)",
            # Alias with prefix
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.unknown,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep both real tables regardless of prefix format
        assert len(filtered) == 2
        assert any("real_table" in urn for urn in filtered)
        assert any("another_real_table" in urn for urn in filtered)


class TestDifferentAliasNames:
    """Test filtering of various alias naming patterns found in production."""

    def test_filter_target_alias(self, mssql_source):
        """Test filtering 'target' alias (found in addAverageDailyPremiumDiscount)."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.timeseries.dbo.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.timeseries.dbo.target,PROD)",
        ]

        # Mock timeseries.dbo.table1 as discovered
        mssql_source.discovered_datasets.add("timeseries.dbo.table1")
        schema_resolver = mssql_source.get_schema_resolver()
        schema_resolver.has_urn = lambda urn: "table1" in urn

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        assert len(filtered) == 1
        assert "table1" in filtered[0]
        assert "target" not in filtered[0]

    def test_filter_multiple_different_aliases(self, mssql_source):
        """Test filtering multiple different alias names in one procedure."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.dst,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.src,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.target,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.temp,PROD)",
        ]

        filtered = mssql_source._filter_upstream_aliases(upstream_urns)

        # Should keep only real_table, filter all aliases
        assert len(filtered) == 1
        assert "real_table" in filtered[0]


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


class TestErrorHandling:
    """Tests for error handling and edge cases."""

    def test_is_qualified_table_urn_malformed_urn(self, mssql_source):
        """Test _is_qualified_table_urn handles malformed URNs gracefully."""
        # Malformed URNs should return False without raising exceptions
        malformed_urns = [
            "",
            "not-a-urn",
            "urn:li:invalid",
            "urn:li:dataset:()",
            "urn:li:dataset:(bad,data)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,,PROD)",
        ]

        for urn in malformed_urns:
            result = mssql_source._is_qualified_table_urn(urn)
            # Should return False for all malformed URNs, not raise
            assert result is False, f"Expected False for malformed URN: {urn}"

    def test_is_qualified_table_urn_with_platform_instance_edge_cases(
        self, mssql_source
    ):
        """Test platform instance prefix handling edge cases."""
        # Empty platform instance
        result = mssql_source._is_qualified_table_urn(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,db.schema.table,PROD)",
            platform_instance="",
        )
        assert result is True

        # Platform instance that's a prefix of the table name
        result = mssql_source._is_qualified_table_urn(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test.db.schema.table,PROD)",
            platform_instance="test",
        )
        assert result is True  # After stripping "test.", "db.schema.table" has 3 parts

        # Platform instance that doesn't match
        result = mssql_source._is_qualified_table_urn(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,other_instance.db.schema.table,PROD)",
            platform_instance="test_instance",
        )
        # No stripping occurs, "other_instance.db.schema.table" has 4 parts
        assert result is True

    def test_filter_upstream_aliases_empty_input(self, mssql_source):
        """Test _filter_upstream_aliases handles empty input."""
        result = mssql_source._filter_upstream_aliases([])
        assert result == []

    def test_filter_upstream_aliases_all_malformed(self, mssql_source):
        """Test _filter_upstream_aliases when all URNs are malformed."""
        malformed_urns = [
            "not-a-urn",
            "urn:li:invalid",
            "",
        ]
        # Conservative behavior: malformed URNs are kept (not filtered out)
        # This prevents accidentally losing valid lineage due to parse errors
        result = mssql_source._filter_upstream_aliases(malformed_urns)
        assert len(result) == len(malformed_urns)

    def test_filter_upstream_aliases_mixed_valid_and_malformed(self, mssql_source):
        """Test _filter_upstream_aliases with mix of valid and malformed URNs."""
        mixed_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "not-a-urn",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
        ]
        result = mssql_source._filter_upstream_aliases(mixed_urns)
        # Should keep valid URNs AND malformed ones (conservative approach)
        assert len(result) == 3

    def test_is_temp_table_exception_handling(self, mssql_source):
        """Test is_temp_table handles exceptions gracefully."""
        # Mock schema_resolver to raise exception
        schema_resolver = mssql_source.get_schema_resolver()
        schema_resolver.has_urn = MagicMock(side_effect=Exception("Simulated error"))

        # Returns True on exception (safer: exclude uncertain items from lineage)
        # Better to miss a real table than to include spurious aliases
        # Logs a warning but doesn't raise
        result = mssql_source.is_temp_table("db.schema.table")
        assert result is True


class TestColumnLineageFiltering:
    """Test column lineage filtering in _filter_procedure_lineage."""

    def test_filter_column_lineage_with_aliases(self, mssql_source):
        """Test that column lineage with alias tables is filtered out."""
        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.metadata.schema_classes import (
            DataJobInputOutputClass,
            FineGrainedLineageClass,
            FineGrainedLineageDownstreamTypeClass,
            FineGrainedLineageUpstreamTypeClass,
        )

        # Create MCPs with column lineage including aliases
        mcps = [
            MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataJob:(urn:li:dataFlow:(mssql,test_proc,PROD),test_proc,PROD)",
                aspect=DataJobInputOutputClass(
                    inputDatasets=[
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,alias_table,PROD)",  # 1-part alias
                    ],
                    outputDatasets=[
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD)",  # 1-part alias
                    ],
                    fineGrainedLineages=[
                        # Valid column lineage (3-part tables, in schema_resolver)
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD),col1)"
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                            downstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD),col2)"
                            ],
                        ),
                        # Column lineage with 1-part alias upstream (filtered by qualification check)
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,alias_table,PROD),col3)"
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                            downstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD),col4)"
                            ],
                        ),
                        # Column lineage with 1-part alias downstream (filtered by qualification check)
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD),col5)"
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                            downstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD),col6)"
                            ],
                        ),
                        # Column lineage with 3-part table NOT in schema_resolver (filtered by alias check)
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db3.dbo.undiscovered_table,PROD),col7)"
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                            downstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD),col8)"
                            ],
                        ),
                    ],
                ),
            )
        ]

        # Filter the MCPs
        filtered_mcps = list(mssql_source._filter_procedure_lineage(mcps, "test_proc"))

        # Should have 1 MCP
        assert len(filtered_mcps) == 1

        aspect = filtered_mcps[0].aspect
        assert isinstance(aspect, DataJobInputOutputClass)

        # Check that aliases are filtered from inputDatasets and outputDatasets
        assert len(aspect.inputDatasets) == 1
        assert "test_instance.db1.dbo.real_table" in aspect.inputDatasets[0]

        assert len(aspect.outputDatasets) == 1
        assert "test_instance.db1.dbo.another_real_table" in aspect.outputDatasets[0]

        # Check that column lineage with aliases is filtered and remapped
        assert aspect.fineGrainedLineages is not None
        # Expecting 2 entries: original valid one + remapped one (dst → another_real_table)
        assert len(aspect.fineGrainedLineages) == 2

        # Both entries should have real_table as upstream and another_real_table as downstream
        for cll in aspect.fineGrainedLineages:
            assert cll.upstreams is not None and len(cll.upstreams) > 0
            assert cll.downstreams is not None and len(cll.downstreams) > 0
            assert "test_instance.db1.dbo.real_table" in cll.upstreams[0]
            assert "test_instance.db1.dbo.another_real_table" in cll.downstreams[0]

    def test_filter_column_lineage_all_filtered(self, mssql_source):
        """Test that when all column lineage is filtered, fineGrainedLineages is None."""
        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.metadata.schema_classes import (
            DataJobInputOutputClass,
            FineGrainedLineageClass,
            FineGrainedLineageDownstreamTypeClass,
            FineGrainedLineageUpstreamTypeClass,
        )

        mcps = [
            MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataJob:(urn:li:dataFlow:(mssql,test_proc,PROD),test_proc,PROD)",
                aspect=DataJobInputOutputClass(
                    inputDatasets=[
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
                    ],
                    outputDatasets=[
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
                    ],
                    fineGrainedLineages=[
                        # All column lineage with aliases (should be filtered)
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,alias,PROD),col1)"
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                            downstreams=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD),col2)"
                            ],
                        ),
                    ],
                ),
            )
        ]

        filtered_mcps = list(mssql_source._filter_procedure_lineage(mcps, "test_proc"))

        assert len(filtered_mcps) == 1
        aspect = filtered_mcps[0].aspect
        assert isinstance(aspect, DataJobInputOutputClass)

        # fineGrainedLineages should be None when all are filtered
        assert aspect.fineGrainedLineages is None
