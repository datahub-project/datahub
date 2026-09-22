"""
Unit tests for MSSQL upstream alias filtering in stored procedures.

Tests the filtering of spurious TSQL aliases that appear in UPDATE/DELETE statements
like: UPDATE t SET col = val FROM schema.table t
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_datajob_input_output_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.mssql.alias_filter import MSSQLAliasFilter
from datahub.ingestion.source.sql.mssql.job_models import (
    MSSQLProceduresContainer,
    ProcedureDependencies,
    ProcedureDependency,
    ProcedureLineageStream,
    StoredProcedure,
)
from datahub.ingestion.source.sql.mssql.source import (
    SQLServerConfig,
    SQLServerSource,
    _is_permission_denied,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
)


class _DriverError(Exception):
    """Stand-in for a pytds driver error, which carries the server error number."""

    def __init__(self, message: str, msg_no: int):
        super().__init__(message)
        self.msg_no = msg_no


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

        source.discovered_datasets = {
            "test_instance.db1.dbo.real_table",
            "test_instance.db1.dbo.another_real_table",
            "test_instance.db2.dbo.cross_db_table",
            # Also add without prefix for some tests
            "db1.dbo.real_table",
            "db1.dbo.another_real_table",
            "db2.dbo.cross_db_table",
        }

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

        def mock_get_schema_resolver() -> MagicMock:
            return schema_resolver

        source.get_schema_resolver = mock_get_schema_resolver  # type: ignore[method-assign,assignment]

        source.aggregator = MagicMock()

        source.report = MagicMock()
        source.ctx = MagicMock()

        # Create MSSQLAliasFilter (the new home for alias filtering methods)
        source.tsql_alias_cleaner = MSSQLAliasFilter(
            is_discovered_table=source.is_discovered_table,
            platform_instance="test_instance",
        )

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

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_keeps_real_tables_in_schema_resolver(self, mssql_source):
        """Test that tables in schema_resolver are kept."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
        ]

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 2

    def test_filter_keeps_real_tables_in_discovered_datasets(self, mssql_source):
        """Test that discovered tables without schemas are kept."""
        # Table in discovered_datasets but not in schema_resolver
        # (This can happen if schema loading failed but table was discovered)
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
        ]

        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 1

    def test_filter_keeps_cross_database_references(self, mssql_source):
        """Test that cross-database references are kept."""
        upstream_urns = [
            # Cross-DB table (different database)
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db2.dbo.cross_db_table,PROD)",
            # Same-DB alias (should be filtered)
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias,PROD)",
        ]

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

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

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_handles_temp_tables_with_hash(self, mssql_source):
        """Test that actual MSSQL temp tables (#temp) are filtered."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            # MSSQL temp table
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.#temp_table,PROD)",
        ]

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 1
        assert "real_table" in filtered[0]

    def test_filter_empty_list(self, mssql_source):
        """Test filtering with empty upstream list."""
        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases([])

        assert filtered == []

    def test_filter_preserves_order(self, mssql_source):
        """Test that filtering preserves the order of kept tables."""
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.alias2,PROD)",
        ]

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 2
        assert "real_table" in filtered[0]
        assert "another_real_table" in filtered[1]


class TestIsTempTableForAliases:
    """Test is_discovered_table() method for real table detection."""

    def test_is_discovered_table_real_table_in_schema_resolver(self, mssql_source):
        """Test that real tables with schemas are marked as discovered."""
        assert mssql_source.is_discovered_table("db1.dbo.real_table")

    def test_is_discovered_table_real_table_in_discovered_datasets(self, mssql_source):
        """Test that discovered real tables are marked as discovered."""
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        assert mssql_source.is_discovered_table("db1.dbo.real_table")

    def test_is_discovered_table_undiscovered_same_db(self, mssql_source):
        """Test that undiscovered same-DB tables are not marked as discovered (likely aliases)."""
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        assert not mssql_source.is_discovered_table("db1.dbo.unknown_alias")

    def test_is_discovered_table_hash_prefix(self, mssql_source):
        """Test that tables with # prefix are not marked as discovered (temp tables)."""
        assert not mssql_source.is_discovered_table("db1.dbo.#temp_table")

    def test_is_discovered_table_cross_db_undiscovered(self, mssql_source):
        """Test that cross-DB undiscovered tables are NOT marked as discovered."""
        mssql_source.get_schema_resolver().has_urn = MagicMock(return_value=False)

        result = mssql_source.is_discovered_table("other_db.dbo.unknown_table")

        assert not result


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

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

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

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

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

        mssql_source.discovered_datasets.add("timeseries.dbo.table1")
        schema_resolver = mssql_source.get_schema_resolver()
        schema_resolver.has_urn = lambda urn: "table1" in urn

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

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

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 1
        assert "real_table" in filtered[0]


class TestUpstreamFilteringIntegration:
    """Integration tests for upstream filtering in lineage generation."""

    def test_procedure_with_update_alias(self, mssql_source):
        """Test filtering aliases from UPDATE statement."""
        # Simulates: UPDATE t SET col = val FROM db1.dbo.users t
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.users,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.t,PROD)",
        ]

        mssql_source.discovered_datasets.add("db1.dbo.users")
        schema_resolver = mssql_source.get_schema_resolver()
        original_has_urn = schema_resolver.has_urn.side_effect

        def new_has_urn(urn):
            if "users" in urn:
                return True
            return original_has_urn(urn) if callable(original_has_urn) else False

        schema_resolver.has_urn = MagicMock(side_effect=new_has_urn)

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 1
        assert "users" in filtered[0]

    def test_procedure_with_delete_alias(self, mssql_source):
        """Test filtering aliases from DELETE statement."""
        # Simulates: DELETE d FROM db1.dbo.logs d WHERE ...
        upstream_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.logs,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.d,PROD)",
        ]

        mssql_source.discovered_datasets.add("db1.dbo.logs")
        schema_resolver = mssql_source.get_schema_resolver()
        original_has_urn = schema_resolver.has_urn.side_effect

        def new_has_urn(urn):
            if "logs" in urn:
                return True
            return original_has_urn(urn) if callable(original_has_urn) else False

        schema_resolver.has_urn = MagicMock(side_effect=new_has_urn)

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

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

        mssql_source.discovered_datasets.add("db1.dbo.orders")
        mssql_source.discovered_datasets.add("db2.dbo.customers")

        schema_resolver = mssql_source.get_schema_resolver()

        def has_urn_for_test(urn):
            return "orders" in urn or "customers" in urn

        schema_resolver.has_urn = MagicMock(side_effect=has_urn_for_test)

        filtered = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            upstream_urns
        )

        assert len(filtered) == 2
        assert any("orders" in urn for urn in filtered)
        assert any("customers" in urn for urn in filtered)


class TestErrorHandling:
    """Tests for error handling and edge cases."""

    def test_is_qualified_table_urn_malformed_urn(self, mssql_source):
        """Test _is_qualified_table_urn handles malformed URNs gracefully."""
        malformed_urns = [
            "",
            "not-a-urn",
            "urn:li:invalid",
            "urn:li:dataset:()",
            "urn:li:dataset:(bad,data)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,,PROD)",
        ]

        for urn in malformed_urns:
            result = mssql_source.tsql_alias_cleaner._is_qualified_table_urn(urn)
            # Should return False for all malformed URNs, not raise
            assert result is False, f"Expected False for malformed URN: {urn}"

    def test_is_qualified_table_urn_with_platform_instance_edge_cases(
        self, mssql_source
    ):
        """Test platform instance prefix handling with fixture's platform_instance='test_instance'."""
        result = mssql_source.tsql_alias_cleaner._is_qualified_table_urn(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,db.schema.table,PROD)"
        )
        assert result is True

        result = mssql_source.tsql_alias_cleaner._is_qualified_table_urn(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db.schema.table,PROD)"
        )
        assert result is True

        result = mssql_source.tsql_alias_cleaner._is_qualified_table_urn(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,other_instance.db.schema.table,PROD)"
        )
        assert result is True

    def test_filter_upstream_aliases_empty_input(self, mssql_source):
        """Test _filter_upstream_aliases handles empty input."""
        result = mssql_source.tsql_alias_cleaner._filter_upstream_aliases([])
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
        result = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(
            malformed_urns
        )
        assert len(result) == len(malformed_urns)

    def test_filter_upstream_aliases_mixed_valid_and_malformed(self, mssql_source):
        """Test _filter_upstream_aliases with mix of valid and malformed URNs."""
        mixed_urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.real_table,PROD)",
            "not-a-urn",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,test_instance.db1.dbo.another_real_table,PROD)",
        ]
        result = mssql_source.tsql_alias_cleaner._filter_upstream_aliases(mixed_urns)
        assert len(result) == 3

    def test_is_discovered_table_exception_handling(self, mssql_source):
        """Test is_discovered_table handles exceptions gracefully."""
        schema_resolver = mssql_source.get_schema_resolver()
        schema_resolver.has_urn = MagicMock(side_effect=Exception("Simulated error"))

        result = mssql_source.is_discovered_table("db.schema.table")
        assert result is False


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
        filtered_mcps = list(
            mssql_source.tsql_alias_cleaner.filter_procedure_lineage(mcps, "test_proc")
        )

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

        filtered_mcps = list(
            mssql_source.tsql_alias_cleaner.filter_procedure_lineage(mcps, "test_proc")
        )

        assert len(filtered_mcps) == 1
        aspect = filtered_mcps[0].aspect
        assert isinstance(aspect, DataJobInputOutputClass)

        # fineGrainedLineages should be None when all are filtered
        assert aspect.fineGrainedLineages is None


class TestProcedureCallLineagePreserved:
    """A dispatcher procedure's lineage is datajob-only and must survive the filter."""

    CALLEE = (
        "urn:li:dataJob:(urn:li:dataFlow:"
        "(mssql,test_instance.db1.dbo.stored_procedures,PROD),callee_proc)"
    )

    def _datajob_only_mcps(self):
        return [
            MetadataChangeProposalWrapper(
                entityUrn=(
                    "urn:li:dataJob:(urn:li:dataFlow:"
                    "(mssql,test_instance.db1.dbo.stored_procedures,PROD),caller_proc)"
                ),
                aspect=DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=[],
                    inputDatajobs=[self.CALLEE],
                ),
            )
        ]

    def _filter(self, mssql_source):
        return MSSQLAliasFilter(
            is_discovered_table=mssql_source.is_discovered_table,
            platform_instance="test_instance",
        )

    def test_datajob_only_lineage_is_kept(self, mssql_source):
        # The keep-check used to inspect the dataset arrays alone, so this aspect was
        # discarded and the procedure-to-procedure edge lost.
        filtered = list(
            self._filter(mssql_source).filter_procedure_lineage(
                self._datajob_only_mcps(), "caller_proc"
            )
        )

        assert len(filtered) == 1
        aspect = filtered[0].aspect
        assert isinstance(aspect, DataJobInputOutputClass)
        assert aspect.inputDatajobs == [self.CALLEE]

    @pytest.mark.parametrize(
        "incremental_lineage,expected",
        [(True, ChangeTypeClass.PATCH), (False, ChangeTypeClass.UPSERT)],
    )
    def test_call_only_lineage_follows_the_normal_transport_rule(
        self, mssql_source, incremental_lineage, expected
    ):
        # Call-only lineage is not special: whether it is sent as a patch is the
        # ordinary `incremental_lineage` choice, the same one made for the dataset
        # lineage on every other procedure.
        mssql_source.config.incremental_lineage = incremental_lineage
        workunits = [
            MetadataWorkUnit(id="test", mcp=mcp) for mcp in self._datajob_only_mcps()
        ]

        out = list(mssql_source._convert_procedure_lineage_to_patch(workunits))

        assert out, incremental_lineage
        assert all(wu.metadata.changeType == expected for wu in out), (
            incremental_lineage
        )

    def test_aspect_with_nothing_left_is_still_dropped(self, mssql_source):
        # The original behaviour must hold when every field ends up empty.
        mcps = [
            MetadataChangeProposalWrapper(
                entityUrn=(
                    "urn:li:dataJob:(urn:li:dataFlow:"
                    "(mssql,test_instance.db1.dbo.stored_procedures,PROD),caller_proc)"
                ),
                aspect=DataJobInputOutputClass(
                    inputDatasets=[
                        "urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD)"
                    ],
                    outputDatasets=[],
                    inputDatajobs=[],
                ),
            )
        ]

        filtered = list(
            mssql_source.tsql_alias_cleaner.filter_procedure_lineage(
                mcps, "caller_proc"
            )
        )

        assert filtered == []


class TestProcedureDependencyPermissionFailure:
    """A procedure whose dependencies are unreadable must not take down the schema.

    `loop_stored_procedures` is a generator handled only at the schema level, so an
    uncaught error here drops every procedure that would have followed it.
    """

    @staticmethod
    def _procedure(name):
        flow = MSSQLProceduresContainer(
            name="db1.dbo.stored_procedures",
            env="PROD",
            db="db1",
            platform_instance="test_instance",
        )
        return StoredProcedure(db="db1", schema="dbo", name=name, flow=flow)

    def test_denial_on_the_first_read_skips_the_second(self, mssql_source):
        # A denial on the upstream read applies to the downstream read of the same
        # procedure, so that one must be skipped rather than retried into a second
        # round-trip and a duplicate warning.
        mssql_source.report = SQLSourceReport()
        mssql_source._dependency_reads_denied = set()
        downstream_calls = []

        def record_downstream(conn, procedure):
            downstream_calls.append(procedure.name)
            return ProcedureLineageStream(dependencies=[])

        with (
            patch.object(
                SQLServerSource,
                "_get_procedure_upstream",
                side_effect=self._denial(229),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_downstream",
                staticmethod(record_downstream),
            ),
        ):
            result = mssql_source._get_procedure_dependencies(
                MagicMock(), self._procedure("proc_a")
            )

        # None, not an empty stream: the properties are omitted rather than reported
        # as "no dependencies".
        assert result.upstream is None
        assert result.downstream is None
        assert downstream_calls == []
        assert len(mssql_source.report.warnings) == 1

    def test_a_query_timeout_costs_only_the_dependency_properties(self, mssql_source):
        # pytds raises a query timeout as the builtin TimeoutError -- an OSError, not
        # a pytds.Error -- so SQLAlchemy never wraps it into a DBAPIError. Handled
        # here it costs the dependency properties, the same as a deadlock on the very
        # same query; left to the per-procedure guard it would cost the procedure.
        mssql_source.report = SQLSourceReport()
        mssql_source._dependency_reads_denied = set()

        with patch.object(
            SQLServerSource,
            "_get_procedure_upstream",
            side_effect=TimeoutError("query timed out"),
        ):
            result = mssql_source._read_dependency_stream(
                lambda: SQLServerSource._get_procedure_upstream(
                    MagicMock(), self._procedure("proc_a")
                ),
                self._procedure("proc_a"),
            )

        assert result is None
        assert len(mssql_source.report.warnings) == 1
        # Transient, so it must not short-circuit the rest of the database the way a
        # permission denial does.
        assert mssql_source._dependency_reads_denied == set()

    def test_a_dead_socket_still_stops_the_schema(self, mssql_source):
        # ConnectionError is an OSError too, but it means the socket is gone rather
        # than one query giving up. It has to keep reaching the caller.
        mssql_source.report = SQLSourceReport()

        def read():
            raise ConnectionResetError("peer reset")

        with pytest.raises(ConnectionError):
            mssql_source._read_dependency_stream(read, self._procedure("proc_a"))

    def test_denied_direction_omits_only_its_own_property(self, mssql_source):
        # A denied upstream must neither discard the successful downstream nor write an
        # empty `procedure_depends_on`, which would read as "queried fine, none found".
        mssql_source.report = SQLSourceReport()
        downstream = ProcedureLineageStream(
            dependencies=[
                ProcedureDependency(
                    db="db1",
                    schema="dbo",
                    name="some_table",
                    type="USER_TABLE",
                    env="PROD",
                    server=None,
                )
            ]
        )
        with (
            patch.object(
                SQLServerSource,
                "_get_procedure_upstream",
                side_effect=OperationalError("stmt", {}, Exception("denied")),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_downstream",
                staticmethod(lambda conn, procedure: downstream),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_code",
                staticmethod(lambda conn, procedure: (None, None)),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_inputs",
                staticmethod(lambda conn, procedure: []),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_properties",
                staticmethod(lambda conn, procedure: {}),
            ),
        ):
            workunits = list(
                mssql_source._process_stored_procedure(
                    MagicMock(), self._procedure("proc_a")
                )
            )

        properties = next(
            wu.metadata.aspect.customProperties
            for wu in workunits
            if isinstance(wu.metadata.aspect, DataJobInfoClass)
        )
        assert "procedure_depends_on" not in properties
        assert "some_table" in properties["depending_on_procedure"]

    def test_later_procedures_still_processed(self, mssql_source):
        # The regression that matters: loop_stored_procedures is a generator, so an
        # uncaught error used to drop every procedure after the failing one.
        mssql_source.report = SQLSourceReport()
        mssql_source.stored_procedures = []

        def selective_failure(conn, procedure):
            if procedure.name == "proc_a":
                # Transient and per-statement, not a denial: a denial would disable
                # the reads for the whole database, which is a different path.
                raise OperationalError(
                    "stmt", {}, _DriverError("Deadlock victim", 1205)
                )
            return ProcedureLineageStream(dependencies=[])

        def no_dependencies(conn, procedure):
            return ProcedureLineageStream(dependencies=[])

        inspector = MagicMock()
        inspector.engine.url.database = "db1"

        with (
            patch.object(
                SQLServerSource,
                "_get_stored_procedures",
                staticmethod(
                    lambda conn, db_name, schema: [
                        dict(db="db1", schema="dbo", name=name)
                        for name in ("proc_a", "proc_b", "proc_c")
                    ]
                ),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_upstream",
                staticmethod(selective_failure),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_downstream",
                staticmethod(no_dependencies),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_code",
                staticmethod(lambda conn, procedure: (None, None)),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_inputs",
                staticmethod(lambda conn, procedure: []),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_properties",
                staticmethod(lambda conn, procedure: {}),
            ),
        ):
            list(
                mssql_source.loop_stored_procedures(
                    inspector, "dbo", mssql_source.config
                )
            )

        # All three reached lineage extraction, including the two after the failure.
        assert [p.name for p in mssql_source.stored_procedures] == [
            "proc_a",
            "proc_b",
            "proc_c",
        ]
        assert len(mssql_source.report.warnings) == 1

    def test_non_dbapi_error_is_caught_by_the_per_procedure_guard(self, mssql_source):
        # The dependency reads have their own catch; this covers the outer guard, which
        # is the only thing standing between an unrelated error and the rest of the
        # schema. _get_procedure_code raising is one realistic way to get here.
        mssql_source.report = SQLSourceReport()
        mssql_source.stored_procedures = []

        def failing_code(conn, procedure):
            if procedure.name == "proc_a":
                raise RuntimeError("something unrelated broke")
            return (None, None)

        inspector = MagicMock()
        inspector.engine.url.database = "db1"

        with (
            patch.object(
                SQLServerSource,
                "_get_stored_procedures",
                staticmethod(
                    lambda conn, db_name, schema: [
                        dict(db="db1", schema="dbo", name=name)
                        for name in ("proc_a", "proc_b")
                    ]
                ),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_dependencies",
                lambda self, conn, procedure: ProcedureDependencies(
                    upstream=None, downstream=None
                ),
            ),
            patch.object(
                SQLServerSource, "_get_procedure_code", staticmethod(failing_code)
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_inputs",
                staticmethod(lambda conn, procedure: []),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_properties",
                staticmethod(lambda conn, procedure: {}),
            ),
        ):
            list(
                mssql_source.loop_stored_procedures(
                    inspector, "dbo", mssql_source.config
                )
            )

        # proc_a died, proc_b still got through.
        assert [p.name for p in mssql_source.stored_procedures] == ["proc_b"]
        # A failure, not a warning: StaleEntityRemovalHandler skips soft-deletion only
        # when the source reports one, and proc_a was not emitted this run.
        assert len(mssql_source.report.failures) == 1
        assert len(mssql_source.report.warnings) == 0

    def test_dead_connection_stops_the_schema_instead_of_grinding_on(
        self, mssql_source
    ):
        # pytds usually fails to invalidate the connection, so without the re-raise
        # every remaining procedure would run against a dead one, each landing in the
        # per-procedure handler. Stop at the first instead and let the schema-level
        # handler record it.
        mssql_source.report = SQLSourceReport()
        mssql_source.stored_procedures = []
        dropped = InterfaceError("stmt", {}, Exception("Server closed connection."))

        def dies_on_proc_b(conn, procedure):
            if procedure.name == "proc_a":
                return (None, None)
            raise dropped

        inspector = MagicMock()
        inspector.engine.url.database = "db1"

        with (
            patch.object(
                SQLServerSource,
                "_get_stored_procedures",
                staticmethod(
                    lambda conn, db_name, schema: [
                        dict(db="db1", schema="dbo", name=name)
                        for name in ("proc_a", "proc_b", "proc_c", "proc_d")
                    ]
                ),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_dependencies",
                lambda self, conn, procedure: ProcedureDependencies(
                    upstream=None, downstream=None
                ),
            ),
            patch.object(
                SQLServerSource, "_get_procedure_code", staticmethod(dies_on_proc_b)
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_inputs",
                staticmethod(lambda conn, procedure: []),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_properties",
                staticmethod(lambda conn, procedure: {}),
            ),
            pytest.raises(InterfaceError),
        ):
            list(
                mssql_source.loop_stored_procedures(
                    inspector, "dbo", mssql_source.config
                )
            )

        # Stopped at proc_b rather than reporting proc_b, proc_c and proc_d one by one.
        assert [p.name for p in mssql_source.stored_procedures] == ["proc_a"]
        assert len(mssql_source.report.failures) == 0

    def test_socket_failure_stops_the_schema_too(self, mssql_source):
        # pytds raises socket failures straight through rather than wrapping them, so
        # they are not DBAPI errors and the check above never sees them. The socket is
        # still gone, so every remaining procedure would fail the same way.
        mssql_source.report = SQLSourceReport()
        mssql_source.stored_procedures = []

        def dies_on_proc_b(conn, procedure):
            if procedure.name == "proc_a":
                return (None, None)
            raise ConnectionResetError("Connection reset by peer")

        inspector = MagicMock()
        inspector.engine.url.database = "db1"

        with (
            patch.object(
                SQLServerSource,
                "_get_stored_procedures",
                staticmethod(
                    lambda conn, db_name, schema: [
                        dict(db="db1", schema="dbo", name=name)
                        for name in ("proc_a", "proc_b", "proc_c")
                    ]
                ),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_dependencies",
                lambda self, conn, procedure: ProcedureDependencies(
                    upstream=None, downstream=None
                ),
            ),
            patch.object(
                SQLServerSource, "_get_procedure_code", staticmethod(dies_on_proc_b)
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_inputs",
                staticmethod(lambda conn, procedure: []),
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_properties",
                staticmethod(lambda conn, procedure: {}),
            ),
            pytest.raises(ConnectionResetError),
        ):
            list(
                mssql_source.loop_stored_procedures(
                    inspector, "dbo", mssql_source.config
                )
            )

        assert [p.name for p in mssql_source.stored_procedures] == ["proc_a"]

    @staticmethod
    def _denial(number):
        # pytds records the server error number on the wrapped driver exception.
        return OperationalError(
            "stmt", {}, _DriverError("The SELECT permission was denied", number)
        )

    def test_error_number_is_read_from_whichever_place_the_driver_left_it(self):
        # pytds exposes it as an attribute; pyodbc and pymssql only put it in the
        # message, in two different shapes.
        assert _is_permission_denied(self._denial(229))
        assert _is_permission_denied(
            OperationalError(
                "stmt",
                {},
                Exception(
                    "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
                    "The SELECT permission was denied on the object "
                    "'sys.sql_expression_dependencies' (229) (SQLExecDirectW)"
                ),
            )
        )
        assert _is_permission_denied(
            OperationalError(
                "stmt", {}, Exception("(229, b'The SELECT permission was denied')")
            )
        )
        # A deadlock is transient and per-statement, so it must not short-circuit.
        assert not _is_permission_denied(self._denial(1205))
        assert not _is_permission_denied(
            OperationalError("stmt", {}, Exception("Transaction (1205) deadlocked"))
        )

    def test_permission_denial_skips_the_rest_of_that_database_only(self, mssql_source):
        mssql_source.report = SQLSourceReport()
        mssql_source._dependency_reads_denied = set()
        calls = []

        def denied_in_db1(conn, procedure):
            calls.append(procedure.db)
            if procedure.db == "db1":
                raise self._denial(229)
            return ProcedureLineageStream(dependencies=[])

        with (
            patch.object(
                SQLServerSource, "_get_procedure_upstream", staticmethod(denied_in_db1)
            ),
            patch.object(
                SQLServerSource,
                "_get_procedure_downstream",
                staticmethod(denied_in_db1),
            ),
        ):
            mssql_source._get_procedure_dependencies(
                MagicMock(), self._procedure("proc_a")
            )
            # Second db1 procedure must not re-run the known-failing queries.
            mssql_source._get_procedure_dependencies(
                MagicMock(), self._procedure("proc_b")
            )
            other = self._procedure("proc_c")
            other.db = "db2"
            result = mssql_source._get_procedure_dependencies(MagicMock(), other)

        # proc_a's denial stopped its own downstream read too; proc_b was skipped
        # entirely; db2 is a different database and still probed both directions.
        assert calls == ["db1", "db2", "db2"]
        assert result.upstream is not None

    def test_transient_error_stays_scoped_to_one_procedure(self, mssql_source):
        # A deadlock victim is an OperationalError too. Caching on it would drop
        # dependency properties for every later procedure in the database.
        mssql_source.report = SQLSourceReport()
        mssql_source._dependency_reads_denied = set()
        deadlock = self._denial(1205)

        with (
            patch.object(
                SQLServerSource, "_get_procedure_upstream", side_effect=deadlock
            ),
            patch.object(
                SQLServerSource, "_get_procedure_downstream", side_effect=deadlock
            ),
        ):
            mssql_source._get_procedure_dependencies(
                MagicMock(), self._procedure("proc_a")
            )

        assert mssql_source._dependency_reads_denied == set()

    @staticmethod
    def _invalidated() -> OperationalError:
        dropped = OperationalError("stmt", {}, Exception("connection reset"))
        dropped.connection_invalidated = True
        return dropped

    @pytest.mark.parametrize(
        "dropped",
        # Both are DBAPIErrors, so without these checks a dead connection would be
        # filed under "grant VIEW DEFINITION". They need separate checks: pytds's
        # ClosedConnectionError is an InterfaceError its dialect does not classify as
        # a disconnect, so connection_invalidated stays False.
        [
            _invalidated(),
            InterfaceError("stmt", {}, Exception("Server closed connection.")),
        ],
        ids=["connection-invalidated", "pytds-interface-error"],
    )
    def test_a_disconnect_is_not_reported_as_a_permission_problem(
        self, mssql_source, dropped
    ):
        mssql_source.report = SQLSourceReport()

        with (
            patch.object(
                SQLServerSource, "_get_procedure_upstream", side_effect=dropped
            ),
            pytest.raises(DBAPIError),
        ):
            mssql_source._get_procedure_dependencies(
                MagicMock(), self._procedure("proc_a")
            )

        assert len(mssql_source.report.warnings) == 0


class TestUpsertDropsManualEdges:
    """Why the patch path exists: a full upsert carries no `*Edges` fields."""

    JOB = "urn:li:dataJob:(urn:li:dataFlow:(mssql,f,PROD),j)"
    UPSTREAM = "urn:li:dataset:(urn:li:dataPlatform:mssql,d,PROD)"

    def _lineage_workunit(self):
        return MetadataChangeProposalWrapper(
            entityUrn=self.JOB,
            aspect=DataJobInputOutputClass(
                inputDatasets=[self.UPSTREAM],
                outputDatasets=[],
                inputDatajobs=[],
            ),
        ).as_workunit()

    def test_flag_off_sends_the_upsert_through_untouched(self, mssql_source):
        # The transport rule, and why the patch path has to exist: with the flag off
        # the aspect goes out exactly as the source built it -- no `*Edges`, so
        # writing it replaces whatever was there.
        mssql_source.config.incremental_lineage = False
        wu = self._lineage_workunit()

        out = list(mssql_source._convert_procedure_lineage_to_patch([wu]))

        assert out == [wu]
        assert out[0].metadata.changeType == ChangeTypeClass.UPSERT
        aspect = out[0].get_aspect_of_type(DataJobInputOutputClass)
        assert aspect is not None
        assert not aspect.inputDatasetEdges
        assert not aspect.inputDatajobEdges

    def test_a_raw_mcp_upsert_is_converted_too(self, mssql_source):
        # The wrapper is what MSSQL emits today, but the guard keys on the envelope,
        # not on the aspect. A raw MCP carries one aspect and overwrites just the
        # same, so letting it past would reinstate the bug this processor prevents.
        mssql_source.config.incremental_lineage = True
        raw = MetadataChangeProposalWrapper(
            entityUrn=self.JOB,
            aspect=DataJobInputOutputClass(
                inputDatasets=[self.UPSTREAM], outputDatasets=[], inputDatajobs=[]
            ),
        ).make_mcp()
        assert isinstance(raw, MetadataChangeProposalClass)

        out = list(
            mssql_source._convert_procedure_lineage_to_patch(
                [MetadataWorkUnit(id="raw", mcp_raw=raw)]
            )
        )

        assert len(out) == 1
        assert out[0].metadata.changeType == ChangeTypeClass.PATCH

    def test_an_existing_patch_is_left_alone(self, mssql_source):
        # Re-converting a patch would double-express an already additive write. What
        # stops it is the typed-aspect lookup, not a change-type test: the patch is a
        # raw MCP, and `try_from_mcpc` deserializes only upserts, so no
        # `DataJobInputOutputClass` comes back. Not that it is serialized -- a raw
        # upsert is too, and that one does convert, as the test above shows. Pinned
        # because the guard is invisible at the call site.
        mssql_source.config.incremental_lineage = True
        already_a_patch = next(
            iter(
                convert_datajob_input_output_to_patch(
                    self.JOB,
                    DataJobInputOutputClass(
                        inputDatasets=[self.UPSTREAM],
                        outputDatasets=[],
                        inputDatajobs=[],
                    ),
                    None,
                )
            )
        )
        assert already_a_patch.get_aspect_of_type(DataJobInputOutputClass) is None

        out = list(mssql_source._convert_procedure_lineage_to_patch([already_a_patch]))

        assert out == [already_a_patch]

    def test_no_upsert_survives_the_conversion(self, mssql_source):
        # The actual regression guard: with the flag on, nothing downstream may still
        # be a full upsert of this aspect, because that is what wipes manual edges.
        # A pass-through conversion fails here.
        mssql_source.config.incremental_lineage = True

        out = list(
            mssql_source._convert_procedure_lineage_to_patch([self._lineage_workunit()])
        )

        assert len(out) == 1
        mcp = out[0].metadata
        assert isinstance(mcp, MetadataChangeProposalClass)
        assert mcp.changeType == ChangeTypeClass.PATCH
        # Not a typed upsert aspect: a patch carries a serialized JSON patch instead.
        assert out[0].get_aspect_of_type(DataJobInputOutputClass) is None
        aspect = mcp.aspect
        assert isinstance(aspect, GenericAspectClass)
        patches = json.loads(aspect.value.decode())
        assert [p["path"] for p in patches] == [f"/inputDatasetEdges/{self.UPSTREAM}"]

    def test_empty_aspect_is_dropped_without_a_warning(self, mssql_source):
        # SQL Agent job steps always emit an empty lineage aspect, so warning here
        # would fire for every job step on a default recipe. Dropping it is still
        # right -- an empty upsert would wipe manual edges.
        mssql_source.config.incremental_lineage = True
        mssql_source.report = SQLSourceReport()
        wu = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataJob:(urn:li:dataFlow:(mssql,f,PROD),step)",
            aspect=DataJobInputOutputClass(
                inputDatasets=[], outputDatasets=[], inputDatajobs=[]
            ),
        ).as_workunit()

        assert list(mssql_source._convert_procedure_lineage_to_patch([wu])) == []
        assert len(mssql_source.report.warnings) == 0

    def test_unconvertible_content_is_reported(self, mssql_source):
        # Had lineage, none of it survived conversion: that is worth surfacing.
        field = (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,{},PROD),{})"
        )
        mssql_source.config.incremental_lineage = True
        mssql_source.report = SQLSourceReport()
        wu = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataJob:(urn:li:dataFlow:(mssql,f,PROD),j)",
            aspect=DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[],
                inputDatajobs=[],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        upstreams=[field.format("d", "a")],
                        # Two downstreams can't be keyed by the patch template.
                        downstreams=[field.format("e", "b"), field.format("e", "c")],
                    )
                ],
            ),
        ).as_workunit()

        assert list(mssql_source._convert_procedure_lineage_to_patch([wu])) == []
        assert len(mssql_source.report.warnings) == 1


class TestPatchConversionOrdering:
    def test_conversion_runs_after_the_default_processors(self, mssql_source):
        """The conversion must not run in get_workunits_internal.

        AutoLowercaseUrnsProcessor and AutoResolveLineageUrnsProcessor both need the
        typed upsert aspect, and the latter skips lineage that already arrived as a
        PATCH. Appending after super() puts the conversion behind both.
        """
        base = SQLAlchemySource.get_workunit_processors(mssql_source)
        processors = mssql_source.get_workunit_processors()

        # Appended after the whole default chain, whichever of it is enabled, so every
        # default processor sees the typed upsert before the conversion runs.
        assert len(processors) == len(base) + 1
        assert processors[-1] == mssql_source._convert_procedure_lineage_to_patch


class TestPatchConverterUrnErrors:
    """A bad URN must cost its own edge, not the procedure's whole aspect."""

    JOB = "urn:li:dataJob:(urn:li:dataFlow:(mssql,f,PROD),j)"
    GOOD = "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.dbo.t,PROD)"

    def test_malformed_urns_are_skipped_not_raised(self):
        # add_input_dataset raises ValueError on a prefix mismatch but InvalidUrnError
        # on an unparseable URN, and the field adders raise only the latter.
        workunits = convert_datajob_input_output_to_patch(
            self.JOB,
            DataJobInputOutputClass(
                inputDatasets=[self.GOOD, "not-a-urn", "urn:li:dataset:(broken"],
                outputDatasets=[],
                inputDatajobs=[],
                inputDatasetFields=["urn:li:schemaField:(broken", self.GOOD],
            ),
            None,
        )

        assert len(workunits) == 1
        mcp = workunits[0].metadata
        assert isinstance(mcp, MetadataChangeProposalClass)
        aspect = mcp.aspect
        assert isinstance(aspect, GenericAspectClass)
        patches = json.loads(aspect.value.decode())
        assert [p["path"] for p in patches] == [f"/inputDatasetEdges/{self.GOOD}"]

    def test_edge_fields_are_carried_through(self):
        # The source doesn't populate these today, but a caller that does must not
        # have its lineage silently dropped.
        other = "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.dbo.u,PROD)"
        workunits = convert_datajob_input_output_to_patch(
            self.JOB,
            DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[],
                inputDatajobs=[],
                inputDatasetEdges=[EdgeClass(destinationUrn=other)],
            ),
            None,
        )

        assert len(workunits) == 1
        mcp = workunits[0].metadata
        assert isinstance(mcp, MetadataChangeProposalClass)
        aspect = mcp.aspect
        assert isinstance(aspect, GenericAspectClass)
        patches = json.loads(aspect.value.decode())
        assert [p["path"] for p in patches] == [f"/inputDatasetEdges/{other}"]
