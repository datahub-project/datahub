"""
Test MSSQL column lineage alias resolution for TSQL UPDATE statements.

This test verifies that unqualified downstream aliases (like 'dst' in "UPDATE dst")
are resolved to the real table name using qualified upstreams from the same entry.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)


@pytest.fixture
def mssql_source():
    """Create a mock MSSQL source for testing."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        username="test",
        password="test",
        database="TestDB",
        env="PROD",
        include_descriptions=False,
    )

    # Mock the parent class's __init__ to avoid DB connections
    with patch("datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__"):
        source = SQLServerSource(config, MagicMock())

        # Set platform attribute (required by is_temp_table)
        source.platform = "mssql"

        # Mock discovered_datasets to ensure is_temp_table works
        source.discovered_datasets = {
            "TestDB.dbo.staging_table",
            "TestDB.dbo.target_table",
            "TestDB.dbo.table1",
            "TestDB.dbo.table2",
        }

        # Mock schema_resolver (required by is_temp_table)
        schema_resolver = MagicMock()
        schema_resolver.platform_instance = None
        schema_resolver.has_urn = MagicMock(return_value=False)
        source.schema_resolver = schema_resolver

    return source


def test_resolve_single_upstream_alias(mssql_source):
    """
    Test resolving unqualified downstream alias when there's exactly one qualified upstream.

    Pattern: UPDATE dst SET ... FROM Table dst
    - Before: downstream='dst', upstream='TestDB.dbo.staging_table'
    - After: downstream='TestDB.dbo.staging_table', upstream='TestDB.dbo.staging_table'
    """
    aspect = DataJobInputOutputClass(
        inputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD)"
        ],
        outputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD)"
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD),share_class_id)"
                ],
                downstreams=[
                    # Unqualified alias 'dst' that should be resolved
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD),share_class_id)"
                ],
            )
        ],
    )

    mssql_source._filter_column_lineage(
        aspect=aspect,
        platform_instance=None,
        procedure_name="test_procedure",
    )

    # Column lineage should be preserved with resolved downstream
    assert aspect.fineGrainedLineages is not None
    assert len(aspect.fineGrainedLineages) == 1

    cll = aspect.fineGrainedLineages[0]
    assert len(cll.upstreams) == 1
    assert len(cll.downstreams) == 1

    # Downstream should be resolved to the qualified table name
    expected_downstream = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD),share_class_id)"
    assert cll.downstreams[0] == expected_downstream


def test_no_resolution_with_multiple_upstreams(mssql_source):
    """
    Test that we don't resolve aliases when there are multiple upstream tables.

    Pattern: UPDATE dst SET dst.col = src.col + other.col FROM T1 dst JOIN T2 src JOIN T3 other
    - Multiple upstreams: can't determine which one 'dst' refers to
    - Should filter out the unqualified downstream entirely
    """
    aspect = DataJobInputOutputClass(
        inputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.table2,PROD)",
        ],
        outputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.table1,PROD)"
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.table1,PROD),col1)",
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.table2,PROD),col2)",
                ],
                downstreams=[
                    # Unqualified alias - can't resolve with multiple upstreams
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD),result_col)"
                ],
            )
        ],
    )

    mssql_source._filter_column_lineage(
        aspect=aspect,
        platform_instance=None,
        procedure_name="test_procedure",
    )

    # Column lineage should be filtered out (no downstream after filtering)
    assert aspect.fineGrainedLineages is None


def test_keep_qualified_downstream_unchanged(mssql_source):
    """
    Test that qualified downstreams are kept unchanged.

    Pattern: UPDATE TestDB.dbo.target_table SET ...
    - Qualified downstream: no resolution needed
    """
    aspect = DataJobInputOutputClass(
        inputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD)"
        ],
        outputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.target_table,PROD)"
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD),share_class_id)"
                ],
                downstreams=[
                    # Already qualified - should be kept as-is
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.target_table,PROD),share_class_id)"
                ],
            )
        ],
    )

    mssql_source._filter_column_lineage(
        aspect=aspect,
        platform_instance=None,
        procedure_name="test_procedure",
    )

    # Column lineage should be preserved unchanged
    assert aspect.fineGrainedLineages is not None
    assert len(aspect.fineGrainedLineages) == 1

    cll = aspect.fineGrainedLineages[0]
    assert len(cll.upstreams) == 1
    assert len(cll.downstreams) == 1

    # Downstream should be unchanged
    expected_downstream = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.target_table,PROD),share_class_id)"
    assert cll.downstreams[0] == expected_downstream


def test_multiple_columns_same_alias(mssql_source):
    """
    Test resolving multiple column lineage entries with the same alias pattern.

    Pattern: UPDATE dst SET dst.col1 = src.col1, dst.col2 = src.col2 FROM Table dst
    - Multiple columns but same alias pattern
    - All should be resolved
    """
    aspect = DataJobInputOutputClass(
        inputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD)"
        ],
        outputDatasets=[
            "urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD)"
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD),col1)"
                ],
                downstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD),col1)"
                ],
            ),
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,TestDB.dbo.staging_table,PROD),col2)"
                ],
                downstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,dst,PROD),col2)"
                ],
            ),
        ],
    )

    mssql_source._filter_column_lineage(
        aspect=aspect,
        platform_instance=None,
        procedure_name="test_procedure",
    )

    # Both column lineage entries should be preserved with resolved downstreams
    assert aspect.fineGrainedLineages is not None
    assert len(aspect.fineGrainedLineages) == 2

    for cll in aspect.fineGrainedLineages:
        assert len(cll.upstreams) == 1
        assert len(cll.downstreams) == 1
        # All downstreams should be resolved to qualified table
        assert "TestDB.dbo.staging_table" in cll.downstreams[0]
        assert "dst" not in cll.downstreams[0].split(",")[-1]  # Check column part
