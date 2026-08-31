"""
Tests for Snowflake lineage consistency fix.

These tests verify that the consistency fix in snowflake_lineage_v2.py correctly:
- Extracts tables from column-level lineage
- Adds missing tables to table-level lineage
- Tracks metrics for monitoring
"""

from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    ColumnUpstreamJob,
    ColumnUpstreamLineage,
    Query,
    SnowflakeLineageExtractor,
    UpstreamColumnNode,
    UpstreamLineageEdge,
    UpstreamTableNode,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)


def create_mock_lineage_extractor() -> SnowflakeLineageExtractor:
    """Create a mock SnowflakeLineageExtractor for testing."""
    config = SnowflakeV2Config(  # type: ignore[call-arg]
        account_id="test_account",
        username="test_user",
        password=SecretStr("test_password"),
        include_column_lineage=True,
    )

    report = SnowflakeV2Report()

    sql_aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config,
        structured_reporter=report,
    )

    mock_connection = MagicMock()
    mock_filters = MagicMock()

    extractor = SnowflakeLineageExtractor(
        config=config,
        report=report,
        connection=mock_connection,
        filters=mock_filters,
        identifiers=identifiers,
        redundant_run_skip_handler=None,
        sql_aggregator=sql_aggregator,
    )

    return extractor


def test_lineage_consistency_fix_adds_missing_table() -> None:
    """Test that tables in column lineage but missing from table lineage are added."""
    extractor = create_mock_lineage_extractor()

    with (
        patch.object(
            extractor,
            "map_query_result_upstreams",
            return_value=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table2,PROD)",
            ],
        ),
        patch.object(
            extractor,
            "map_query_result_fine_upstreams",
            return_value=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
                            column="col_a",
                        )
                    ],
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_b",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table2,PROD)",
                            column="col_b",
                        )
                    ],
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_c",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table3,PROD)",
                            column="col_c",
                        )
                    ],
                ),
            ],
        ),
    ):
        query = Query(
            query_id="test_query_123",
            query_text="SELECT * FROM table",
            start_time="2024-01-01T00:00:00.000000",
        )

        db_row = UpstreamLineageEdge(
            DOWNSTREAM_TABLE_NAME="db.schema.target",
            DOWNSTREAM_TABLE_DOMAIN="TABLE",
            UPSTREAM_TABLES=[
                UpstreamTableNode(
                    upstream_object_name="db.schema.table1",
                    upstream_object_domain="TABLE",
                    query_id="test_query_123",
                ),
                UpstreamTableNode(
                    upstream_object_name="db.schema.table2",
                    upstream_object_domain="TABLE",
                    query_id="test_query_123",
                ),
            ],
            UPSTREAM_COLUMNS=[
                ColumnUpstreamLineage(
                    column_name="col_a",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table1",
                                    object_domain="TABLE",
                                    column_name="col_a",
                                )
                            ],
                            query_id="test_query_123",
                        )
                    ],
                ),
                ColumnUpstreamLineage(
                    column_name="col_b",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table2",
                                    object_domain="TABLE",
                                    column_name="col_b",
                                )
                            ],
                            query_id="test_query_123",
                        )
                    ],
                ),
                ColumnUpstreamLineage(
                    column_name="col_c",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table3",
                                    object_domain="TABLE",
                                    column_name="col_c",
                                )
                            ],
                            query_id="test_query_123",
                        )
                    ],
                ),
            ],
        )

        result = extractor.get_known_query_lineage(query, "db.schema.target", db_row)

        assert result is not None
        assert len(result.upstreams) == 3

        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table3,PROD)"
            in result.upstreams
        )

        assert extractor.report.num_tables_added_from_column_lineage == 1


def test_lineage_consistency_no_fix_needed() -> None:
    """Test that no fix is applied when lineage is already consistent."""
    extractor = create_mock_lineage_extractor()

    with (
        patch.object(
            extractor,
            "map_query_result_upstreams",
            return_value=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table2,PROD)",
            ],
        ),
        patch.object(
            extractor,
            "map_query_result_fine_upstreams",
            return_value=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
                            column="col_a",
                        )
                    ],
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_b",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table2,PROD)",
                            column="col_b",
                        )
                    ],
                ),
            ],
        ),
    ):
        query = Query(
            query_id="test_query_456",
            query_text="SELECT * FROM table",
            start_time="2024-01-01T00:00:00.000000",
        )

        db_row = UpstreamLineageEdge(
            DOWNSTREAM_TABLE_NAME="db.schema.target",
            DOWNSTREAM_TABLE_DOMAIN="TABLE",
            UPSTREAM_TABLES=[
                UpstreamTableNode(
                    upstream_object_name="db.schema.table1",
                    upstream_object_domain="TABLE",
                    query_id="test_query_456",
                ),
                UpstreamTableNode(
                    upstream_object_name="db.schema.table2",
                    upstream_object_domain="TABLE",
                    query_id="test_query_456",
                ),
            ],
        )

        result = extractor.get_known_query_lineage(query, "db.schema.target", db_row)

        assert result is not None
        assert len(result.upstreams) == 2

        assert extractor.report.num_tables_added_from_column_lineage == 0


def test_lineage_consistency_multiple_missing_tables() -> None:
    """Test that multiple missing tables are all added."""
    extractor = create_mock_lineage_extractor()

    with (
        patch.object(
            extractor,
            "map_query_result_upstreams",
            return_value=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
            ],
        ),
        patch.object(
            extractor,
            "map_query_result_fine_upstreams",
            return_value=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_a",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
                            column="col_a",
                        )
                    ],
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_b",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table2,PROD)",
                            column="col_b",
                        )
                    ],
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_c",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table3,PROD)",
                            column="col_c",
                        )
                    ],
                ),
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.target,PROD)",
                        column="col_d",
                    ),
                    upstreams=[
                        ColumnRef(
                            table="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table4,PROD)",
                            column="col_d",
                        )
                    ],
                ),
            ],
        ),
    ):
        query = Query(
            query_id="test_query_789",
            query_text="SELECT * FROM table",
            start_time="2024-01-01T00:00:00.000000",
        )

        db_row = UpstreamLineageEdge(
            DOWNSTREAM_TABLE_NAME="db.schema.target",
            DOWNSTREAM_TABLE_DOMAIN="TABLE",
            UPSTREAM_TABLES=[
                UpstreamTableNode(
                    upstream_object_name="db.schema.table1",
                    upstream_object_domain="TABLE",
                    query_id="test_query_789",
                ),
            ],
            UPSTREAM_COLUMNS=[
                ColumnUpstreamLineage(
                    column_name="col_a",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table1",
                                    object_domain="TABLE",
                                    column_name="col_a",
                                )
                            ],
                            query_id="test_query_789",
                        )
                    ],
                ),
                ColumnUpstreamLineage(
                    column_name="col_b",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table2",
                                    object_domain="TABLE",
                                    column_name="col_b",
                                )
                            ],
                            query_id="test_query_789",
                        )
                    ],
                ),
                ColumnUpstreamLineage(
                    column_name="col_c",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table3",
                                    object_domain="TABLE",
                                    column_name="col_c",
                                )
                            ],
                            query_id="test_query_789",
                        )
                    ],
                ),
                ColumnUpstreamLineage(
                    column_name="col_d",
                    upstreams=[
                        ColumnUpstreamJob(
                            column_upstreams=[
                                UpstreamColumnNode(
                                    object_name="db.schema.table4",
                                    object_domain="TABLE",
                                    column_name="col_d",
                                )
                            ],
                            query_id="test_query_789",
                        )
                    ],
                ),
            ],
        )

        result = extractor.get_known_query_lineage(query, "db.schema.target", db_row)

        assert result is not None
        assert len(result.upstreams) == 4

        assert extractor.report.num_tables_added_from_column_lineage == 3


def test_empty_directsources_metric_tracking() -> None:
    """Test that queries with empty directSources are tracked in metrics."""
    extractor = create_mock_lineage_extractor()

    with (
        patch.object(
            extractor,
            "map_query_result_upstreams",
            return_value=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
            ],
        ),
        patch.object(extractor, "map_query_result_fine_upstreams", return_value=None),
    ):
        query = Query(
            query_id="test_query_empty_ds",
            query_text="SELECT * FROM table",
            start_time="2024-01-01T00:00:00.000000",
        )

        db_row = UpstreamLineageEdge(
            DOWNSTREAM_TABLE_NAME="db.schema.target",
            DOWNSTREAM_TABLE_DOMAIN="TABLE",
            UPSTREAM_TABLES=[
                UpstreamTableNode(
                    upstream_object_name="db.schema.table1",
                    upstream_object_domain="TABLE",
                    query_id="test_query_empty_ds",
                ),
            ],
            UPSTREAM_COLUMNS=None,
        )

        result = extractor.get_known_query_lineage(query, "db.schema.target", db_row)

        assert result is not None

        assert extractor.report.num_queries_with_empty_directsources == 1


def test_column_lineage_disabled() -> None:
    """Test that fix doesn't run when column lineage is disabled."""
    config = SnowflakeV2Config(  # type: ignore[call-arg]
        account_id="test_account",
        username="test_user",
        password=SecretStr("test_password"),
        include_column_lineage=False,
    )

    report = SnowflakeV2Report()

    sql_aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config,
        structured_reporter=report,
    )

    mock_connection = MagicMock()
    mock_filters = MagicMock()

    extractor = SnowflakeLineageExtractor(
        config=config,
        report=report,
        connection=mock_connection,
        filters=mock_filters,
        identifiers=identifiers,
        redundant_run_skip_handler=None,
        sql_aggregator=sql_aggregator,
    )

    with (
        patch.object(
            extractor,
            "map_query_result_upstreams",
            return_value=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_account.db.schema.table1,PROD)",
            ],
        ),
        patch.object(extractor, "map_query_result_fine_upstreams", return_value=[]),
    ):
        query = Query(
            query_id="test_query_no_col",
            query_text="SELECT * FROM table",
            start_time="2024-01-01T00:00:00.000000",
        )

        db_row = UpstreamLineageEdge(
            DOWNSTREAM_TABLE_NAME="db.schema.target",
            DOWNSTREAM_TABLE_DOMAIN="TABLE",
            UPSTREAM_TABLES=[
                UpstreamTableNode(
                    upstream_object_name="db.schema.table1",
                    upstream_object_domain="TABLE",
                    query_id="test_query_no_col",
                )
            ],
            UPSTREAM_COLUMNS=[],
        )

        result = extractor.get_known_query_lineage(query, "db.schema.target", db_row)

        assert result is not None
        assert len(result.upstreams) == 1

        assert extractor.report.num_tables_added_from_column_lineage == 0

        assert result.column_lineage is None
