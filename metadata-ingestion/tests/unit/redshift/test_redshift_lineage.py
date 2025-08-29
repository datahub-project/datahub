from datetime import datetime
from functools import partial
from typing import Dict, List, Union
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.lineage_v2 import (
    LineageCollectorType,
    LineageDatasetPlatform,
    RedshiftSqlLineageV2,
    parse_alter_table_rename,
)
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnTransformation,
    DownstreamColumnRef,
)
from tests.unit.redshift.redshift_query_mocker import mock_cursor


def test_get_sources_from_query():
    test_query = """
        select * from my_schema.my_table
    """
    lineage_extractor = get_lineage_extractor()
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.my_schema.my_table,PROD)"
    )


def test_get_sources_from_query_with_only_table_name():
    test_query = """
        select * from my_table
    """
    lineage_extractor = get_lineage_extractor()
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.my_table,PROD)"
    )


def test_get_sources_from_query_with_database():
    test_query = """
        select * from test.my_schema.my_table
    """
    lineage_extractor = get_lineage_extractor()
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.my_schema.my_table,PROD)"
    )


def test_get_sources_from_query_with_non_default_database():
    test_query = """
        select * from test2.my_schema.my_table
    """
    lineage_extractor = get_lineage_extractor()
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test2.my_schema.my_table,PROD)"
    )


def test_get_sources_from_query_with_only_table():
    test_query = """
        select * from my_table
    """
    lineage_extractor = get_lineage_extractor()
    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert (
        lineage.urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.my_table,PROD)"
    )


def test_parse_alter_table_rename():
    assert parse_alter_table_rename("public", "alter table foo rename to bar") == (
        "public",
        "foo",
        "bar",
    )
    assert parse_alter_table_rename(
        "public", "alter table second_schema.storage_v2_stg rename to storage_v2; "
    ) == (
        "second_schema",
        "storage_v2_stg",
        "storage_v2",
    )


def get_lineage_extractor() -> RedshiftSqlLineageV2:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        resolve_temp_table_in_lineage=True,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    report = RedshiftReport()

    lineage_extractor = RedshiftSqlLineageV2(
        config, report, PipelineContext(run_id="foo"), config.database
    )

    return lineage_extractor


def test_cll():
    test_query = """
        select a,b,c from db.public.customer inner join db.public.order on db.public.customer.id = db.public.order.customer_id
    """

    lineage_extractor = get_lineage_extractor()

    _, cll = lineage_extractor._get_sources_from_query(db_name="db", query=test_query)

    assert cll == [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="a"),
            upstreams=[],
            logic=ColumnTransformation(is_direct_copy=True, column_logic='"a" AS "a"'),
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="b"),
            upstreams=[],
            logic=ColumnTransformation(is_direct_copy=True, column_logic='"b" AS "b"'),
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="c"),
            upstreams=[],
            logic=ColumnTransformation(is_direct_copy=True, column_logic='"c" AS "c"'),
        ),
    ]


def cursor_execute_side_effect(cursor: MagicMock, query: str) -> None:
    mock_cursor(cursor=cursor, query=query)


def mock_redshift_connection() -> MagicMock:
    connection = MagicMock()

    cursor = MagicMock()

    connection.cursor.return_value = cursor

    cursor.execute.side_effect = partial(cursor_execute_side_effect, cursor)

    return connection


def test_external_schema_get_upstream_schema_success():
    schema = RedshiftSchema(
        name="schema",
        database="XXXXXXXX",
        type="external",
        option='{"SCHEMA":"sales_schema"}',
        external_platform="redshift",
    )

    assert schema.get_upstream_schema_name() == "sales_schema"


def test_external_schema_no_upstream_schema():
    schema = RedshiftSchema(
        name="schema",
        database="XXXXXXXX",
        type="external",
        option=None,
        external_platform="redshift",
    )

    assert schema.get_upstream_schema_name() is None


def test_local_schema_no_upstream_schema():
    schema = RedshiftSchema(
        name="schema",
        database="XXXXXXXX",
        type="local",
        option='{"some_other_option":"x"}',
        external_platform=None,
    )

    assert schema.get_upstream_schema_name() is None


def test_make_filtered_target():
    lineage_extractor = get_lineage_extractor()

    # Set up known_urns
    lineage_extractor.known_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.known_table,PROD)"
    }

    # Mock LineageRow
    lineage_row = MagicMock()
    lineage_row.target_schema = "public"
    lineage_row.target_table = "known_table"

    target = lineage_extractor._make_filtered_target(lineage_row)
    assert target is not None
    assert (
        target.urn()
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.known_table,PROD)"
    )

    # Test with unknown table
    lineage_row.target_table = "unknown_table"
    target = lineage_extractor._make_filtered_target(lineage_row)
    assert target is None


def test_get_s3_path():
    lineage_extractor = get_lineage_extractor()

    # Test with S3 config that strips URLs
    lineage_extractor.config.s3_lineage_config = MagicMock()
    lineage_extractor.config.s3_lineage_config.strip_urls = True
    lineage_extractor.config.s3_lineage_config.path_specs = []
    lineage_extractor.config.s3_lineage_config.ignore_non_path_spec_path = False

    path = lineage_extractor._get_s3_path("s3://bucket/path/to/file.csv")
    assert path == "s3://bucket/path/to"


def test_build_s3_path_from_row():
    lineage_extractor = get_lineage_extractor()

    # Test valid S3 path
    filename = "s3://bucket/path/to/file.csv"
    path = lineage_extractor._build_s3_path_from_row(filename)
    assert path == "bucket/path/to"

    # Test non-S3 path (should raise ValueError)
    with pytest.raises(ValueError):
        lineage_extractor._build_s3_path_from_row("file://local/path")


def test_get_sources():
    lineage_extractor = get_lineage_extractor()

    # Test SQL parser lineage type with DDL
    sources, cll = lineage_extractor._get_sources(
        lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
        db_name="test",
        source_schema=None,
        source_table=None,
        ddl="SELECT * FROM test.public.source_table",
        filename=None,
    )
    assert isinstance(sources, list)

    # Test COPY lineage type with S3 filename
    sources, cll = lineage_extractor._get_sources(
        lineage_type=LineageCollectorType.COPY,
        db_name="test",
        source_schema=None,
        source_table=None,
        ddl=None,
        filename="s3://bucket/path/to/file.csv",
    )
    assert len(sources) == 1
    assert sources[0].platform == LineageDatasetPlatform.S3

    # Test with schema and table
    sources, cll = lineage_extractor._get_sources(
        lineage_type=LineageCollectorType.QUERY_SCAN,
        db_name="test",
        source_schema="public",
        source_table="source_table",
        ddl=None,
        filename=None,
    )
    assert len(sources) == 1
    assert sources[0].platform == LineageDatasetPlatform.REDSHIFT


def test_get_target_lineage():
    lineage_extractor = get_lineage_extractor()

    # Mock LineageRow
    lineage_row = MagicMock()
    lineage_row.target_schema = "public"
    lineage_row.target_table = "target_table"
    lineage_row.filename = None

    all_tables_set = {"test": {"public": {"target_table"}}}

    # Test normal table lineage
    target = lineage_extractor._get_target_lineage(
        alias_db_name="test",
        lineage_row=lineage_row,
        lineage_type=LineageCollectorType.QUERY_SCAN,
        all_tables_set=all_tables_set,
    )
    assert target is not None
    assert target.dataset.platform == LineageDatasetPlatform.REDSHIFT

    # Test UNLOAD lineage with S3 target
    lineage_row.filename = "s3://bucket/output/file.csv"
    target = lineage_extractor._get_target_lineage(
        alias_db_name="test",
        lineage_row=lineage_row,
        lineage_type=LineageCollectorType.UNLOAD,
        all_tables_set=all_tables_set,
    )
    assert target is not None
    assert target.dataset.platform == LineageDatasetPlatform.S3


def test_process_table_renames_integration():
    """Integration test for _process_table_renames method."""
    lineage_extractor = get_lineage_extractor()

    # Mock connection using the query mocker
    connection = mock_redshift_connection()

    # Initial all_tables structure
    all_tables = {
        "test": {"public": {"new_table_name"}, "analytics": {"new_analytics_table"}}
    }

    lineage_extractor.start_time = lineage_extractor.config.start_time
    lineage_extractor.end_time = lineage_extractor.config.end_time

    # Process table renames
    table_renames, updated_all_tables = lineage_extractor._process_table_renames(
        database="test", connection=connection, all_tables=all_tables
    )

    # Verify results
    assert len(table_renames) == 2

    # Verify first rename
    new_urn1 = (
        "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.new_table_name,PROD)"
    )
    assert new_urn1 in table_renames
    assert (
        table_renames[new_urn1].original_urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.public.old_table_name,PROD)"
    )

    # Verify second rename
    new_urn2 = "urn:li:dataset:(urn:li:dataPlatform:redshift,test.analytics.new_analytics_table,PROD)"
    assert new_urn2 in table_renames
    assert (
        table_renames[new_urn2].original_urn
        == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.analytics.old_analytics_table,PROD)"
    )

    # Verify all_tables was updated
    assert "old_table_name" in updated_all_tables["test"]["public"]
    assert "new_table_name" in updated_all_tables["test"]["public"]
    assert "old_analytics_table" in updated_all_tables["test"]["analytics"]
    assert "new_analytics_table" in updated_all_tables["test"]["analytics"]


def test_build():
    lineage_extractor = get_lineage_extractor()

    connection: MagicMock = mock_redshift_connection()

    # Mock tables and schemas
    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        "test": {"public": []}
    }

    db_schemas = {
        "test": {
            "public": RedshiftSchema(
                name="public",
                database="test",
                type="local",
                option=None,
                external_platform=None,
            )
        }
    }

    # Test build method doesn't raise exception
    lineage_extractor.build(connection, all_tables, db_schemas)
