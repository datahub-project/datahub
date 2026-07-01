from datetime import datetime, timezone
from functools import partial
from typing import Dict, List, Union
from unittest.mock import MagicMock

import pytest

import datahub.metadata.schema_classes as m
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.lineage import (
    LineageCollectorType,
    LineageDatasetPlatform,
    RedshiftSqlLineage,
    parse_alter_table_rename,
)
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftDataDictionary,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.usage.usage_common import normalize_timestamp_to_utc
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery
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


def test_get_sources_from_query_with_none_default_schema():
    # Regression test: previously `_get_sources_from_query` wrapped
    # `self.config.default_schema` with `str(...)`, which turned a Python
    # `None` into the literal string "None" and produced URNs like
    # `urn:li:dataset:(...,db.none.tbl,PROD)` for unqualified table refs.
    # The value must be passed through unchanged so the schema qualifier
    # is dropped entirely from the URN (`filter(None, ...)` in
    # SchemaResolver.get_urn_for_table), instead of being literally "None".
    test_query = """
        insert into target select * from source
    """
    lineage_extractor = get_lineage_extractor()
    lineage_extractor.config.default_schema = None

    lineage_datasets, _ = lineage_extractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]

    assert ".none." not in lineage.urn
    assert ".None." not in lineage.urn
    assert (
        lineage.urn == "urn:li:dataset:(urn:li:dataPlatform:redshift,test.source,PROD)"
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


def test_parse_alter_table_rename_none_default_schema_with_qualifier():
    # When the query already qualifies the schema, default_schema=None is fine:
    # the parsed schema is used and no fallback is needed.
    assert parse_alter_table_rename(
        None, "alter table my_schema.foo rename to bar"
    ) == (
        "my_schema",
        "foo",
        "bar",
    )


def test_parse_alter_table_rename_raises_on_none_schema():
    # Regression guard for the ALTER TABLE rename path of the original
    # `str(None)` bug: when the query lacks a schema qualifier AND
    # default_schema is None, the function must raise instead of returning
    # None (which would silently produce `db.None.tbl` URNs in the caller).
    with pytest.raises(ValueError, match="default_schema is None"):
        parse_alter_table_rename(None, "alter table foo rename to bar")


def get_lineage_extractor() -> RedshiftSqlLineage:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        resolve_temp_table_in_lineage=True,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    report = RedshiftReport()

    lineage_extractor = RedshiftSqlLineage(
        config, report, PipelineContext(run_id="foo"), config.database
    )

    return lineage_extractor


def test_user_urn_none_username_returns_none():
    # NULL users are common for internal Redshift queries; the guard must
    # return None rather than building a bogus urn:li:corpuser: from "".
    assert get_lineage_extractor()._user_urn(None) is None


def test_user_urn_strips_domain_when_email_already_present():
    urn = get_lineage_extractor()._user_urn("alice@company.com")
    assert str(urn) == "urn:li:corpuser:alice"


def test_user_urn_is_local_part_regardless_of_email_domain():
    # The urn id is always the local part of the username; a configured
    # email_domain does not appear in the urn (it is stripped before the urn
    # is built), so a bare username yields urn:li:corpuser:<username>.
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        email_domain="example.com",
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    extractor = RedshiftSqlLineage(
        config, RedshiftReport(), PipelineContext(run_id="foo"), config.database
    )
    assert str(extractor._user_urn("bob")) == "urn:li:corpuser:bob"


def test_table_pattern_filters_aggregator_usage():
    # table_pattern denies are pushed into the aggregator via is_allowed_table, so
    # usage is not attributed to excluded tables (matching other SQL connectors).
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=True,
        table_pattern={"deny": [".*denied.*"]},
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    extractor = RedshiftSqlLineage(
        config, RedshiftReport(), PipelineContext(run_id="foo"), config.database
    )
    # Mark both tables as known so they aren't treated as temp tables (temp
    # tables are filtered before is_allowed_table is consulted).
    extractor.known_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.kept,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.denied,PROD)",
    }
    ts = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    for table in ("kept", "denied"):
        extractor.aggregator.add_observed_query(
            ObservedQuery(
                query=f"select a from dev.public.{table}",
                default_db="dev",
                default_schema="public",
                timestamp=ts,
            )
        )
    usage_urns = [
        str(mcp.entityUrn)
        for mcp in extractor.aggregator.gen_metadata()
        if isinstance(mcp.aspect, m.DatasetUsageStatisticsClass)
    ]
    assert any("public.kept" in urn for urn in usage_urns)
    assert not any("denied" in urn for urn in usage_urns)


def test_query_usage_statistics_emitted_without_column_usage():
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=False,
        include_query_usage_statistics=True,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    extractor = RedshiftSqlLineage(
        config, RedshiftReport(), PipelineContext(run_id="foo"), config.database
    )
    assert extractor.generate_query_usage is True
    assert extractor.run_unified_queries is True
    assert extractor.generate_usage is False
    assert extractor.aggregator.generate_query_usage_statistics is True

    extractor.known_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.events,PROD)",
    }
    ts = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    query_text = "select event_id from dev.public.events"
    for _ in range(2):
        extractor.aggregator.add_observed_query(
            ObservedQuery(
                query=query_text,
                default_db="dev",
                default_schema="public",
                timestamp=ts,
            )
        )

    query_usage_aspects = [
        mcp.aspect
        for mcp in extractor.aggregator.gen_metadata()
        if isinstance(mcp.aspect, m.QueryUsageStatisticsClass)
    ]
    assert len(query_usage_aspects) == 1
    assert query_usage_aspects[0].queryCount == 2


def test_query_usage_statistics_accepts_naive_utc_timestamps():
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=False,
        include_query_usage_statistics=True,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    extractor = RedshiftSqlLineage(
        config, RedshiftReport(), PipelineContext(run_id="foo"), config.database
    )
    extractor.known_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.events,PROD)",
    }
    ts = normalize_timestamp_to_utc(datetime(2024, 1, 2, 0, 0, 0))
    extractor.aggregator.add_observed_query(
        ObservedQuery(
            query="select event_id from dev.public.events",
            default_db="dev",
            default_schema="public",
            timestamp=ts,
        )
    )

    query_usage_aspects = [
        mcp.aspect
        for mcp in extractor.aggregator.gen_metadata()
        if isinstance(mcp.aspect, m.QueryUsageStatisticsClass)
    ]
    assert len(query_usage_aspects) == 1
    assert query_usage_aspects[0].queryCount == 1


def test_query_usage_statistics_disabled_when_flag_off():
    """When include_query_usage_statistics=False, the aggregator must not be wired
    to generate query usage stats and no QueryUsageStatistics aspects are emitted,
    even though usage statistics are otherwise enabled."""
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=False,
        include_query_usage_statistics=False,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    extractor = RedshiftSqlLineage(
        config, RedshiftReport(), PipelineContext(run_id="foo"), config.database
    )
    assert extractor.generate_query_usage is False
    assert extractor.aggregator.generate_query_usage_statistics is False
    # Neither column nor query usage is enabled, so the unified feed should not run.
    assert extractor.run_unified_queries is False

    extractor.known_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.events,PROD)",
    }
    ts = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    for _ in range(2):
        extractor.aggregator.add_observed_query(
            ObservedQuery(
                query="select event_id from dev.public.events",
                default_db="dev",
                default_schema="public",
                timestamp=ts,
            )
        )

    query_usage_aspects = [
        mcp.aspect
        for mcp in extractor.aggregator.gen_metadata()
        if isinstance(mcp.aspect, m.QueryUsageStatisticsClass)
    ]
    assert query_usage_aspects == []


def test_unified_queries_failure_is_reported_as_failure(monkeypatch):
    # In v2 mode the unified feed is the sole usage producer, so a failure (e.g.
    # a missing SELECT grant on STL_QUERYTEXT) must surface as a report failure,
    # not a warning that still exits successfully.
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=True,
        start_time=datetime(2024, 1, 1, 12, 0, 0).isoformat() + "Z",
        end_time=datetime(2024, 1, 10, 12, 0, 0).isoformat() + "Z",
    )
    extractor = RedshiftSqlLineage(
        config, RedshiftReport(), PipelineContext(run_id="foo"), config.database
    )

    def boom(conn, query, parameters=None):
        raise RuntimeError("permission denied for relation stl_querytext")

    monkeypatch.setattr(RedshiftDataDictionary, "get_query_result", staticmethod(boom))
    # Must not raise, and must record a failure (not just a warning).
    extractor._populate_unified_queries(MagicMock())
    assert extractor.report.failures
    assert not extractor.report.warnings


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


def test_populate_lineage_agg_drains_cursor_before_processing(monkeypatch):
    """The Redshift cursor must be fully drained before the (slow) SQL-parsing
    processors run, so the live cursor isn't held open through aggregation and
    timed out by Redshift on large query histories.

    Pre-fix, fetch and process interleaved (fetch-0, process, fetch-1, ...),
    keeping the cursor open for the whole parse. The fix drains into a local
    FileBackedList first, so all fetches complete before any processing.
    """
    lineage_extractor = get_lineage_extractor()
    events: List[str] = []

    def fake_get_lineage_rows(conn, query):
        for i in range(3):
            events.append(f"fetch-{i}")
            yield MagicMock()

    monkeypatch.setattr(
        RedshiftDataDictionary,
        "get_lineage_rows",
        staticmethod(fake_get_lineage_rows),
    )

    def processor(_row):
        events.append("process")

    lineage_extractor._populate_lineage_agg(
        query="select 1",
        lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
        processor=processor,
        connection=MagicMock(),
    )

    assert events == [
        "fetch-0",
        "fetch-1",
        "fetch-2",
        "process",
        "process",
        "process",
    ]


def test_populate_unified_queries_produces_column_level_usage(monkeypatch):
    """Queries-v2 unified feed: _populate_unified_queries feeds all queries to the
    lineage aggregator once, which then produces DatasetUsageStatistics with
    fieldCounts for explicitly referenced columns."""

    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="acryl.io",
        include_usage_statistics=True,
        include_column_usage_stats=True,
        include_operational_stats=False,
        start_time="2021-09-15T00:00:00Z",
        end_time="2021-09-16T00:00:00Z",
    )
    report = RedshiftReport()
    lineage_extractor = RedshiftSqlLineage(
        config, report, PipelineContext(run_id="test-unified"), config.database
    )

    # Register the schema for dev.public.t1 so SELECT * would also resolve, and
    # so the aggregator knows the table is not a temp table.
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.t1,PROD)"
    lineage_extractor.aggregator.register_schema(
        urn=dataset_urn,
        schema=m.SchemaMetadataClass(
            schemaName="dev.public.t1",
            platform="urn:li:dataPlatform:redshift",
            version=0,
            hash="",
            platformSchema=m.OtherSchemaClass(rawSchema=""),
            fields=[
                m.SchemaFieldClass(
                    fieldPath="col_a",
                    type=m.SchemaFieldDataTypeClass(type=m.NumberTypeClass()),
                    nativeDataType="int",
                ),
                m.SchemaFieldClass(
                    fieldPath="col_b",
                    type=m.SchemaFieldDataTypeClass(type=m.StringTypeClass()),
                    nativeDataType="varchar",
                ),
            ],
        ),
    )
    lineage_extractor.known_urns = {dataset_urn}

    # Build a fake cursor that returns one row matching the list_all_queries_sql
    # column layout: query_id, query_text, username, starttime, session_id.
    # DB-API requires description to be a sequence of 7-item sequences; use tuples.
    fake_cursor = MagicMock()
    fake_cursor.description = [
        ("query_id",),
        ("query_text",),
        ("username",),
        ("starttime",),
        ("session_id",),
    ]
    fake_cursor.fetchmany.side_effect = [
        [
            # Row with empty query_text must be skipped (e.g. text reconstruction
            # produced nothing), without affecting the rest of the batch.
            [
                2,
                "",
                "bob",
                datetime(2021, 9, 15, 9, 0, 0, tzinfo=timezone.utc),
                "43",
            ],
            [
                1,
                "select col_a, col_b from public.t1",
                "alice",
                datetime(2021, 9, 15, 9, 0, 0, tzinfo=timezone.utc),
                "42",
            ],
        ],
        [],
    ]

    monkeypatch.setattr(
        RedshiftDataDictionary,
        "get_query_result",
        staticmethod(lambda conn, query, parameters=None: fake_cursor),
    )

    lineage_extractor._populate_unified_queries(MagicMock())

    field_paths: set = set()
    for mcp in lineage_extractor.aggregator.gen_metadata():
        asp = mcp.aspect
        if isinstance(asp, m.DatasetUsageStatisticsClass):
            field_paths.update(f.fieldPath for f in (asp.fieldCounts or []))

    assert {"col_a", "col_b"} <= field_paths, field_paths


def test_populate_unified_queries_produces_lineage(monkeypatch):
    """Queries-v2 unified feed: _populate_unified_queries produces UpstreamLineage
    from write (INSERT INTO ... SELECT ...) statements, proving lineage is derived
    from the unified feed rather than a separate path."""

    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="acryl.io",
        include_usage_statistics=True,
        include_column_usage_stats=True,
        include_operational_stats=False,
        start_time="2021-09-15T00:00:00Z",
        end_time="2021-09-16T00:00:00Z",
    )
    report = RedshiftReport()
    lineage_extractor = RedshiftSqlLineage(
        config, report, PipelineContext(run_id="test-lineage"), config.database
    )

    src_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.src,PROD)"
    tgt_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.tgt,PROD)"

    def _make_schema(name: str, urn: str) -> m.SchemaMetadataClass:
        return m.SchemaMetadataClass(
            schemaName=name,
            platform="urn:li:dataPlatform:redshift",
            version=0,
            hash="",
            platformSchema=m.OtherSchemaClass(rawSchema=""),
            fields=[
                m.SchemaFieldClass(
                    fieldPath="id",
                    type=m.SchemaFieldDataTypeClass(type=m.NumberTypeClass()),
                    nativeDataType="int",
                ),
                m.SchemaFieldClass(
                    fieldPath="name",
                    type=m.SchemaFieldDataTypeClass(type=m.StringTypeClass()),
                    nativeDataType="varchar",
                ),
            ],
        )

    lineage_extractor.aggregator.register_schema(
        urn=src_urn, schema=_make_schema("dev.public.src", src_urn)
    )
    lineage_extractor.aggregator.register_schema(
        urn=tgt_urn, schema=_make_schema("dev.public.tgt", tgt_urn)
    )
    lineage_extractor.known_urns = {src_urn, tgt_urn}

    # DB-API description uses tuples (sequence of at least 1 element per column).
    fake_cursor = MagicMock()
    fake_cursor.description = [
        ("query_id",),
        ("query_text",),
        ("username",),
        ("starttime",),
        ("session_id",),
    ]
    fake_cursor.fetchmany.side_effect = [
        [
            [
                2,
                "INSERT INTO public.tgt SELECT id, name FROM public.src",
                "bob",
                datetime(2021, 9, 15, 10, 0, 0, tzinfo=timezone.utc),
                "99",
            ]
        ],
        [],
    ]

    monkeypatch.setattr(
        RedshiftDataDictionary,
        "get_query_result",
        staticmethod(lambda conn, query, parameters=None: fake_cursor),
    )

    lineage_extractor._populate_unified_queries(MagicMock())

    upstream_urns: set = set()
    for mcp in lineage_extractor.aggregator.gen_metadata():
        if mcp.entityUrn == tgt_urn:
            asp = mcp.aspect
            if isinstance(asp, m.UpstreamLineageClass):
                for upstream in asp.upstreams or []:
                    upstream_urns.add(upstream.dataset)

    assert src_urn in upstream_urns, (
        f"Expected {src_urn!r} in upstreams of {tgt_urn!r}, got: {upstream_urns}"
    )


def test_usage_only_via_sql_parsing_no_lineage_edges(monkeypatch):
    """C1 regression guard: when all lineage flags are off but include_column_usage_stats=True,
    the aggregator must be built with generate_lineage=False so no UpstreamLineage
    aspects are emitted, while DatasetUsageStatistics aspects still are."""

    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="acryl.io",
        # All lineage flags explicitly off.
        include_table_lineage=False,
        include_view_lineage=False,
        include_copy_lineage=False,
        include_unload_lineage=False,
        include_share_lineage=False,
        include_table_rename_lineage=False,
        # Usage via SQL parsing on.
        include_usage_statistics=True,
        include_column_usage_stats=True,
        include_operational_stats=False,
        start_time="2021-09-15T00:00:00Z",
        end_time="2021-09-16T00:00:00Z",
    )
    report = RedshiftReport()
    lineage_extractor = RedshiftSqlLineage(
        config, report, PipelineContext(run_id="test-usage-only"), config.database
    )

    # Aggregator must not generate lineage when all lineage flags are off.
    assert lineage_extractor.generate_usage is True
    assert lineage_extractor.aggregator.generate_lineage is False

    src_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.src,PROD)"
    tgt_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.tgt,PROD)"

    def _make_schema(name: str) -> m.SchemaMetadataClass:
        return m.SchemaMetadataClass(
            schemaName=name,
            platform="urn:li:dataPlatform:redshift",
            version=0,
            hash="",
            platformSchema=m.OtherSchemaClass(rawSchema=""),
            fields=[
                m.SchemaFieldClass(
                    fieldPath="id",
                    type=m.SchemaFieldDataTypeClass(type=m.NumberTypeClass()),
                    nativeDataType="int",
                ),
            ],
        )

    lineage_extractor.aggregator.register_schema(
        urn=src_urn, schema=_make_schema("dev.public.src")
    )
    lineage_extractor.aggregator.register_schema(
        urn=tgt_urn, schema=_make_schema("dev.public.tgt")
    )
    lineage_extractor.known_urns = {src_urn, tgt_urn}

    # Feed an INSERT…SELECT which would produce lineage if generate_lineage were True.
    fake_cursor = MagicMock()
    fake_cursor.description = [
        ("query_id",),
        ("query_text",),
        ("username",),
        ("starttime",),
        ("session_id",),
    ]
    fake_cursor.fetchmany.side_effect = [
        [
            [
                1,
                "INSERT INTO public.tgt SELECT id FROM public.src",
                "alice",
                datetime(2021, 9, 15, 9, 0, 0, tzinfo=timezone.utc),
                "10",
            ]
        ],
        [],
    ]

    monkeypatch.setattr(
        RedshiftDataDictionary,
        "get_query_result",
        staticmethod(lambda conn, query, parameters=None: fake_cursor),
    )

    lineage_extractor._populate_unified_queries(MagicMock())

    usage_found = False
    lineage_found = False
    for mcp in lineage_extractor.aggregator.gen_metadata():
        asp = mcp.aspect
        if isinstance(asp, m.DatasetUsageStatisticsClass):
            usage_found = True
        if isinstance(asp, m.UpstreamLineageClass):
            lineage_found = True

    assert usage_found, (
        "Expected DatasetUsageStatistics aspects when include_column_usage_stats=True"
    )
    assert not lineage_found, (
        "Expected no UpstreamLineage aspects when all lineage flags are off"
    )
