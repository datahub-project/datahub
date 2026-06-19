import datetime
from typing import Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery


def _row(
    schema_name: Optional[str],
    digest_text: Optional[str],
    count_star: int,
    last_seen: Optional[datetime.datetime],
) -> MagicMock:
    row = MagicMock()
    row.SCHEMA_NAME = schema_name
    row.DIGEST_TEXT = digest_text
    row.COUNT_STAR = count_star
    row.LAST_SEEN = last_seen
    return row


def _glog_row(
    user_host: Optional[str],
    thread_id: int,
    command_type: str,
    argument: str,
    event_time: Optional[datetime.datetime] = None,
) -> MagicMock:
    row = MagicMock()
    row.user_host = user_host
    row.thread_id = thread_id
    row.command_type = command_type
    row.argument = argument
    row.event_time = event_time or datetime.datetime(2023, 6, 1, 10, 30, 0)
    return row


def _source(**config_overrides: object) -> MySQLSource:
    config = MySQLConfig(include_usage_statistics=True, **config_overrides)
    return MySQLSource(config, PipelineContext(run_id="mysql-usage-test"))


def _patch_rows(rows: list) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value = rows
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def test_create_aggregator_toggles_usage_flags():
    enabled = _source().aggregator
    assert enabled.generate_usage_statistics
    assert enabled.generate_query_usage_statistics
    assert enabled.usage_config is not None

    disabled = MySQLSource(
        MySQLConfig(), PipelineContext(run_id="mysql-usage-test")
    ).aggregator
    assert not disabled.generate_usage_statistics
    assert not disabled.generate_query_usage_statistics
    assert disabled.usage_config is None


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_fetch_performance_schema_maps_digest_rows(mock_create_engine):
    naive_last_seen = datetime.datetime(2023, 6, 1, 10, 30, 0)
    mock_create_engine.return_value = _patch_rows(
        [_row("appdb", "SELECT * FROM `orders` WHERE id = ?", 7, naive_last_seen)]
    )

    observed = list(_source()._fetch_performance_schema_queries())

    assert len(observed) == 1
    query = observed[0]
    assert query.query == "SELECT * FROM `orders` WHERE id = ?"
    # Two-tier mapping: schema becomes default_schema so URNs render as schema.table.
    assert query.default_schema == "appdb"
    assert query.default_db is None
    assert query.usage_multiplier == 7
    # Digests are aggregated across users, so no actor can be attributed.
    assert query.user is None
    assert query.timestamp == naive_last_seen.replace(tzinfo=datetime.timezone.utc)


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_fetch_performance_schema_filters_system_and_empty_rows(mock_create_engine):
    last_seen = datetime.datetime(2023, 6, 1, 10, 30, 0)
    mock_create_engine.return_value = _patch_rows(
        [
            _row("appdb", "SELECT 1", 4, last_seen),
            _row("performance_schema", "SELECT 1", 9, last_seen),
            _row("mysql", "SELECT 1", 9, last_seen),
            _row("appdb", "SELECT 1", 0, last_seen),
        ]
    )

    observed = list(_source()._fetch_performance_schema_queries())

    # Only the single non-system row with a positive execution count survives.
    assert [q.usage_multiplier for q in observed] == [4]


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_fetch_performance_schema_respects_database_pattern(mock_create_engine):
    last_seen = datetime.datetime(2023, 6, 1, 10, 30, 0)
    mock_create_engine.return_value = _patch_rows(
        [
            _row("keep_db", "SELECT 1", 2, last_seen),
            _row("drop_db", "SELECT 1", 2, last_seen),
        ]
    )

    source = _source(database_pattern={"allow": ["keep_db"]})
    observed = list(source._fetch_performance_schema_queries())

    assert [q.default_schema for q in observed] == ["keep_db"]


def test_usage_workunits_generated_for_two_tier_table():
    source = _source()
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    with patch.object(
        source,
        "_fetch_performance_schema_queries",
        return_value=[
            ObservedQuery(
                query="SELECT id FROM orders",
                timestamp=now,
                default_schema="appdb",
                usage_multiplier=12,
            )
        ],
    ):
        workunits = list(source._generate_aggregator_workunits())

    # The digest resolves to the two-tier URN mysql.appdb.orders and produces a
    # usage-statistics aspect carrying the aggregated execution count.
    usage_stats = [
        (wu.get_urn(), aspect)
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(DatasetUsageStatisticsClass)) is not None
    ]
    assert usage_stats, "expected a datasetUsageStatistics aspect to be emitted"
    assert all("appdb.orders" in urn for urn, _ in usage_stats)
    assert any(aspect.totalSqlQueries == 12 for _, aspect in usage_stats)


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_maps_literal_queries_with_user(mock_create_engine):
    event_time = datetime.datetime(2023, 6, 1, 10, 30, 0)
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row("root[root] @ localhost []", 5, "Init DB", "appdb"),
            _glog_row(
                "analyst[analyst] @ 10.0.0.1 [10.0.0.1]",
                5,
                "Query",
                "SELECT id FROM orders WHERE id = 42",
                event_time,
            ),
        ]
    )

    observed = list(_source(usage_source="general_log")._fetch_general_log_queries())

    assert len(observed) == 1
    query = observed[0]
    # Literal text is preserved (not normalized to `?`).
    assert query.query == "SELECT id FROM orders WHERE id = 42"
    # Login user is parsed from user_host and attributed.
    assert str(query.user) == str(CorpUserUrn("analyst"))
    # Init DB row set the session's current database for two-tier resolution.
    assert query.default_schema == "appdb"
    assert query.usage_multiplier == 1
    assert query.timestamp == event_time.replace(tzinfo=datetime.timezone.utc)


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_maps_username_to_email_with_domain(mock_create_engine):
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row("svc[svc] @ host []", 1, "Init DB", "appdb"),
            _glog_row("svc[svc] @ host []", 1, "Query", "SELECT id FROM orders"),
            # Already an email-like login: left untouched.
            _glog_row(
                "jane@corp.com[jane@corp.com] @ host []",
                2,
                "Init DB",
                "appdb",
            ),
            _glog_row(
                "jane@corp.com[jane@corp.com] @ host []",
                2,
                "Query",
                "SELECT id FROM orders",
            ),
        ]
    )

    observed = list(
        _source(
            usage_source="general_log", user_email_domain="corp.com"
        )._fetch_general_log_queries()
    )

    users = [str(q.user) for q in observed]
    assert str(CorpUserUrn("svc@corp.com")) in users
    assert str(CorpUserUrn("jane@corp.com")) in users


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_keeps_raw_username_without_email_domain(mock_create_engine):
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row("jdoe[jdoe] @ host []", 1, "Init DB", "appdb"),
            _glog_row("jdoe[jdoe] @ host []", 1, "Query", "SELECT id FROM orders"),
        ]
    )

    observed = list(_source(usage_source="general_log")._fetch_general_log_queries())

    # No user_email_domain configured: the raw login is kept as-is.
    assert str(observed[0].user) == str(CorpUserUrn("jdoe"))


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_tracks_use_and_skips_non_dml(mock_create_engine):
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row("root[root] @ localhost []", 7, "Query", "SET autocommit=1"),
            _glog_row("root[root] @ localhost []", 7, "Query", "USE sales"),
            _glog_row("root[root] @ localhost []", 7, "Query", "SHOW TABLES"),
            _glog_row(
                "root[root] @ localhost []",
                7,
                "Query",
                "INSERT INTO summary SELECT * FROM orders",
            ),
        ]
    )

    observed = list(_source(usage_source="general_log")._fetch_general_log_queries())

    # SET and SHOW are dropped; only the INSERT...SELECT survives, scoped to the
    # database set by the preceding USE statement.
    assert [q.query for q in observed] == ["INSERT INTO summary SELECT * FROM orders"]
    assert observed[0].default_schema == "sales"


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_filters_system_schema_and_database_pattern(mock_create_engine):
    rows = [
        _glog_row("root[root] @ localhost []", 1, "Init DB", "mysql"),
        _glog_row("root[root] @ localhost []", 1, "Query", "SELECT * FROM user"),
        _glog_row("root[root] @ localhost []", 2, "Init DB", "drop_db"),
        _glog_row("root[root] @ localhost []", 2, "Query", "SELECT * FROM t"),
        _glog_row("root[root] @ localhost []", 3, "Init DB", "keep_db"),
        _glog_row("root[root] @ localhost []", 3, "Query", "SELECT * FROM t"),
    ]
    mock_create_engine.return_value = _patch_rows(rows)

    source = _source(
        usage_source="general_log", database_pattern={"allow": ["keep_db"]}
    )
    observed = list(source._fetch_general_log_queries())

    assert [q.default_schema for q in observed] == ["keep_db"]
