import datetime
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine as real_create_engine
from sqlalchemy.pool import NullPool

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import (
    MySQLConfig,
    MySQLSource,
    _parse_general_log_user,
)
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
def test_usage_connection_pins_utc_and_disposes_engine(mock_create_engine):
    engine = _patch_rows(
        [_row("appdb", "SELECT 1", 1, datetime.datetime(2023, 6, 1, 10, 30, 0))]
    )
    mock_create_engine.return_value = engine

    list(_source()._fetch_performance_schema_queries())

    conn = engine.connect.return_value.__enter__.return_value
    executed = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("SET time_zone = '+00:00'" in sql for sql in executed), (
        f"session tz must be pinned to UTC; executed {executed}"
    )
    # Single-use engine must be disposed so the usage fetch leaks no connections.
    engine.dispose.assert_called_once()


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_usage_connection_builds_valid_nullpool_engine(mock_create_engine):
    url = "mysql+pymysql://u:p@h/db"
    # create_engine validates pool kwargs eagerly (no connection), so this is the
    # failure this PR fixes: NullPool rejects QueuePool sizing options.
    with pytest.raises(TypeError):
        real_create_engine(url, poolclass=NullPool, max_overflow=10)

    # Delegate to the real create_engine so its validation runs on what the
    # source builds, then return a mock so no DB is contacted.
    def _validate_then_mock(engine_url: str, **kwargs: Any) -> MagicMock:
        real_create_engine(engine_url, **kwargs).dispose()
        return _patch_rows([])

    mock_create_engine.side_effect = _validate_then_mock

    # Set options directly instead of enabling profiling (which needs the full
    # get_workunits path) to mimic _add_default_options' injection.
    source = _source(
        options={
            "max_overflow": 10,
            "pool_size": 5,
            "pool_timeout": 30,
            "pool_use_lifo": True,
        }
    )
    # Must not raise: the usage engine strips every QueuePool-only option.
    list(source._fetch_performance_schema_queries())


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
    # Usage/lineage is only attributed to tables we ingested (is_temp_table).
    source.discovered_datasets.add("appdb.orders")
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
            usage_source="general_log", email_domain="corp.com"
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

    # No email_domain configured: the raw login is kept as-is.
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


def test_parse_general_log_user_handles_user_at_server_login():
    # A login whose account name embeds `@` (e.g. `user@server`) appears inside
    # the brackets and must be captured verbatim.
    assert (
        _parse_general_log_user("analyst@prod[analyst@prod] @ localhost []")
        == "analyst@prod"
    )


def test_general_log_user_urn_preserves_at_and_skips_email_domain():
    # When the captured login already contains `@`, the configured email_domain
    # must not be appended (it already looks like an address / has a server part).
    source = _source(usage_source="general_log", email_domain="corp.com")
    urn = source._general_log_user_urn("svc@host[svc@host] @ localhost []")
    assert str(urn) == str(CorpUserUrn("svc@host"))


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_learns_db_from_connect_event(mock_create_engine):
    # Clients that connect with a default database emit a `Connect` event
    # ("<user>@<host> on <db> using <proto>") rather than an explicit `Init DB`.
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row(
                "root[root] @ localhost []",
                4,
                "Connect",
                "root@localhost on appdb using Socket",
            ),
            _glog_row("root[root] @ localhost []", 4, "Query", "SELECT id FROM orders"),
        ]
    )

    observed = list(_source(usage_source="general_log")._fetch_general_log_queries())

    assert len(observed) == 1
    assert observed[0].default_schema == "appdb"


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_connect_without_db_leaves_session_unknown(mock_create_engine):
    # A Connect that selects no default database ("... on  using ...") sets no
    # schema, so a later query on that session has nothing to filter on and is
    # dropped.
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row(
                "root[root] @ localhost []",
                6,
                "Connect",
                "root@localhost on  using Socket",
            ),
            _glog_row("root[root] @ localhost []", 6, "Query", "SELECT id FROM orders"),
        ]
    )

    observed = list(_source(usage_source="general_log")._fetch_general_log_queries())

    assert observed == []


@patch("datahub.ingestion.source.sql.mysql.create_engine")
def test_general_log_drops_query_from_unknown_session(mock_create_engine):
    # A Query on a thread with no preceding Init DB/USE has no known schema, so
    # the system-schema / database_pattern filters can't apply. It must be
    # dropped rather than emitted unfiltered.
    mock_create_engine.return_value = _patch_rows(
        [
            _glog_row(
                "root[root] @ localhost []", 9, "Query", "SELECT * FROM mysql.user"
            ),
            _glog_row("root[root] @ localhost []", 9, "Query", "SELECT id FROM orders"),
        ]
    )

    observed = list(_source(usage_source="general_log")._fetch_general_log_queries())

    assert observed == []


def test_is_allowed_table_respects_pattern_and_system_schemas():
    source = _source(database_pattern={"allow": ["keep_db"]})
    assert source._is_allowed_table("keep_db.orders")
    assert not source._is_allowed_table("drop_db.orders")
    # System schemas are never allowed, regardless of the pattern.
    assert not source._is_allowed_table("mysql.user")


def test_is_temp_table_flags_undiscovered_tables():
    source = _source()
    source.discovered_datasets.add("appdb.orders")
    # A table we ingested is real; anything else (temp tables, filtered-out
    # databases, phantom db.db.table names) is treated as temp so it is not
    # emitted as its own dataset.
    assert not source._is_temp_table("appdb.orders")
    assert source._is_temp_table("appdb.tmp_scratch")
    assert source._is_temp_table("appdb.appdb.orders")


def test_usage_skips_phantom_entity_from_mis_quoted_identifier():
    # A single backticked identifier with an embedded dot parses as one table
    # name, so the two-tier default_schema is prepended and the URN doubles the
    # database (appdb.appdb.tmp_upsert). Since that phantom is not a discovered
    # table it must be suppressed rather than emitted as a column-less dataset.
    source = _source()
    source.discovered_datasets.add("appdb.orders")
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    with patch.object(
        source,
        "_fetch_performance_schema_queries",
        return_value=[
            ObservedQuery(
                query="INSERT INTO `appdb.tmp_upsert` SELECT id FROM orders",
                timestamp=now,
                default_schema="appdb",
                usage_multiplier=3,
            )
        ],
    ):
        urns = {wu.get_urn() for wu in source._generate_aggregator_workunits()}

    assert not any("appdb.appdb" in urn for urn in urns), (
        f"phantom doubled-database URN must not be emitted; got {urns}"
    )


def test_usage_does_not_attribute_to_filtered_out_database():
    # A query running in an allowed database that references a table in a
    # filtered-out database must not resurrect that table as an entity.
    source = _source(database_pattern={"allow": ["keep_db"]})
    source.discovered_datasets.add("keep_db.orders")
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    with patch.object(
        source,
        "_fetch_performance_schema_queries",
        return_value=[
            ObservedQuery(
                query="INSERT INTO orders SELECT id FROM drop_db.secrets",
                timestamp=now,
                default_schema="keep_db",
                usage_multiplier=5,
            )
        ],
    ):
        urns = {wu.get_urn() for wu in source._generate_aggregator_workunits()}

    assert not any("drop_db" in urn for urn in urns), (
        f"filtered-out database must not appear in emitted URNs; got {urns}"
    )


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
