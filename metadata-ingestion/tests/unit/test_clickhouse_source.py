from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest
from sqlalchemy.engine.url import make_url

import datahub.ingestion.source.sql.clickhouse as clickhouse
import datahub.sql_parsing.sqlglot_lineage as sqlglot_lineage
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig, ClickHouseSource
from datahub.ingestion.source.sql.clickhouse_connection import CLICKHOUSE_CLIENT_NAME
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
    UpstreamLineageClass,
)


def test_clickhouse_uri_https():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "uri_opts": {"protocol": "https"},
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.drivername == "clickhouse"
    assert url.username == "user"
    assert url.password == "password"
    assert url.host == "host"
    assert url.port == 1111
    assert url.database == "db"
    assert url.query.get("protocol") == "https"
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_native():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "scheme": "clickhouse+native",
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.drivername == "clickhouse+native"
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME
    assert "header__User-Agent" not in url.query


def test_clickhouse_uri_native_secure():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
            "uri_opts": {"secure": True},
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("secure") == "True"
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_default_password():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.password is None
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_native_secure_backward_compatibility():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
            "secure": True,
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("secure") == "True"
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_https_backward_compatibility():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "protocol": "https",
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("protocol") == "https"
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME


def test_clickhouse_uri_preserves_user_supplied_ua():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "uri_opts": {"header__User-Agent": "mycorp"},
        }
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("header__User-Agent") == "mycorp"


def test_clickhouse_sqlalchemy_uri_gets_client_identity():
    # A hand-written sqlalchemy_uri (the docs-preferred form) is still tagged.
    config = ClickHouseConfig.model_validate(
        {"sqlalchemy_uri": "clickhouse://user:password@host:1111/db"}
    )
    url = make_url(config.get_sql_alchemy_url())
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME


# Query log extraction tests


def test_query_log_deny_usernames_validation_valid():
    """Test that valid usernames are accepted."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "query_log_deny_usernames": [
                "system",
                "default",
                "admin-user",
                "test_user123",
            ],
        }
    )
    assert set(config.query_log_deny_usernames) == {
        "system",
        "default",
        "admin-user",
        "test_user123",
    }


def test_query_log_deny_usernames_validation_invalid():
    """Test that invalid usernames are rejected (SQL injection prevention)."""

    # SQL injection attempt
    with pytest.raises(ValueError, match="Invalid username"):
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "query_log_deny_usernames": ["system'; DROP TABLE users;--"],
            }
        )

    # Username with quotes
    with pytest.raises(ValueError, match="Invalid username"):
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "query_log_deny_usernames": ["user'name"]}
        )


def test_is_temp_table():
    """Test that is_temp_table correctly identifies temporary tables."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    # Tables that should match temporary patterns
    assert config.is_temp_table("_temp_table")
    assert config.is_temp_table("db.tmp_staging")
    assert config.is_temp_table("db.temp_data")
    assert config.is_temp_table("db._inner_mv")

    # Tables that should NOT match
    assert not config.is_temp_table("normal_table")
    assert not config.is_temp_table("db.regular_table")
    assert not config.is_temp_table("my_db.production_table")


def test_is_temp_table_custom_patterns():
    """Test is_temp_table with custom patterns."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "temporary_tables_pattern": [
                r".*\.staging_.*",  # Any table with staging_ prefix
                r"^test_.*",  # Tables starting with test_
            ],
        }
    )

    assert config.is_temp_table("db.staging_data")
    assert config.is_temp_table("test_table")
    # Default patterns no longer match with custom patterns
    assert not config.is_temp_table("_temp_table")


class _FakeRow:
    def __init__(self, mapping: dict):
        self._mapping = mapping


class _FakeEngine:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *args, **kwargs):
        return iter(self._rows)


def test_query_log_lineage_resolves_unqualified_tables(monkeypatch):
    # Both sides must pick the database up from current_database, or the lineage
    # lands on orphan URNs with no database part.
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-15T00:00:00Z",
        }
    )
    source = ClickHouseSource(config, PipelineContext(run_id="test"))

    rows = [
        _FakeRow(
            {
                "query_id": "q1",
                "query": "INSERT INTO daily_agg SELECT col_a FROM raw_events",
                "query_kind": "Insert",
                "user": "alice",
                "event_time": datetime(2020, 4, 14, 6, 0, 0, tzinfo=timezone.utc),
                "current_database": "my_db",
                "normalized_query_hash": 12345,
            }
        )
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    lineage = [
        wu.metadata
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]

    assert len(lineage) == 1
    aspect = lineage[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)
    assert (
        lineage[0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:clickhouse,my_db.daily_agg,PROD)"
    )
    assert [u.dataset for u in aspect.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,my_db.raw_events,PROD)"
    ]


def test_query_log_lineage_does_not_over_qualify(monkeypatch):
    # current_database must only fill an empty slot: names that already carry their own
    # database must not become my_db.analytics_marts.daily_agg.
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-15T00:00:00Z",
        }
    )
    source = ClickHouseSource(config, PipelineContext(run_id="test"))

    rows = [
        _FakeRow(
            {
                "query_id": "q1",
                "query": (
                    "INSERT INTO analytics_marts.daily_agg "
                    "SELECT col_a FROM analytics_raw.raw_events"
                ),
                "query_kind": "Insert",
                "user": "alice",
                "event_time": datetime(2020, 4, 14, 6, 0, 0, tzinfo=timezone.utc),
                "current_database": "my_db",
                "normalized_query_hash": 12345,
            }
        )
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    lineage = [
        wu.metadata
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]

    assert len(lineage) == 1
    aspect = lineage[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)
    assert (
        lineage[0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:clickhouse,analytics_marts.daily_agg,PROD)"
    )
    assert [u.dataset for u in aspect.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,analytics_raw.raw_events,PROD)"
    ]


def test_query_log_query_only_fetches_columns_it_reads():
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-15T00:00:00Z",
        }
    )
    source = ClickHouseSource(config, PipelineContext(run_id="test"))

    sql = source._build_query_log_query()

    for unused in ("query_duration_ms", "read_rows", "written_rows"):
        assert unused not in sql

    for needed in (
        "query_id",
        "query_kind",
        "current_database",
        "normalized_query_hash",
    ):
        assert needed in sql

    assert "event_time >= '2020-04-14 00:00:00'" in sql
    assert "event_time < '2020-04-15 00:00:00'" in sql


def test_query_log_row_without_hash_is_skipped_and_reported():
    # normalized_query_hash is a non-nullable UInt64, so this only happens if our
    # own SELECT loses the column. Skipping loudly beats grouping every row
    # together under a missing key.
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-15T00:00:00Z",
        }
    )
    source = ClickHouseSource(config, PipelineContext(run_id="test"))

    row: Dict[str, Any] = {
        "query_id": "q1",
        "query": "INSERT INTO daily_agg SELECT col_a FROM raw_events",
        "query_kind": "Insert",
        "user": "alice",
        "event_time": datetime(2020, 4, 14, 6, 0, 0, tzinfo=timezone.utc),
        "current_database": "my_db",
    }

    assert source._parse_query_log_row(row) is None
    assert len(source.report.warnings) == 1

    row["normalized_query_hash"] = 12345
    observed = source._parse_query_log_row(row)
    assert observed is not None
    assert observed.query_hash == "12345"


def _query_log_source() -> ClickHouseSource:
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "include_usage_statistics": True,
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-16T00:00:00Z",
        }
    )
    return ClickHouseSource(config, PipelineContext(run_id="test"))


def _insert_row(
    *,
    query_id: str,
    user: str = "alice",
    database: str = "my_db",
    hash_value: Optional[int] = 12345,
    day: int = 14,
    literal: str = "a",
) -> _FakeRow:
    # Same shape every time; only the literal changes, as in a real query log.
    return _FakeRow(
        {
            "query_id": query_id,
            "query": (
                "INSERT INTO daily_agg SELECT col_a FROM raw_events "
                f"WHERE col_b = '{literal}'"
            ),
            "query_kind": "Insert",
            "user": user,
            "event_time": datetime(2020, 4, day, 6, 0, 0, tzinfo=timezone.utc),
            "current_database": database,
            "normalized_query_hash": hash_value,
        }
    )


def _usage_for(source: ClickHouseSource, urn: str) -> List[DatasetUsageStatisticsClass]:
    return [
        wu.metadata.aspect
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
        and wu.metadata.entityUrn == urn
    ]


def _lineage_urns(source: ClickHouseSource) -> Set[Optional[str]]:
    return {
        wu.metadata.entityUrn
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    }


_RAW_EVENTS = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,my_db.raw_events,PROD)"


def test_grouping_preserves_usage_counts(monkeypatch):
    # If grouping drops the occurrence count, totalSqlQueries silently collapses.
    source = _query_log_source()
    rows = [_insert_row(query_id=f"q{i}", literal=str(i)) for i in range(20)]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    usage = _usage_for(source, _RAW_EVENTS)

    assert len(usage) == 1
    assert usage[0].totalSqlQueries == 20


def test_grouping_separates_databases(monkeypatch):
    # Identical text under two databases shares a hash but resolves to different
    # tables; merging them would lose one side's lineage entirely.
    source = _query_log_source()
    rows = [
        _insert_row(query_id="q1", database="db_a"),
        _insert_row(query_id="q2", database="db_b"),
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    assert _lineage_urns(source) == {
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,db_a.daily_agg,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:clickhouse,db_b.daily_agg,PROD)",
    }


def test_grouping_separates_users(monkeypatch):
    source = _query_log_source()
    rows = [_insert_row(query_id=f"a{i}", user="alice") for i in range(3)] + [
        _insert_row(query_id=f"b{i}", user="bob") for i in range(2)
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    usage = _usage_for(source, _RAW_EVENTS)

    assert len(usage) == 1
    aspect = usage[0]
    assert aspect.totalSqlQueries == 5
    assert aspect.uniqueUserCount == 2
    assert aspect.userCounts is not None
    assert {c.user.split(":")[-1]: c.count for c in aspect.userCounts} == {
        "alice": 3,
        "bob": 2,
    }


def test_grouping_separates_time_buckets(monkeypatch):
    # datasetUsageStatistics is a timeseries aspect: one per bucket.
    source = _query_log_source()
    rows = [_insert_row(query_id=f"d14-{i}", day=14) for i in range(4)] + [
        _insert_row(query_id=f"d15-{i}", day=15) for i in range(6)
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    usage = _usage_for(source, _RAW_EVENTS)

    assert sorted(u.totalSqlQueries or 0 for u in usage) == [4, 6]


def test_one_parse_per_shape_across_users_and_buckets(monkeypatch):
    # Handing every batch of a shape the same SQL text is a performance property,
    # not a correctness one - no output changes if it regresses - so assert the
    # parse count directly.
    source = _query_log_source()
    rows = [
        # The literal is unique per row, so each batch's first row - the one that
        # would be parsed without the substitution - carries a different string.
        _insert_row(
            query_id=f"{user}-{day}-{i}",
            user=user,
            day=day,
            literal=f"{user}-{day}-{i}",
        )
        for user in ("alice", "bob")
        for day in (14, 15)
        for i in range(3)
    ]  # one shape x 2 users x 2 buckets x 3 executions = 12 rows, 4 batches
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    sqlglot_lineage._sqlglot_lineage_cached.cache_clear()
    list(source._extract_query_log())

    assert sqlglot_lineage._sqlglot_lineage_cached.cache_info().misses == 1


def test_query_log_query_skips_rows_that_touch_no_real_table():
    # ClickHouse resolves SELECT 1 to system.one and numbers() to
    # _table_function.numbers, so the fetch drops them without matching query text.
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "include_query_log_lineage": True,
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-15T00:00:00Z",
        }
    )
    source = ClickHouseSource(config, PipelineContext(run_id="test"))

    sql = source._build_query_log_query()

    for prefix in (
        "system.",
        "_table_function.",
        "information_schema.",
        "INFORMATION_SCHEMA.",
    ):
        assert f"NOT startsWith(t, '{prefix}')" in sql


def _select_row(
    *,
    query_id: str = "s1",
    user: str = "alice",
    database: str = "my_db",
    hash_value: Optional[int] = 54321,
    day: int = 14,
    tables: Tuple[str, ...] = ("my_db.raw_events",),
    columns: Tuple[str, ...] = ("my_db.raw_events.col_a",),
) -> _FakeRow:
    return _FakeRow(
        {
            "query_id": query_id,
            "query": "SELECT col_a FROM raw_events WHERE col_b = 'x'",
            "query_kind": "Select",
            "user": user,
            "event_time": datetime(2020, 4, day, 6, 0, 0, tzinfo=timezone.utc),
            "current_database": database,
            "normalized_query_hash": hash_value,
            # Joined, as the fetch asks ClickHouse to return them - the HTTP
            # driver does not hand arrays back as lists.
            "tables_joined": "\n".join(tables),
            "columns_joined": "\n".join(columns),
        }
    )


def test_usage_credits_every_table_clickhouse_resolved(monkeypatch):
    # ClickHouse already resolved the read down to tables and columns, so usage
    # comes straight off the row. A query spanning two tables credits both.
    source = _query_log_source()
    rows = [
        _select_row(
            tables=("my_db.raw_events", "my_db.dim_users"),
            columns=(
                "my_db.raw_events.col_a",
                "my_db.raw_events.col_b",
                "my_db.dim_users.col_c",
            ),
        )
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    by_urn = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    }

    dim_users = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,my_db.dim_users,PROD)"
    assert set(by_urn) == {_RAW_EVENTS, dim_users}
    assert _field_counts(by_urn[_RAW_EVENTS]) == {"col_a": 1, "col_b": 1}
    assert _field_counts(by_urn[dim_users]) == {"col_c": 1}


def test_usage_counts_columns_the_parser_would_miss(monkeypatch):
    # The whole point of using ClickHouse's own column list: a filter-only column
    # never appears in a parsed SELECT's column lineage, so it used to go
    # uncounted. Same for the expansion of a star.
    source = _query_log_source()
    rows = [
        _select_row(
            columns=(
                "my_db.raw_events.col_a",
                "my_db.raw_events.col_filtered_on",
            )
        )
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    usage = _usage_for(source, _RAW_EVENTS)

    assert _field_counts(usage[0]) == {"col_a": 1, "col_filtered_on": 1}


def test_usage_does_not_parse_selects(monkeypatch):
    # Skipping the parse is the reason this path exists, and nothing in the
    # output would change if it regressed - so assert the parse count directly.
    source = _query_log_source()
    rows = [_select_row(query_id=f"s{i}", day=14 + i % 2) for i in range(6)]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    sqlglot_lineage._sqlglot_lineage_cached.cache_clear()
    list(source._extract_query_log())

    assert sqlglot_lineage._sqlglot_lineage_cached.cache_info().misses == 0


def test_usage_produces_no_lineage(monkeypatch):
    source = _query_log_source()
    monkeypatch.setattr(
        clickhouse, "create_engine", lambda *a, **kw: _FakeEngine([_select_row()])
    )

    assert _lineage_urns(source) == set()


def test_usage_skips_system_tables_within_a_kept_row(monkeypatch):
    # The fetch keeps a row if ANY table is a real one, so a row can still carry
    # system entries alongside the table we care about.
    source = _query_log_source()
    rows = [
        _select_row(
            tables=("my_db.raw_events", "system.one"),
            columns=("my_db.raw_events.col_a", "system.one.dummy"),
        )
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    urns = {
        wu.metadata.entityUrn
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    }

    assert urns == {_RAW_EVENTS}


def test_usage_skips_clickhouse_temporary_tables(monkeypatch):
    # ClickHouse reports temporary tables under a pseudo-database. Minting URNs
    # for those would put usage on datasets that were never ingested.
    source = _query_log_source()
    rows = [
        _select_row(
            tables=("my_db.raw_events", "_temporary_and_external_tables.scratch"),
            columns=(
                "my_db.raw_events.col_a",
                "_temporary_and_external_tables.scratch.col_x",
            ),
        )
    ]
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    urns = {
        wu.metadata.entityUrn
        for wu in source._extract_query_log()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    }

    assert urns == {_RAW_EVENTS}


def test_usage_aggregates_users_and_buckets(monkeypatch):
    source = _query_log_source()
    rows = (
        [_select_row(query_id=f"a{i}", user="alice") for i in range(3)]
        + [_select_row(query_id=f"b{i}", user="bob") for i in range(2)]
        + [_select_row(query_id=f"c{i}", day=15) for i in range(4)]
    )
    monkeypatch.setattr(clickhouse, "create_engine", lambda *a, **kw: _FakeEngine(rows))

    usage = _usage_for(source, _RAW_EVENTS)

    by_total = {u.totalSqlQueries or 0: u for u in usage}
    assert sorted(by_total) == [4, 5]
    day14 = by_total[5]
    assert day14.uniqueUserCount == 2
    assert day14.userCounts is not None
    assert {c.user.split(":")[-1]: c.count for c in day14.userCounts} == {
        "alice": 3,
        "bob": 2,
    }


def test_usage_row_without_hash_is_skipped_and_reported(monkeypatch):
    source = _query_log_source()
    row = _select_row()
    del row._mapping["normalized_query_hash"]
    monkeypatch.setattr(
        clickhouse, "create_engine", lambda *a, **kw: _FakeEngine([row])
    )

    assert _usage_for(source, _RAW_EVENTS) == []
    assert len(source.report.warnings) == 1


def _field_counts(aspect: DatasetUsageStatisticsClass) -> Dict[str, int]:
    assert aspect.fieldCounts is not None
    return {f.fieldPath: f.count for f in aspect.fieldCounts}


def test_query_log_query_fetches_selects_only_for_usage():
    # A Select writes no table, so it reaches neither the lineage map nor an
    # operation. With usage off it would be fetched and parsed to produce nothing.
    base = {
        "host_port": "localhost:8123",
        "start_time": "2020-04-14T00:00:00Z",
        "end_time": "2020-04-15T00:00:00Z",
    }

    lineage_only = ClickHouseSource(
        ClickHouseConfig.model_validate({**base, "include_query_log_lineage": True}),
        PipelineContext(run_id="test"),
    )
    assert "'Select'" not in lineage_only._build_query_log_query()

    assert "tables" in lineage_only._build_query_log_query()  # used by the filter
    assert "columns" not in lineage_only._build_query_log_query()

    with_usage = ClickHouseSource(
        ClickHouseConfig.model_validate({**base, "include_usage_statistics": True}),
        PipelineContext(run_id="test"),
    )
    sql = with_usage._build_query_log_query()
    assert "'Select'" in sql
    # Joined server-side: the HTTP driver returns arrays as their printed form.
    assert "arrayStringConcat(columns" in sql
