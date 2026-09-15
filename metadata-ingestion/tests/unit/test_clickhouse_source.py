from datetime import datetime, timezone

import pytest
from sqlalchemy.engine.url import make_url

import datahub.ingestion.source.sql.clickhouse as clickhouse
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig, ClickHouseSource
from datahub.ingestion.source.sql.clickhouse_connection import CLICKHOUSE_CLIENT_NAME
from datahub.metadata.schema_classes import UpstreamLineageClass


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
