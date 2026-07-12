from datetime import datetime, timezone

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.usage.clickhouse_usage import (
    ClickHouseUsageConfig,
    ClickHouseUsageSource,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass


class _FakeRow:
    def __init__(self, mapping: dict):
        self._mapping = mapping


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeEngine:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *args, **kwargs):
        return _FakeResult(self._rows)


def _make_config():
    return ClickHouseUsageConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "email_domain": "example.com",
            "start_time": "2020-04-14T00:00:00Z",
            "end_time": "2020-04-15T00:00:00Z",
        }
    )


def _make_source(rows, monkeypatch):
    source = ClickHouseUsageSource(
        config=_make_config(),
        ctx=PipelineContext(run_id="test"),
    )
    monkeypatch.setattr(source, "_make_sql_engine", lambda: _FakeEngine(rows))
    return source


def test_observed_queries_mapping(monkeypatch):
    rows = [
        _FakeRow(
            {
                "query_id": "q1",
                "query": "SELECT col_a FROM my_db.events",
                "username": "  alice  ",
                "starttime": datetime(2020, 4, 14, 6, 0, 0),
                "endtime": datetime(2020, 4, 14, 6, 0, 1),
                "normalized_query_hash": 12345,
            }
        ),
        _FakeRow(
            {
                "query_id": "q2",
                "query": "",  # empty query is skipped
                "username": "bob",
                "starttime": datetime(2020, 4, 14, 6, 5, 0),
                "endtime": datetime(2020, 4, 14, 6, 5, 1),
                "normalized_query_hash": 0,
            }
        ),
        _FakeRow(
            {
                "query_id": "q3",
                "query": "SELECT col_b FROM my_db.events",
                "username": "carol@corp.example",  # already an email
                "starttime": datetime(2020, 4, 14, 6, 10, 0),
                "endtime": datetime(2020, 4, 14, 6, 10, 1),
                "normalized_query_hash": 67890,
            }
        ),
    ]
    source = _make_source(rows, monkeypatch)

    observed = list(source._get_observed_queries())

    assert [o.query for o in observed] == [
        "SELECT col_a FROM my_db.events",
        "SELECT col_b FROM my_db.events",
    ]
    # Username is stripped and the email_domain is appended when absent.
    assert str(observed[0].user) == "urn:li:corpuser:alice@example.com"
    # An address that already contains '@' is used verbatim.
    assert str(observed[1].user) == "urn:li:corpuser:carol@corp.example"
    # Timestamps are made timezone-aware (UTC).
    assert observed[0].timestamp == datetime(2020, 4, 14, 6, 0, 0, tzinfo=timezone.utc)
    # ClickHouse is 2-level; default_db must not be passed to the parser.
    assert observed[0].default_db is None
    assert observed[0].query_hash == "12345"


def test_usage_statistics_generated_via_aggregator(monkeypatch):
    rows = [
        _FakeRow(
            {
                "query_id": f"q{i}",
                "query": "SELECT col_a FROM my_db.events",
                "username": "alice",
                "starttime": datetime(2020, 4, 14, 6, 0, 0),
                "endtime": datetime(2020, 4, 14, 6, 0, 1),
                "normalized_query_hash": 12345,
            }
        )
        for i in range(3)
    ]
    source = _make_source(rows, monkeypatch)

    usage_aspects = [
        wu.metadata.aspect
        for wu in source.get_workunits_internal()
        if isinstance(getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass)
    ]

    assert usage_aspects, "expected DatasetUsageStatistics to be produced"
    total_query_count = sum(a.totalSqlQueries or 0 for a in usage_aspects)
    assert total_query_count == 3
    source.close()


def test_is_allowed_table_respects_patterns():
    config = ClickHouseUsageConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "email_domain": "example.com",
            "database_pattern": {"deny": ["system"]},
        }
    )
    source = ClickHouseUsageSource(config=config, ctx=PipelineContext(run_id="test"))

    assert source._is_allowed_table("my_db.events")
    assert not source._is_allowed_table("system.query_log")
    source.close()
