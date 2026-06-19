import datetime
from typing import Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass
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
