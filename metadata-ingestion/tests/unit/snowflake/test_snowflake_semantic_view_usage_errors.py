"""Failure and skip paths in SemanticViewUsageExtractor.

The happy paths live in test_snowflake_semantic_view_usage.py; these tests cover
query failures, rows for views that were not discovered, and malformed JSON
columns in the account_usage results.
"""

from datetime import datetime, timezone
from typing import List

from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_semantic_view_usage import (
    SemanticViewUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass
from tests.unit.snowflake.conftest import (
    QueryHandler,
    as_mock,
    fake_connection,
    log_keys,
    make_v2_config,
)

BUCKET = datetime(2024, 1, 1, tzinfo=timezone.utc)
VIEW = "db.schema.view"


def _make_extractor(dispatch: QueryHandler) -> SemanticViewUsageExtractor:
    config = make_v2_config(
        email_domain="example.com",
        semantic_views={
            "enabled": True,
            "include_usage": True,
            "include_queries": True,
        },
    )
    report = SnowflakeV2Report()
    return SemanticViewUsageExtractor(
        config=config,
        report=report,
        connection=fake_connection(dispatch),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
    )


def _usage_row(view_name: str, **overrides: object) -> dict:
    row = {
        "SEMANTIC_VIEW_NAME": view_name,
        "BUCKET_START_TIME": BUCKET,
        "TOTAL_QUERIES": 3,
        "UNIQUE_USERS": 1,
        "DIRECT_SQL_QUERIES": 3,
        "CORTEX_ANALYST_QUERIES": 0,
        "AVG_EXECUTION_TIME_MS": None,
        "TOTAL_ROWS_PRODUCED": None,
        "USER_COUNTS": '[{"user_name": "alice", "query_count": 3}]',
        "TOP_SQL_QUERIES": ["select 1"],
    }
    row.update(overrides)
    return row


def test_usage_query_failure_is_a_warning_not_a_crash() -> None:
    extractor = _make_extractor(lambda _sql: RuntimeError("account_usage down"))

    assert list(extractor.get_semantic_view_usage_workunits({VIEW})) == []
    assert "Failed to extract semantic view usage statistics" in log_keys(
        extractor.report.warnings
    )


def test_usage_rows_for_undiscovered_views_and_bad_json_are_tolerated() -> None:
    """Rows for unknown views are dropped and malformed JSON columns degrade
    to empty values, neither of which is worth a warning."""
    rows = [
        _usage_row("DB.SCHEMA.OTHER"),
        _usage_row("", TOP_SQL_QUERIES=None),
        _usage_row(
            "DB.SCHEMA.VIEW",
            USER_COUNTS="not json",
            TOP_SQL_QUERIES="also not json",
        ),
    ]
    extractor = _make_extractor(lambda _sql: rows)

    workunits = list(extractor.get_semantic_view_usage_workunits({VIEW}))

    assert len(workunits) == 1
    stats = workunits[0].get_aspect_of_type(DatasetUsageStatisticsClass)
    assert stats is not None
    assert stats.totalSqlQueries == 3
    assert stats.userCounts == []
    assert not stats.topSqlQueries
    assert extractor.report.warnings == []


def test_query_fetch_failure_and_empty_discovery_emit_nothing() -> None:
    failing = _make_extractor(lambda _sql: RuntimeError("down"))
    assert list(failing.get_semantic_view_query_workunits({VIEW})) == []
    assert "Failed to extract semantic view queries" in log_keys(
        failing.report.warnings
    )

    idle = _make_extractor(lambda _sql: [])
    assert list(idle.get_semantic_view_query_workunits(set())) == []
    as_mock(idle.connection).query.assert_not_called()


def test_query_rows_without_a_view_name_or_for_unknown_views_are_skipped() -> None:
    rows: List[dict] = [
        {"SEMANTIC_VIEW_NAME": None, "QUERY_ID": "q0"},
        {"SEMANTIC_VIEW_NAME": "DB.SCHEMA.OTHER", "QUERY_ID": "q1"},
    ]
    extractor = _make_extractor(lambda _sql: rows)

    assert list(extractor.get_semantic_view_query_workunits({VIEW})) == []
    assert extractor.report.warnings == []
