"""Failure and skip paths in SemanticViewUsageExtractor.

The happy paths live in test_snowflake_semantic_view_usage.py; these tests cover
query failures, rows for views that were not discovered, and malformed JSON
columns in the account_usage results.
"""

from datetime import datetime, timezone
from typing import Callable, List, Union
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_semantic_view_usage import (
    SemanticViewUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass

BUCKET = datetime(2024, 1, 1, tzinfo=timezone.utc)
VIEW = "db.schema.view"
QueryResult = Union[List[dict], Exception]


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _make_extractor(
    dispatch: Callable[[str], QueryResult],
) -> SemanticViewUsageExtractor:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "email_domain": "example.com",
            "semantic_views": {
                "enabled": True,
                "include_usage": True,
                "include_queries": True,
            },
        }
    )
    report = SnowflakeV2Report()
    connection = MagicMock()

    def query(sql: str) -> List[dict]:
        result = dispatch(sql)
        if isinstance(result, Exception):
            raise result
        return list(result)

    connection.query.side_effect = query
    return SemanticViewUsageExtractor(
        config=config,
        report=report,
        connection=connection,
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
    assert [w.message for w in extractor.report.warnings] == [
        "Failed to extract semantic view usage statistics"
    ]


def test_usage_rows_for_undiscovered_views_and_bad_json_are_tolerated() -> None:
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
    assert [w.message for w in failing.report.warnings] == [
        "Failed to extract semantic view queries"
    ]

    idle = _make_extractor(lambda _sql: [])
    assert list(idle.get_semantic_view_query_workunits(set())) == []
    _as_mock(idle.connection).query.assert_not_called()


def test_query_rows_without_a_view_name_or_for_unknown_views_are_skipped() -> None:
    rows: List[dict] = [
        {"SEMANTIC_VIEW_NAME": None, "QUERY_ID": "q0"},
        {"SEMANTIC_VIEW_NAME": "DB.SCHEMA.OTHER", "QUERY_ID": "q1"},
    ]
    extractor = _make_extractor(lambda _sql: rows)

    assert list(extractor.get_semantic_view_query_workunits({VIEW})) == []
    assert extractor.report.warnings == []
