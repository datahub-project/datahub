"""End-to-end flow of SnowflakeQueriesExtractor and SnowflakeQueriesSource.

The extractor's row parsing is covered in test_snowflake_queries.py; these tests
drive get_workunits_internal as a whole against a dispatching connection so the
user lookup, copy-history and query-log fetches, stored-procedure tracking,
audit-log caching and cleanup are exercised together.
"""

import json
import pathlib
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Union
from unittest.mock import MagicMock, patch

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeFilterConfig,
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesExtractor,
    SnowflakeQueriesExtractorConfig,
    SnowflakeQueriesSource,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping

START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 2, tzinfo=timezone.utc)
QueryResult = Union[List[dict], Exception]
Dispatch = Callable[[str], QueryResult]


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _audit_row(
    query_id: str, query_type: str, query_text: str, **overrides: object
) -> dict:
    row = {
        "QUERY_ID": query_id,
        "QUERY_TEXT": query_text,
        "QUERY_START_TIME": START,
        "QUERY_TYPE": query_type,
        "ROWS_INSERTED": 0,
        "ROWS_UPDATED": 0,
        "ROWS_DELETED": 0,
        "USER_NAME": "ANALYST",
        "ROLE_NAME": "ROLE",
        "SESSION_ID": "1",
        "WAREHOUSE_NAME": "WH",
        "DATABASE_NAME": "DB",
        "SCHEMA_NAME": "S",
        "DEFAULT_DB": "DB",
        "DEFAULT_SCHEMA": "S",
        "ROOT_QUERY_ID": None,
        "QUERY_COUNT": 1,
        "QUERY_SECONDARY_FINGERPRINT": None,
        "QUERY_DURATION": 10,
        "OBJECTS_MODIFIED": json.dumps([]),
        "DIRECT_OBJECTS_ACCESSED": json.dumps([]),
        "OBJECT_MODIFIED_BY_DDL": None,
    }
    row.update(overrides)
    return row


def _make_extractor(
    dispatch: Dispatch,
    local_temp_path: Optional[pathlib.Path] = None,
    **config_overrides: object,
) -> SnowflakeQueriesExtractor:
    identifier_config = SnowflakeIdentifierConfig()
    report = SourceReport()
    config = SnowflakeQueriesExtractorConfig(
        window=BaseTimeWindowConfig(start_time=START, end_time=END),
        local_temp_path=local_temp_path,
        **config_overrides,  # type: ignore[arg-type]
    )
    connection = MagicMock()

    def query(sql: str) -> List[dict]:
        result = dispatch(sql)
        if isinstance(result, Exception):
            raise result
        return list(result)

    connection.query.side_effect = query
    return SnowflakeQueriesExtractor(
        connection=connection,
        config=config,
        structured_report=report,
        filters=SnowflakeFilter(
            filter_config=SnowflakeFilterConfig(), structured_reporter=report
        ),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=identifier_config, structured_reporter=report
        ),
        discovered_tables=["db.s.target", "db.s.src"],
    )


def _dispatch_for(rows: Dict[str, QueryResult]) -> Dispatch:
    def dispatch(sql: str) -> QueryResult:
        upper = sql.upper()
        for marker, result in rows.items():
            if marker in upper:
                return result
        raise AssertionError(f"Unexpected query: {sql[:80]}")

    return dispatch


def test_audit_log_feeds_users_copy_history_and_query_log_into_lineage(
    tmp_path: pathlib.Path,
) -> None:
    insert = _audit_row(
        "q-insert",
        "INSERT",
        "insert into db.s.target select * from db.s.src",
        OBJECTS_MODIFIED=json.dumps(
            [{"objectName": "DB.S.TARGET", "objectDomain": "Table", "columns": []}]
        ),
        DIRECT_OBJECTS_ACCESSED=json.dumps(
            [{"objectName": "DB.S.SRC", "objectDomain": "Table", "columns": []}]
        ),
    )
    call = _audit_row("q-call", "CALL", "call db.s.my_proc()")
    rows: Dict[str, QueryResult] = {
        "SNOWFLAKE.ACCOUNT_USAGE.USERS": [
            {"NAME": "ANALYST", "EMAIL": "analyst@example.com"},
            {"NAME": "BROKEN"},  # missing EMAIL: reported, not fatal
        ],
        "COPY_HISTORY": [{"row": 1}, {"row": 2}],
        "QUERY_HISTORY": [insert, call],
    }
    extractor = _make_extractor(_dispatch_for(rows), local_temp_path=tmp_path)
    copy_mapping = KnownLineageMapping(
        upstream_urn="urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)",
        downstream_urn=extractor.identifiers.gen_dataset_urn("db.s.target"),
    )

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_queries.SnowflakeLineageExtractor._process_external_lineage_result_row",
        side_effect=[copy_mapping, KeyError("bad row")],
    ):
        workunits = list(extractor.get_workunits_internal())

    assert extractor.report.num_users == 1
    assert len(extractor.structured_reporter.warnings) == 2  # user row, copy row
    assert extractor.report.stored_proc_lineage is not None
    assert extractor.report.audit_log_path == str(tmp_path / "audit_log.sqlite")
    urns = {wu.get_urn() for wu in workunits}
    assert extractor.identifiers.gen_dataset_urn("db.s.target") in urns
    extractor.close()
    # With an explicit local_temp_path the audit log is kept for reuse...
    assert (tmp_path / "audit_log.sqlite").exists()

    # ...and a second extractor over the same path replays it without refetching.
    cached = _make_extractor(
        _dispatch_for({"SNOWFLAKE.ACCOUNT_USAGE.USERS": []}),
        local_temp_path=tmp_path,
    )
    list(cached.get_workunits_internal())
    cached.close()


def test_query_log_fetch_is_skipped_when_no_query_feature_is_enabled() -> None:
    extractor = _make_extractor(
        _dispatch_for({"SNOWFLAKE.ACCOUNT_USAGE.USERS": []}),
        include_lineage=False,
        include_queries=False,
        include_usage_statistics=False,
        include_query_usage_statistics=False,
        include_operations=False,
    )

    assert list(extractor.get_workunits_internal()) == []
    issued = [
        c.args[0].upper() for c in _as_mock(extractor.connection).query.call_args_list
    ]
    assert not any("QUERY_HISTORY" in q for q in issued)
    extractor.close()
    # The default temp dir audit log is removed on close.
    assert not pathlib.Path(extractor.report.audit_log_path or "").exists()


def test_temp_and_allowed_table_checks_use_patterns_and_discovered_tables() -> None:
    extractor = _make_extractor(
        _dispatch_for({}), temporary_tables_pattern=["^db\\.s\\.tmp_.*"]
    )

    assert extractor.is_temp_table("db.s.tmp_1")
    assert extractor.is_temp_table("db.s.undiscovered")  # allowed by pattern but unseen
    assert not extractor.is_temp_table("db.s.target")
    assert extractor.is_allowed_table("db.s.target")
    assert not extractor.is_allowed_table("db.s.undiscovered")


def test_queries_source_delegates_to_extractor_and_closes_connection() -> None:
    native = MagicMock()
    native.cursor.return_value.execute.return_value = []
    config = {
        "connection": {"account_id": "acct", "username": "u", "password": "p"},
        "window": {"start_time": START.isoformat(), "end_time": END.isoformat()},
    }

    with patch("snowflake.connector.connect", return_value=native):
        source = SnowflakeQueriesSource.create(config, PipelineContext(run_id="t"))

    assert source.get_report().queries_extractor is source.queries_extractor.report
    with patch.object(
        source.queries_extractor, "get_workunits_internal", return_value=iter([])
    ):
        assert list(source.get_workunits_internal()) == []
    assert source.report.window == source.config.window

    source.close()
    native.close.assert_called_once()
