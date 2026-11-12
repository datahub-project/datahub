from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import ProgrammingError

from datahub.ingestion.source.sql.mssql.query import (
    AZURE_SQL_DATABASE_ENGINE_EDITION,
    MSSQLQuery,
    QueryHistoryWindow,
)
from datahub.ingestion.source.sql.mssql.query_lineage_extractor import (
    MSSQLLineageExtractor,
    QueryLogCache,
    query_log_file_pattern,
)
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig
from datahub.ingestion.source.sql.sql_common import SQLSourceReport

_WINDOW = QueryHistoryWindow(
    start_time=datetime(2026, 1, 1), end_time=datetime(2026, 1, 2)
)


def _config(**overrides: Any) -> SQLServerConfig:
    return SQLServerConfig.model_validate(
        {
            "username": "sa",
            "password": "test",
            "host_port": "localhost:1433",
            "database": "TestDB",
            "include_query_lineage": True,
            **overrides,
        }
    )


def _result(rows: List[Dict[str, Any]]) -> Mock:
    result = Mock()
    result.mappings.return_value.fetchone.return_value = rows[0] if rows else None
    result.mappings.return_value.fetchall.return_value = rows
    result.mappings.return_value.all.return_value = rows
    result.mappings.return_value.__iter__ = lambda _: iter(rows)
    return result


def _on_prem() -> List[Mock]:
    """Engine edition and version lookups a log source runs on SQL Server."""
    return [
        _result([{"engine_edition": 3, "current_database": "TestDB"}]),
        _result([{"version": "16.0", "major_version": 16}]),
    ]


def _extractor(
    config: SQLServerConfig, results: List[Union[Mock, Exception]]
) -> Tuple[MSSQLLineageExtractor, Mock, SQLSourceReport]:
    connection = Mock()
    connection.engine.url.database = "TestDB"
    connection.execute.side_effect = results
    report = SQLSourceReport()
    return (
        MSSQLLineageExtractor(config, connection, report, Mock(), "dbo"),
        connection,
        report,
    )


def _log_row(
    query_id: str,
    text: str,
    user: Optional[str],
    count: int,
    truncated: int = 0,
    unmapped: int = 0,
    wrapped: int = 0,
    database: str = "TestDB",
) -> Dict[str, Any]:
    return {
        "query_id": query_id,
        "query_text": text,
        "execution_count": count,
        "total_exec_time_ms": 0.0,
        "database_name": database,
        "last_execution_time_utc": datetime(2026, 1, 1, 9),
        "window_execution_count": count,
        "user_name": user,
        "session_key": None,
        "is_truncated": truncated,
        "is_unmapped": unmapped,
        "wrapped_execution_count": wrapped,
    }


def test_query_log_file_pattern_local_and_blob() -> None:
    assert (
        query_log_file_pattern("/audit/dh_audit_0A1B2C3D_0_1343577783201.sqlaudit")
        == "/audit/dh_audit_0A1B2C3D*.sqlaudit"
    )
    # Blob URLs read by prefix and reject wildcards.
    assert (
        query_log_file_pattern(
            "https://acct.blob.core.windows.net/xe/dh_xe_0_134357778320380000.xel"
        )
        == "https://acct.blob.core.windows.net/xe/dh_xe_"
    )


@pytest.mark.parametrize(
    "login, email_domain, expected",
    [
        ("Jane.Doe@Example.com", None, "urn:li:corpuser:jane.doe@example.com"),
        ("CORP\\jdoe", "example.com", "urn:li:corpuser:jdoe@example.com"),
        ("svc_reporting", None, "urn:li:corpuser:svc_reporting"),
        ("", None, None),
        (None, None, None),
    ],
)
def test_login_mapped_to_corp_user(
    login: Optional[str], email_domain: Optional[str], expected: Optional[str]
) -> None:
    extractor, _, _ = _extractor(
        _config(query_history_source="audit_log", email_domain=email_domain), []
    )
    urn = extractor._user_urn(login)
    assert (urn.urn() if urn else None) == expected


def test_log_source_requires_query_lineage() -> None:
    with pytest.raises(ValueError, match="include_query_lineage"):
        _config(include_query_lineage=False, query_history_source="audit_log")


def test_query_store_rejects_log_only_settings() -> None:
    with pytest.raises(ValueError, match="query_history_path"):
        _config(query_history_path="/audit/x*.sqlaudit")


@pytest.mark.parametrize("database_scoped", [True, False])
def test_extended_events_database_filter(database_scoped: bool) -> None:
    """A server-scoped session must tie each row to a database; only a
    database-scoped (Azure SQL Database) session can attribute rows without
    one to the current database."""
    query, _ = MSSQLQuery.get_query_history_from_extended_events(
        path="/xe/x*.xel",
        window=_WINDOW,
        limit=10,
        min_calls=1,
        exclude_patterns=None,
        database_scoped=database_scoped,
    )
    expected = (
        "COALESCE(x.action_database_name, DB_NAME())"
        if database_scoped
        else "COALESCE(x.action_database_name, DB_NAME(x.action_database_id))"
    )
    sql = str(query)
    assert f"{expected} AS database_name" in sql
    # Rows the session couldn't tie to a database are dropped.
    assert f"AND {expected} IS NOT NULL" in sql


@pytest.mark.parametrize("database_scoped", [True, False])
def test_extended_events_discovery_requires_database_action_when_server_scoped(
    database_scoped: bool,
) -> None:
    """Rows from a server-scoped session without database_name/database_id are
    all dropped later, so discovery must not pick such a session."""
    sql = str(MSSQLQuery.discover_extended_events_files(database_scoped))
    assert ("'database_name', 'database_id'" in sql) == (not database_scoped)


@pytest.mark.parametrize("statements_truncated", [True, False])
def test_audit_log_reassembles_split_statements(statements_truncated: bool) -> None:
    """SQL Server splits statements over 4000 characters across records that
    share a sequence_group_id; they must be joined in sequence order. Only
    Azure truncates instead, so only there is a 4000-character record dropped."""
    query, _ = MSSQLQuery.get_query_history_from_audit_log(
        path="/audit/x*.sqlaudit",
        window=_WINDOW,
        limit=10,
        min_calls=1,
        exclude_patterns=None,
        statements_truncated=statements_truncated,
    )
    sql = " ".join(str(query).split())
    assert "WITHIN GROUP (ORDER BY sequence_number)" in sql
    assert "GROUP BY session_id, sequence_group_id, event_time" in sql
    assert ("MAX(LEN(statement)) >= 4000" in sql) == statements_truncated


def test_audit_log_rows_fold_users_and_skip_truncated() -> None:
    """The SQL returns one row per (statement, user, bucket), already grouped on
    the unwrapped statement. Rows of one statement fold into one query with
    per-user executions; truncated statements are skipped and counted."""
    rows = [
        _log_row("h1", "SELECT a FROM t WHERE id = @P1", "CORP\\alice", 2, wrapped=2),
        _log_row(
            "h1", "SELECT a FROM t WHERE id = @P1", "bob@example.com", 1, wrapped=1
        ),
        _log_row("h4", "SELECT c FROM v", "CORP\\alice", 5, truncated=1),
    ]
    extractor, connection, report = _extractor(
        _config(
            query_history_source="audit_log",
            query_history_path="/audit/x*.sqlaudit",
            email_domain="example.com",
            min_query_calls=2,
        ),
        [*_on_prem(), _result(rows)],
    )

    queries = extractor.extract_query_history()

    assert [q.query_text for q in queries] == ["SELECT a FROM t WHERE id = @P1"]
    assert report.num_query_log_truncated_statements == 5
    assert report.num_query_log_rpc_executions_unwrapped == 3
    _, params = connection.execute.call_args.args
    assert params["path"] == "/audit/x*.sqlaudit"
    # min_query_calls is applied in SQL to each statement's in-window total.
    assert params["min_calls"] == 2
    observed = extractor._build_observed_queries(queries[0])
    assert {
        (q.user.urn() if q.user else None, q.usage_multiplier) for q in observed
    } == {
        ("urn:li:corpuser:alice@example.com", 2),
        ("urn:li:corpuser:bob@example.com", 1),
    }


def test_query_log_rejects_sql_server_before_2017() -> None:
    extractor, _, report = _extractor(
        _config(query_history_source="audit_log", query_history_path="/a*.sqlaudit"),
        [
            _result([{"engine_edition": 3, "current_database": "TestDB"}]),
            _result([{"version": "13.0", "major_version": 13}]),
        ],
    )

    assert extractor.extract_query_history() == []
    assert report.failures


def test_audit_log_on_azure_without_path_reports_failure() -> None:
    """Azure SQL Database has no T-SQL view of its audit location."""
    extractor, _, report = _extractor(
        _config(query_history_source="audit_log"),
        [
            _result(
                [
                    {
                        "engine_edition": AZURE_SQL_DATABASE_ENGINE_EDITION,
                        "current_database": "TestDB",
                    }
                ]
            )
        ],
    )

    assert extractor.extract_query_history() == []
    assert report.failures


def test_extended_events_path_discovered_from_running_session() -> None:
    extractor, connection, _ = _extractor(
        _config(query_history_source="extended_events"),
        [
            _result(
                [
                    {
                        "engine_edition": AZURE_SQL_DATABASE_ENGINE_EDITION,
                        "current_database": "TestDB",
                    }
                ]
            ),
            _result(
                [
                    {
                        "name": "dh_xe",
                        "file_name": "https://acct.blob.core.windows.net/xe/dh_xe_0_1343.xel",
                    }
                ]
            ),
            _result([]),
        ],
    )

    extractor.extract_query_history()

    discovery_sql = str(connection.execute.call_args_list[1].args[0])
    # Azure SQL Database event sessions are database-scoped.
    assert "sys.dm_xe_database_sessions" in discovery_sql
    _, params = connection.execute.call_args_list[2].args
    assert params["path"] == "https://acct.blob.core.windows.net/xe/dh_xe_"
    assert extractor.query_log_found


def test_database_without_query_log_warns() -> None:
    """Database-scoped audits may cover only some databases; the source fails
    the run only when no database had a log."""
    extractor, _, report = _extractor(
        _config(query_history_source="extended_events"), [*_on_prem(), _result([])]
    )

    assert extractor.extract_query_history() == []
    assert report.warnings
    assert not report.failures
    assert not extractor.query_log_found


def test_query_log_discovery_permission_error_reports_failure() -> None:
    extractor, _, report = _extractor(
        _config(query_history_source="audit_log"),
        [*_on_prem(), ProgrammingError("SELECT", {}, Exception("permission denied"))],
    )

    assert extractor.extract_query_history() == []
    assert report.failures


def test_unmapped_executes_are_counted_not_emitted() -> None:
    """An sp_execute whose prepare isn't in the window can't be attributed, so
    it's skipped and counted; executions of wrapped SQL are counted too."""
    rows = [
        _log_row("h1", "SELECT a FROM t WHERE id = @P0", "alice", 5, wrapped=5),
        _log_row("h2", "<unmapped sp_execute>", "alice", 3, unmapped=1),
    ]
    extractor, _, report = _extractor(
        _config(query_history_source="audit_log", query_history_path="/a*.sqlaudit"),
        [*_on_prem(), _result(rows)],
    )

    queries = extractor.extract_query_history()

    assert [q.query_text for q in queries] == ["SELECT a FROM t WHERE id = @P0"]
    assert report.num_query_log_unmapped_executions == 3
    assert report.num_query_log_rpc_executions_unwrapped == 5


def test_query_log_is_read_once_and_split_by_database() -> None:
    """A server audit covers every database, so the per-database extractors of
    one run share a single read and each keeps only its own rows."""
    rows = [
        _log_row("h1", "SELECT a FROM t", "alice", 2, database="SalesDB"),
        _log_row("h2", "SELECT b FROM u", "bob", 4, database="HrDB"),
    ]
    cache: QueryLogCache = {}
    config = _config(
        query_history_source="audit_log", query_history_path="/a*.sqlaudit"
    )

    def run(database: str, results: List[Mock]) -> Tuple[List[str], Mock]:
        connection = Mock()
        connection.engine.url.database = database
        connection.execute.side_effect = results
        extractor = MSSQLLineageExtractor(
            config, connection, SQLSourceReport(), Mock(), "dbo", query_log_cache=cache
        )
        return [q.query_text for q in extractor.extract_query_history()], connection

    def context(database: str) -> List[Mock]:
        return [
            _result([{"engine_edition": 3, "current_database": database}]),
            _result([{"version": "16.0", "major_version": 16}]),
        ]

    sales, _ = run("SalesDB", [*context("SalesDB"), _result(rows)])
    hr, hr_connection = run("HrDB", context("HrDB"))

    assert sales == ["SELECT a FROM t"]
    assert hr == ["SELECT b FROM u"]
    # Only the server-context lookups ran for the second database.
    assert hr_connection.execute.call_count == 2


def test_log_sql_groups_on_inner_sql_and_maps_sp_execute() -> None:
    """Ranking happens on the unwrapped statement, so parameter values of one
    query don't compete, and sp_execute rows are joined to their prepare by
    session and handle."""
    query, _ = MSSQLQuery.get_query_history_from_audit_log(
        path="/audit/x*.sqlaudit",
        window=_WINDOW,
        limit=10,
        min_calls=1,
        exclude_patterns=None,
        statements_truncated=False,
    )
    sql = " ".join(str(query).split())
    assert "HASHBYTES('SHA2_256', stmt_key)" in sql
    assert "PARTITION BY database_name, is_unmapped" in sql
    assert "pr.prep_handle = n.exec_handle AND pr.session_id = n.session_id" in sql
    assert "INTO #dh_query_log" in sql


def test_mostly_unmapped_executions_raise_a_warning() -> None:
    """When many sp_execute calls can't be tied to a prepare, query counts are
    partial and the report says so instead of only bumping a counter."""
    rows = [
        _log_row("h1", "SELECT a FROM t WHERE id = @P0", "alice", 5, wrapped=5),
        _log_row("h2", "<unmapped sp_execute>", "alice", 5, unmapped=1),
    ]
    extractor, _, report = _extractor(
        _config(query_history_source="audit_log", query_history_path="/a*.sqlaudit"),
        [*_on_prem(), _result(rows)],
    )

    extractor.extract_query_history()

    assert report.num_query_log_unmapped_executions == 5
    assert any(
        w.title == "Prepared-statement executions not attributed"
        for w in report.warnings
    )


def test_log_sql_covers_cursor_prepares_and_prepares_before_the_window() -> None:
    """Cursor prepares are unwrapped and sp_cursorexecute is attributed like
    sp_execute. Prepares from before the window are loaded, for sessions that
    executed in it, only to resolve handles; they aren't counted."""
    query, params = MSSQLQuery.get_query_history_from_extended_events(
        path="/xe/x*.xel",
        window=_WINDOW,
        limit=10,
        min_calls=1,
        exclude_patterns=None,
        database_scoped=False,
    )
    sql = " ".join(str(query).split())
    # The pre-window scan is bounded, not a read of the whole retained log.
    assert params["prepare_start_time"] == _WINDOW.start_time - timedelta(hours=24)
    assert "sp[_]cursorprepexec" in sql and "sp[_]cursorexecute" in sql
    assert (
        "CAST(timestamp_utc AS DATETIME2(7)) >= :prepare_start_time "
        "AND CAST(timestamp_utc AS DATETIME2(7)) < :start_time "
        "AND (event_data LIKE N'%sp[_]prep%'"
    ) in sql
    assert (
        "SELECT event_time, session_id, user_name, statement, is_rpc, is_truncated, database_name, 0 AS in_window"
        in sql
    )
    assert "AND in_window = 1" in sql


def test_temp_table_statements_keep_their_session() -> None:
    """A statement that touches a #temp table comes back once per session; each
    stays its own parser session so the temp table resolves within it, while
    the SQL keeps other statements grouped across sessions."""
    insert = "INSERT INTO tgt SELECT id FROM #stage"
    rows = [
        {
            **_log_row("h1", insert, "alice", 1),
            "session_key": "session:51:alice",
        },
        {
            **_log_row("h1", insert, "alice", 1),
            "session_key": "session:52:alice",
        },
    ]
    extractor, _, _ = _extractor(
        _config(query_history_source="audit_log", query_history_path="/a*.sqlaudit"),
        [*_on_prem(), _result(rows)],
    )

    queries = extractor.extract_query_history()

    sessions = {
        observed.session_id
        for query in queries
        for observed in extractor._build_observed_queries(query)
    }
    assert sessions == {
        "queryid:session:51:alice",
        "queryid:session:52:alice",
    }
    sql = " ".join(
        str(
            MSSQLQuery.get_query_history_from_audit_log(
                path="/a*.sqlaudit",
                window=_WINDOW,
                limit=10,
                min_calls=1,
                exclude_patterns=None,
                statements_truncated=False,
            )[0]
        ).split()
    )
    assert (
        "CASE WHEN stmt_key LIKE N'%#%' THEN session_id END AS temp_session_id" in sql
    )
