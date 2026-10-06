from datetime import datetime
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
    query_log_file_pattern,
    unwrap_rpc_statement,
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
    result.mappings.return_value.__iter__ = lambda _: iter(rows)
    return result


def _on_prem() -> List[Mock]:
    """Engine edition and version lookups a log source runs on SQL Server."""
    return [
        _result([{"engine_edition": 3}]),
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
    query_id: str, text: str, user: Optional[str], count: int, truncated: int = 0
) -> Dict[str, Any]:
    return {
        "query_id": query_id,
        "query_text": text,
        "execution_count": count,
        "total_exec_time_ms": 0.0,
        "database_name": "TestDB",
        "last_execution_time_utc": datetime(2026, 1, 1, 9),
        "window_execution_count": count,
        "user_name": user,
        "is_truncated": truncated,
    }


@pytest.mark.parametrize(
    "statement, expected",
    [
        (
            "exec sp_executesql N'SELECT a FROM t WHERE id = @P1',N'@P1 INT',@P1=3",
            "SELECT a FROM t WHERE id = @P1",
        ),
        (
            "declare @p1 int\r\nset @p1=1\r\nexec sp_prepexec @p1 output,"
            "N'@P1 int',N'SELECT id FROM t WHERE x > @P1',10\r\nselect @p1",
            "SELECT id FROM t WHERE x > @P1",
        ),
        (
            "exec sp_prepexec @p1 output,NULL,N'SELECT name FROM t WHERE x = ''a'''",
            "SELECT name FROM t WHERE x = 'a'",
        ),
        ("SELECT 1 FROM t", None),
        ("exec sp_unprepare 1", None),
        # Dynamic SQL from a variable: there is no inner literal to take.
        ("EXEC sp_executesql @sql, N'@id int', @id=1", None),
        # A batch that only contains a call keeps its own lineage.
        (
            "INSERT INTO a SELECT * FROM b; EXEC sp_executesql N'UPDATE c SET x = 1'",
            None,
        ),
    ],
)
def test_unwrap_rpc_statement(statement: str, expected: Optional[str]) -> None:
    """Drivers send parameterized SQL as RPC wrappers; the inner SQL is what
    the parser needs to find tables. Anything else must be left alone."""
    assert unwrap_rpc_statement(statement) == expected


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
    """A server-scoped session must tie rows to the database; only a
    database-scoped (Azure SQL Database) session can trust rows without one."""
    query, _ = MSSQLQuery.get_query_history_from_extended_events(
        path="/xe/x*.xel",
        window=_WINDOW,
        limit=10,
        exclude_patterns=None,
        database_scoped=database_scoped,
    )
    assert ("database_name IS NULL" in str(query)) == database_scoped


def test_audit_log_rows_regroup_parameter_values_and_carry_users() -> None:
    """The SQL groups on raw text, where each set of RPC parameter values is a
    different statement. After unwrapping they must be one query with the
    summed count, the per-user executions, and min_query_calls applied to it."""
    rows = [
        _log_row(
            "h1",
            "exec sp_executesql N'SELECT a FROM t WHERE id = @P1',N'@P1 INT',@P1=3",
            "CORP\\alice",
            2,
        ),
        _log_row(
            "h2",
            "exec sp_executesql N'SELECT a FROM t WHERE id = @P1',N'@P1 INT',@P1=4",
            "bob@example.com",
            1,
        ),
        _log_row("h3", "SELECT b FROM u", "CORP\\alice", 1),
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
    assert queries[0].execution_count == 3
    assert report.num_query_log_truncated_statements == 5
    _, params = connection.execute.call_args.args
    assert params["path"] == "/audit/x*.sqlaudit"
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
            _result([{"engine_edition": 3}]),
            _result([{"version": "13.0", "major_version": 13}]),
        ],
    )

    assert extractor.extract_query_history() == []
    assert report.failures


def test_audit_log_on_azure_without_path_reports_failure() -> None:
    """Azure SQL Database has no T-SQL view of its audit location."""
    extractor, _, report = _extractor(
        _config(query_history_source="audit_log"),
        [_result([{"engine_edition": AZURE_SQL_DATABASE_ENGINE_EDITION}])],
    )

    assert extractor.extract_query_history() == []
    assert report.failures


def test_extended_events_path_discovered_from_running_session() -> None:
    extractor, connection, _ = _extractor(
        _config(query_history_source="extended_events"),
        [
            _result([{"engine_edition": AZURE_SQL_DATABASE_ENGINE_EDITION}]),
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
