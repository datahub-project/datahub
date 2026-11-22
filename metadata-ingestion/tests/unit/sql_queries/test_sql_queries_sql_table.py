import pathlib
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, Mock, patch

import pytest
import time_machine
from sqlalchemy.exc import OperationalError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.sql_queries.config import (
    SqlQueriesSourceConfig,
    SqlTableQuerySourceConfig,
)
from datahub.ingestion.source.sql_queries.readers.sql_table import (
    SqlTableQueryLogReader,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport
from datahub.ingestion.source.sql_queries.source import SqlQueriesSource
from datahub.metadata.urns import CorpUserUrn
from datahub.testing import mce_helpers

GOLDEN_DIR = pathlib.Path(__file__).parent / "golden"
CREATE_ENGINE = "datahub.ingestion.source.sql_queries.readers.sql_table.create_engine"

FIELD_MAPPINGS = {
    "query": "sql_text",
    "timestamp": "event_time",
    "user": "user_name",
    "session_id": "conn_id",
    "default_schema": "db",
}

AuditRow = Tuple[str, Any, Optional[str], Optional[int], Optional[str]]
AuditDb = Callable[[List[AuditRow]], str]


@pytest.fixture
def audit_db(tmp_path: pathlib.Path) -> AuditDb:
    """A SQLite audit table; returns a function that loads rows and gives its URI."""
    path = tmp_path / "audit.db"

    def _load(rows: List[AuditRow]) -> str:
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TABLE audit (event_time TEXT, sql_text BLOB, "
                "user_name TEXT, conn_id INTEGER, db TEXT)"
            )
            conn.executemany("INSERT INTO audit VALUES (?, ?, ?, ?, ?)", rows)
        return f"sqlite:///{path}"

    return _load


def _sql_config(uri: str, **overrides: Any) -> SqlTableQuerySourceConfig:
    base: Dict[str, Any] = {
        "type": "sql_table",
        # host_port is required by SQLAlchemyConnectionConfig but unused once
        # sqlalchemy_uri is set.
        "connection": {
            "scheme": "sqlite",
            "host_port": "unused",
            "sqlalchemy_uri": uri,
        },
        "query": "SELECT * FROM audit ORDER BY event_time",
        "field_mappings": FIELD_MAPPINGS,
    }
    base.update(overrides)
    return SqlTableQuerySourceConfig.model_validate(base)


def _reader(uri: str, **overrides: Any) -> SqlTableQueryLogReader:
    return SqlTableQueryLogReader(
        _sql_config(uri, **overrides), SqlQueriesSourceReport()
    )


def _source(uri: str, **overrides: Any) -> SqlQueriesSource:
    config = SqlQueriesSourceConfig.model_validate(
        {
            "platform": "mysql",
            "use_schema_resolver": False,
            "query_source": _sql_config(
                uri, email_domain="example.com", **overrides
            ).model_dump(),
        }
    )
    return SqlQueriesSource(
        PipelineContext(run_id="sql-table-test", graph=Mock(spec=DataHubGraph)),
        config,
    )


class TestReader:
    def test_reads_and_maps_rows(self, audit_db: AuditDb) -> None:
        uri = audit_db(
            [
                # MySQL/MariaDB return BLOB audit text as bytes; it must be
                # decoded, not str()-reprd into a b'...' literal.
                ("2026-01-01 10:00:00", b"select 1", "svc_user", 7, "sales"),
                ("2026-01-01 10:00:01", b"", "svc_user", 7, "sales"),
                ("2026-01-01 10:00:02", "select \\'x\\'", "  ", None, None),
            ]
        )
        reader = _reader(uri, email_domain="example.com")

        entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select \\'x\\'"]
        assert entries[0].user == CorpUserUrn("svc_user@example.com")
        assert entries[0].session_id == "7"
        assert entries[0].default_schema == "sales"
        # A blank actor doesn't become a domain-only "@example.com" user.
        assert entries[1].user is None
        assert reader.report.num_entries_skipped_no_query == 1

    def test_unescape_sql_is_opt_in(self, audit_db: AuditDb) -> None:
        uri = audit_db([("2026-01-01 10:00:00", "select \\'x\\'", None, None, None)])

        (entry,) = _reader(uri, unescape_sql=True).get_query_entries()

        assert entry.query == "select 'x'"

    def test_query_runs_verbatim(self, audit_db: AuditDb) -> None:
        uri = audit_db([("2026-01-01 10:00:00", "select 1", "u", 1, "sales")])
        # ":n" would be a missing bind parameter under text(), and "%" is
        # significant to pyformat drivers when parameters are passed.
        query = (
            "SELECT * FROM audit WHERE user_name || ':n' != 'x:n' "
            "AND sql_text LIKE '%1' -- :p"
        )

        (entry,) = _reader(uri, query=query).get_query_entries()

        assert entry.query == "select 1"

    def test_columns_match_case_insensitively(self, audit_db: AuditDb) -> None:
        uri = audit_db([("2026-01-01 10:00:00", "select 1", "u", 1, "sales")])
        reader = _reader(uri, field_mappings={"query": "SQL_TEXT", "user": "User_Name"})

        (entry,) = reader.get_query_entries()

        assert entry.query == "select 1"
        assert entry.user == CorpUserUrn("u")

    def test_missing_column_fails_and_lists_the_columns(
        self, audit_db: AuditDb
    ) -> None:
        uri = audit_db([("2026-01-01 10:00:00", "select 1", "u", 1, "sales")])
        reader = _reader(uri, field_mappings={"query": "statement"})

        with pytest.raises(ValueError, match="Columns returned: event_time, sql_text"):
            list(reader.get_query_entries())

        assert [f.title for f in reader.report.failures] == ["Query source read error"]

    def test_malformed_row_is_skipped_with_its_row_number(
        self, audit_db: AuditDb
    ) -> None:
        uri = audit_db(
            [
                ("2026-01-01 10:00:00", "select 1", None, None, None),
                ("not-a-date", "select 2", None, None, None),
                ("2026-01-01 10:00:02", "select 3", None, None, None),
            ]
        )
        reader = _reader(uri, query="SELECT * FROM audit")

        entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select 3"]
        (warning,) = [
            w
            for w in reader.report.warnings
            if w.title == "Error processing query entry"
        ]
        assert warning.context and warning.context[0].startswith("row 2: select 2")

    def test_unparseable_url_does_not_leak_the_password(self) -> None:
        reader = _reader("mysql+pymysql://user:hunter2@host:notaport/db")

        with pytest.raises(ValueError) as exc_info:
            list(reader.get_query_entries())

        assert "hunter2" not in str(exc_info.value)
        (failure,) = reader.report.failures
        assert "hunter2" not in str(failure)

    def test_connection_error_is_reported_and_engine_disposed(self) -> None:
        engine = MagicMock()
        engine.connect.side_effect = OperationalError("connect", {}, Exception("down"))
        reader = _reader("sqlite://")

        with patch(CREATE_ENGINE, return_value=engine), pytest.raises(OperationalError):
            list(reader.get_query_entries())

        assert [f.title for f in reader.report.failures] == ["Query source read error"]
        engine.dispose.assert_called_once()

    def test_streams_with_a_server_side_cursor(self) -> None:
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        result = conn.execution_options.return_value.exec_driver_sql.return_value
        result.keys.return_value = ["sql_text"]
        result.mappings.return_value.yield_per.return_value = iter(
            [{"sql_text": "select 1"}]
        )
        reader = _reader(
            "sqlite://",
            field_mappings={"query": "sql_text"},
            batch_size=50,
            connection={
                "scheme": "sqlite",
                "host_port": "unused",
                "sqlalchemy_uri": "sqlite://",
                "options": {"pool_pre_ping": True},
            },
        )

        with patch(CREATE_ENGINE, return_value=engine) as create_engine:
            assert [e.query for e in reader.get_query_entries()] == ["select 1"]

        assert create_engine.call_args.kwargs == {"pool_pre_ping": True}
        conn.execution_options.assert_called_once_with(
            stream_results=True, no_parameters=True
        )
        result.mappings.return_value.yield_per.assert_called_once_with(50)


class TestSource:
    @time_machine.travel("2026-01-02 00:00:00Z", tick=False)
    def test_golden(
        self, audit_db: AuditDb, pytestconfig: pytest.Config, tmp_path: pathlib.Path
    ) -> None:
        uri = audit_db(
            [
                (
                    "2026-01-01 10:00:00",
                    b"create temporary table tmp_orders as select * from orders",
                    "etl",
                    7,
                    "sales",
                ),
                ("2026-01-01 10:00:30", b"", "etl", 7, None),
                (
                    "2026-01-01 10:01:00",
                    b"insert into order_summary select * from tmp_orders",
                    "etl",
                    7,
                    "sales",
                ),
                (
                    "2026-01-01 10:02:00",
                    "select * from customers",
                    "analyst",
                    8,
                    "crm",
                ),
            ]
        )
        source = _source(uri)

        workunits = list(source.get_workunits())

        assert not source.report.failures
        output = tmp_path / "sql_table.json"
        write_metadata_file(output, [wu.metadata for wu in workunits])
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output,
            golden_path=GOLDEN_DIR / "sql_table.json",
        )

    def test_rows_without_sql_fail_the_run(self, audit_db: AuditDb) -> None:
        uri = audit_db([("2026-01-01 10:00:00", b"", "etl", 7, None)])
        source = _source(uri)

        list(source.get_workunits_internal())

        assert any(f.title == "No records had SQL" for f in source.report.failures)
