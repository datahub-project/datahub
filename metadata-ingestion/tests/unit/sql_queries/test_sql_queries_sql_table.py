from typing import Any, Dict, List
from unittest.mock import patch

from datahub.ingestion.source.sql_queries import (
    ElasticsearchQuerySourceConfig,
    SqlQueriesSourceConfig,
    SqlQueriesSourceReport,
    SqlTableQueryLogReader,
    SqlTableQuerySourceConfig,
)
from datahub.metadata.urns import CorpUserUrn


def _sql_config(**overrides: Any) -> SqlTableQuerySourceConfig:
    base: Dict[str, Any] = {
        "type": "sql_table",
        "connection": {"scheme": "mysql", "host_port": "localhost:3306"},
        "query": "SELECT * FROM audit",
        "field_mappings": {
            "query": "sql_text",
            "timestamp": "ts",
            "user": "usr",
            "default_schema": "db",
        },
    }
    base.update(overrides)
    return SqlTableQuerySourceConfig.model_validate(base)


def _reader(**overrides: Any) -> SqlTableQueryLogReader:
    return SqlTableQueryLogReader(_sql_config(**overrides), SqlQueriesSourceReport())


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> "_FakeResult":
        return self

    def yield_per(self, _n: int) -> Any:
        return iter(self._rows)


class _FakeConn:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False

    def execution_options(self, **_k: Any) -> "_FakeConn":
        return self

    def execute(self, *_a: Any, **_k: Any) -> _FakeResult:
        return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows
        self.disposed = False

    def connect(self) -> _FakeConn:
        return _FakeConn(self._rows)

    def dispose(self) -> None:
        self.disposed = True


class TestQuerySourceDiscriminatedUnion:
    def test_sql_table_source_parses(self):
        config = SqlQueriesSourceConfig.model_validate(
            {
                "platform": "mysql",
                "query_source": {
                    "type": "sql_table",
                    "connection": {"scheme": "mysql", "host_port": "localhost:3306"},
                    "query": "SELECT * FROM audit",
                },
            }
        )
        assert isinstance(config.query_source, SqlTableQuerySourceConfig)
        assert config.query_source.query == "SELECT * FROM audit"

    def test_elasticsearch_source_still_parses(self):
        config = SqlQueriesSourceConfig.model_validate(
            {
                "platform": "mysql",
                "query_source": {"type": "elasticsearch", "index": "audit-*"},
            }
        )
        assert isinstance(config.query_source, ElasticsearchQuerySourceConfig)


class TestSqlTableQueryLogReaderMapping:
    def test_maps_fields(self):
        entry = _reader(email_domain="example.com")._to_entry(
            {
                "sql_text": "update orders set status='shipped' where id=1",
                "ts": "2026-04-13T12:52:15Z",
                "usr": "svc_user",
                "db": "sales",
            }
        )
        assert entry is not None
        assert entry.query == "update orders set status='shipped' where id=1"
        assert entry.user == CorpUserUrn("svc_user@example.com")
        assert entry.default_schema == "sales"

    def test_skips_rows_without_sql(self):
        assert _reader()._to_entry({"sql_text": "", "usr": "svc_user"}) is None
        assert _reader()._to_entry({"usr": "svc_user"}) is None

    def test_unescape_sql_opt_in(self):
        row = {"sql_text": "select \\'x\\'"}
        assert _reader()._to_entry(row).query == "select \\'x\\'"
        assert _reader(unescape_sql=True)._to_entry(row).query == "select 'x'"


class TestSqlTableQueryLogReaderExecute:
    def test_get_query_entries_skips_non_sql_and_disposes(self):
        reader = _reader()
        rows = [{"sql_text": "select 1"}, {"sql_text": ""}, {"sql_text": "select 2"}]
        engine = _FakeEngine(rows)
        with patch("sqlalchemy.create_engine", return_value=engine):
            entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select 2"]
        assert reader.report.num_entries_processed == 2
        assert engine.disposed is True

    def test_get_query_entries_tolerates_malformed_row(self):
        reader = _reader()
        rows = [
            {"sql_text": "select 1"},
            {"sql_text": "select 2", "ts": "not-a-date"},
            {"sql_text": "select 3"},
        ]
        with patch("sqlalchemy.create_engine", return_value=_FakeEngine(rows)):
            entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select 3"]
        assert reader.report.num_entries_processed == 2
        assert reader.report.num_entries_failed == 1
