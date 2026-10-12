from unittest.mock import patch

import pytest

from datahub.ingestion.source.sql_queries import (
    ElasticsearchQueryLogReader,
    ElasticsearchQuerySourceConfig,
    SqlQueriesSourceConfig,
    SqlQueriesSourceReport,
)
from datahub.metadata.urns import CorpUserUrn


def _es_config(**overrides):
    base = {
        "index": "audit-*",
        "field_mappings": {
            "query": "object",
            "timestamp": "@timestamp",
            "user": "username",
            "session_id": "connectionid",
            "default_schema": "database",
        },
    }
    base.update(overrides)
    return ElasticsearchQuerySourceConfig.model_validate(base)


def _reader(**overrides):
    return ElasticsearchQueryLogReader(
        _es_config(**overrides), SqlQueriesSourceReport()
    )


class TestSqlQueriesSourceConfigQuerySource:
    def test_requires_exactly_one_source(self):
        with pytest.raises(ValueError, match="Exactly one"):
            SqlQueriesSourceConfig.model_validate({"platform": "mysql"})

    def test_rejects_both_sources(self):
        with pytest.raises(ValueError, match="Exactly one"):
            SqlQueriesSourceConfig.model_validate(
                {
                    "platform": "mysql",
                    "query_file": "queries.json",
                    "query_source": {"type": "elasticsearch", "index": "audit-*"},
                }
            )

    def test_query_source_parses(self):
        config = SqlQueriesSourceConfig.model_validate(
            {
                "platform": "mysql",
                "query_source": {
                    "type": "elasticsearch",
                    "index": "audit-*",
                    "connection": {"host": "es.internal:9200"},
                    "field_mappings": {"query": "object"},
                },
            }
        )
        assert config.query_file is None
        assert config.query_source is not None
        assert config.query_source.index == "audit-*"
        assert config.query_source.connection.host == "es.internal:9200"


class TestElasticsearchQueryLogReaderMapping:
    def test_maps_fields_and_unescapes_sql(self):
        entry = _reader(email_domain="example.com")._to_entry(
            {
                "object": "update orders set status=\\'shipped\\' where id=1",
                "@timestamp": "2026-04-13T12:52:15Z",
                "username": "svc_user",
                "connectionid": 8187999,
                "database": "sales",
            }
        )
        assert entry is not None
        assert entry.query == "update orders set status='shipped' where id=1"
        assert entry.user == CorpUserUrn("svc_user@example.com")
        assert entry.session_id == "8187999"
        assert entry.default_schema == "sales"
        assert entry.timestamp is not None

    def test_skips_documents_without_sql(self):
        # A connect/disconnect audit event carries no SQL in the query field.
        assert _reader()._to_entry({"username": "svc_user", "object": ""}) is None
        assert _reader()._to_entry({"username": "svc_user"}) is None

    def test_blank_user_with_email_domain_is_dropped(self):
        # A blank actor must not be turned into a domain-only "@example.com" URN.
        entry = _reader(email_domain="example.com")._to_entry(
            {"object": "select 1", "username": "   "}
        )
        assert entry is not None
        assert entry.user is None

    def test_unescape_disabled_leaves_sql_untouched(self):
        entry = _reader(unescape_sql=False)._to_entry(
            {"object": "select \\'x\\'", "@timestamp": "2026-04-13T12:52:15Z"}
        )
        assert entry is not None
        assert entry.query == "select \\'x\\'"

    def test_nested_field_path(self):
        reader = _reader(field_mappings={"query": "event.sql", "user": "actor.name"})
        entry = reader._to_entry(
            {"event": {"sql": "select 1"}, "actor": {"name": "svc_user"}}
        )
        assert entry is not None
        assert entry.query == "select 1"
        assert entry.user == CorpUserUrn("svc_user")


class TestElasticsearchQueryLogReaderScan:
    def test_get_query_entries_skips_non_sql_and_counts(self):
        reader = _reader()
        hits = [
            {"_id": "1", "_source": {"object": "select 1", "database": "sales"}},
            {"_id": "2", "_source": {"object": "", "username": "svc_user"}},
            {"_id": "3", "_source": {"object": "select 2"}},
        ]
        # scan is patched, so the client built by get_client is never used for I/O.
        with patch("opensearchpy.helpers.scan", return_value=iter(hits)) as mock_scan:
            entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select 2"]
        assert reader.report.num_entries_processed == 2

        # Mapped timestamp => ascending sort with a stable _doc tiebreaker, ordered
        # scroll, and the configured keepalive.
        kwargs = mock_scan.call_args.kwargs
        assert kwargs["query"]["sort"] == [
            {"@timestamp": {"order": "asc"}},
            {"_doc": "asc"},
        ]
        assert kwargs["preserve_order"] is True
        assert kwargs["scroll"] == "5m"

    def test_get_query_entries_tolerates_malformed_document(self):
        reader = _reader()
        hits = [
            {"_id": "1", "_source": {"object": "select 1"}},
            # Unparseable timestamp fails QueryEntry validation in _to_entry.
            {"_id": "2", "_source": {"object": "select 2", "@timestamp": "not-a-date"}},
            {"_id": "3", "_source": {"object": "select 3"}},
        ]
        with patch("opensearchpy.helpers.scan", return_value=iter(hits)):
            entries = list(reader.get_query_entries())

        # The bad row is skipped and counted; valid rows after it still yield.
        assert [e.query for e in entries] == ["select 1", "select 3"]
        assert reader.report.num_entries_processed == 2
        assert reader.report.num_entries_failed == 1
        assert len(reader.report.warnings) == 1
