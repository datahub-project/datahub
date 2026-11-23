import pathlib
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pytest
import time_machine
from opensearchpy.exceptions import (
    ConnectionError as OpenSearchConnectionError,
    NotFoundError,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.elastic_search_common import (
    ElasticsearchConnectionConfig,
)
from datahub.ingestion.source.sql_queries.config import (
    ElasticsearchQuerySourceConfig,
    SqlQueriesSourceConfig,
)
from datahub.ingestion.source.sql_queries.readers.elasticsearch import (
    ElasticsearchQueryLogReader,
)
from datahub.ingestion.source.sql_queries.readers.mapped import lookup_path
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport
from datahub.ingestion.source.sql_queries.source import SqlQueriesSource
from datahub.metadata.urns import CorpUserUrn
from datahub.testing import mce_helpers

GOLDEN_DIR = pathlib.Path(__file__).parent / "golden"
SCAN = "datahub.ingestion.source.sql_queries.readers.elasticsearch.scan"

FIELD_MAPPINGS = {
    "query": "object",
    "timestamp": "@timestamp",
    "user": "username",
    "session_id": "connectionid",
    "default_schema": "database",
}


def _es_config(**overrides: Any) -> ElasticsearchQuerySourceConfig:
    base: Dict[str, Any] = {
        "type": "elasticsearch",
        "index": "audit-*",
        "field_mappings": FIELD_MAPPINGS,
    }
    base.update(overrides)
    return ElasticsearchQuerySourceConfig.model_validate(base)


def _reader(**overrides: Any) -> ElasticsearchQueryLogReader:
    return ElasticsearchQueryLogReader(
        _es_config(**overrides), SqlQueriesSourceReport()
    )


def _hit(doc_id: str, **source: Any) -> Dict[str, Any]:
    return {"_index": "audit-2026.01", "_id": doc_id, "_source": source}


@pytest.fixture(autouse=True)
def mock_get_client():
    # Reading goes through the patched scan; the client itself does no I/O.
    with patch.object(ElasticsearchConnectionConfig, "get_client") as mock:
        yield mock


def _source(**overrides: Any) -> SqlQueriesSource:
    config = SqlQueriesSourceConfig.model_validate(
        {
            "platform": "mysql",
            "use_schema_resolver": False,
            "query_source": {
                "type": "elasticsearch",
                "index": "audit-*",
                "field_mappings": FIELD_MAPPINGS,
                "email_domain": "example.com",
                **overrides,
            },
        }
    )
    return SqlQueriesSource(
        PipelineContext(run_id="es-test", graph=Mock(spec=DataHubGraph)), config
    )


class TestConfig:
    @pytest.mark.parametrize(
        "config",
        [
            pytest.param({"platform": "mysql"}, id="neither"),
            pytest.param(
                {
                    "platform": "mysql",
                    "query_file": "queries.json",
                    "query_source": {"type": "elasticsearch", "index": "audit-*"},
                },
                id="both",
            ),
        ],
    )
    def test_exactly_one_source(self, config: Dict[str, Any]) -> None:
        with pytest.raises(ValueError, match="Exactly one"):
            SqlQueriesSourceConfig.model_validate(config)

    @pytest.mark.parametrize(
        "query_source",
        [
            pytest.param({"type": "kafka", "index": "audit-*"}, id="unknown_type"),
            pytest.param({"index": "audit-*"}, id="missing_type"),
            pytest.param({"type": "elasticsearch"}, id="missing_index"),
        ],
    )
    def test_invalid_query_source_rejected(self, query_source: Dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            SqlQueriesSourceConfig.model_validate(
                {"platform": "mysql", "query_source": query_source}
            )


class TestLookupPath:
    def test_nested_path(self) -> None:
        assert lookup_path({"event": {"sql": "select 1"}}, "event.sql") == "select 1"

    def test_flattened_dotted_key_is_matched_first(self) -> None:
        assert lookup_path({"db.statement": "select 1"}, "db.statement") == "select 1"

    def test_array_takes_first_element(self) -> None:
        record = {"principal": [{"user": ["alice", "bob"]}]}
        assert lookup_path(record, "principal.user") == "alice"

    def test_missing_path_is_none(self) -> None:
        assert lookup_path({"event": "not-a-dict"}, "event.sql") is None


class TestMapping:
    def test_maps_fields_and_unescapes_sql(self) -> None:
        entry = _reader(email_domain="example.com")._to_entry(
            _hit(
                "1",
                object="update orders set status=\\'shipped\\' where id=1",
                **{"@timestamp": "2026-04-13T12:52:15Z"},
                username="svc_user",
                connectionid=8187999,
                database="sales",
            )
        )
        assert entry is not None
        assert entry.query == "update orders set status='shipped' where id=1"
        assert entry.user == CorpUserUrn("svc_user@example.com")
        assert entry.session_id == "8187999"
        assert entry.default_schema == "sales"
        assert entry.timestamp is not None

    def test_documents_without_sql_are_skipped(self) -> None:
        # A connect/disconnect audit event carries no SQL in the query field.
        assert _reader()._to_entry(_hit("1", username="u", object="")) is None
        assert _reader()._to_entry(_hit("2", username="u")) is None

    def test_blank_user_with_email_domain_is_dropped(self) -> None:
        # A blank actor must not become a domain-only "@example.com" URN.
        entry = _reader(email_domain="example.com")._to_entry(
            _hit("1", object="select 1", username="   ")
        )
        assert entry is not None
        assert entry.user is None

    def test_unescape_disabled_leaves_sql_untouched(self) -> None:
        entry = _reader(unescape_sql=False)._to_entry(
            _hit("1", object="select \\'x\\'")
        )
        assert entry is not None
        assert entry.query == "select \\'x\\'"


class TestQuery:
    def test_query_filter_and_tolerant_timestamp_sort(self) -> None:
        time_range = {"range": {"@timestamp": {"gte": "now-30d"}}}
        query = _reader(query_filter=time_range).build_query()
        # No exists filter on the query field, so a wrong mapping reaches the
        # "No records had SQL" check instead of looking like an empty index.
        assert query["query"]["bool"]["filter"] == [time_range]
        assert query["sort"] == [
            {"@timestamp": {"order": "asc", "unmapped_type": "date"}},
            {"_doc": "asc"},
        ]

    def test_no_timestamp_mapping_means_unordered_scroll(self) -> None:
        reader = _reader(field_mappings={"query": "object", "timestamp": None})
        with patch(SCAN, return_value=iter([])) as mock_scan:
            list(reader.get_query_entries())

        assert "sort" not in mock_scan.call_args.kwargs["query"]
        assert mock_scan.call_args.kwargs["preserve_order"] is False


class TestReader:
    def test_reads_in_order_with_configured_scroll_and_client(
        self, mock_get_client: Mock
    ) -> None:
        reader = _reader(batch_size=250, request_timeout_seconds=90, max_retries=5)
        hits = [
            _hit("1", object="select 1", database="sales"),
            _hit("2", object="", username="svc_user"),
            _hit("3", object="select 2"),
        ]
        with patch(SCAN, return_value=iter(hits)) as mock_scan:
            entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select 2"]
        assert reader.report.num_entries_processed == 2
        assert reader.report.num_entries_skipped_no_query == 1
        kwargs = mock_scan.call_args.kwargs
        assert kwargs["preserve_order"] is True
        assert kwargs["size"] == 250
        assert kwargs["scroll"] == "5m"
        assert kwargs["request_timeout"] == 90
        client_kwargs = mock_get_client.call_args.kwargs
        assert client_kwargs == {
            "timeout": 90,
            "retry_on_timeout": True,
            "max_retries": 5,
        }

    def test_malformed_document_is_skipped_with_its_index_and_id(self) -> None:
        reader = _reader()
        hits = [
            _hit("1", object="select 1"),
            # Unparseable timestamp fails QueryEntry validation.
            _hit("2", object="select 2", **{"@timestamp": "not-a-date"}),
            _hit("3", object="select 3"),
        ]
        with patch(SCAN, return_value=iter(hits)):
            entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["select 1", "select 3"]
        assert reader.report.num_entries_failed == 1
        (warning,) = [
            w
            for w in reader.report.warnings
            if w.title == "Error processing query entry"
        ]
        assert warning.context and warning.context[0].startswith("audit-2026.01/2 ")

    def test_connection_error_is_reported_and_raised(self) -> None:
        reader = _reader()
        with (
            patch(SCAN, side_effect=OpenSearchConnectionError("N/A", "refused", None)),
            pytest.raises(OpenSearchConnectionError),
        ):
            list(reader.get_query_entries())

        assert [f.title for f in reader.report.failures] == ["Query source read error"]

    def test_expired_scroll_mid_run_is_reported_and_raised(self) -> None:
        def hits():
            yield _hit("1", object="select 1")
            raise NotFoundError(404, "search_context_missing_exception", {})

        reader = _reader()
        seen: List[str] = []
        with patch(SCAN, return_value=hits()), pytest.raises(NotFoundError):
            for entry in reader.get_query_entries():
                seen.append(entry.query)

        assert seen == ["select 1"]
        assert [f.title for f in reader.report.failures] == ["Query source read error"]

    def test_mapped_field_missing_from_every_record_is_warned(self) -> None:
        reader = _reader(field_mappings={**FIELD_MAPPINGS, "user": "user_name"})
        hits = [
            _hit(
                "1",
                object="select 1",
                **{"@timestamp": "2026-01-01T10:00:00Z"},
                username="a",
                connectionid=1,
                database="sales",
            )
        ]
        with patch(SCAN, return_value=iter(hits)):
            list(reader.get_query_entries())

        (warning,) = [
            w for w in reader.report.warnings if w.title == "Mapped field never found"
        ]
        assert "field_mappings.user" in (warning.message or "")


class TestSource:
    @time_machine.travel("2026-01-02 00:00:00Z", tick=False)
    def test_golden(self, pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
        hits = [
            _hit(
                "1",
                object="create temporary table tmp_orders as select * from orders",
                **{"@timestamp": "2026-01-01T10:00:00Z"},
                username="etl",
                connectionid=7,
                database="sales",
            ),
            # A connect event: no SQL, skipped.
            _hit(
                "2",
                **{"@timestamp": "2026-01-01T10:00:30Z"},
                username="etl",
                connectionid=7,
            ),
            _hit(
                "3",
                object="insert into order_summary select * from tmp_orders",
                **{"@timestamp": "2026-01-01T10:01:00Z"},
                username="etl",
                connectionid=7,
                database="sales",
            ),
            _hit(
                "4",
                object="select * from customers where name = \\'x\\'",
                **{"@timestamp": "2026-01-01T10:02:00Z"},
                username="analyst",
                connectionid=8,
                database="crm",
            ),
        ]
        source = _source()
        with patch(SCAN, return_value=iter(hits)):
            workunits = list(source.get_workunits())

        assert not source.report.failures
        output = tmp_path / "elasticsearch.json"
        write_metadata_file(output, [wu.metadata for wu in workunits])
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output,
            golden_path=GOLDEN_DIR / "elasticsearch.json",
        )

    def test_no_sql_in_any_record_fails_the_run(self) -> None:
        source = _source(field_mappings={**FIELD_MAPPINGS, "query": "QUERY_TEXT"})
        with patch(SCAN, return_value=iter([_hit("1", object="select 1")])):
            list(source.get_workunits_internal())

        failure = next(
            f for f in source.report.failures if f.title == "No records had SQL"
        )
        assert list(failure.context or []) == [
            "1 records read from elasticsearch:audit-*"
        ]

    def test_all_records_malformed_points_at_field_mappings(self) -> None:
        hits = [_hit("1", object="select 1", **{"@timestamp": "not-a-date"})]
        source = _source()
        with patch(SCAN, return_value=iter(hits)):
            list(source.get_workunits_internal())

        failure = next(
            f
            for f in source.report.failures
            if f.title == "All entries failed to parse"
        )
        assert "query_source" in (failure.message or "")

    def test_empty_index_warns_with_the_index(self) -> None:
        source = _source()
        with patch(SCAN, return_value=iter([])):
            list(source.get_workunits_internal())

        warning = next(w for w in source.report.warnings if w.title == "Empty input")
        assert list(warning.context or []) == ["elasticsearch:audit-*"]
