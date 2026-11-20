import json
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Tuple
from unittest.mock import Mock, patch

import pytest
import time_machine
from google.auth.credentials import AnonymousCredentials
from google.auth.transport.requests import AuthorizedSession
from pytest_httpserver import HTTPServer

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_session import build_authorized_session
from datahub.ingestion.source.sql_queries.config import (
    SECOPS_MAX_EVENTS_PER_SEARCH,
    SecOpsQuerySourceConfig,
    SqlQueriesSourceConfig,
)
from datahub.ingestion.source.sql_queries.readers.secops import (
    SecOpsApiError,
    SecOpsQueryLogReader,
    TimeWindow,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport
from datahub.ingestion.source.sql_queries.source import SqlQueriesSource
from datahub.metadata.urns import CorpUserUrn
from datahub.testing import mce_helpers

GOLDEN_DIR = pathlib.Path(__file__).parent / "golden"
START = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
BUILD_SESSION = (
    "datahub.ingestion.source.sql_queries.readers.secops.build_authorized_session"
)


def _config(**overrides: Any) -> SecOpsQuerySourceConfig:
    base: Dict[str, Any] = {
        "type": "secops",
        "project_id": "my-project",
        "location": "europe",
        "instance_id": "11111111-2222-3333-4444-555555555555",
        "udm_query": 'metadata.log_type = "MY_DB_AUDIT"',
        "field_mappings": {
            "query": "target.process.commandLine",
            "default_db": "target.resource.name",
        },
    }
    base.update(overrides)
    return SecOpsQuerySourceConfig.model_validate(base)


def _event(sql: str, minute: int, user: str = "alice", **udm: Any) -> Dict[str, Any]:
    ts = (START + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "name": f"events/{minute}",
        "udm": {
            "metadata": {"eventTimestamp": ts},
            "principal": {"user": {"userid": user}},
            "target": {
                "process": {"commandLine": sql},
                "resource": {"name": "sales"},
            },
            **udm,
        },
    }


class _FakeResponse:
    def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = json.dumps(body)

    def json(self) -> Dict[str, Any]:
        return self._body


Respond = Callable[[datetime, datetime], Tuple[List[Any], bool]]


class _FakeSession:
    """Answers udmSearch from `respond(start, end)` and records each window."""

    def __init__(self, respond: Respond) -> None:
        self._respond = respond
        self.windows: List[Tuple[datetime, datetime]] = []
        self.urls: List[str] = []
        self.closed = False

    def get(self, url: str, params: Dict[str, Any], timeout: int) -> _FakeResponse:
        start = datetime.fromisoformat(params["timeRange.startTime"])
        end = datetime.fromisoformat(params["timeRange.endTime"])
        self.urls.append(url)
        self.windows.append((start, end))
        events, more = self._respond(start, end)
        return _FakeResponse({"events": events, "moreDataAvailable": more})

    def close(self) -> None:
        self.closed = True


def _reader(config: SecOpsQuerySourceConfig, hours: float = 2) -> SecOpsQueryLogReader:
    return SecOpsQueryLogReader(
        config,
        SqlQueriesSourceReport(),
        time_range=TimeWindow(START, START + timedelta(hours=hours)),
    )


def _read(
    config: SecOpsQuerySourceConfig, session: Any, hours: float = 2
) -> Tuple[List[Any], SqlQueriesSourceReport]:
    reader = _reader(config, hours)
    with patch.object(SecOpsQueryLogReader, "_session", return_value=session):
        entries = list(reader.get_query_entries())
    return entries, reader.report


class TestConfig:
    def test_endpoint_and_instance_name(self) -> None:
        config = SqlQueriesSourceConfig.model_validate(
            {"platform": "bigquery", "query_source": _config().model_dump()}
        )
        source = config.query_source
        assert isinstance(source, SecOpsQuerySourceConfig)
        assert source.api_base_url() == "https://europe-chronicle.googleapis.com"
        assert source.instance_name() == (
            "projects/my-project/locations/europe/"
            "instances/11111111-2222-3333-4444-555555555555"
        )

    def test_query_mapping_is_required(self) -> None:
        with pytest.raises(ValueError):
            _config(field_mappings={"user": "principal.user.userid"})

    def test_min_window_cannot_exceed_window(self) -> None:
        with pytest.raises(ValueError, match="min_window_seconds"):
            _config(window_minutes=1, min_window_seconds=61)


class TestSession:
    def test_service_account_key_is_used_with_the_scope(self) -> None:
        credential = GCPCredential(
            private_key_id="id",
            private_key="key",
            client_email="reader@my-project.iam.gserviceaccount.com",
            client_id="1",
        )
        with (
            patch(
                "google.oauth2.service_account.Credentials.from_service_account_info"
            ) as from_info,
            patch("google.auth.default") as adc,
        ):
            build_authorized_session(credential, ["scope-a"])

        assert from_info.call_args.args[0]["client_email"] == credential.client_email
        assert from_info.call_args.kwargs["scopes"] == ["scope-a"]
        adc.assert_not_called()

    def test_application_default_credentials_without_a_key(self) -> None:
        with patch(
            "google.auth.default", return_value=(AnonymousCredentials(), None)
        ) as adc:
            build_authorized_session(None, ["scope-a"])

        adc.assert_called_once_with(scopes=["scope-a"])

    def test_rate_limited_call_is_retried(self, httpserver: HTTPServer) -> None:
        path = (
            "/v1/projects/my-project/locations/europe/"
            "instances/11111111-2222-3333-4444-555555555555:udmSearch"
        )
        httpserver.expect_ordered_request(path).respond_with_data(
            "slow down", status=429, headers={"Retry-After": "0"}
        )
        httpserver.expect_ordered_request(path).respond_with_json(
            {"events": [_event("SELECT 1", 0)], "moreDataAvailable": False}
        )
        reader = _reader(_config(endpoint=httpserver.url_for("/")), hours=1)

        with patch(
            BUILD_SESSION, return_value=AuthorizedSession(AnonymousCredentials())
        ):
            entries = list(reader.get_query_entries())

        assert [e.query for e in entries] == ["SELECT 1"]
        assert not reader.report.failures


class TestMapping:
    def test_udm_fields_map_to_query_entries(self) -> None:
        session = _FakeSession(
            lambda start, end: (
                [_event("SELECT * FROM orders", 5)] if start == START else [],
                False,
            )
        )
        entries, report = _read(_config(email_domain="example.com"), session)

        assert [e.query for e in entries] == ["SELECT * FROM orders"]
        assert entries[0].user == CorpUserUrn("alice@example.com")
        assert entries[0].default_db == "sales"
        assert session.urls[0] == (
            "https://europe-chronicle.googleapis.com/v1/projects/my-project/locations/"
            "europe/instances/11111111-2222-3333-4444-555555555555:udmSearch"
        )
        assert session.closed
        assert report.secops_searches == 2

    def test_repeated_fields_take_the_first_element(self) -> None:
        event = _event("SELECT 1", 0)
        event["udm"]["principal"]["user"] = {"emailAddresses": ["bob@example.com"]}
        reader = _reader(
            _config(
                field_mappings={
                    "query": "target.process.commandLine",
                    "user": "principal.user.emailAddresses",
                }
            )
        )
        entry = reader._to_entry(event)
        assert entry is not None and entry.user == CorpUserUrn("bob@example.com")

    def test_malformed_event_is_skipped_and_named(self) -> None:
        bad = _event("SELECT 2", 1)
        bad["udm"]["principal"]["user"]["userid"] = ""
        bad["udm"]["metadata"]["eventTimestamp"] = "not-a-date"
        events = [_event("SELECT 1", 0), bad]
        entries, report = _read(
            _config(), _FakeSession(lambda s, e: (events if s == START else [], False))
        )

        assert [e.query for e in entries] == ["SELECT 1"]
        assert report.num_entries_failed == 1
        (warning,) = [
            w for w in report.warnings if w.title == "Error processing query entry"
        ]
        assert warning.context and warning.context[0].startswith("events/1 ")


class TestWindows:
    def test_full_window_is_split_and_read_oldest_first(self) -> None:
        # The first hour holds more than one call returns, so it is halved.
        def respond(start: datetime, end: datetime) -> Tuple[List[Any], bool]:
            if start == START and end - start == timedelta(hours=1):
                return [], True
            return [_event(f"SELECT {start.minute}", start.minute + 1)], False

        session = _FakeSession(respond)
        entries, report = _read(_config(), session)

        assert session.windows[1:] == [
            (START, START + timedelta(minutes=30)),
            (START + timedelta(minutes=30), START + timedelta(hours=1)),
            (START + timedelta(hours=1), START + timedelta(hours=2)),
        ]
        assert [e.query for e in entries] == ["SELECT 0", "SELECT 30", "SELECT 0"]
        assert report.secops_windows_truncated == 0

    def test_nested_splits_cover_the_range_once_in_order(self) -> None:
        # Anything wider than 15 minutes is "full", so the hour splits twice.
        def respond(start: datetime, end: datetime) -> Tuple[List[Any], bool]:
            return [], end - start > timedelta(minutes=15)

        session = _FakeSession(respond)
        _read(_config(), session, hours=1)

        read = [w for w in session.windows if w[1] - w[0] <= timedelta(minutes=15)]
        assert read == [
            (START + timedelta(minutes=m), START + timedelta(minutes=m + 15))
            for m in (0, 15, 30, 45)
        ]

    def test_last_window_is_clipped_to_the_range(self) -> None:
        session = _FakeSession(lambda s, e: ([], False))
        _read(_config(), session, hours=1.5)

        assert session.windows == [
            (START, START + timedelta(hours=1)),
            (START + timedelta(hours=1), START + timedelta(hours=1.5)),
        ]

    def test_empty_range_makes_no_calls(self) -> None:
        session = _FakeSession(lambda s, e: ([], False))
        entries, _ = _read(_config(), session, hours=0)

        assert entries == []
        assert session.windows == []

    def test_window_that_cannot_be_split_is_reported(self) -> None:
        config = _config(window_minutes=1, min_window_seconds=60)
        events = [_event("SELECT 1", 0)]
        entries, report = _read(
            config,
            _FakeSession(lambda s, e: (events if s == START else [], True)),
            hours=1 / 60,
        )

        assert len(entries) == 1
        assert report.secops_windows_truncated == 1
        assert any(
            str(SECOPS_MAX_EVENTS_PER_SEARCH) in str(w.message) for w in report.warnings
        )

    def test_events_within_a_window_are_ordered_by_timestamp(self) -> None:
        # Temp tables resolve from earlier statements, so order matters.
        events = [
            _event("INSERT INTO t SELECT * FROM tmp", 9),
            _event("CREATE TEMP", 2),
        ]
        entries, _ = _read(
            _config(), _FakeSession(lambda s, e: (events, False)), hours=1
        )
        assert [e.query for e in entries] == [
            "CREATE TEMP",
            "INSERT INTO t SELECT * FROM tmp",
        ]

    def test_timestamps_of_different_precision_sort_by_time(self) -> None:
        # `.` sorts before `Z`, so string order would put 05.500Z ahead of 05Z.
        def at(sql: str, ts: str) -> Dict[str, Any]:
            event = _event(sql, 0)
            event["udm"]["metadata"]["eventTimestamp"] = ts
            return event

        events = [
            at("third", "2026-10-01T00:00:05.500Z"),
            at("second", "2026-10-01T00:00:05Z"),
            at("first", "2026-10-01T02:00:04.999999999+02:00"),
        ]
        entries, _ = _read(
            _config(), _FakeSession(lambda s, e: (events, False)), hours=1
        )
        assert [e.query for e in entries] == ["first", "second", "third"]

    def test_event_without_timestamp_sorts_first_and_is_reported(self) -> None:
        undated = _event("undated", 5)
        del undated["udm"]["metadata"]["eventTimestamp"]
        events = [_event("dated", 1), undated]
        entries, report = _read(
            _config(), _FakeSession(lambda s, e: (events, False)), hours=1
        )

        assert [e.query for e in entries] == ["undated", "dated"]
        assert report.secops_events_without_timestamp == 1
        assert any(
            w.title == "SecOps events without a timestamp" for w in report.warnings
        )


class TestErrors:
    def test_error_response_is_reported_with_its_body(self) -> None:
        session = Mock()
        session.get.return_value = _FakeResponse(
            {"error": {"status": "PERMISSION_DENIED"}}, status_code=403
        )
        reader = _reader(_config(), hours=1)

        with patch.object(SecOpsQueryLogReader, "_session", return_value=session):
            with pytest.raises(SecOpsApiError, match="PERMISSION_DENIED"):
                list(reader.get_query_entries())

        assert [f.title for f in reader.report.failures] == ["Query source read error"]
        session.close.assert_called_once()


class TestSource:
    @time_machine.travel("2026-10-02 00:00:00Z", tick=False)
    def test_golden(self, pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
        events = [
            _event(
                "create temporary table tmp_orders as select * from orders",
                10,
                user="etl",
                network={"sessionId": "7"},
            ),
            _event("", 11, user="etl"),
            _event(
                "insert into order_summary select * from tmp_orders",
                12,
                user="etl",
                network={"sessionId": "7"},
            ),
            _event("select * from customers", 13, user="analyst"),
        ]
        config = SqlQueriesSourceConfig.model_validate(
            {
                "platform": "mysql",
                "use_schema_resolver": False,
                "usage": {
                    "start_time": START.isoformat(),
                    "end_time": (START + timedelta(days=1)).isoformat(),
                },
                "query_source": _config(
                    email_domain="example.com",
                    window_minutes=1440,
                    field_mappings={
                        "query": "target.process.commandLine",
                        "session_id": "network.sessionId",
                        "default_schema": "target.resource.name",
                    },
                ).model_dump(),
            }
        )
        source = SqlQueriesSource(
            PipelineContext(run_id="secops-test", graph=Mock(spec=DataHubGraph)),
            config,
        )
        session = _FakeSession(lambda s, e: (events, False))

        with patch.object(SecOpsQueryLogReader, "_session", return_value=session):
            workunits = list(source.get_workunits())

        assert not source.report.failures
        assert session.windows == [(START, START + timedelta(days=1))]
        output = tmp_path / "secops.json"
        write_metadata_file(output, [wu.metadata for wu in workunits])
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output,
            golden_path=GOLDEN_DIR / "secops.json",
        )
