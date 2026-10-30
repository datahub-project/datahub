from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Tuple
from unittest.mock import patch

from datahub.ingestion.source.sql_queries import (
    SECOPS_MAX_EVENTS_PER_SEARCH,
    SecOpsQueryLogReader,
    SecOpsQuerySourceConfig,
    SqlQueriesSourceConfig,
    SqlQueriesSourceReport,
)
from datahub.metadata.urns import CorpUserUrn

START = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)


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
        "start_time": START.isoformat(),
        "end_time": (START + timedelta(hours=2)).isoformat(),
    }
    base.update(overrides)
    return SecOpsQuerySourceConfig.model_validate(base)


def _event(sql: str, minute: int, user: str = "alice") -> Dict[str, Any]:
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
        },
    }


class _FakeResponse:
    def __init__(self, body: Dict[str, Any]) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        pass

    def json(self) -> Dict[str, Any]:
        return self._body


class _FakeSession:
    """Answers udmSearch from `respond(start, end)` and records each window."""

    def __init__(
        self, respond: Callable[[datetime, datetime], Tuple[List[Any], bool]]
    ) -> None:
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


def _read(
    config: SecOpsQuerySourceConfig, session: _FakeSession
) -> Tuple[List[Any], SqlQueriesSourceReport]:
    report = SqlQueriesSourceReport()
    reader = SecOpsQueryLogReader(config, report)
    with patch.object(SecOpsQueryLogReader, "_session", return_value=session):
        entries = list(reader.get_query_entries())
    return entries, report


def test_secops_source_parses_in_the_union() -> None:
    config = SqlQueriesSourceConfig.model_validate(
        {
            "platform": "bigquery",
            "query_source": _config().model_dump(),
        }
    )
    source = config.query_source
    assert isinstance(source, SecOpsQuerySourceConfig)
    assert source.api_base_url() == "https://europe-chronicle.googleapis.com"
    assert source.instance_name() == (
        "projects/my-project/locations/europe/"
        "instances/11111111-2222-3333-4444-555555555555"
    )


def test_udm_fields_map_to_query_entries() -> None:
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


def test_repeated_fields_take_the_first_element() -> None:
    event = _event("SELECT 1", 0)
    event["udm"]["principal"]["user"] = {"emailAddresses": ["bob@example.com"]}
    reader = SecOpsQueryLogReader(
        _config(
            field_mappings={
                "query": "target.process.commandLine",
                "user": "principal.user.emailAddresses",
            }
        ),
        SqlQueriesSourceReport(),
    )
    entry = reader._to_entry(event)
    assert entry is not None and entry.user == CorpUserUrn("bob@example.com")


def test_full_window_is_split_and_read_oldest_first() -> None:
    # The first hour holds more than one call returns, so it is halved.
    def respond(start: datetime, end: datetime) -> Tuple[List[Any], bool]:
        if start == START and end - start == timedelta(hours=1):
            return [], True
        return [_event(f"SELECT {start.minute}", start.minute + 1)], False

    entries, report = _read(_config(), session := _FakeSession(respond))

    assert session.windows[1:] == [
        (START, START + timedelta(minutes=30)),
        (START + timedelta(minutes=30), START + timedelta(hours=1)),
        (START + timedelta(hours=1), START + timedelta(hours=2)),
    ]
    assert [e.query for e in entries] == ["SELECT 0", "SELECT 30", "SELECT 0"]
    assert report.secops_windows_truncated == 0


def test_window_that_cannot_be_split_is_reported() -> None:
    config = _config(
        end_time=(START + timedelta(minutes=1)).isoformat(),
        window_minutes=1,
        min_window_seconds=60,
    )
    events = [_event("SELECT 1", 0)]
    entries, report = _read(config, _FakeSession(lambda s, e: (events, True)))

    assert len(entries) == 1
    assert report.secops_windows_truncated == 1
    assert any(
        str(SECOPS_MAX_EVENTS_PER_SEARCH) in str(w.message) for w in report.warnings
    )


def test_events_within_a_window_are_ordered_by_timestamp() -> None:
    # Temp tables resolve from earlier statements, so order matters.
    events = [_event("INSERT INTO t SELECT * FROM tmp", 9), _event("CREATE TEMP", 2)]
    entries, _ = _read(
        _config(end_time=(START + timedelta(hours=1)).isoformat()),
        _FakeSession(lambda s, e: (events, False)),
    )
    assert [e.query for e in entries] == [
        "CREATE TEMP",
        "INSERT INTO t SELECT * FROM tmp",
    ]


def test_events_without_sql_are_skipped() -> None:
    event = _event("", 0)
    entries, report = _read(
        _config(end_time=(START + timedelta(hours=1)).isoformat()),
        _FakeSession(lambda s, e: ([event], False)),
    )
    assert entries == []
    assert report.num_entries_failed == 0
