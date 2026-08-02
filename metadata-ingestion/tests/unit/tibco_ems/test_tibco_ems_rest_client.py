import pytest
import requests
from requests_mock import Mocker

from datahub.ingestion.source.tibco_ems.config import TibcoEmsSourceConfig
from datahub.ingestion.source.tibco_ems.constants import HEADER_AUTHORIZATION
from datahub.ingestion.source.tibco_ems.models import DestinationType
from datahub.ingestion.source.tibco_ems.rest_client import (
    TibcoEmsRestClient,
    _next_cursor,
    _records,
)

_BASE_URL = "https://ems.example.com:8080"


def _client(**overrides: object) -> TibcoEmsRestClient:
    config = TibcoEmsSourceConfig.model_validate(
        {"base_url": _BASE_URL, "username": "u", "password": "p", **overrides}
    )
    return TibcoEmsRestClient(config)


def test_bearer_token_sets_authorization_header() -> None:
    config = TibcoEmsSourceConfig.model_validate(
        {"base_url": _BASE_URL, "token": "secret"}
    )
    client = TibcoEmsRestClient(config)
    assert client.session.headers[HEADER_AUTHORIZATION] == "Bearer secret"


def test_basic_auth_sets_session_auth() -> None:
    client = _client()
    assert client.session.auth == ("u", "p")


def test_ca_certificate_overrides_verify() -> None:
    client = _client(ca_certificate_path="/tmp/ca.pem")
    assert client.session.verify == "/tmp/ca.pem"


def test_records_handles_bare_list_and_envelope() -> None:
    assert _records([{"a": 1}, "skip", 2], "queues") == [{"a": 1}]
    assert _records({"queues": [{"a": 1}]}, "queues") == [{"a": 1}]
    assert _records("unexpected", "queues") == []


def test_records_ignores_the_errors_array() -> None:
    # A real envelope serialises "errors" before the records, so reading the first
    # array in the envelope would return nothing at all.
    payload = {
        "errors": [],
        "first": "cursor-a",
        "next": "",
        "queues": [{"name": "q1"}],
    }
    assert _records(payload, "queues") == [{"name": "q1"}]


def test_fetch_follows_the_next_cursor(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", json={})
    requests_mock.get(
        f"{_BASE_URL}/system/ems/queues",
        [
            {"json": {"errors": [], "next": "page-2", "queues": [{"name": "q1"}]}},
            {"json": {"errors": [], "next": "", "queues": [{"name": "q2"}]}},
        ],
    )
    listing = _client().fetch_queues()

    assert [q.name for q in listing.records] == ["q1", "q2"]
    assert requests_mock.request_history[-1].qs["cursor"] == ["page-2"]


def test_fetch_stops_when_the_cursor_repeats(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", json={})
    requests_mock.get(
        f"{_BASE_URL}/system/ems/queues",
        json={"errors": [], "next": "stuck", "queues": [{"name": "q1"}]},
    )
    assert len(_client().fetch_queues().records) == 2


def test_fetch_surfaces_envelope_errors(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", json={})
    requests_mock.get(
        f"{_BASE_URL}/system/ems/queues",
        json={"errors": ["group2 unreachable"], "next": "", "queues": []},
    )
    assert _client().fetch_queues().errors == ["group2 unreachable"]


def test_next_cursor_treats_empty_as_exhausted() -> None:
    assert _next_cursor({"next": "abc"}) == "abc"
    assert _next_cursor({"next": ""}) is None
    assert _next_cursor({}) is None


def test_connect_called_once(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", json={})
    requests_mock.get(f"{_BASE_URL}/system/ems/queues", json=[])
    client = _client()
    client.fetch_queues()
    client.fetch_queues()
    connect_calls = [
        r for r in requests_mock.request_history if r.url.endswith("/connect")
    ]
    assert len(connect_calls) == 1


def test_fetch_queues_and_topics_set_destination_type(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", json={})
    requests_mock.get(f"{_BASE_URL}/system/ems/queues", json=[{"name": "q1"}])
    requests_mock.get(f"{_BASE_URL}/system/ems/topics", json=[{"name": "t1"}])
    client = _client()

    queues = client.fetch_queues()
    topics = client.fetch_topics()
    assert queues.records[0].destination_type is DestinationType.QUEUE
    assert topics.records[0].destination_type is DestinationType.TOPIC


def test_fetch_raises_on_http_error(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", status_code=401)
    client = _client()
    with pytest.raises(requests.exceptions.HTTPError):
        client.fetch_queues()
