import pytest
import requests
from requests_mock import Mocker

from datahub.ingestion.source.tibco_ems.config import TibcoEmsSourceConfig
from datahub.ingestion.source.tibco_ems.constants import HEADER_AUTHORIZATION
from datahub.ingestion.source.tibco_ems.models import DestinationType
from datahub.ingestion.source.tibco_ems.rest_client import (
    TibcoEmsRestClient,
    _as_object_list,
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


def test_as_object_list_handles_bare_list_and_envelope() -> None:
    assert _as_object_list([{"a": 1}, "skip", 2]) == [{"a": 1}]
    assert _as_object_list({"queues": [{"a": 1}]}) == [{"a": 1}]
    assert _as_object_list("unexpected") == []


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
    assert queues[0].destination_type is DestinationType.QUEUE
    assert topics[0].destination_type is DestinationType.TOPIC


def test_fetch_raises_on_http_error(requests_mock: Mocker) -> None:
    requests_mock.post(f"{_BASE_URL}/connect", status_code=401)
    client = _client()
    with pytest.raises(requests.exceptions.HTTPError):
        client.fetch_queues()
