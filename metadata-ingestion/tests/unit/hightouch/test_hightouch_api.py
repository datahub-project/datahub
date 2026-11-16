from typing import Any, Dict

import pytest
import requests

from datahub.ingestion.source.hightouch import hightouch_api
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient

BASE_URL = "https://api.hightouch.com/api/v1"


def make_client() -> HightouchAPIClient:
    return HightouchAPIClient(
        HightouchAPIConfig(api_key="secret", base_url=BASE_URL),
        HightouchSourceReport(),
    )


def source(source_id: int) -> Dict[str, Any]:
    return {
        "id": source_id,
        "name": f"s{source_id}",
        "slug": f"s{source_id}",
        "type": "snowflake",
        "workspaceId": 1,
    }


def test_lists_without_has_more_page_until_a_short_page(requests_mock, monkeypatch):
    monkeypatch.setattr(hightouch_api, "PAGE_SIZE", 2)
    requests_mock.get(
        f"{BASE_URL}/sources",
        [
            {"json": {"data": [source(1), source(2)]}},
            {"json": {"data": [source(3)]}},
        ],
    )
    client = make_client()
    assert [s.id for s in client.get_sources()] == ["1", "2", "3"]
    assert [r.qs["offset"] for r in requests_mock.request_history] == [["0"], ["2"]]
    assert requests_mock.request_history[0].headers["Authorization"] == "Bearer secret"


def test_has_more_false_stops_paging(requests_mock, monkeypatch):
    monkeypatch.setattr(hightouch_api, "PAGE_SIZE", 1)
    requests_mock.get(
        f"{BASE_URL}/sources", json={"data": [source(1)], "hasMore": False}
    )
    assert len(make_client().get_sources()) == 1
    assert requests_mock.call_count == 1


def test_pagination_cap_is_a_failure(requests_mock, monkeypatch):
    monkeypatch.setattr(hightouch_api, "PAGE_SIZE", 1)
    monkeypatch.setattr(hightouch_api, "MAX_PAGES", 2)
    requests_mock.get(
        f"{BASE_URL}/sources", json={"data": [source(1)], "hasMore": True}
    )
    client = make_client()
    assert len(client.get_sources()) == 2
    assert [f.title for f in client.report.failures] == ["Paginated response truncated"]


def test_unparseable_entity_is_skipped_as_a_failure(requests_mock):
    requests_mock.get(f"{BASE_URL}/sources", json={"data": [source(1), {"id": 2}]})
    client = make_client()
    assert [s.id for s in client.get_sources()] == ["1"]
    assert client.report.entity_parse_failures == {"source": 1}
    assert client.report.failures


@pytest.mark.parametrize("status", [401, 403, 404])
def test_contracts_without_access_are_a_warning(requests_mock, status):
    requests_mock.get(f"{BASE_URL}/events/contracts", status_code=status)
    client = make_client()
    assert client.get_contracts() is None
    assert client.report.warnings
    assert not client.report.failures


def test_contracts_that_cannot_be_read_are_a_failure(requests_mock):
    requests_mock.get(
        f"{BASE_URL}/events/contracts", exc=requests.exceptions.ConnectionError
    )
    client = make_client()
    assert client.get_contracts() is None
    assert [f.title for f in client.report.failures] == [
        "Could not fetch Event Contracts"
    ]


def test_unparseable_sync_run_is_only_a_warning(requests_mock):
    requests_mock.get(
        f"{BASE_URL}/syncs/401/runs",
        json={"data": [{"id": 1, "status": "success"}], "hasMore": False},
    )
    client = make_client()
    assert client.get_sync_runs("401", limit=5) == []
    assert requests_mock.last_request.qs["limit"] == ["5"]
    assert client.report.warnings
    assert not client.report.failures
