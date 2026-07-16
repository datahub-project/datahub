from typing import Any, Dict, Optional, Tuple

import pytest

from datahub.ingestion.source.collibra.client import Cfg, CollibraClient

BASE_URL = "http://mock-collibra.test"

# Three pages of two items each; the third has a null cursor so paging stops.
ASSET_TYPE_PAGES: Dict[Optional[str], dict] = {
    None: {"results": [{"id": "1"}, {"id": "2"}], "nextCursor": "c1"},
    "c1": {"results": [{"id": "3"}, {"id": "4"}], "nextCursor": "c2"},
    "c2": {"results": [{"id": "5"}, {"id": "6"}], "nextCursor": None},
}


def _paged(pages: Dict[Optional[str], dict]) -> Any:
    def callback(request: Any, context: Any) -> dict:
        # requests_mock lowercases qs values; our cursors are already lowercase.
        cursor = request.qs.get("cursor", [None])[0]
        return pages[cursor]

    return callback


def register_mock_api(
    request_mock: Any, job_states: Tuple[str, ...] = ("RUNNING", "COMPLETED")
) -> None:
    request_mock.post(
        f"{BASE_URL}/rest/oauth/v2/token",
        json={"access_token": "test_token"},
    )
    request_mock.get(
        f"{BASE_URL}/rest/2.0/assetTypes",
        json=_paged(ASSET_TYPE_PAGES),
    )
    request_mock.get(
        f"{BASE_URL}/rest/2.0/communities",
        json={
            "results": [{"id": "community-1"}, {"id": "community-2"}],
            "nextCursor": None,
        },
    )
    request_mock.get(
        f"{BASE_URL}/rest/2.0/users",
        json={
            "results": [{"id": "user-1"}, {"id": "user-2"}],
            "nextCursor": None,
        },
    )
    request_mock.post(
        f"{BASE_URL}/graphql/knowledgeGraph/v1",
        json={"data": {"assets": [{"id": "term-1"}]}},
    )
    request_mock.post(
        f"{BASE_URL}/rest/2.0/outputModule/export/json-job",
        json={"id": "job1"},
    )

    states = list(job_states)

    def job_callback(request: Any, context: Any) -> dict:
        state = states.pop(0) if len(states) > 1 else states[0]
        return {"state": state, "result": {"message": {"id": "file9"}}}

    request_mock.get(f"{BASE_URL}/rest/2.0/jobs/job1", json=job_callback)
    request_mock.get(
        f"{BASE_URL}/rest/2.0/outputModule/files/file9",
        content=b"col1,col2\n",
    )


def _client() -> CollibraClient:
    return CollibraClient(
        Cfg(
            url=BASE_URL,
            client_id="test-client",
            client_secret="test-secret",
            poll_interval_s=0,
            max_workers=2,
        )
    )


@pytest.mark.integration
def test_cursor_pagination_walks_all_pages(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client()
    ids = [a["id"] for a in client.asset_types()]
    assert ids == ["1", "2", "3", "4", "5", "6"]


@pytest.mark.integration
def test_paginate_sends_paging_params_and_cursor(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client()
    list(client.asset_types())

    reqs = [
        r
        for r in requests_mock.request_history
        if "/rest/2.0/assettypes" in r.url.lower()
    ]
    assert len(reqs) == 3
    # first page: paging params set, no cursor
    assert reqs[0].qs.get("limit") == ["1000"]
    assert reqs[0].qs.get("countlimit") == ["0"]
    assert "cursor" not in reqs[0].qs
    # subsequent pages carry the cursor returned by the previous page
    assert reqs[1].qs.get("cursor") == ["c1"]
    assert reqs[2].qs.get("cursor") == ["c2"]


@pytest.mark.integration
def test_parallel_extraction_collects_all_partitions(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client()
    items = client.extract_parallel(
        [
            ("/rest/2.0/communities", {}),
            ("/rest/2.0/users", {}),
        ]
    )
    ids = sorted(a["id"] for a in items)
    assert ids == ["community-1", "community-2", "user-1", "user-2"]


@pytest.mark.integration
def test_output_module_submit_poll_download(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client()
    assert client.output_export({"any": "viewconfig"}) == b"col1,col2\n"


@pytest.mark.integration
def test_output_module_failed_job_raises(requests_mock: Any) -> None:
    register_mock_api(requests_mock, job_states=("ERROR",))
    client = _client()
    with pytest.raises(RuntimeError):
        client.output_export({"any": "viewconfig"})


@pytest.mark.integration
def test_oauth_bearer_token_attached(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client()
    list(client.asset_types())

    data_reqs = [
        r for r in requests_mock.request_history if "assettypes" in r.url.lower()
    ]
    assert data_reqs
    assert data_reqs[0].headers.get("Authorization") == "Bearer test_token"


@pytest.mark.integration
def test_graphql_post_returns_parsed_json(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client()
    resp = client.graphql("query { assets { id } }", {"limit": 10})
    assert resp["data"]["assets"][0]["id"] == "term-1"

    gql = [
        r for r in requests_mock.request_history if "knowledgegraph" in r.url.lower()
    ]
    assert gql[0].json()["query"].startswith("query")
    assert gql[0].json()["variables"] == {"limit": 10}
