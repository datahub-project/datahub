from typing import Any, Dict, List, Optional, Tuple

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.collibra.client import CollibraApiError, CollibraClient
from datahub.ingestion.source.collibra.config import CollibraSourceConfig
from datahub.ingestion.source.collibra.report import CollibraSourceReport

BASE_URL = "http://mock-collibra.test"

# assetTypes served two ways: cursor pages (default) and offset pages (fallback).
CURSOR_PAGES: Dict[Optional[str], dict] = {
    None: {
        "results": [
            {"id": "1", "name": "Business Term"},
            {"id": "2", "name": "Data Element"},
        ],
        "nextCursor": "c1",
    },
    "c1": {"results": [{"id": "3", "name": "Policy"}], "nextCursor": None},
}
OFFSET_PAGES: Dict[int, dict] = {
    0: {
        "results": [
            {"id": "1", "name": "Business Term"},
            {"id": "2", "name": "Data Element"},
        ]
    },
    2: {"results": [{"id": "3", "name": "Policy"}]},
}


def _asset_types_cb(request: Any, context: Any) -> dict:
    qs = request.qs
    if "offset" in qs:
        return OFFSET_PAGES.get(int(qs["offset"][0]), {"results": []})
    return CURSOR_PAGES[qs.get("cursor", [None])[0]]


def register_mock_api(
    request_mock: Any,
    info_version: str = "2024.05",
    job_states: Tuple[str, ...] = ("RUNNING", "COMPLETED"),
) -> None:
    request_mock.get(
        f"{BASE_URL}/rest/2.0/application/info", json={"version": info_version}
    )
    request_mock.post(
        f"{BASE_URL}/rest/oauth/v2/token", json={"access_token": "test_token"}
    )
    request_mock.get(f"{BASE_URL}/rest/2.0/assetTypes", json=_asset_types_cb)
    request_mock.get(
        f"{BASE_URL}/rest/2.0/communities",
        json={
            "results": [
                {"id": "co-1", "name": "Finance"},
                {"id": "co-2", "name": "HR"},
            ],
            "nextCursor": None,
        },
    )
    request_mock.get(
        f"{BASE_URL}/rest/2.0/users",
        json={"results": [{"id": "u-1", "name": "alice"}], "nextCursor": None},
    )
    request_mock.get(
        f"{BASE_URL}/rest/2.0/assets", json={"results": [{"id": "a-1"}], "total": 42}
    )
    request_mock.post(
        f"{BASE_URL}/rest/2.0/outputModule/export/json-job", json={"id": "job1"}
    )

    states = list(job_states)

    def job_cb(request: Any, context: Any) -> dict:
        state = states.pop(0) if len(states) > 1 else states[0]
        return {"state": state, "result": {"message": {"id": "file9"}}}

    request_mock.get(f"{BASE_URL}/rest/2.0/jobs/job1", json=job_cb)
    request_mock.get(
        f"{BASE_URL}/rest/2.0/outputModule/files/file9", content=b'[{"id": "r1"}]'
    )


def _config(**overrides: Any) -> CollibraSourceConfig:
    base: Dict[str, Any] = dict(
        url=BASE_URL,
        client_id="test-client",
        client_secret="test-secret",
        poll_interval=0.0,
    )
    base.update(overrides)
    return CollibraSourceConfig(**base)


def _client(
    report: Optional[CollibraSourceReport] = None, **cfg: Any
) -> CollibraClient:
    return CollibraClient(_config(**cfg), report=report)


# --- capability probe / version gating ---------------------------------------
@pytest.mark.integration
def test_use_graphql_by_version(requests_mock: Any) -> None:
    register_mock_api(requests_mock, info_version="2024.05")
    assert _client().use_graphql() is True

    register_mock_api(requests_mock, info_version="2020.01")
    assert _client().use_graphql() is False


@pytest.mark.integration
def test_force_paging_disables_graphql(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    assert _client(force_paging=True).use_graphql() is False


# --- cursor + offset paging ---------------------------------------------------
@pytest.mark.integration
def test_cursor_pagination_walks_all_pages(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    ids = [e.id for e in _client().asset_types()]
    assert ids == ["1", "2", "3"]


@pytest.mark.integration
def test_paginate_sends_paging_params_and_cursor(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    _client().asset_types()
    reqs = [
        r
        for r in requests_mock.request_history
        if "/rest/2.0/assettypes" in r.url.lower()
    ]
    assert len(reqs) == 2
    assert reqs[0].qs.get("limit") == ["1000"]
    assert reqs[0].qs.get("countlimit") == ["0"]
    assert "cursor" not in reqs[0].qs
    assert reqs[1].qs.get("cursor") == ["c1"]


@pytest.mark.integration
def test_offset_paging_fallback(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client(force_offset_paging=True, page_size=2)
    ids = [e.id for e in client.asset_types()]
    assert ids == ["1", "2", "3"]
    offsets = [
        r.qs.get("offset", [None])[0]
        for r in requests_mock.request_history
        if "/rest/2.0/assettypes" in r.url.lower()
    ]
    assert offsets == ["0", "2"]


@pytest.mark.integration
def test_count_returns_total(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    assert _client().count_assets("some-type") == 42


# --- filtering ----------------------------------------------------------------
@pytest.mark.integration
def test_asset_type_pattern_filters_and_counts(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    report = CollibraSourceReport()
    client = _client(
        report=report, asset_type_pattern=AllowDenyPattern(deny=["Policy"])
    )
    names = [e.name for e in client.asset_types()]
    assert names == ["Business Term", "Data Element"]
    assert report.filtered == 1


# --- auth ---------------------------------------------------------------------
@pytest.mark.integration
def test_oauth_bearer_attached(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    _client().users()
    data = [r for r in requests_mock.request_history if "/users" in r.url.lower()]
    assert data[0].headers.get("Authorization") == "Bearer test_token"


@pytest.mark.integration
def test_basic_auth(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client(
        auth_method="basic",
        username="u",
        password="p",
        client_id=None,
        client_secret=None,
    )
    client.users()
    data = [r for r in requests_mock.request_history if "/users" in r.url.lower()]
    assert data[0].headers.get("Authorization", "").startswith("Basic ")


@pytest.mark.integration
def test_token_auth(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    client = _client(
        auth_method="token", token="jwt-abc", client_id=None, client_secret=None
    )
    client.users()
    data = [r for r in requests_mock.request_history if "/users" in r.url.lower()]
    assert data[0].headers.get("Authorization") == "Bearer jwt-abc"


@pytest.mark.integration
def test_token_refresh_on_401(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    # First data call 401s; after re-auth the retry succeeds.
    requests_mock.get(
        f"{BASE_URL}/rest/2.0/users",
        [
            {"status_code": 401},
            {
                "json": {"results": [{"id": "u-1"}], "nextCursor": None},
                "status_code": 200,
            },
        ],
    )
    users = _client().users()
    assert [u.id for u in users] == ["u-1"]
    token_reqs = [
        r for r in requests_mock.request_history if "/oauth/" in r.url.lower()
    ]
    assert len(token_reqs) == 2  # initial + refresh


@pytest.mark.integration
def test_verify_ssl_disabled_warns(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    report = CollibraSourceReport()
    _client(report=report, verify_ssl=False)
    assert any("verify_ssl" in w for w in report.warnings_list)


# --- GraphQL ------------------------------------------------------------------
@pytest.mark.integration
def test_graphql_returns_data(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    requests_mock.post(
        f"{BASE_URL}/graphql/knowledgeGraph/v1",
        json={"data": {"assets": [{"id": "term-1"}]}},
    )
    data = _client().graphql("query { assets { id } }", {"limit": 10})
    assert data["assets"][0]["id"] == "term-1"


@pytest.mark.integration
def test_graphql_errors_raise(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    # GraphQL returns HTTP 200 with an errors array — must not be treated as data.
    requests_mock.post(
        f"{BASE_URL}/graphql/knowledgeGraph/v1",
        json={"errors": [{"message": "boom"}]},
    )
    with pytest.raises(CollibraApiError):
        _client().graphql("query { bad }")


@pytest.mark.integration
def test_graphql_paginate(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    pages: Dict[Optional[str], dict] = {
        None: {"data": {"conn": {"nodes": [{"id": "a"}], "cursor": "g1"}}},
        "g1": {"data": {"conn": {"nodes": [{"id": "b"}], "cursor": None}}},
    }

    def gql_cb(request: Any, context: Any) -> dict:
        return pages[request.json()["variables"]["cursor"]]

    requests_mock.post(f"{BASE_URL}/graphql/knowledgeGraph/v1", json=gql_cb)

    def extract(data: dict) -> Tuple[List[dict], Optional[str]]:
        conn = data["conn"]
        return conn["nodes"], conn["cursor"]

    rows = list(_client().graphql_paginate("query", {}, extract))
    assert [r["id"] for r in rows] == ["a", "b"]


# --- Output Module ------------------------------------------------------------
@pytest.mark.integration
def test_output_module_submit_poll_download(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    assert _client().output_export({"any": "viewconfig"}) == [{"id": "r1"}]


@pytest.mark.integration
def test_output_module_failed_job_raises(requests_mock: Any) -> None:
    register_mock_api(requests_mock, job_states=("ERROR",))
    with pytest.raises(CollibraApiError):
        _client().output_export({"any": "viewconfig"})


@pytest.mark.integration
def test_output_module_poll_timeout(requests_mock: Any) -> None:
    register_mock_api(requests_mock, job_states=("RUNNING",))
    with pytest.raises(CollibraApiError):
        _client(poll_max_attempts=3).output_export({"any": "viewconfig"})


# --- parallel extraction ------------------------------------------------------
@pytest.mark.integration
def test_extract_parallel_tolerates_partition_failure(requests_mock: Any) -> None:
    register_mock_api(requests_mock)
    report = CollibraSourceReport()
    client = _client(report=report)

    def good() -> List[dict]:
        return [{"id": "a"}]

    def bad() -> List[dict]:
        raise RuntimeError("boom")

    results = client.extract_parallel([good, bad, good])
    assert len(results) == 2
    assert report.partitions_failed == 1
