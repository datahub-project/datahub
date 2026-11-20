import requests_mock

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    RegularAPIResolver,
    SessionWithTimeout,
)

WORKSPACE_ID = "64ED5CAD-7C10-4684-8180-826122881108"


def _resolver() -> RegularAPIResolver:
    resolver = RegularAPIResolver.__new__(RegularAPIResolver)
    resolver._base_url = "https://api.powerbi.com/v1.0/myorg/groups"
    resolver._request_session = SessionWithTimeout(timeout=10)
    resolver.get_authorization_header = lambda: {"Authorization": "Bearer t"}  # type: ignore[method-assign]
    return resolver


def test_workspace_users_without_graph_id_fall_back_to_identifier() -> None:
    # Groups and service principals in GET /groups/{id}/users omit graphId;
    # dropping them would lose workspace ownership for exactly the principals
    # that most often own shared content.
    body = {
        "value": [
            {
                "identifier": "user@example.com",
                "displayName": "A User",
                "graphId": "11111111-1111-4111-8111-111111111111",
                "principalType": "User",
                "groupUserAccessRight": "Admin",
            },
            {
                "identifier": "22222222-2222-4222-8222-222222222222",
                "displayName": "Analytics Team",
                "principalType": "Group",
                "groupUserAccessRight": "Member",
            },
        ]
    }
    with requests_mock.Mocker() as m:
        m.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/users",
            json=body,
        )
        users = _resolver().get_workspace_users(WORKSPACE_ID)

    assert [u.displayName for u in users] == ["A User", "Analytics Team"]
    assert users[1].graphId == "22222222-2222-4222-8222-222222222222"


def test_latest_refresh_prefers_the_newest_completed_entry() -> None:
    # Newest first from the API: a trailing Failed refresh must not hide the
    # last successful one, and the request asks for a few entries, not one.
    body = {
        "value": [
            {"requestId": "r-failed", "status": "Failed", "refreshType": "Scheduled"},
            {
                "requestId": "r-ok",
                "status": "Completed",
                "refreshType": "Scheduled",
                "startTime": "2024-01-02T03:04:05Z",
                "endTime": "2024-01-02T03:05:05Z",
            },
        ]
    }
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/ds-1/refreshes"
    with requests_mock.Mocker() as m:
        m.get(url, json=body)
        refresh = _resolver().get_latest_dataset_refresh(WORKSPACE_ID, "ds-1")
        assert m.last_request.qs == {"$top": ["5"]}
        m.get(url, json={"value": []})
        assert _resolver().get_latest_dataset_refresh(WORKSPACE_ID, "ds-1") is None

    assert refresh is not None
    assert refresh.request_id == "r-ok"
    assert refresh.status == "Completed"
