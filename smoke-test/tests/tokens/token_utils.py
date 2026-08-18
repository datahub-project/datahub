import time
from typing import Any, Callable, Dict, List, Optional

from tests.utils import get_frontend_url, wait_for_writes_to_sync


def token_name_filter(name: str) -> Dict[str, Any]:
    return {
        "field": "name",
        "condition": "EQUAL",
        "values": [name],
    }


def list_access_tokens(session, filters: List[Dict[str, Any]]) -> Dict[str, Any]:
    token_input: Dict[str, Any] = {"start": 0, "count": 20}
    if filters:
        token_input["filters"] = filters

    json_payload = {
        "query": """query listAccessTokens($input: ListAccessTokenInput!) {
            listAccessTokens(input: $input) {
              start
              count
              total
              tokens {
                urn
                id
                actorUrn
                ownerUrn
              }
            }
        }""",
        "variables": {"input": token_input},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json_payload)
    response.raise_for_status()
    return response.json()


def revoke_access_token(session, token_id: str) -> Dict[str, Any]:
    json_payload = {
        "query": """mutation revokeAccessToken($tokenId: String!) {
            revokeAccessToken(tokenId: $tokenId)
        }""",
        "variables": {"tokenId": token_id},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json_payload)
    response.raise_for_status()
    return response.json()


def revoke_tokens_matching(session, filters: List[Dict[str, Any]]) -> None:
    res_data = list_access_tokens(session, filters)
    assert res_data
    assert res_data["data"]

    tokens = res_data["data"]["listAccessTokens"]["tokens"]
    if tokens:
        for metadata in tokens:
            revoke_access_token(session, metadata["id"])
        wait_for_writes_to_sync(mae_only=True)


def _poll_access_token_listing(
    session,
    filters: List[Dict[str, Any]],
    *,
    is_done: Callable[[Dict[str, Any]], bool],
    timeout_detail: str,
    before_poll: Optional[Callable[[], None]] = None,
    max_timeout_in_sec: int = 60,
) -> Dict[str, Any]:
    """Shared ES-backed listAccessTokens poll loop used by wait helpers."""
    start = time.time()
    last_listing: Dict[str, Any] | None = None
    while time.time() - start < max_timeout_in_sec:
        if before_poll is not None:
            before_poll()
        res_data = list_access_tokens(session, filters)
        assert res_data
        assert res_data["data"]
        listing = res_data["data"]["listAccessTokens"]
        assert listing is not None
        last_listing = listing
        if is_done(listing):
            return res_data
        time.sleep(1)
    raise AssertionError(
        f"{timeout_detail} after {max_timeout_in_sec}s (last listing={last_listing})"
    )


def wait_for_no_tokens_matching(
    session,
    filters: List[Dict[str, Any]],
    *,
    max_timeout_in_sec: int = 60,
) -> None:
    """Revoke matching tokens and poll until listAccessTokens is empty.

    Token search is ES-backed; revoke + wait_for_writes_to_sync is not always
    enough under xdist load before the next assertion.
    """
    _poll_access_token_listing(
        session,
        filters,
        before_poll=lambda: revoke_tokens_matching(session, filters),
        is_done=lambda listing: listing.get("total") == 0 and not listing.get("tokens"),
        timeout_detail=f"Timed out waiting for tokens matching {filters} to clear",
        max_timeout_in_sec=max_timeout_in_sec,
    )


def wait_for_tokens_matching(
    session,
    filters: List[Dict[str, Any]],
    *,
    expected_count: int = 1,
    max_timeout_in_sec: int = 60,
) -> Dict[str, Any]:
    """Poll listAccessTokens until at least expected_count tokens match.

    createAccessToken + wait_for_writes_to_sync is not always enough under xdist
    before listAccessTokens (ES-backed) reflects the new token. Uses >= so leftover
    duplicates surface as assertion failures in the caller instead of a full timeout.
    """

    def _ready(listing: Dict[str, Any]) -> bool:
        tokens = listing.get("tokens") or []
        if len(tokens) < expected_count:
            return False
        total = listing.get("total")
        return total is None or total >= expected_count

    return _poll_access_token_listing(
        session,
        filters,
        is_done=_ready,
        timeout_detail=(
            f"Timed out waiting for >= {expected_count} token(s) matching {filters}"
        ),
        max_timeout_in_sec=max_timeout_in_sec,
    )


def assert_no_tokens_matching(session, filters: List[Dict[str, Any]]) -> None:
    res_data = list_access_tokens(session, filters)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listAccessTokens"]["total"] == 0
    assert not res_data["data"]["listAccessTokens"]["tokens"]


def assert_graphql_mutation_succeeded(res_data: Dict[str, Any]) -> None:
    errors = res_data.get("errors")
    assert not errors, errors
    assert res_data.get("data")


def getUserId(session):
    response = session.get(
        f"{get_frontend_url()}/openapi/operations/identity/user/urn",
        params={"skipCache": "true"},
    )

    response.raise_for_status()
    return response.json()


def removeUser(session, urn):
    # Remove user
    json = {
        "query": """mutation removeUser($urn: String!) {
            removeUser(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)

    response.raise_for_status()
    return response.json()


def listUsers(session):
    input = {
        "start": 0,
        "count": 20,
    }

    # list users
    json = {
        "query": """query listUsers($input: ListUsersInput!) {
            listUsers(input: $input) {
              start
              count
              total
              users {
                username
              }
            }
        }""",
        "variables": {"input": input},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)

    response.raise_for_status()
    return response.json()


def wait_for_user_in_list(
    session,
    username: str,
    *,
    present: bool = True,
    max_timeout_in_sec: int = 120,
) -> None:
    """Poll listUsers until a username is visible or gone.

    listUsers is backed by Elasticsearch search, which can lag behind MCP/MCL
    consumers even after wait_for_writes_to_sync() returns.
    """
    start = time.time()
    last_users: list[dict[str, str]] | None = None
    while time.time() - start < max_timeout_in_sec:
        res_data = listUsers(session)
        users = res_data.get("data", {}).get("listUsers", {}).get("users", [])
        last_users = users if users is not None else []
        found = {"username": username} in last_users
        if found == present:
            return
        time.sleep(1)
    state = "present" if present else "absent"
    raise AssertionError(
        f"Timed out waiting for {username} to be {state} in listUsers after "
        f"{max_timeout_in_sec}s (last users: {last_users})"
    )
