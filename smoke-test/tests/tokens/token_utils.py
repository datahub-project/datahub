import time

from tests.utils import get_frontend_url


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
