import uuid


def execute_graphql(
    auth_session,
    query: str,
    variables: dict | None = None,
    no_sync_wait: bool = False,
) -> dict:
    """Execute a GraphQL query against the frontend API.

    no_sync_wait=True uses auth_session.raw_post to skip TestSessionWrapper's
    automatic wait_for_writes_to_sync() call. Use for all-but-the-last call in
    a batch of writes where only the state after the whole batch matters.
    """
    payload = {"query": query, "variables": variables or {}}
    if no_sync_wait:
        response = auth_session.raw_post(
            f"{auth_session.frontend_url()}/api/graphql", json=payload
        )
    else:
        response = auth_session.post(
            f"{auth_session.frontend_url()}/api/graphql", json=payload
        )
    response.raise_for_status()
    return response.json()


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
