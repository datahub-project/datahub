"""
Utils for proposal tests.
"""


def execute_gql(auth_session, query, variables=None):
    """
    Helper for sending GraphQL requests via the auth_session's post method.
    Raises an HTTP error on bad status, returns the parsed JSON response on success.
    """
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=payload
    )
    response.raise_for_status()

    return response.json()
