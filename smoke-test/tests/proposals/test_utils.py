"""
Utils for proposal tests.
"""

ACTION_REQUEST_ASSIGNEES = """
query getActionRequestAssignee($input: GetActionRequestAssigneeInput!) {
  getActionRequestAssignee(input: $input)
}
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


def validate_assignee_is_correct(auth_session, dataset_urn, request_type: str):
    """
    Validate that the assignee is correct.
    """
    assignees_res = execute_gql(
        auth_session=auth_session,
        query=ACTION_REQUEST_ASSIGNEES,
        variables={
            "input": {
                "resourceUrn": dataset_urn,
                "actionRequestType": "TAG_ASSOCIATION",
            }
        },
    )
    assignees = assignees_res["data"]["getActionRequestAssignee"]
    assert "urn:li:corpuser:admin" in assignees
    assert "urn:li:dataHubRole:Admin" in assignees
    assert "urn:li:dataHubRole:Editor" in assignees
