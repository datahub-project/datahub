"""
Utils for proposal tests.
"""

from tests.utils import execute_graphql

ACTION_REQUEST_ASSIGNEES = """
query getActionRequestAssignee($input: GetActionRequestAssigneeInput!) {
  getActionRequestAssignee(input: $input)
}
"""


def validate_assignee_is_correct(auth_session, dataset_urn, request_type: str):
    """
    Validate that the assignee is correct.
    """
    assignees_res = execute_graphql(
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
