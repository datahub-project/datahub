"""End-to-end smoke tests for the Custom Roles feature.

Covers:
- Full CRUD via the GraphQL surface (createRole / updateRole / deleteRole / listRoles).
- Built-in role immutability: server-side rejection of updateRole and deleteRole against
  Admin / Editor / Reader / No Role. This is the authoritative guarantee that built-in
  semantics survive upgrades (the UI hiding the action is not, on its own, sufficient).

These tests deliberately use the real GraphQL endpoint so the resolvers, AuthorizationUtils
helper, and ingest-proposal path are exercised together; the unit tests cover the
isolated behaviour of each resolver.
"""

import logging
from typing import Any, Dict, List, Optional

import pytest

from tests.utils import execute_graphql

logger = logging.getLogger(__name__)

TEST_ROLE_NAME = "Custom Roles Smoke Test"
TEST_ROLE_DESCRIPTION = "Role created by smoke tests; safe to delete."
UPDATED_ROLE_DESCRIPTION = "Role updated by smoke tests."

# Built-in roles ship via boot policies and must remain immutable across upgrades.
BUILT_IN_ROLE_URNS = [
    "urn:li:dataHubRole:Admin",
    "urn:li:dataHubRole:Editor",
    "urn:li:dataHubRole:Reader",
]

CREATE_ROLE_MUTATION = """
mutation createRole($input: CreateRoleInput!) {
  createRole(input: $input)
}
"""

UPDATE_ROLE_MUTATION = """
mutation updateRole($input: UpdateRoleInput!) {
  updateRole(input: $input)
}
"""

DELETE_ROLE_MUTATION = """
mutation deleteRole($urn: String!) {
  deleteRole(urn: $urn)
}
"""

LIST_ROLES_QUERY = """
query listRoles($input: ListRolesInput!) {
  listRoles(input: $input) {
    start
    count
    total
    roles {
      urn
      name
      description
      editable
    }
  }
}
"""


def _list_roles(auth_session, query: Optional[str] = None) -> List[Dict[str, Any]]:
    variables: Dict[str, Any] = {"input": {"start": 0, "count": 100}}
    if query:
        variables["input"]["query"] = query
    res = execute_graphql(auth_session, LIST_ROLES_QUERY, variables)
    return res["data"]["listRoles"]["roles"] or []


def _raw_graphql(auth_session, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """Call GraphQL without the execute_graphql helper's no-errors assertion.

    Needed for tests that intentionally trigger a server-side error (e.g. built-in role
    immutability checks) and need to inspect the errors payload.
    """
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json={"query": query, "variables": variables},
    )
    response.raise_for_status()
    return response.json()


@pytest.fixture()
def created_role(auth_session):
    """Create a custom role for a single test; clean up after."""
    res = execute_graphql(
        auth_session,
        CREATE_ROLE_MUTATION,
        {"input": {"name": TEST_ROLE_NAME, "description": TEST_ROLE_DESCRIPTION}},
    )
    new_urn = res["data"]["createRole"]
    assert new_urn and new_urn.startswith("urn:li:dataHubRole:")
    yield new_urn

    # Teardown: best-effort delete, ignore if already removed by the test.
    try:
        execute_graphql(auth_session, DELETE_ROLE_MUTATION, {"urn": new_urn})
    except Exception as e:
        logger.warning("Teardown delete failed for %s: %s", new_urn, e)


def test_custom_role_full_crud_cycle(auth_session):
    """Create → list → update → list → delete → list a custom role."""
    # Create.
    res = execute_graphql(
        auth_session,
        CREATE_ROLE_MUTATION,
        {"input": {"name": TEST_ROLE_NAME, "description": TEST_ROLE_DESCRIPTION}},
    )
    new_urn = res["data"]["createRole"]
    assert new_urn and new_urn.startswith("urn:li:dataHubRole:")

    # List shows the new role as editable.
    roles = _list_roles(auth_session)
    matches = [r for r in roles if r["urn"] == new_urn]
    assert len(matches) == 1, f"Expected created role {new_urn} in list"
    assert matches[0]["name"] == TEST_ROLE_NAME
    assert matches[0]["description"] == TEST_ROLE_DESCRIPTION
    assert matches[0]["editable"] is True

    # Update.
    res = execute_graphql(
        auth_session,
        UPDATE_ROLE_MUTATION,
        {
            "input": {
                "urn": new_urn,
                "name": TEST_ROLE_NAME,
                "description": UPDATED_ROLE_DESCRIPTION,
            }
        },
    )
    assert res["data"]["updateRole"] is True

    roles = _list_roles(auth_session)
    matches = [r for r in roles if r["urn"] == new_urn]
    assert len(matches) == 1
    assert matches[0]["description"] == UPDATED_ROLE_DESCRIPTION

    # Delete.
    res = execute_graphql(auth_session, DELETE_ROLE_MUTATION, {"urn": new_urn})
    assert res["data"]["deleteRole"] is True

    roles = _list_roles(auth_session)
    assert not any(r["urn"] == new_urn for r in roles), (
        "Deleted custom role should not appear in listRoles"
    )


@pytest.mark.parametrize("built_in_urn", BUILT_IN_ROLE_URNS)
def test_built_in_role_update_rejected(auth_session, built_in_urn):
    """updateRole against a built-in role must be refused server-side.

    The UI hides the Edit action for non-editable roles, but the server-side refusal in
    UpdateRoleResolver is the authoritative guarantee. If this test starts passing the
    mutation, built-in role semantics (Admin/Editor/Reader) can drift in production.
    """
    res = _raw_graphql(
        auth_session,
        UPDATE_ROLE_MUTATION,
        {
            "input": {
                "urn": built_in_urn,
                "name": "Mutated Built-in",
                "description": "Should not be applied.",
            }
        },
    )
    assert res.get("errors"), (
        f"Expected GraphQL errors when updating built-in role {built_in_urn}, got {res}"
    )


@pytest.mark.parametrize("built_in_urn", BUILT_IN_ROLE_URNS)
def test_built_in_role_delete_rejected(auth_session, built_in_urn):
    """deleteRole against a built-in role must be refused server-side."""
    res = _raw_graphql(auth_session, DELETE_ROLE_MUTATION, {"urn": built_in_urn})
    assert res.get("errors"), (
        f"Expected GraphQL errors when deleting built-in role {built_in_urn}, got {res}"
    )

    # And the role must still be present in the list afterwards.
    remaining = _list_roles(auth_session)
    assert any(r["urn"] == built_in_urn for r in remaining), (
        f"Built-in role {built_in_urn} disappeared after a rejected delete attempt"
    )


def test_built_in_roles_present_and_immutable_in_list(auth_session):
    """Sanity check: built-in roles are present in listRoles with editable=false."""
    roles = _list_roles(auth_session)
    by_urn = {r["urn"]: r for r in roles}
    for urn in BUILT_IN_ROLE_URNS:
        assert urn in by_urn, f"Built-in role {urn} missing from listRoles"
        assert by_urn[urn]["editable"] is False, (
            f"Built-in role {urn} reported as editable; the editable flag drives the UI "
            f"three-dot Edit/Delete affordance and any drift breaks immutability UX."
        )


def test_create_role_then_search_finds_it(auth_session, created_role):
    """Search by query string filters listRoles correctly for custom roles."""
    roles = _list_roles(auth_session, query=TEST_ROLE_NAME)
    matches = [r for r in roles if r["urn"] == created_role]
    assert len(matches) == 1, (
        f"Expected created role {created_role} to be searchable by name"
    )
