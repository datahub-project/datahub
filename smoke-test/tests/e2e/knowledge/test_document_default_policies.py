"""
E2E authorization smoke tests for the default document policies.

A user with no role can create a document and becomes its owner. The owner can edit and delete
the document. A user who is not an owner cannot edit it, through GraphQL or through the OpenAPI
entity endpoint.
"""

import logging
from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from tests.e2e.knowledge.document_helpers import execute_graphql
from tests.e2e.privileges.utils import is_graphql_auth_denied
from tests.e2e.utils import TestSessionWrapper, unique_suffix
from utilities.domains import Domain
from utilities.multi_user import cleanup_step_actor_user, make_step_actor_user

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.domain(Domain.PLATFORM),
]

CREATE_DOCUMENT_MUTATION = """
mutation createDocument($input: CreateDocumentInput!) {
  createDocument(input: $input)
}
"""

UPDATE_DOCUMENT_MUTATION = """
mutation updateDocumentContents($input: UpdateDocumentContentsInput!) {
  updateDocumentContents(input: $input)
}
"""

DELETE_DOCUMENT_MUTATION = """
mutation deleteDocument($urn: String!) {
  deleteDocument(urn: $urn)
}
"""


@dataclass
class RolelessUsers:
    author_urn: str
    author_session: TestSessionWrapper
    other_urn: str
    other_session: TestSessionWrapper


@pytest.fixture(scope="module")
def roleless_users(auth_session) -> Iterator[RolelessUsers]:
    author_urn, author_session = make_step_actor_user(auth_session, "doc-author")
    other_urn, other_session = make_step_actor_user(auth_session, "doc-other")
    try:
        yield RolelessUsers(author_urn, author_session, other_urn, other_session)
    finally:
        author_session.destroy()
        other_session.destroy()
        cleanup_step_actor_user(auth_session, author_urn)
        cleanup_step_actor_user(auth_session, other_urn)


def _create_document(session) -> str:
    document_input = {
        "id": f"smoke-doc-policy-{unique_suffix()}",
        "subType": "reference",
        "title": "Default document policy smoke test",
        "contents": {"text": "Initial content"},
    }
    res = execute_graphql(session, CREATE_DOCUMENT_MUTATION, {"input": document_input})
    assert "errors" not in res, res
    return res["data"]["createDocument"]


def _update_document(session, urn: str, text: str) -> dict:
    return execute_graphql(
        session,
        UPDATE_DOCUMENT_MUTATION,
        {"input": {"urn": urn, "contents": {"text": text}}},
    )


def _delete_document_quietly(session, urn: str) -> None:
    try:
        execute_graphql(session, DELETE_DOCUMENT_MUTATION, {"urn": urn})
    except Exception:
        logger.warning("Failed to delete document %s", urn, exc_info=True)


def test_roleless_user_manages_own_document(roleless_users):
    """Create, edit and delete all work for the creator, with no role assigned."""
    urn = _create_document(roleless_users.author_session)
    try:
        update_res = _update_document(
            roleless_users.author_session, urn, "Edited by the owner"
        )
        assert "errors" not in update_res, update_res
        assert update_res["data"]["updateDocumentContents"] is True

        delete_res = execute_graphql(
            roleless_users.author_session, DELETE_DOCUMENT_MUTATION, {"urn": urn}
        )
        assert "errors" not in delete_res, delete_res
        assert delete_res["data"]["deleteDocument"] is True
    finally:
        # A no-op after the delete above; removes the document if an assertion failed first.
        _delete_document_quietly(roleless_users.author_session, urn)


def test_roleless_user_cannot_edit_document_of_another_user(roleless_users):
    """The all-users grant is create-only, so it must not extend to edits."""
    urn = _create_document(roleless_users.author_session)
    try:
        res = _update_document(
            roleless_users.other_session, urn, "Edited by a non-owner"
        )
        assert is_graphql_auth_denied(res), res
    finally:
        _delete_document_quietly(roleless_users.author_session, urn)


def test_roleless_user_cannot_overwrite_document_through_openapi(roleless_users):
    """A write to an existing document needs edit access, so the create grant must not allow it."""
    urn = _create_document(roleless_users.author_session)
    other_session = roleless_users.other_session
    try:
        response = other_session.post(
            f"{other_session.gms_url()}/openapi/v3/entity/document",
            params={"async": "false"},
            json=[{"urn": urn, "status": {"value": {"removed": True}}}],
        )
        assert response.status_code == 403, response.text
    finally:
        _delete_document_quietly(roleless_users.author_session, urn)
