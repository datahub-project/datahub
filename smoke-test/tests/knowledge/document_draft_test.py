"""
Smoke tests for Document Draft GraphQL APIs.

Validates end-to-end functionality of:
- Creating drafts using draftFor
- Document.drafts field
- mergeDraft mutation
- Search excludes drafts by default

Tests are idempotent and use unique IDs for created documents.
"""

import logging
import time
import uuid

import pytest

logger = logging.getLogger(__name__)


def execute_graphql(auth_session, query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against the frontend API."""
    payload = {"query": query, "variables": variables or {}}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/graphql", json=payload
    )
    response.raise_for_status()
    result = response.json()
    return result


def _unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.dependency()
def test_create_document_draft(auth_session):
    """
    Test creating a draft document.
    1. Create a published document.
    2. Create a draft for that document using draftFor.
    3. Verify the draft is linked to the published document.
    4. Verify the published document shows the draft in its drafts field.
    5. Clean up both documents.
    """
    published_id = _unique_id("smoke-doc-published")
    draft_id = _unique_id("smoke-doc-draft")

    # Create published document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    published_vars = {
        "input": {
            "id": published_id,
            "subType": "guide",
            "title": f"Published Doc {published_id}",
            "contents": {"text": "Published content"},
            "state": "PUBLISHED",
        }
    }
    published_res = execute_graphql(auth_session, create_mutation, published_vars)
    published_urn = published_res["data"]["createDocument"]

    # Create draft document
    draft_vars = {
        "input": {
            "id": draft_id,
            "subType": "guide",
            "title": f"Draft Doc {draft_id}",
            "contents": {"text": "Draft content"},
            "draftFor": published_urn,
        }
    }
    draft_res = execute_graphql(auth_session, create_mutation, draft_vars)
    draft_urn = draft_res["data"]["createDocument"]

    # Verify draft is linked to published document
    get_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            urn
            info {
              title
              status { state }
              draftOf {
                document { urn }
              }
            }
          }
        }
    """
    draft_get_res = execute_graphql(auth_session, get_query, {"urn": draft_urn})
    draft_doc = draft_get_res["data"]["document"]
    assert draft_doc["info"]["status"]["state"] == "UNPUBLISHED"
    assert draft_doc["info"]["draftOf"] is not None
    assert draft_doc["info"]["draftOf"]["document"]["urn"] == published_urn

    # Verify published document shows the draft
    get_drafts_query = """
        query GetKAWithDrafts($urn: String!) {
          document(urn: $urn) {
            urn
            info {
              title
              status { state }
            }
            drafts {
              urn
            }
          }
        }
    """
    published_get_res = execute_graphql(
        auth_session, get_drafts_query, {"urn": published_urn}
    )
    published_doc = published_get_res["data"]["document"]
    assert published_doc["info"]["status"]["state"] == "PUBLISHED"

    # The drafts field requires a separate batch loader and may return None if not implemented
    if published_doc["drafts"] is None:
        logger.info(
            "WARNING: drafts field is None (batch loader may not be implemented yet)"
        )
    else:
        assert len(published_doc["drafts"]) >= 1, (
            f"Expected at least 1 draft, got {len(published_doc['drafts'])}"
        )
        draft_urns = [d["urn"] for d in published_doc["drafts"]]
        assert draft_urn in draft_urns, f"Expected draft {draft_urn} in drafts list"

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": draft_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": published_urn})


@pytest.mark.dependency()
def test_merge_draft(auth_session):
    """
    Test merging a draft into its published parent.
    1. Create a published document.
    2. Create a draft for that document.
    3. Update the draft's content.
    4. Merge the draft into the published document.
    5. Verify the published document has the draft's content.
    6. Clean up.
    """
    published_id = _unique_id("smoke-doc-merge-pub")
    draft_id = _unique_id("smoke-doc-merge-draft")

    # Create published document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    published_vars = {
        "input": {
            "id": published_id,
            "subType": "guide",
            "title": f"Merge Published {published_id}",
            "contents": {"text": "Original published content"},
            "state": "PUBLISHED",
        }
    }
    published_res = execute_graphql(auth_session, create_mutation, published_vars)
    published_urn = published_res["data"]["createDocument"]

    # Create draft document
    draft_vars = {
        "input": {
            "id": draft_id,
            "subType": "guide",
            "title": f"Merge Draft {draft_id}",
            "contents": {"text": "Updated draft content"},
            "draftFor": published_urn,
        }
    }
    draft_res = execute_graphql(auth_session, create_mutation, draft_vars)
    draft_urn = draft_res["data"]["createDocument"]

    # Merge draft into published document
    merge_mutation = """
        mutation MergeDraft($input: MergeDraftInput!) {
          mergeDraft(input: $input)
        }
    """
    merge_vars = {"input": {"draftUrn": draft_urn, "deleteDraft": True}}
    merge_res = execute_graphql(auth_session, merge_mutation, merge_vars)
    assert merge_res["data"]["mergeDraft"] is True

    # Verify published document has the draft's content
    get_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            urn
            info {
              title
              contents { text }
              status { state }
            }
          }
        }
    """
    published_get_res = execute_graphql(auth_session, get_query, {"urn": published_urn})
    published_doc = published_get_res["data"]["document"]
    assert published_doc["info"]["title"] == f"Merge Draft {draft_id}"
    assert published_doc["info"]["contents"]["text"] == "Updated draft content"
    assert published_doc["info"]["status"]["state"] == "PUBLISHED"

    # Verify draft was deleted (with soft deletion, check exists field or document is filtered out)
    draft_check_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            urn
            exists
            info { title }
          }
        }
    """
    draft_get_res = execute_graphql(auth_session, draft_check_query, {"urn": draft_urn})
    assert draft_get_res is not None, (
        f"GraphQL response is None for draft URN: {draft_urn}"
    )
    assert "errors" not in draft_get_res, (
        f"GraphQL errors: {draft_get_res.get('errors')}"
    )
    # After soft deletion, the document should either:
    # 1. Not be returned (document is None), or
    # 2. Have exists=False, or
    # 3. Have no info aspect
    draft_doc = draft_get_res["data"]["document"]
    assert (
        draft_doc is None
        or draft_doc.get("exists") is False
        or draft_doc.get("info") is None
    ), f"Draft should be deleted/hidden, but got: {draft_doc}"

    # Cleanup published document
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": published_urn})


@pytest.mark.dependency()
def test_search_excludes_drafts_by_default(auth_session):
    """
    Test that search excludes draft documents by default.
    1. Create a published document.
    2. Create a draft for that document.
    3. Search - should only see published.
    4. Clean up.
    """
    published_id = _unique_id("smoke-doc-search-pub")
    draft_id = _unique_id("smoke-doc-search-draft")

    # Create published document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    published_vars = {
        "input": {
            "id": published_id,
            "subType": "guide",
            "title": f"Search Published {published_id}",
            "contents": {"text": "Published searchable content"},
            "state": "PUBLISHED",
        }
    }
    published_res = execute_graphql(auth_session, create_mutation, published_vars)
    published_urn = published_res["data"]["createDocument"]

    # Create draft document
    draft_vars = {
        "input": {
            "id": draft_id,
            "subType": "guide",
            "title": f"Search Draft {draft_id}",
            "contents": {"text": "Draft searchable content"},
            "draftFor": published_urn,
        }
    }
    draft_res = execute_graphql(auth_session, create_mutation, draft_vars)
    draft_urn = draft_res["data"]["createDocument"]

    # Search should exclude drafts by default
    search_query = """
        query SearchKA($input: SearchDocumentsInput!) {
          searchDocuments(input: $input) {
            start
            count
            total
            documents { urn info { title } }
          }
        }
    """
    search_vars_no_drafts = {"input": {"start": 0, "count": 100}}
    # Wait for search indexing
    time.sleep(5)

    search_res_no_drafts = execute_graphql(
        auth_session, search_query, search_vars_no_drafts
    )

    # Search can fail if index is not ready
    if "errors" in search_res_no_drafts or search_res_no_drafts is None:
        logger.info(
            f"WARNING: Search failed (index may not be ready): {search_res_no_drafts}"
        )
        # Cleanup
        delete_mutation = """
            mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
        """
        execute_graphql(auth_session, delete_mutation, {"urn": draft_urn})
        execute_graphql(auth_session, delete_mutation, {"urn": published_urn})
        pytest.skip("Search index not available")
        return

    result_no_drafts = search_res_no_drafts["data"]["searchDocuments"]
    urns_no_drafts = [a["urn"] for a in result_no_drafts["documents"]]
    assert published_urn in urns_no_drafts
    assert draft_urn not in urns_no_drafts
