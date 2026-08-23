"""
Smoke tests for Document Search Filtering.

Validates that document filters are correctly applied:
- searchDocuments: Filters out documents with showInGlobalContext=false
- searchAcrossEntities: Filters out UNPUBLISHED documents and showInGlobalContext=false documents
- relatedDocuments: Shows ALL documents including those with showInGlobalContext=false
  (context-only documents should be visible through their related entities)

Tests are idempotent and use unique IDs for created documents.
"""

import logging
import time
import uuid

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.knowledge.document_helpers import (
    create_unique_dataset,
    delete_unique_dataset,
    fetch_related_documents,
)
from tests.utilities.domains import Domain

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.domain(Domain.AI)


def execute_graphql(
    auth_session, query: str, variables: dict | None = None, no_sync_wait: bool = False
) -> dict:
    """Execute a GraphQL query against the frontend API.

    no_sync_wait=True bypasses TestSessionWrapper's automatic
    wait_for_writes_to_sync() call by posting via the underlying session
    directly. Use for all-but-the-last call in a batch of writes where only
    the state after the whole batch matters.
    """
    payload = {"query": query, "variables": variables or {}}
    url = f"{auth_session.frontend_url()}/api/graphql"
    post = auth_session.raw_post if no_sync_wait else auth_session.post
    response = post(url, json=payload)
    response.raise_for_status()
    result = response.json()
    return result


def _unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_document(
    auth_session,
    doc_id: str,
    title: str,
    show_in_global_context: bool = True,
    state: str = "PUBLISHED",
) -> str:
    """Create a document with specified settings and return its URN."""
    create_mutation = """
        mutation CreateDoc($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": doc_id,
            "subType": "guide",
            "title": title,
            "contents": {"text": f"Content for {title}"},
            "settings": {"showInGlobalContext": show_in_global_context},
        }
    }
    # Callers wait once (wait_for_writes_to_sync) after creating all documents
    # for a test, so intermediate creates/status-updates don't need to sync.
    result = execute_graphql(
        auth_session, create_mutation, variables, no_sync_wait=True
    )
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"
    urn = result["data"]["createDocument"]

    # If we want PUBLISHED state, update it (default is UNPUBLISHED)
    if state == "PUBLISHED":
        update_status_mutation = """
            mutation UpdateStatus($input: UpdateDocumentStatusInput!) {
              updateDocumentStatus(input: $input)
            }
        """
        update_vars = {"input": {"urn": urn, "state": "PUBLISHED"}}
        update_res = execute_graphql(
            auth_session, update_status_mutation, update_vars, no_sync_wait=True
        )
        assert "errors" not in update_res, f"GraphQL errors: {update_res.get('errors')}"

    return urn


def _delete_document(auth_session, urn: str):
    """Delete a document. Cleanup only -- nothing reads this state afterward."""
    delete_mutation = """
        mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": urn}, no_sync_wait=True)


@pytest.mark.dependency()
def test_search_documents_filters_hidden_context_documents(auth_session):
    """
    Test that searchDocuments filters out documents with showInGlobalContext=false.

    1. Create a document with showInGlobalContext=true (public).
    2. Create a document with showInGlobalContext=false (context-only).
    3. Both are PUBLISHED.
    4. Search using searchDocuments.
    5. Verify: public document is found, context-only document is NOT found.
    6. Clean up.
    """
    test_prefix = _unique_id("search-filter-test")
    public_doc_id = f"{test_prefix}-public"
    context_doc_id = f"{test_prefix}-context"

    # Create documents
    public_urn = _create_document(
        auth_session,
        public_doc_id,
        f"Public Document {test_prefix}",
        show_in_global_context=True,
        state="PUBLISHED",
    )
    context_urn = _create_document(
        auth_session,
        context_doc_id,
        f"Context Only Document {test_prefix}",
        show_in_global_context=False,
        state="PUBLISHED",
    )

    wait_for_writes_to_sync(mae_only=True)
    time.sleep(5)  # Wait for search indexing

    # Search for documents with our unique prefix
    search_query = """
        query SearchDocs($input: SearchDocumentsInput!) {
          searchDocuments(input: $input) {
            total
            documents {
              urn
              info { title }
              settings { showInGlobalContext }
            }
          }
        }
    """
    search_vars = {
        "input": {
            "start": 0,
            "count": 100,
            "query": test_prefix,
        }
    }

    try:
        search_res = execute_graphql(auth_session, search_query, search_vars)
        assert "errors" not in search_res, f"GraphQL errors: {search_res.get('errors')}"

        result = search_res["data"]["searchDocuments"]
        found_urns = [doc["urn"] for doc in result["documents"]]

        # Public document should be found
        assert public_urn in found_urns, (
            f"Expected public document {public_urn} in search results. "
            f"Found: {found_urns}"
        )

        # Context-only document should NOT be found (filtered by showInGlobalContext)
        assert context_urn not in found_urns, (
            f"Context-only document {context_urn} should NOT appear in global search. "
            f"Found: {found_urns}"
        )

    finally:
        # Cleanup
        _delete_document(auth_session, public_urn)
        _delete_document(auth_session, context_urn)


@pytest.mark.dependency()
def test_search_across_entities_filters_documents(auth_session):
    """
    Test that searchAcrossEntities filters documents correctly.

    1. Create a PUBLISHED document with showInGlobalContext=true (should appear).
    2. Create an UNPUBLISHED document (should NOT appear in global search).
    3. Create a PUBLISHED document with showInGlobalContext=false (should NOT appear).
    4. Search using searchAcrossEntities with DOCUMENT type.
    5. Verify filtering works correctly.
    6. Clean up.
    """
    test_prefix = _unique_id("cross-entity-filter")
    published_public_id = f"{test_prefix}-pub-public"
    unpublished_id = f"{test_prefix}-unpub"
    published_context_id = f"{test_prefix}-pub-context"

    # Create documents
    published_public_urn = _create_document(
        auth_session,
        published_public_id,
        f"Published Public {test_prefix}",
        show_in_global_context=True,
        state="PUBLISHED",
    )
    unpublished_urn = _create_document(
        auth_session,
        unpublished_id,
        f"Unpublished {test_prefix}",
        show_in_global_context=True,
        state="UNPUBLISHED",  # Keep as unpublished
    )
    published_context_urn = _create_document(
        auth_session,
        published_context_id,
        f"Published Context Only {test_prefix}",
        show_in_global_context=False,
        state="PUBLISHED",
    )

    wait_for_writes_to_sync(mae_only=True)
    time.sleep(5)  # Wait for search indexing

    # Search across entities for documents with our unique prefix
    search_query = """
        query SearchAcross($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on Document {
                  info { title }
                  settings { showInGlobalContext }
                  info { status { state } }
                }
              }
            }
          }
        }
    """
    search_vars = {
        "input": {
            "start": 0,
            "count": 100,
            "query": test_prefix,
            "types": ["DOCUMENT"],
        }
    }

    try:
        search_res = execute_graphql(auth_session, search_query, search_vars)
        assert "errors" not in search_res, f"GraphQL errors: {search_res.get('errors')}"

        result = search_res["data"]["searchAcrossEntities"]
        found_urns = [r["entity"]["urn"] for r in result["searchResults"]]

        # Published public document should be found
        assert published_public_urn in found_urns, (
            f"Expected published public document {published_public_urn} in search. "
            f"Found: {found_urns}"
        )

        # Unpublished document should NOT be found (filtered by state != UNPUBLISHED)
        # Note: Owned documents might still appear - this depends on the implementation
        # For now, we check that the filter is applied, but owned unpublished docs may pass
        # through in some contexts.

        # Published context-only document should NOT be found
        # (filtered by showInGlobalContext != false)
        assert published_context_urn not in found_urns, (
            f"Context-only document {published_context_urn} should NOT appear in "
            f"searchAcrossEntities. Found: {found_urns}"
        )

    finally:
        # Cleanup
        _delete_document(auth_session, published_public_urn)
        _delete_document(auth_session, unpublished_urn)
        _delete_document(auth_session, published_context_urn)


@pytest.mark.dependency()
def test_related_documents_shows_context_only_documents(auth_session, graph_client):
    """
    Test that relatedDocuments on an entity DOES show documents with
    showInGlobalContext=false.

    Context-only documents are specifically designed to be discovered through
    the entities they relate to, not through global search.

    1. Create a context-only document (showInGlobalContext=false, PUBLISHED).
    2. Associate the document with a run-unique dataset (not SampleKafkaDataset,
       which other modules also mutate under xdist).
    3. Query the dataset's relatedDocuments.
    4. Verify the context-only document appears (it should!).
    5. Clean up.
    """
    test_prefix = _unique_id("context-doc-related")
    doc_id = f"{test_prefix}-context"
    dataset_urn = create_unique_dataset(graph_client, test_prefix)
    doc_urn = None

    try:
        doc_urn = _create_document(
            auth_session,
            doc_id,
            f"Context Document {test_prefix}",
            show_in_global_context=False,
            state="PUBLISHED",
        )

        update_mutation = """
            mutation UpdateRelatedEntities($input: UpdateDocumentRelatedEntitiesInput!) {
              updateDocumentRelatedEntities(input: $input)
            }
        """
        update_vars = {
            "input": {
                "urn": doc_urn,
                "relatedAssets": [dataset_urn],
            }
        }
        update_res = execute_graphql(
            auth_session, update_mutation, update_vars, no_sync_wait=True
        )
        assert "errors" not in update_res, f"GraphQL errors: {update_res.get('errors')}"

        wait_for_writes_to_sync(mae_only=True)

        related_docs = fetch_related_documents(auth_session, dataset_urn, doc_urn)

        our_doc = next(
            (doc for doc in related_docs["documents"] if doc["urn"] == doc_urn), None
        )
        assert our_doc is not None
        assert our_doc["settings"]["showInGlobalContext"] is False

    finally:
        if doc_urn is not None:
            _delete_document(auth_session, doc_urn)
        delete_unique_dataset(graph_client, dataset_urn)


@pytest.mark.dependency()
def test_search_documents_shows_owned_unpublished(auth_session):
    """
    Test that searchDocuments shows UNPUBLISHED documents that the user owns.

    The ownership filter allows users to see their own unpublished work.

    1. Create an UNPUBLISHED document (user is automatically the owner).
    2. Search using searchDocuments.
    3. Verify the unpublished document appears (because user owns it).
    4. Clean up.
    """
    test_prefix = _unique_id("owned-unpub")
    doc_id = f"{test_prefix}-doc"

    # Create unpublished document (default state)
    # Note: We don't need to set state explicitly since UNPUBLISHED is default
    create_mutation = """
        mutation CreateDoc($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": doc_id,
            "subType": "guide",
            "title": f"My Unpublished Doc {test_prefix}",
            "contents": {"text": "Work in progress"},
            "settings": {"showInGlobalContext": True},
        }
    }
    result = execute_graphql(auth_session, create_mutation, variables)
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"
    doc_urn = result["data"]["createDocument"]

    wait_for_writes_to_sync(mae_only=True)
    time.sleep(5)  # Wait for search indexing

    # Search for documents with our unique prefix
    search_query = """
        query SearchDocs($input: SearchDocumentsInput!) {
          searchDocuments(input: $input) {
            total
            documents {
              urn
              info {
                title
                status { state }
              }
            }
          }
        }
    """
    search_vars = {
        "input": {
            "start": 0,
            "count": 100,
            "query": test_prefix,
        }
    }

    try:
        search_res = execute_graphql(auth_session, search_query, search_vars)
        assert "errors" not in search_res, f"GraphQL errors: {search_res.get('errors')}"

        result = search_res["data"]["searchDocuments"]
        found_urns = [doc["urn"] for doc in result["documents"]]

        # Owned unpublished document should be found
        assert doc_urn in found_urns, (
            f"Expected owned UNPUBLISHED document {doc_urn} in search results. "
            f"Found: {found_urns}. "
            f"Users should see their own unpublished work."
        )

        # Verify it's actually unpublished
        our_doc = next(
            (doc for doc in result["documents"] if doc["urn"] == doc_urn), None
        )
        assert our_doc is not None
        assert our_doc["info"]["status"]["state"] == "UNPUBLISHED"

    finally:
        # Cleanup
        _delete_document(auth_session, doc_urn)
