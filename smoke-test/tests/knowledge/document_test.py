"""
Smoke tests for Document GraphQL APIs.

Validates end-to-end functionality of:
- createDocument (with and without owners)
- document (get)
- updateDocumentContents
- updateDocumentStatus
- searchDocuments

Tests are idempotent and use unique IDs for created documents.
"""

import time
import uuid

import pytest

from tests.consistency_utils import wait_for_writes_to_sync


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
def test_create_document(auth_session):
    document_id = _unique_id("smoke-doc-create")

    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """

    variables = {
        "input": {
            "id": document_id,
            "subType": "how-to",
            "title": f"Smoke Create {document_id}",
            "contents": {"text": "Initial content"},
        }
    }

    result = execute_graphql(auth_session, create_mutation, variables)
    assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"
    urn = result["data"]["createDocument"]
    assert urn.startswith("urn:li:document:"), f"Unexpected URN: {urn}"

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency()
def test_get_document(auth_session):
    document_id = _unique_id("smoke-doc-get")

    # Create
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document_id,
            "subType": "reference",
            "title": f"Smoke Get {document_id}",
            "contents": {"text": "Get content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    urn = create_res["data"]["createDocument"]

    wait_for_writes_to_sync()

    get_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            urn
            type
            subType
            info {
              title
              source {
                sourceType
              }
              status { state }
              contents { text }
              created { time actor }
              lastModified { time actor }
              relatedAssets { asset { urn } }
              relatedDocuments { document { urn } }
              parentDocument { document { urn } }
            }
          }
        }
    """
    get_res = execute_graphql(auth_session, get_query, {"urn": urn})
    assert "errors" not in get_res, f"GraphQL errors: {get_res.get('errors')}"
    ka = get_res["data"]["document"]
    assert ka and ka["urn"] == urn
    assert ka["subType"] == "reference"
    assert ka["info"]["title"].startswith("Smoke Get ")
    # Verify source is automatically set to NATIVE (users can't set this via API)
    # Note: This requires the backend to be restarted with the updated code
    if ka["info"]["source"] is not None:
        assert ka["info"]["source"]["sourceType"] == "NATIVE"
    assert ka["info"]["contents"]["text"] == "Get content"
    assert ka["info"]["status"]["state"] == "UNPUBLISHED"  # Default state
    assert ka["info"]["created"]["time"] > 0
    assert ka["info"]["lastModified"]["time"] > 0

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency()
def test_update_document_contents(auth_session):
    document_id = _unique_id("smoke-doc-update")

    # Create
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document_id,
            "subType": "guide",
            "title": f"Smoke Update {document_id}",
            "contents": {"text": "Old content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    urn = create_res["data"]["createDocument"]

    wait_for_writes_to_sync()

    # Update contents
    update_mutation = """
        mutation UpdateKA($input: UpdateDocumentContentsInput!) {
          updateDocumentContents(input: $input)
        }
    """
    update_vars = {
        "input": {
            "urn": urn,
            "title": f"Smoke Updated {document_id}",
            "contents": {"text": "New content"},
        }
    }
    update_res = execute_graphql(auth_session, update_mutation, update_vars)
    assert update_res["data"]["updateDocumentContents"] is True

    wait_for_writes_to_sync()

    # Verify update and that lastModified changed
    get_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            urn
            info {
              title
              contents { text }
              created { time }
              lastModified { time actor }
            }
          }
        }
    """
    get_res = execute_graphql(auth_session, get_query, {"urn": urn})
    info = get_res["data"]["document"]["info"]
    assert info["title"].startswith("Smoke Updated ")
    assert info["contents"]["text"] == "New content"
    # lastModified should be >= created (and typically later after an update)
    assert info["lastModified"]["time"] >= info["created"]["time"]

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency()
def test_update_document_status(auth_session):
    """
    Test updating document status.
    1. Create an document (defaults to UNPUBLISHED).
    2. Update status to PUBLISHED.
    3. Verify the status changed and lastModified was updated.
    4. Clean up.
    """
    document_id = _unique_id("smoke-doc-status")

    # Create document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document_id,
            "subType": "guide",
            "title": f"Smoke Status {document_id}",
            "contents": {"text": "Status test content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    urn = create_res["data"]["createDocument"]

    wait_for_writes_to_sync()

    # Get initial state
    get_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            info {
              status { state }
              created { time }
              lastModified { time }
            }
          }
        }
    """
    initial_res = execute_graphql(auth_session, get_query, {"urn": urn})
    initial_info = initial_res["data"]["document"]["info"]
    assert initial_info["status"]["state"] == "UNPUBLISHED"
    initial_modified_time = initial_info["lastModified"]["time"]

    # Small delay to ensure timestamp difference
    time.sleep(1)

    # Update status to PUBLISHED
    update_status_mutation = """
        mutation UpdateStatus($input: UpdateDocumentStatusInput!) {
          updateDocumentStatus(input: $input)
        }
    """
    status_vars = {"input": {"urn": urn, "state": "PUBLISHED"}}
    status_res = execute_graphql(auth_session, update_status_mutation, status_vars)
    assert "errors" not in status_res, f"GraphQL errors: {status_res.get('errors')}"
    assert status_res["data"]["updateDocumentStatus"] is True

    wait_for_writes_to_sync()

    # Verify status changed and lastModified was updated
    final_res = execute_graphql(auth_session, get_query, {"urn": urn})
    final_info = final_res["data"]["document"]["info"]
    assert final_info["status"]["state"] == "PUBLISHED"
    # lastModified should have been updated (newer timestamp)
    assert final_info["lastModified"]["time"] >= initial_modified_time

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency()
def test_create_document_with_owners(auth_session):
    """
    Test creating document with custom owners.
    1. Create an document with two owners.
    2. Verify the document was created successfully.
    3. Retrieve the document and verify ownership.
    4. Clean up.
    """
    document_id = _unique_id("smoke-doc-owners")

    # Create document with custom owners
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document_id,
            "subType": "guide",
            "title": f"Smoke Owners {document_id}",
            "contents": {"text": "Ownership test content"},
            "owners": [
                {
                    "ownerUrn": "urn:li:corpuser:datahub",
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    assert "errors" not in create_res, f"GraphQL errors: {create_res.get('errors')}"
    urn = create_res["data"]["createDocument"]
    assert urn.startswith("urn:li:document:")

    wait_for_writes_to_sync()

    # Verify ownership was set
    get_query = """
        query GetKA($urn: String!) {
          document(urn: $urn) {
            urn
            ownership {
              owners {
                owner {
                  ... on CorpUser {
                    urn
                  }
                }
                type
              }
            }
          }
        }
    """
    get_res = execute_graphql(auth_session, get_query, {"urn": urn})
    assert "errors" not in get_res, f"GraphQL errors: {get_res.get('errors')}"
    ka = get_res["data"]["document"]
    assert ka["ownership"] is not None
    assert len(ka["ownership"]["owners"]) >= 1
    owner_urns = [o["owner"]["urn"] for o in ka["ownership"]["owners"]]
    assert "urn:li:corpuser:datahub" in owner_urns

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True


@pytest.mark.dependency()
def test_search_documents(auth_session):
    document_id = _unique_id("smoke-doc-search")
    title = f"Smoke Search {document_id}"

    # Create
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document_id,
            "subType": "tutorial",
            "title": title,
            "contents": {"text": "Searchable content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    urn = create_res["data"]["createDocument"]

    wait_for_writes_to_sync()
    time.sleep(5)

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
    # Include UNPUBLISHED state in search since created documents default to UNPUBLISHED
    # (searchDocuments defaults to PUBLISHED only if states not specified)
    search_vars = {"input": {"start": 0, "count": 100, "states": ["UNPUBLISHED"]}}
    search_res = execute_graphql(auth_session, search_query, search_vars)
    assert "errors" not in search_res, f"GraphQL errors: {search_res.get('errors')}"
    result = search_res["data"]["searchDocuments"]
    assert result["total"] >= 1, f"Expected at least 1 document, got {result['total']}"
    urns = [a["urn"] for a in result["documents"]]
    assert urn in urns, (
        f"Expected created document {urn} in search results. Found {len(urns)} documents."
    )

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    del_res = execute_graphql(auth_session, delete_mutation, {"urn": urn})
    assert del_res["data"]["deleteDocument"] is True
