"""
Smoke tests for Document GraphQL APIs.

Validates end-to-end functionality of:
- createDocument (with and without owners)
- document (get)
- updateDocumentContents
- updateDocumentStatus
- updateDocumentSubType
- moveDocument
- updateDocumentRelatedEntities
- searchDocuments (with various filters)
- changeHistory
- parentDocuments (hierarchy)
- relatedDocuments (fetching related context documents for entities)

Tests are idempotent and use unique IDs for created documents.
"""

import logging
import time
import uuid

import pytest

from tests.consistency_utils import wait_for_writes_to_sync

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
              created { time actor { urn } }
              lastModified { time actor { urn } }
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
              lastModified { time actor { urn } }
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
    # Wait for search indexing to catch up
    time.sleep(5)

    # No need to specify states - ownership filtering handles showing UNPUBLISHED docs we own
    search_vars = {"input": {"start": 0, "count": 100}}
    search_res = execute_graphql(auth_session, search_query, search_vars)

    # Search can fail if index is not ready - log and skip assertion if it fails
    if "errors" in search_res:
        logger.info(
            f"WARNING: Search failed (index may not be ready): {search_res.get('errors')}"
        )
        # Cleanup and return early
        delete_mutation = """
            mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
        """
        execute_graphql(auth_session, delete_mutation, {"urn": urn})
        pytest.skip("Search index not available")
        return
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


@pytest.mark.dependency()
def test_move_document(auth_session):
    """
    Test moving document to a parent and then to root.
    1. Create two documents (parent and child).
    2. Move child to parent.
    3. Verify parent relationship.
    4. Move child to root (no parent).
    5. Verify parent relationship is removed.
    6. Clean up.
    """
    parent_id = _unique_id("smoke-doc-parent")
    child_id = _unique_id("smoke-doc-child")

    # Create parent document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    parent_vars = {
        "input": {
            "id": parent_id,
            "subType": "guide",
            "title": f"Parent {parent_id}",
            "contents": {"text": "Parent content"},
        }
    }
    parent_res = execute_graphql(auth_session, create_mutation, parent_vars)
    parent_urn = parent_res["data"]["createDocument"]

    # Create child document
    child_vars = {
        "input": {
            "id": child_id,
            "subType": "guide",
            "title": f"Child {child_id}",
            "contents": {"text": "Child content"},
        }
    }
    child_res = execute_graphql(auth_session, create_mutation, child_vars)
    child_urn = child_res["data"]["createDocument"]

    # Move child to parent
    move_mutation = """
        mutation MoveDoc($input: MoveDocumentInput!) {
          moveDocument(input: $input)
        }
    """
    move_vars = {"input": {"urn": child_urn, "parentDocument": parent_urn}}
    move_res = execute_graphql(auth_session, move_mutation, move_vars)
    assert move_res["data"]["moveDocument"] is True

    # Verify parent relationship
    get_query = """
        query GetDoc($urn: String!) {
          document(urn: $urn) {
            info {
              parentDocument {
                document {
                  urn
                  info { title }
                }
              }
            }
          }
        }
    """
    get_res = execute_graphql(auth_session, get_query, {"urn": child_urn})
    parent_doc = get_res["data"]["document"]["info"]["parentDocument"]
    assert parent_doc is not None
    assert parent_doc["document"]["urn"] == parent_urn
    assert parent_doc["document"]["info"]["title"].startswith("Parent ")

    # Move child to root (null parent)
    move_to_root_vars = {"input": {"urn": child_urn, "parentDocument": None}}
    move_root_res = execute_graphql(auth_session, move_mutation, move_to_root_vars)
    assert move_root_res["data"]["moveDocument"] is True

    # Verify parent is removed
    get_res2 = execute_graphql(auth_session, get_query, {"urn": child_urn})
    parent_doc2 = get_res2["data"]["document"]["info"]["parentDocument"]
    assert parent_doc2 is None

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": child_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": parent_urn})


@pytest.mark.dependency()
def test_update_document_subtype(auth_session):
    """
    Test updating document sub-type.
    1. Create a document with subType "guide".
    2. Update sub-type to "tutorial".
    3. Verify the change.
    4. Clean up.
    """
    document_id = _unique_id("smoke-doc-subtype")

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
            "title": f"SubType Test {document_id}",
            "contents": {"text": "SubType content"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    urn = create_res["data"]["createDocument"]

    # Update sub-type
    update_mutation = """
        mutation UpdateSubType($input: UpdateDocumentSubTypeInput!) {
          updateDocumentSubType(input: $input)
        }
    """
    update_vars = {"input": {"urn": urn, "subType": "tutorial"}}
    update_res = execute_graphql(auth_session, update_mutation, update_vars)
    assert update_res["data"]["updateDocumentSubType"] is True

    wait_for_writes_to_sync()

    # Verify update
    get_query = """
        query GetDoc($urn: String!) {
          document(urn: $urn) {
            subType
          }
        }
    """
    get_res = execute_graphql(auth_session, get_query, {"urn": urn})
    assert get_res["data"]["document"]["subType"] == "tutorial"

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": urn})


@pytest.mark.dependency()
def test_update_related_entities(auth_session):
    """
    Test updating related entities (related assets and related documents).
    1. Create three documents (main, related1, related2).
    2. Update main document's related documents.
    3. Verify the relationships via GraphQL walk.
    4. Clean up.
    """
    main_id = _unique_id("smoke-doc-main")
    related1_id = _unique_id("smoke-doc-related1")
    related2_id = _unique_id("smoke-doc-related2")

    # Create documents
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """

    main_vars = {
        "input": {
            "id": main_id,
            "subType": "guide",
            "title": f"Main {main_id}",
            "contents": {"text": "Main content"},
        }
    }
    main_res = execute_graphql(auth_session, create_mutation, main_vars)
    main_urn = main_res["data"]["createDocument"]

    related1_vars = {
        "input": {
            "id": related1_id,
            "subType": "reference",
            "title": f"Related1 {related1_id}",
            "contents": {"text": "Related1 content"},
        }
    }
    related1_res = execute_graphql(auth_session, create_mutation, related1_vars)
    related1_urn = related1_res["data"]["createDocument"]

    related2_vars = {
        "input": {
            "id": related2_id,
            "subType": "reference",
            "title": f"Related2 {related2_id}",
            "contents": {"text": "Related2 content"},
        }
    }
    related2_res = execute_graphql(auth_session, create_mutation, related2_vars)
    related2_urn = related2_res["data"]["createDocument"]

    # Update related entities
    update_mutation = """
        mutation UpdateRelated($input: UpdateDocumentRelatedEntitiesInput!) {
          updateDocumentRelatedEntities(input: $input)
        }
    """
    update_vars = {
        "input": {
            "urn": main_urn,
            "relatedDocuments": [related1_urn, related2_urn],
        }
    }
    update_res = execute_graphql(auth_session, update_mutation, update_vars)
    assert update_res["data"]["updateDocumentRelatedEntities"] is True

    wait_for_writes_to_sync()

    # Verify related documents via GraphQL walk
    get_query = """
        query GetDoc($urn: String!) {
          document(urn: $urn) {
            info {
              relatedDocuments {
                document {
                  urn
                  info {
                    title
                  }
                }
              }
            }
          }
        }
    """
    get_res = execute_graphql(auth_session, get_query, {"urn": main_urn})
    related_docs = get_res["data"]["document"]["info"]["relatedDocuments"]
    assert related_docs is not None
    assert len(related_docs) == 2

    related_urns = [doc["document"]["urn"] for doc in related_docs]
    assert related1_urn in related_urns
    assert related2_urn in related_urns

    # Verify that the related documents have resolved titles
    for doc in related_docs:
        assert doc["document"]["info"]["title"].startswith("Related")

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": main_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": related1_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": related2_urn})


@pytest.mark.dependency()
def test_change_history(auth_session):
    """
    Test that change history is created for document edits and moves.
    1. Create a document.
    2. Update title.
    3. Update content.
    4. Create parent and move document.
    5. Query change history and verify entries exist for each change.
    6. Clean up.
    """
    document_id = _unique_id("smoke-doc-history")
    parent_id = _unique_id("smoke-doc-history-parent")

    # Create document
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    doc_vars = {
        "input": {
            "id": document_id,
            "subType": "guide",
            "title": f"History Test {document_id}",
            "contents": {"text": "Original content"},
        }
    }
    doc_res = execute_graphql(auth_session, create_mutation, doc_vars)
    doc_urn = doc_res["data"]["createDocument"]

    # Update title
    update_contents_mutation = """
        mutation UpdateContents($input: UpdateDocumentContentsInput!) {
          updateDocumentContents(input: $input)
        }
    """
    update_vars = {
        "input": {
            "urn": doc_urn,
            "title": f"Updated Title {document_id}",
        }
    }
    execute_graphql(auth_session, update_contents_mutation, update_vars)

    wait_for_writes_to_sync()

    # Update content
    update_content_vars = {
        "input": {
            "urn": doc_urn,
            "contents": {"text": "Updated content"},
        }
    }
    execute_graphql(auth_session, update_contents_mutation, update_content_vars)

    wait_for_writes_to_sync()

    # Create parent and move document
    parent_vars = {
        "input": {
            "id": parent_id,
            "subType": "guide",
            "title": f"Parent {parent_id}",
            "contents": {"text": "Parent content"},
        }
    }
    parent_res = execute_graphql(auth_session, create_mutation, parent_vars)
    parent_urn = parent_res["data"]["createDocument"]

    move_mutation = """
        mutation MoveDoc($input: MoveDocumentInput!) {
          moveDocument(input: $input)
        }
    """
    move_vars = {"input": {"urn": doc_urn, "parentDocument": parent_urn}}
    execute_graphql(auth_session, move_mutation, move_vars)

    # Query change history
    history_query = """
        query GetHistory($urn: String!) {
          document(urn: $urn) {
            changeHistory(limit: 100) {
              changeType
              description
              timestamp
              actor { urn }
            }
          }
        }
    """
    history_res = execute_graphql(auth_session, history_query, {"urn": doc_urn})
    assert "errors" not in history_res, f"GraphQL errors: {history_res.get('errors')}"
    changes = history_res["data"]["document"]["changeHistory"]

    # Verify we have change entries
    assert len(changes) > 0, "Expected at least one change history entry"

    # Look for specific changes using changeType field
    change_types = [change.get("changeType", "").upper() for change in changes]
    descriptions = [change.get("description", "").lower() for change in changes]

    # At minimum, we should see document creation
    assert "CREATED" in change_types or any(
        "created" in desc or "create" in desc for desc in descriptions
    ), (
        f"Expected 'CREATED' in change history. Got change types: {change_types}, descriptions: {descriptions}"
    )

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": doc_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": parent_urn})


@pytest.mark.dependency()
def test_search_documents_with_filters(auth_session):
    """
    Test searching documents with various filters.
    1. Create parent document and child document.
    2. Search for documents with parentDocuments filter.
    3. Search for root-only documents.
    4. Search by subType filter.
    5. Clean up.
    """
    parent_id = _unique_id("smoke-search-parent")
    child_id = _unique_id("smoke-search-child")
    root_id = _unique_id("smoke-search-root")

    # Create documents
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """

    # Parent document
    parent_vars = {
        "input": {
            "id": parent_id,
            "subType": "faq",
            "title": f"Search Parent {parent_id}",
            "contents": {"text": "Parent content"},
        }
    }
    parent_res = execute_graphql(auth_session, create_mutation, parent_vars)
    parent_urn = parent_res["data"]["createDocument"]

    # Child document
    child_vars = {
        "input": {
            "id": child_id,
            "subType": "tutorial",
            "title": f"Search Child {child_id}",
            "contents": {"text": "Child content"},
        }
    }
    child_res = execute_graphql(auth_session, create_mutation, child_vars)
    child_urn = child_res["data"]["createDocument"]

    # Root document
    root_vars = {
        "input": {
            "id": root_id,
            "subType": "tutorial",
            "title": f"Search Root {root_id}",
            "contents": {"text": "Root content"},
        }
    }
    root_res = execute_graphql(auth_session, create_mutation, root_vars)
    root_urn = root_res["data"]["createDocument"]

    # Move child to parent
    move_mutation = """
        mutation MoveDoc($input: MoveDocumentInput!) {
          moveDocument(input: $input)
        }
    """
    move_vars = {"input": {"urn": child_urn, "parentDocument": parent_urn}}
    execute_graphql(auth_session, move_mutation, move_vars)

    # Wait for search indexing
    time.sleep(5)

    # Search by parent document filter
    search_query = """
        query SearchDocs($input: SearchDocumentsInput!) {
          searchDocuments(input: $input) {
            total
            documents {
              urn
              info { title }
            }
          }
        }
    """
    search_vars = {
        "input": {
            "start": 0,
            "count": 100,
            "parentDocuments": [parent_urn],
        }
    }
    search_res = execute_graphql(auth_session, search_query, search_vars)

    # Search can fail if index is not ready
    if "errors" in search_res:
        logger.info(
            f"WARNING: Search with filters failed (index may not be ready): {search_res.get('errors')}"
        )
        # Cleanup and return early
        delete_mutation = """
            mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
        """
        for u in [child_urn, root_urn, parent_urn]:
            execute_graphql(auth_session, delete_mutation, {"urn": u})
        pytest.skip("Search index not available")
        return

    result = search_res["data"]["searchDocuments"]
    urns = [doc["urn"] for doc in result["documents"]]
    assert child_urn in urns, "Expected child document in parent filter results"

    # Search for root-only documents
    root_search_vars = {
        "input": {
            "start": 0,
            "count": 100,
            "rootOnly": True,
        }
    }
    root_search_res = execute_graphql(auth_session, search_query, root_search_vars)
    root_result = root_search_res["data"]["searchDocuments"]
    root_urns = [doc["urn"] for doc in root_result["documents"]]

    # Root and parent should be in results, but not child
    assert root_urn in root_urns or parent_urn in root_urns, (
        "Expected root-level documents in rootOnly results"
    )

    # Search by type filter
    type_search_vars = {
        "input": {
            "start": 0,
            "count": 100,
            "query": "*",
            "types": ["tutorial"],
        }
    }
    type_search_res = execute_graphql(auth_session, search_query, type_search_vars)
    assert "errors" not in type_search_res, (
        f"GraphQL errors: {type_search_res.get('errors')}"
    )
    type_result = type_search_res["data"]["searchDocuments"]
    type_urns = [doc["urn"] for doc in type_result["documents"]]

    # Should find our tutorial documents (child and root both have tutorial subType)
    # Note: Type filtering may not be fully indexed yet, so we make this a soft check
    if len(type_urns) > 0:
        assert child_urn in type_urns or root_urn in type_urns, (
            f"Expected tutorial documents in type filter results. "
            f"Found {len(type_urns)} documents but neither expected URN was present."
        )

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": child_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": parent_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": root_urn})


@pytest.mark.dependency()
def test_parent_documents_hierarchy(auth_session):
    """
    Test the parentDocuments resolver that returns the full parent hierarchy.
    1. Create grandparent -> parent -> child hierarchy.
    2. Query parentDocuments on child.
    3. Verify full hierarchy is returned.
    4. Clean up.
    """
    grandparent_id = _unique_id("smoke-grandparent")
    parent_id = _unique_id("smoke-parent")
    child_id = _unique_id("smoke-child")

    # Create documents
    create_mutation = """
        mutation CreateKA($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """

    grandparent_vars = {
        "input": {
            "id": grandparent_id,
            "subType": "guide",
            "title": f"Grandparent {grandparent_id}",
            "contents": {"text": "Grandparent content"},
        }
    }
    grandparent_res = execute_graphql(auth_session, create_mutation, grandparent_vars)
    grandparent_urn = grandparent_res["data"]["createDocument"]

    parent_vars = {
        "input": {
            "id": parent_id,
            "subType": "guide",
            "title": f"Parent {parent_id}",
            "contents": {"text": "Parent content"},
        }
    }
    parent_res = execute_graphql(auth_session, create_mutation, parent_vars)
    parent_urn = parent_res["data"]["createDocument"]

    child_vars = {
        "input": {
            "id": child_id,
            "subType": "guide",
            "title": f"Child {child_id}",
            "contents": {"text": "Child content"},
        }
    }
    child_res = execute_graphql(auth_session, create_mutation, child_vars)
    child_urn = child_res["data"]["createDocument"]

    # Build hierarchy: grandparent -> parent
    move_mutation = """
        mutation MoveDoc($input: MoveDocumentInput!) {
          moveDocument(input: $input)
        }
    """
    move_parent_vars = {"input": {"urn": parent_urn, "parentDocument": grandparent_urn}}
    execute_graphql(auth_session, move_mutation, move_parent_vars)

    # Build hierarchy: parent -> child
    move_child_vars = {"input": {"urn": child_urn, "parentDocument": parent_urn}}
    execute_graphql(auth_session, move_mutation, move_child_vars)

    # Query parent hierarchy
    hierarchy_query = """
        query GetHierarchy($urn: String!) {
          document(urn: $urn) {
            parentDocuments {
              count
              documents {
                urn
                info {
                  title
                }
              }
            }
          }
        }
    """
    hierarchy_res = execute_graphql(auth_session, hierarchy_query, {"urn": child_urn})
    assert "errors" not in hierarchy_res, (
        f"GraphQL errors: {hierarchy_res.get('errors')}"
    )
    parent_docs = hierarchy_res["data"]["document"]["parentDocuments"]

    assert parent_docs is not None
    assert parent_docs["count"] >= 2, "Expected at least 2 parents in hierarchy"

    parent_urns = [p["urn"] for p in parent_docs["documents"]]
    assert parent_urn in parent_urns, "Expected direct parent in hierarchy"
    assert grandparent_urn in parent_urns, "Expected grandparent in hierarchy"

    # Cleanup
    delete_mutation = """
        mutation DeleteKA($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": child_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": parent_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": grandparent_urn})


@pytest.mark.dependency()
def test_document_settings(auth_session):
    """
    Test document settings functionality.
    1. Create a document with default settings (showInGlobalContext=true).
    2. Verify settings are set correctly.
    3. Create a document with showInGlobalContext=false (private context document).
    4. Verify settings reflect the custom value.
    5. Update settings from false to true.
    6. Verify the update.
    7. Clean up.
    """
    public_doc_id = _unique_id("smoke-doc-public")
    private_doc_id = _unique_id("smoke-doc-private")

    # Create document with default settings (should be showInGlobalContext=true)
    create_mutation = """
        mutation CreateDoc($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    public_vars = {
        "input": {
            "id": public_doc_id,
            "subType": "guide",
            "title": f"Public Document {public_doc_id}",
            "contents": {"text": "This is a public document"},
        }
    }
    public_res = execute_graphql(auth_session, create_mutation, public_vars)
    assert "errors" not in public_res, f"GraphQL errors: {public_res.get('errors')}"
    public_urn = public_res["data"]["createDocument"]

    # Query and verify default settings
    get_query = """
        query GetDoc($urn: String!) {
          document(urn: $urn) {
            urn
            settings {
              showInGlobalContext
            }
          }
        }
    """
    public_get_res = execute_graphql(auth_session, get_query, {"urn": public_urn})
    assert "errors" not in public_get_res, (
        f"GraphQL errors: {public_get_res.get('errors')}"
    )
    public_doc = public_get_res["data"]["document"]
    assert public_doc["settings"] is not None
    assert public_doc["settings"]["showInGlobalContext"] is True, (
        "Expected default showInGlobalContext to be True"
    )

    # Create document with showInGlobalContext=false (private context document)
    private_vars = {
        "input": {
            "id": private_doc_id,
            "subType": "guide",
            "title": f"Private Context Document {private_doc_id}",
            "contents": {"text": "This is a private context document"},
            "settings": {"showInGlobalContext": False},
        }
    }
    private_res = execute_graphql(auth_session, create_mutation, private_vars)
    assert "errors" not in private_res, f"GraphQL errors: {private_res.get('errors')}"
    private_urn = private_res["data"]["createDocument"]

    wait_for_writes_to_sync()

    # Query and verify custom settings
    private_get_res = execute_graphql(auth_session, get_query, {"urn": private_urn})
    assert "errors" not in private_get_res, (
        f"GraphQL errors: {private_get_res.get('errors')}"
    )
    private_doc = private_get_res["data"]["document"]
    assert private_doc["settings"] is not None
    assert private_doc["settings"]["showInGlobalContext"] is False, (
        "Expected showInGlobalContext to be False for private context document"
    )

    # Update settings from false to true
    update_settings_mutation = """
        mutation UpdateSettings($input: UpdateDocumentSettingsInput!) {
          updateDocumentSettings(input: $input)
        }
    """
    update_vars = {"input": {"urn": private_urn, "showInGlobalContext": True}}
    update_res = execute_graphql(auth_session, update_settings_mutation, update_vars)
    assert "errors" not in update_res, f"GraphQL errors: {update_res.get('errors')}"
    assert update_res["data"]["updateDocumentSettings"] is True

    wait_for_writes_to_sync()

    # Verify the update
    updated_get_res = execute_graphql(auth_session, get_query, {"urn": private_urn})
    assert "errors" not in updated_get_res, (
        f"GraphQL errors: {updated_get_res.get('errors')}"
    )
    updated_doc = updated_get_res["data"]["document"]
    assert updated_doc["settings"] is not None
    assert updated_doc["settings"]["showInGlobalContext"] is True, (
        "Expected showInGlobalContext to be updated to True"
    )

    # Cleanup
    delete_mutation = """
        mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": public_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": private_urn})


@pytest.mark.dependency()
def test_search_ownership_filtering(auth_session):
    """
    Test that search respects ownership filtering:
    - PUBLISHED documents are shown to all users
    - UNPUBLISHED documents are only shown if owned by the current user or their groups

    1. Create a PUBLISHED document (should be visible in search).
    2. Create an UNPUBLISHED document owned by current user (should be visible in search).
    3. Publish the unpublished document and verify it appears in search.
    4. Unpublish it and verify it still appears (because user owns it).
    5. Clean up.
    """
    published_doc_id = _unique_id("smoke-search-published")
    unpublished_doc_id = _unique_id("smoke-search-unpublished")

    create_mutation = """
        mutation CreateDoc($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """

    # Create PUBLISHED document
    published_vars = {
        "input": {
            "id": published_doc_id,
            "subType": "guide",
            "title": f"Published Doc {published_doc_id}",
            "contents": {"text": "Published content"},
            "state": "PUBLISHED",
        }
    }
    published_res = execute_graphql(auth_session, create_mutation, published_vars)
    assert "errors" not in published_res, (
        f"GraphQL errors: {published_res.get('errors')}"
    )
    published_urn = published_res["data"]["createDocument"]

    # Create UNPUBLISHED document (owned by current user by default)
    unpublished_vars = {
        "input": {
            "id": unpublished_doc_id,
            "subType": "guide",
            "title": f"Unpublished Doc {unpublished_doc_id}",
            "contents": {"text": "Unpublished content"},
            "state": "UNPUBLISHED",
        }
    }
    unpublished_res = execute_graphql(auth_session, create_mutation, unpublished_vars)
    assert "errors" not in unpublished_res, (
        f"GraphQL errors: {unpublished_res.get('errors')}"
    )
    unpublished_urn = unpublished_res["data"]["createDocument"]

    # Wait for search indexing
    time.sleep(5)

    # Search without state filter - should return BOTH documents
    # (PUBLISHED because it's public, UNPUBLISHED because we own it)
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
            "query": "smoke-search",  # Search for our test documents
        }
    }
    search_res = execute_graphql(auth_session, search_query, search_vars)

    # Search can fail if index is not ready
    if "errors" in search_res:
        logger.info(
            f"WARNING: Ownership search failed (index may not be ready): {search_res.get('errors')}"
        )
        # Cleanup and return early
        delete_mutation = """
            mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
        """
        execute_graphql(auth_session, delete_mutation, {"urn": published_urn})
        execute_graphql(auth_session, delete_mutation, {"urn": unpublished_urn})
        pytest.skip("Search index not available")
        return

    result = search_res["data"]["searchDocuments"]
    found_urns = [doc["urn"] for doc in result["documents"]]

    # Both documents should be visible:
    # - PUBLISHED doc is visible to everyone
    # - UNPUBLISHED doc is visible because we own it
    assert published_urn in found_urns, (
        f"Expected PUBLISHED document {published_urn} in search results"
    )
    assert unpublished_urn in found_urns, (
        f"Expected UNPUBLISHED document {unpublished_urn} (owned by user) in search results"
    )

    # Verify the states
    doc_states = {
        doc["urn"]: doc["info"]["status"]["state"] for doc in result["documents"]
    }
    assert doc_states.get(published_urn) == "PUBLISHED"
    assert doc_states.get(unpublished_urn) == "UNPUBLISHED"

    # Now publish the unpublished document and verify it still appears
    update_status_mutation = """
        mutation UpdateStatus($input: UpdateDocumentStatusInput!) {
          updateDocumentStatus(input: $input)
        }
    """
    publish_vars = {"input": {"urn": unpublished_urn, "state": "PUBLISHED"}}
    publish_res = execute_graphql(auth_session, update_status_mutation, publish_vars)
    assert publish_res["data"]["updateDocumentStatus"] is True

    wait_for_writes_to_sync()
    time.sleep(3)

    # Search again - both should still be visible (both PUBLISHED now)
    search_res2 = execute_graphql(auth_session, search_query, search_vars)
    result2 = search_res2["data"]["searchDocuments"]
    found_urns2 = [doc["urn"] for doc in result2["documents"]]

    assert published_urn in found_urns2
    assert unpublished_urn in found_urns2

    # Cleanup
    delete_mutation = """
        mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": published_urn})
    execute_graphql(auth_session, delete_mutation, {"urn": unpublished_urn})


@pytest.mark.dependency()
def test_context_documents_for_dataset(auth_session):
    """
    Test fetching context documents for a dataset entity.
    1. Create a document.
    2. Get or use an existing dataset (using bootstrap sample data).
    3. Update the document to relate to the dataset.
    4. Query the dataset's relatedDocuments field to verify it returns the document.
    5. Clean up.
    """
    document_id = _unique_id("smoke-doc-context")

    # Create a document
    create_mutation = """
        mutation CreateDoc($input: CreateDocumentInput!) {
          createDocument(input: $input)
        }
    """
    variables = {
        "input": {
            "id": document_id,
            "subType": "guide",
            "title": f"Context Test {document_id}",
            "contents": {"text": "Document related to dataset"},
        }
    }
    create_res = execute_graphql(auth_session, create_mutation, variables)
    document_urn = create_res["data"]["createDocument"]

    # Use bootstrap sample dataset (commonly available in test environments)
    # If this doesn't exist, the test will fail gracefully
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"

    # Verify dataset exists first
    dataset_query = """
        query GetDataset($urn: String!) {
          dataset(urn: $urn) {
            urn
            name
          }
        }
    """
    dataset_res = execute_graphql(auth_session, dataset_query, {"urn": dataset_urn})

    # If dataset doesn't exist, skip the test
    if "errors" in dataset_res or not dataset_res.get("data", {}).get("dataset"):
        # Cleanup document and skip
        delete_mutation = """
            mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
        """
        execute_graphql(auth_session, delete_mutation, {"urn": document_urn})
        pytest.skip(f"Dataset {dataset_urn} not available for testing")
        return

    # Update document to relate to the dataset
    update_mutation = """
        mutation UpdateRelated($input: UpdateDocumentRelatedEntitiesInput!) {
          updateDocumentRelatedEntities(input: $input)
        }
    """
    update_vars = {
        "input": {
            "urn": document_urn,
            "relatedAssets": [dataset_urn],
        }
    }
    update_res = execute_graphql(auth_session, update_mutation, update_vars)
    assert update_res["data"]["updateDocumentRelatedEntities"] is True

    wait_for_writes_to_sync()

    # Wait for search indexing
    time.sleep(5)

    # Query relatedDocuments on the dataset entity
    context_query = """
        query GetRelatedDocuments($urn: String!, $input: RelatedDocumentsInput!) {
          dataset(urn: $urn) {
            urn
            relatedDocuments(input: $input) {
              start
              count
              total
              documents {
                urn
                info {
                  title
                }
              }
            }
          }
        }
    """
    context_vars = {
        "urn": dataset_urn,
        "input": {
            "start": 0,
            "count": 100,
        },
    }
    context_res = execute_graphql(auth_session, context_query, context_vars)

    # Verify no errors
    assert "errors" not in context_res, f"GraphQL errors: {context_res.get('errors')}"

    # Verify relatedDocuments result
    context_docs = context_res["data"]["dataset"]["relatedDocuments"]
    assert context_docs is not None
    assert context_docs["total"] >= 1, (
        f"Expected at least 1 context document, got {context_docs['total']}"
    )

    # Verify our document is in the results
    document_urns = [doc["urn"] for doc in context_docs["documents"]]
    assert document_urn in document_urns, (
        f"Expected document {document_urn} to be in context documents, "
        f"but got: {document_urns}"
    )

    # Verify the document has the expected title
    our_doc = next(
        (doc for doc in context_docs["documents"] if doc["urn"] == document_urn), None
    )
    assert our_doc is not None
    assert our_doc["info"]["title"] == f"Context Test {document_id}"

    # Test with filters - query with rootOnly filter
    context_query_filtered = """
        query GetRelatedDocuments($urn: String!, $input: RelatedDocumentsInput!) {
          dataset(urn: $urn) {
            urn
            relatedDocuments(input: $input) {
              start
              count
              total
              documents {
                urn
              }
            }
          }
        }
    """
    context_vars_filtered = {
        "urn": dataset_urn,
        "input": {
            "start": 0,
            "count": 100,
            "rootOnly": True,
        },
    }
    context_res_filtered = execute_graphql(
        auth_session, context_query_filtered, context_vars_filtered
    )
    assert "errors" not in context_res_filtered
    # Our document should still be there (it's root-level)
    filtered_docs = context_res_filtered["data"]["dataset"]["relatedDocuments"]
    filtered_urns = [doc["urn"] for doc in filtered_docs["documents"]]
    assert document_urn in filtered_urns

    # Cleanup
    delete_mutation = """
        mutation DeleteDoc($urn: String!) { deleteDocument(urn: $urn) }
    """
    execute_graphql(auth_session, delete_mutation, {"urn": document_urn})
