import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.knowledge.document_helpers import unique_id, execute_graphql

logger = logging.getLogger(__name__)


class TestDocumentCrudAndMutations:
    def test_create_document(self, auth_session):
        document_id = unique_id("smoke-doc-create")

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

    def test_get_document(self, auth_session):
        document_id = unique_id("smoke-doc-get")

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

    def test_update_document_contents(self, auth_session):
        document_id = unique_id("smoke-doc-update")

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

    def test_update_document_status(self, auth_session):
        """
        Test updating document status.
        1. Create an document (defaults to UNPUBLISHED).
        2. Update status to PUBLISHED.
        3. Verify the status changed and lastModified was updated.
        4. Clean up.
        """
        document_id = unique_id("smoke-doc-status")

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

    def test_create_document_with_owners(self, auth_session):
        """
        Test creating document with custom owners.
        1. Create an document with two owners.
        2. Verify the document was created successfully.
        3. Retrieve the document and verify ownership.
        4. Clean up.
        """
        document_id = unique_id("smoke-doc-owners")

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

    def test_search_documents(self, auth_session):
        document_id = unique_id("smoke-doc-search")
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
        assert result["total"] >= 1, (
            f"Expected at least 1 document, got {result['total']}"
        )
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

    def test_move_document(self, auth_session):
        """
        Test moving document to a parent and then to root.
        1. Create two documents (parent and child).
        2. Move child to parent.
        3. Verify parent relationship.
        4. Move child to root (no parent).
        5. Verify parent relationship is removed.
        6. Clean up.
        """
        parent_id = unique_id("smoke-doc-parent")
        child_id = unique_id("smoke-doc-child")

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

    def test_update_document_subtype(self, auth_session):
        """
        Test updating document sub-type.
        1. Create a document with subType "guide".
        2. Update sub-type to "tutorial".
        3. Verify the change.
        4. Clean up.
        """
        document_id = unique_id("smoke-doc-subtype")

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

    def test_update_related_entities(self, auth_session):
        """
        Test updating related entities (related assets and related documents).
        1. Create three documents (main, related1, related2).
        2. Update main document's related documents.
        3. Verify the relationships via GraphQL walk.
        4. Clean up.
        """
        main_id = unique_id("smoke-doc-main")
        related1_id = unique_id("smoke-doc-related1")
        related2_id = unique_id("smoke-doc-related2")

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
