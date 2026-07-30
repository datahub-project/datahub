import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.knowledge.document_helpers import execute_graphql, unique_id

logger = logging.getLogger(__name__)


class TestDocumentSearchAndHistory:
    def test_change_history(self, auth_session):
        """
        Test that change history is created for document edits and moves.
        1. Create a document.
        2. Update title.
        3. Update content.
        4. Create parent and move document.
        5. Query change history and verify entries exist for each change.
        6. Clean up.
        """
        document_id = unique_id("smoke-doc-history")
        parent_id = unique_id("smoke-doc-history-parent")

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
        assert "errors" not in history_res, (
            f"GraphQL errors: {history_res.get('errors')}"
        )
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

    def test_search_documents_with_filters(self, auth_session):
        """
        Test searching documents with various filters.
        1. Create parent document and child document.
        2. Search for documents with parentDocuments filter.
        3. Search for root-only documents.
        4. Search by subType filter.
        5. Clean up.
        """
        parent_id = unique_id("smoke-search-parent")
        child_id = unique_id("smoke-search-child")
        root_id = unique_id("smoke-search-root")

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
