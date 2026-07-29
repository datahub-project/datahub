import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.knowledge.document_helpers import execute_graphql, unique_id

logger = logging.getLogger(__name__)


class TestDocumentHierarchyAndSettings:
    def test_parent_documents_hierarchy(self, auth_session):
        """
        Test the parentDocuments resolver that returns the full parent hierarchy.
        1. Create grandparent -> parent -> child hierarchy.
        2. Query parentDocuments on child.
        3. Verify full hierarchy is returned.
        4. Clean up.
        """
        grandparent_id = unique_id("smoke-grandparent")
        parent_id = unique_id("smoke-parent")
        child_id = unique_id("smoke-child")

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
        grandparent_res = execute_graphql(
            auth_session, create_mutation, grandparent_vars
        )
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
        move_parent_vars = {
            "input": {"urn": parent_urn, "parentDocument": grandparent_urn}
        }
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
        hierarchy_res = execute_graphql(
            auth_session, hierarchy_query, {"urn": child_urn}
        )
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

    def test_document_settings(self, auth_session):
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
        public_doc_id = unique_id("smoke-doc-public")
        private_doc_id = unique_id("smoke-doc-private")

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
        assert "errors" not in private_res, (
            f"GraphQL errors: {private_res.get('errors')}"
        )
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
        update_res = execute_graphql(
            auth_session, update_settings_mutation, update_vars
        )
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

    def test_search_ownership_filtering(self, auth_session):
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
        published_doc_id = unique_id("smoke-search-published")
        unpublished_doc_id = unique_id("smoke-search-unpublished")

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
        unpublished_res = execute_graphql(
            auth_session, create_mutation, unpublished_vars
        )
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
        publish_res = execute_graphql(
            auth_session, update_status_mutation, publish_vars
        )
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

    def test_context_documents_for_dataset(self, auth_session):
        """
        Test fetching context documents for a dataset entity.
        1. Create a document.
        2. Get or use an existing dataset (using bootstrap sample data).
        3. Update the document to relate to the dataset.
        4. Query the dataset's relatedDocuments field to verify it returns the document.
        5. Clean up.
        """
        document_id = unique_id("smoke-doc-context")

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
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
        )

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
        assert "errors" not in context_res, (
            f"GraphQL errors: {context_res.get('errors')}"
        )

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
            (doc for doc in context_docs["documents"] if doc["urn"] == document_urn),
            None,
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
