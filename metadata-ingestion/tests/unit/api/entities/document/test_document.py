"""
Comprehensive unit tests for the Document entity API.

Tests cover:
- Document creation with various parameters
- Document updates
- Document retrieval
- Document search with filters
- Publishing and unpublishing
- Document deletion
- Error handling and validation
- Edge cases
"""

from unittest.mock import Mock

import pytest

from datahub.api.entities.document import Document
from datahub.api.entities.document.document import (
    DocumentOperationError,
    DocumentValidationError,
)
from datahub.ingestion.graph.client import DataHubGraph

# Fixtures


@pytest.fixture
def mock_graph():
    """Create a mock DataHubGraph instance."""
    return Mock(spec=DataHubGraph)


@pytest.fixture
def document_api(mock_graph):
    """Create a Document API instance with mocked graph."""
    return Document(graph=mock_graph)


# Test Document Initialization


class TestDocumentInitialization:
    def test_init_with_valid_graph(self, mock_graph):
        """Test successful initialization with a valid graph."""
        doc_api = Document(graph=mock_graph)
        assert doc_api.graph == mock_graph

    def test_init_with_none_graph(self):
        """Test initialization fails with None graph."""
        with pytest.raises(
            DocumentValidationError, match="DataHubGraph client cannot be None"
        ):
            Document(graph=None)  # type: ignore[arg-type]


# Test Document Creation


class TestDocumentCreation:
    def test_create_document_minimal(self, document_api, mock_graph):
        """Test creating a document with minimal required fields."""
        mock_graph.execute_graphql.return_value = {
            "createDocument": "urn:li:document:abc-123"
        }

        urn = document_api.create(text="# My Document\n\nContent here")

        assert urn == "urn:li:document:abc-123"
        mock_graph.execute_graphql.assert_called_once()
        call_args = mock_graph.execute_graphql.call_args
        assert (
            call_args[1]["variables"]["input"]["contents"]["text"]
            == "# My Document\n\nContent here"
        )
        assert call_args[1]["variables"]["input"]["state"] == "UNPUBLISHED"

    def test_create_document_with_all_fields(self, document_api, mock_graph):
        """Test creating a document with all optional fields."""
        mock_graph.execute_graphql.return_value = {
            "createDocument": "urn:li:document:full-doc"
        }

        urn = document_api.create(
            text="# Full Document",
            title="Full Document Title",
            id="custom-id",
            sub_type="Tutorial",
            state="PUBLISHED",
            owners=[{"owner": "urn:li:corpuser:john", "type": "TECHNICAL_OWNER"}],
            parent_document="urn:li:document:parent",
            related_assets=["urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)"],
            related_documents=["urn:li:document:related"],
        )

        assert urn == "urn:li:document:full-doc"
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["title"] == "Full Document Title"
        assert input_data["id"] == "custom-id"
        assert input_data["subType"] == "Tutorial"
        assert input_data["state"] == "PUBLISHED"
        assert len(input_data["owners"]) == 1
        assert input_data["parentDocument"] == "urn:li:document:parent"

    def test_create_document_with_empty_text(self, document_api):
        """Test that creating a document with empty text fails."""
        with pytest.raises(
            DocumentValidationError, match="Document text cannot be empty"
        ):
            document_api.create(text="")

    def test_create_document_with_whitespace_only_text(self, document_api):
        """Test that creating a document with whitespace-only text fails."""
        with pytest.raises(
            DocumentValidationError, match="Document text cannot be empty"
        ):
            document_api.create(text="   \n\t  ")

    def test_create_document_with_invalid_state(self, document_api):
        """Test that creating a document with invalid state fails."""
        with pytest.raises(DocumentValidationError, match="Invalid state"):
            document_api.create(text="Content", state="INVALID_STATE")

    def test_create_document_with_invalid_parent_urn(self, document_api):
        """Test that creating a document with invalid parent URN fails."""
        with pytest.raises(DocumentValidationError):
            document_api.create(text="Content", parent_document="invalid-urn")

    def test_create_document_with_invalid_owner(self, document_api):
        """Test that creating a document with invalid owner fails."""
        with pytest.raises(DocumentValidationError):
            document_api.create(
                text="Content", owners=[{"owner": "", "type": "TECHNICAL_OWNER"}]
            )

    def test_create_document_with_malformed_owner(self, document_api):
        """Test that creating a document with malformed owner fails."""
        with pytest.raises(DocumentValidationError):
            document_api.create(
                text="Content",
                owners=[{"owner": "urn:li:corpuser:john"}],  # Missing 'type' field
            )

    def test_create_document_graphql_error(self, document_api, mock_graph):
        """Test handling of GraphQL errors during creation."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(DocumentOperationError, match="GraphQL operation failed"):
            document_api.create(text="Content")

    def test_create_document_no_urn_returned(self, document_api, mock_graph):
        """Test handling when no URN is returned."""
        mock_graph.execute_graphql.return_value = {"createDocument": None}

        with pytest.raises(DocumentOperationError, match="No URN returned"):
            document_api.create(text="Content")


# Test Document Updates


class TestDocumentUpdate:
    def test_update_document_text_only(self, document_api, mock_graph):
        """Test updating only the text of a document."""
        mock_graph.execute_graphql.return_value = {"updateDocumentContents": True}

        success = document_api.update(
            urn="urn:li:document:abc-123", text="Updated content"
        )

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["urn"] == "urn:li:document:abc-123"
        assert input_data["contents"]["text"] == "Updated content"
        assert "title" not in input_data

    def test_update_document_title_only(self, document_api, mock_graph):
        """Test updating only the title of a document."""
        mock_graph.execute_graphql.return_value = {"updateDocumentContents": True}

        success = document_api.update(
            urn="urn:li:document:abc-123", title="Updated Title"
        )

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["title"] == "Updated Title"
        assert "contents" not in input_data

    def test_update_document_all_fields(self, document_api, mock_graph):
        """Test updating all fields of a document."""
        mock_graph.execute_graphql.return_value = {"updateDocumentContents": True}

        success = document_api.update(
            urn="urn:li:document:abc-123",
            text="Updated content",
            title="Updated Title",
            sub_type="FAQ",
        )

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["contents"]["text"] == "Updated content"
        assert input_data["title"] == "Updated Title"
        assert input_data["subType"] == "FAQ"

    def test_update_document_with_invalid_urn(self, document_api):
        """Test that updating with invalid URN fails."""
        with pytest.raises(DocumentValidationError):
            document_api.update(urn="invalid-urn", text="Content")

    def test_update_document_with_no_fields(self, document_api):
        """Test that updating with no fields fails."""
        with pytest.raises(DocumentValidationError, match="At least one of"):
            document_api.update(urn="urn:li:document:abc-123")

    def test_update_document_with_empty_text(self, document_api):
        """Test that updating with empty text fails."""
        with pytest.raises(
            DocumentValidationError, match="Document text cannot be empty"
        ):
            document_api.update(urn="urn:li:document:abc-123", text="   ")

    def test_update_document_graphql_error(self, document_api, mock_graph):
        """Test handling of GraphQL errors during update."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(DocumentOperationError):
            document_api.update(urn="urn:li:document:abc-123", text="Content")


# Test Document Retrieval


class TestDocumentRetrieval:
    def test_get_document_exists(self, document_api, mock_graph):
        """Test retrieving an existing document."""
        mock_response = {
            "document": {
                "urn": "urn:li:document:abc-123",
                "type": "DOCUMENT",
                "subType": "Tutorial",
                "info": {
                    "title": "My Document",
                    "contents": {"text": "Content"},
                    "status": {"state": "PUBLISHED"},
                },
                "exists": True,
            }
        }
        mock_graph.execute_graphql.return_value = mock_response

        document = document_api.get(urn="urn:li:document:abc-123")

        assert document is not None
        assert document["urn"] == "urn:li:document:abc-123"
        assert document["info"]["title"] == "My Document"
        assert document["info"]["contents"]["text"] == "Content"

    def test_get_document_not_found(self, document_api, mock_graph):
        """Test retrieving a non-existent document."""
        mock_graph.execute_graphql.return_value = {"document": None}

        document = document_api.get(urn="urn:li:document:abc-123")

        assert document is None

    def test_get_document_exists_false(self, document_api, mock_graph):
        """Test retrieving a document marked as not existing."""
        mock_response = {
            "document": {"urn": "urn:li:document:abc-123", "exists": False}
        }
        mock_graph.execute_graphql.return_value = mock_response

        document = document_api.get(urn="urn:li:document:abc-123")

        assert document is None

    def test_get_document_with_invalid_urn(self, document_api):
        """Test that getting with invalid URN fails."""
        with pytest.raises(DocumentValidationError):
            document_api.get(urn="invalid-urn")

    def test_get_document_graphql_error(self, document_api, mock_graph):
        """Test handling of GraphQL errors during retrieval."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(DocumentOperationError):
            document_api.get(urn="urn:li:document:abc-123")


# Test Document Search


class TestDocumentSearch:
    def test_search_documents_no_filters(self, document_api, mock_graph):
        """Test searching documents without filters."""
        mock_response = {
            "searchDocuments": {
                "total": 10,
                "documents": [
                    {"urn": "urn:li:document:1", "info": {"title": "Doc 1"}},
                    {"urn": "urn:li:document:2", "info": {"title": "Doc 2"}},
                ],
                "facets": [],
            }
        }
        mock_graph.execute_graphql.return_value = mock_response

        results = document_api.search()

        assert results["total"] == 10
        assert len(results["documents"]) == 2

    def test_search_documents_with_query(self, document_api, mock_graph):
        """Test searching documents with a query."""
        mock_response = {"searchDocuments": {"total": 3, "documents": [], "facets": []}}
        mock_graph.execute_graphql.return_value = mock_response

        results = document_api.search(query="machine learning")

        assert results["total"] == 3
        call_args = mock_graph.execute_graphql.call_args
        assert call_args[1]["variables"]["input"]["query"] == "machine learning"

    def test_search_documents_with_filters(self, document_api, mock_graph):
        """Test searching documents with multiple filters."""
        mock_response = {"searchDocuments": {"total": 5, "documents": [], "facets": []}}
        mock_graph.execute_graphql.return_value = mock_response

        results = document_api.search(
            query="tutorial",
            types=["Tutorial", "FAQ"],
            states=["PUBLISHED"],
            domains=["urn:li:domain:engineering"],
            count=20,
        )

        assert results["total"] == 5
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["query"] == "tutorial"
        assert input_data["types"] == ["Tutorial", "FAQ"]
        assert input_data["states"] == ["PUBLISHED"]
        assert input_data["domains"] == ["urn:li:domain:engineering"]
        assert input_data["count"] == 20

    def test_search_documents_with_pagination(self, document_api, mock_graph):
        """Test searching documents with pagination."""
        mock_response = {
            "searchDocuments": {"total": 100, "documents": [], "facets": []}
        }
        mock_graph.execute_graphql.return_value = mock_response

        results = document_api.search(start=20, count=10)

        assert results["total"] == 100
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["start"] == 20
        assert input_data["count"] == 10

    def test_search_documents_root_only(self, document_api, mock_graph):
        """Test searching only root-level documents."""
        mock_response = {"searchDocuments": {"total": 5, "documents": [], "facets": []}}
        mock_graph.execute_graphql.return_value = mock_response

        document_api.search(root_only=True)

        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["rootOnly"] is True

    def test_search_documents_include_drafts(self, document_api, mock_graph):
        """Test searching with drafts included."""
        mock_response = {"searchDocuments": {"total": 8, "documents": [], "facets": []}}
        mock_graph.execute_graphql.return_value = mock_response

        document_api.search(include_drafts=True)

        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["includeDrafts"] is True

    def test_search_documents_with_invalid_start(self, document_api):
        """Test that searching with negative start fails."""
        with pytest.raises(DocumentValidationError, match="start must be non-negative"):
            document_api.search(start=-1)

    def test_search_documents_with_invalid_count(self, document_api):
        """Test that searching with invalid count fails."""
        with pytest.raises(DocumentValidationError, match="count must be positive"):
            document_api.search(count=0)

    def test_search_documents_with_excessive_count(self, document_api):
        """Test that searching with excessive count fails."""
        with pytest.raises(DocumentValidationError, match="count cannot exceed 10000"):
            document_api.search(count=10001)

    def test_search_documents_with_invalid_state(self, document_api):
        """Test that searching with invalid state fails."""
        with pytest.raises(DocumentValidationError, match="Invalid state"):
            document_api.search(states=["INVALID_STATE"])

    def test_search_documents_with_invalid_domain(self, document_api):
        """Test that searching with invalid domain URN fails."""
        with pytest.raises(DocumentValidationError, match="Invalid domain URN"):
            document_api.search(domains=["invalid-domain"])

    def test_search_documents_empty_result(self, document_api, mock_graph):
        """Test searching when no results are found."""
        mock_response = {"searchDocuments": None}
        mock_graph.execute_graphql.return_value = mock_response

        results = document_api.search()

        assert results["total"] == 0
        assert results["documents"] == []
        assert results["facets"] == []

    def test_search_documents_graphql_error(self, document_api, mock_graph):
        """Test handling of GraphQL errors during search."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(DocumentOperationError):
            document_api.search()


# Test Document Publishing


class TestDocumentPublishing:
    def test_publish_document(self, document_api, mock_graph):
        """Test publishing a document."""
        mock_graph.execute_graphql.return_value = {"updateDocumentStatus": True}

        success = document_api.publish(urn="urn:li:document:abc-123")

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["urn"] == "urn:li:document:abc-123"
        assert input_data["state"] == "PUBLISHED"

    def test_unpublish_document(self, document_api, mock_graph):
        """Test unpublishing a document."""
        mock_graph.execute_graphql.return_value = {"updateDocumentStatus": True}

        success = document_api.unpublish(urn="urn:li:document:abc-123")

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["urn"] == "urn:li:document:abc-123"
        assert input_data["state"] == "UNPUBLISHED"

    def test_publish_document_with_invalid_urn(self, document_api):
        """Test that publishing with invalid URN fails."""
        with pytest.raises(DocumentValidationError):
            document_api.publish(urn="invalid-urn")

    def test_publish_document_graphql_error(self, document_api, mock_graph):
        """Test handling of GraphQL errors during publishing."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(DocumentOperationError):
            document_api.publish(urn="urn:li:document:abc-123")


# Test Document Deletion


class TestDocumentDeletion:
    def test_delete_document(self, document_api, mock_graph):
        """Test deleting a document."""
        mock_graph.execute_graphql.return_value = {"deleteDocument": True}

        success = document_api.delete(urn="urn:li:document:abc-123")

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        assert call_args[1]["variables"]["urn"] == "urn:li:document:abc-123"

    def test_delete_document_with_invalid_urn(self, document_api):
        """Test that deleting with invalid URN fails."""
        with pytest.raises(DocumentValidationError):
            document_api.delete(urn="invalid-urn")

    def test_delete_document_graphql_error(self, document_api, mock_graph):
        """Test handling of GraphQL errors during deletion."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(DocumentOperationError):
            document_api.delete(urn="urn:li:document:abc-123")


# Test Advanced Operations


class TestAdvancedOperations:
    def test_update_sub_type(self, document_api, mock_graph):
        """Test updating document sub-type."""
        mock_graph.execute_graphql.return_value = {"updateDocumentSubType": True}

        success = document_api.update_sub_type(
            urn="urn:li:document:abc-123", sub_type="FAQ"
        )

        assert success is True

    def test_update_related_entities(self, document_api, mock_graph):
        """Test updating related entities."""
        mock_graph.execute_graphql.return_value = {
            "updateDocumentRelatedEntities": True
        }

        success = document_api.update_related_entities(
            urn="urn:li:document:abc-123",
            related_assets=["urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)"],
            related_documents=["urn:li:document:related"],
        )

        assert success is True

    def test_move_document_to_parent(self, document_api, mock_graph):
        """Test moving a document to a parent."""
        mock_graph.execute_graphql.return_value = {"moveDocument": True}

        success = document_api.move(
            urn="urn:li:document:abc-123", parent_document="urn:li:document:parent"
        )

        assert success is True

    def test_move_document_to_root(self, document_api, mock_graph):
        """Test moving a document to root level."""
        mock_graph.execute_graphql.return_value = {"moveDocument": True}

        success = document_api.move(urn="urn:li:document:abc-123", parent_document=None)

        assert success is True
        call_args = mock_graph.execute_graphql.call_args
        input_data = call_args[1]["variables"]["input"]
        assert input_data["parentDocument"] is None

    def test_exists_document_true(self, document_api, mock_graph):
        """Test checking if a document exists (exists)."""
        mock_response = {"document": {"urn": "urn:li:document:abc-123", "exists": True}}
        mock_graph.execute_graphql.return_value = mock_response

        exists = document_api.exists(urn="urn:li:document:abc-123")

        assert exists is True

    def test_exists_document_false(self, document_api, mock_graph):
        """Test checking if a document exists (does not exist)."""
        mock_graph.execute_graphql.return_value = {"document": None}

        exists = document_api.exists(urn="urn:li:document:abc-123")

        assert exists is False

    def test_exists_document_operation_error(self, document_api, mock_graph):
        """Test checking existence when operation fails."""
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        exists = document_api.exists(urn="urn:li:document:abc-123")

        assert exists is False


# Test URN Validation


class TestURNValidation:
    def test_validate_valid_document_urn(self, document_api):
        """Test validation of a valid document URN."""
        # Should not raise any exception
        document_api._validate_urn("urn:li:document:abc-123")

    def test_validate_empty_urn(self, document_api):
        """Test validation of an empty URN."""
        with pytest.raises(
            DocumentValidationError, match="Document URN cannot be empty"
        ):
            document_api._validate_urn("")

    def test_validate_wrong_entity_type(self, document_api):
        """Test validation of URN with wrong entity type."""
        with pytest.raises(DocumentValidationError, match="Expected document URN"):
            document_api._validate_urn(
                "urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)"
            )

    def test_validate_malformed_urn(self, document_api):
        """Test validation of a malformed URN."""
        with pytest.raises(DocumentValidationError, match="Invalid document URN"):
            document_api._validate_urn("not-a-urn")


# Test Edge Cases


class TestEdgeCases:
    def test_create_document_with_unicode(self, document_api, mock_graph):
        """Test creating a document with Unicode characters."""
        mock_graph.execute_graphql.return_value = {
            "createDocument": "urn:li:document:unicode"
        }

        urn = document_api.create(
            text="# 文档\n\n这是一个包含中文的文档 🚀", title="Unicode Document 文档"
        )

        assert urn == "urn:li:document:unicode"

    def test_create_document_with_very_long_text(self, document_api, mock_graph):
        """Test creating a document with very long text."""
        mock_graph.execute_graphql.return_value = {
            "createDocument": "urn:li:document:long"
        }

        long_text = "A" * 100000  # 100KB of text
        urn = document_api.create(text=long_text)

        assert urn == "urn:li:document:long"

    def test_search_with_special_characters(self, document_api, mock_graph):
        """Test searching with special characters in query."""
        mock_response = {"searchDocuments": {"total": 1, "documents": [], "facets": []}}
        mock_graph.execute_graphql.return_value = mock_response

        results = document_api.search(query="test & special | characters")

        assert results["total"] == 1

    def test_update_document_return_false(self, document_api, mock_graph):
        """Test update operation returning False."""
        mock_graph.execute_graphql.return_value = {"updateDocumentContents": False}

        success = document_api.update(urn="urn:li:document:abc-123", text="Content")

        assert success is False

    def test_multiple_owners_same_user(self, document_api, mock_graph):
        """Test creating document with same user as multiple owner types."""
        mock_graph.execute_graphql.return_value = {
            "createDocument": "urn:li:document:multi-owner"
        }

        urn = document_api.create(
            text="Content",
            owners=[
                {"owner": "urn:li:corpuser:john", "type": "TECHNICAL_OWNER"},
                {"owner": "urn:li:corpuser:john", "type": "BUSINESS_OWNER"},
            ],
        )

        assert urn == "urn:li:document:multi-owner"
