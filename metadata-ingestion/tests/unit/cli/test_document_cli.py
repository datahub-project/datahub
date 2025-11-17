"""
Unit tests for the document CLI.

Tests that CLI commands properly invoke the Document API and handle errors.
"""

import json
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from datahub.api.entities.document.document import (
    DocumentOperationError,
    DocumentValidationError,
)
from datahub.cli.document_cli import document


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_document_api():
    """Mock the Document API class."""
    with patch("datahub.cli.document_cli.Document") as mock:
        instance = Mock()
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_graph():
    """Mock the DataHubGraph."""
    with patch("datahub.cli.document_cli.get_default_graph") as mock:
        yield mock


# Test Document Create Command


class TestDocumentCreateCLI:
    def test_create_minimal(self, runner, mock_document_api, mock_graph):
        """Test creating a document with minimal options."""
        mock_document_api.create.return_value = "urn:li:document:test-123"

        result = runner.invoke(
            document, ["create", "--text", "# Test Document\n\nContent here"]
        )

        assert result.exit_code == 0
        assert "Successfully created document" in result.output
        assert "urn:li:document:test-123" in result.output
        mock_document_api.create.assert_called_once()

    def test_create_with_all_options(self, runner, mock_document_api, mock_graph):
        """Test creating a document with all options."""
        mock_document_api.create.return_value = "urn:li:document:full-doc"

        result = runner.invoke(
            document,
            [
                "create",
                "--text",
                "# Tutorial",
                "--title",
                "My Tutorial",
                "--sub-type",
                "Tutorial",
                "--state",
                "published",
                "--owner",
                "urn:li:corpuser:john:TECHNICAL_OWNER",
            ],
        )

        assert result.exit_code == 0
        assert "Successfully created document" in result.output
        mock_document_api.create.assert_called_once()

        # Verify the call arguments
        call_args = mock_document_api.create.call_args
        assert call_args[1]["text"] == "# Tutorial"
        assert call_args[1]["title"] == "My Tutorial"
        assert call_args[1]["sub_type"] == "Tutorial"
        assert call_args[1]["state"] == "PUBLISHED"

    def test_create_from_file(self, runner, mock_document_api, mock_graph):
        """Test creating a document from a file."""
        mock_document_api.create.return_value = "urn:li:document:from-file"

        with runner.isolated_filesystem():
            with open("test.md", "w") as f:
                f.write("# From File\n\nFile content")

            result = runner.invoke(
                document, ["create", "--file", "test.md", "--title", "From File"]
            )

        assert result.exit_code == 0
        assert "Successfully created document" in result.output
        mock_document_api.create.assert_called_once()
        call_args = mock_document_api.create.call_args
        assert "# From File" in call_args[1]["text"]

    def test_create_without_text_or_file(self, runner, mock_document_api, mock_graph):
        """Test that create fails without text or file."""
        result = runner.invoke(document, ["create"])

        assert result.exit_code != 0
        assert "Either --text or --file must be provided" in result.output

    def test_create_validation_error(self, runner, mock_document_api, mock_graph):
        """Test handling of validation errors."""
        mock_document_api.create.side_effect = DocumentValidationError("Invalid state")

        result = runner.invoke(document, ["create", "--text", "Content"])

        assert result.exit_code == 1
        assert "Validation error" in result.output
        assert "Invalid state" in result.output

    def test_create_operation_error(self, runner, mock_document_api, mock_graph):
        """Test handling of operation errors."""
        mock_document_api.create.side_effect = DocumentOperationError("GraphQL failed")

        result = runner.invoke(document, ["create", "--text", "Content"])

        assert result.exit_code == 1
        assert "Operation failed" in result.output
        assert "GraphQL failed" in result.output


# Test Document Update Command


class TestDocumentUpdateCLI:
    def test_update_text(self, runner, mock_document_api, mock_graph):
        """Test updating document text."""
        mock_document_api.update.return_value = True

        result = runner.invoke(
            document,
            ["update", "urn:li:document:test-123", "--text", "Updated content"],
        )

        assert result.exit_code == 0
        assert "Successfully updated document" in result.output
        mock_document_api.update.assert_called_once()

    def test_update_title(self, runner, mock_document_api, mock_graph):
        """Test updating document title."""
        mock_document_api.update.return_value = True

        result = runner.invoke(
            document, ["update", "urn:li:document:test-123", "--title", "New Title"]
        )

        assert result.exit_code == 0
        assert "Successfully updated document" in result.output

    def test_update_from_file(self, runner, mock_document_api, mock_graph):
        """Test updating document from file."""
        mock_document_api.update.return_value = True

        with runner.isolated_filesystem():
            with open("updated.md", "w") as f:
                f.write("# Updated Content")

            result = runner.invoke(
                document, ["update", "urn:li:document:test-123", "--file", "updated.md"]
            )

        assert result.exit_code == 0
        assert "Successfully updated document" in result.output

    def test_update_without_fields(self, runner, mock_document_api, mock_graph):
        """Test that update fails without any fields."""
        result = runner.invoke(document, ["update", "urn:li:document:test-123"])

        assert result.exit_code != 0
        assert "At least one of" in result.output


# Test Document Get Command


class TestDocumentGetCLI:
    def test_get_json_format(self, runner, mock_document_api, mock_graph):
        """Test getting a document in JSON format."""
        mock_document_api.get.return_value = {
            "urn": "urn:li:document:test-123",
            "type": "DOCUMENT",
            "info": {"title": "Test Doc", "contents": {"text": "Content"}},
        }

        result = runner.invoke(
            document, ["get", "urn:li:document:test-123", "--format", "json"]
        )

        assert result.exit_code == 0
        # Should output valid JSON
        output_data = json.loads(result.output)
        assert output_data["urn"] == "urn:li:document:test-123"

    def test_get_text_format(self, runner, mock_document_api, mock_graph):
        """Test getting a document in text format."""
        mock_document_api.get.return_value = {
            "urn": "urn:li:document:test-123",
            "type": "DOCUMENT",
            "subType": "Tutorial",
            "info": {
                "title": "Test Doc",
                "status": {"state": "PUBLISHED"},
                "contents": {"text": "# Content"},
            },
        }

        result = runner.invoke(
            document, ["get", "urn:li:document:test-123", "--format", "text"]
        )

        assert result.exit_code == 0
        assert "URN: urn:li:document:test-123" in result.output
        assert "Title: Test Doc" in result.output
        assert "Sub-Type: Tutorial" in result.output
        assert "Status: PUBLISHED" in result.output
        assert "# Content" in result.output

    def test_get_not_found(self, runner, mock_document_api, mock_graph):
        """Test getting a non-existent document."""
        mock_document_api.get.return_value = None

        result = runner.invoke(document, ["get", "urn:li:document:nonexistent"])

        assert result.exit_code == 1
        assert "Document not found" in result.output


# Test Document Search Command


class TestDocumentSearchCLI:
    def test_search_no_filters(self, runner, mock_document_api, mock_graph):
        """Test searching without filters."""
        mock_document_api.search.return_value = {
            "total": 2,
            "documents": [
                {
                    "urn": "urn:li:document:1",
                    "info": {
                        "title": "Doc 1",
                        "status": {"state": "PUBLISHED"},
                        "contents": {"text": "Content 1"},
                    },
                },
                {
                    "urn": "urn:li:document:2",
                    "info": {
                        "title": "Doc 2",
                        "status": {"state": "PUBLISHED"},
                        "contents": {"text": "Content 2"},
                    },
                },
            ],
        }

        result = runner.invoke(document, ["search"])

        assert result.exit_code == 0
        assert "Found 2 document(s)" in result.output
        assert "Doc 1" in result.output
        assert "Doc 2" in result.output

    def test_search_with_query(self, runner, mock_document_api, mock_graph):
        """Test searching with a query."""
        mock_document_api.search.return_value = {"total": 1, "documents": []}

        result = runner.invoke(document, ["search", "--query", "machine learning"])

        assert result.exit_code == 0
        mock_document_api.search.assert_called_once()
        call_args = mock_document_api.search.call_args
        assert call_args[1]["query"] == "machine learning"

    def test_search_with_filters(self, runner, mock_document_api, mock_graph):
        """Test searching with filters."""
        mock_document_api.search.return_value = {"total": 0, "documents": []}

        result = runner.invoke(
            document,
            ["search", "--type", "Tutorial", "--state", "published", "--count", "20"],
        )

        assert result.exit_code == 0
        mock_document_api.search.assert_called_once()
        call_args = mock_document_api.search.call_args
        assert call_args[1]["types"] == ["Tutorial"]
        assert call_args[1]["states"] == ["PUBLISHED"]
        assert call_args[1]["count"] == 20

    def test_search_json_format(self, runner, mock_document_api, mock_graph):
        """Test search with JSON output."""
        mock_document_api.search.return_value = {
            "total": 1,
            "documents": [{"urn": "urn:li:document:1"}],
        }

        result = runner.invoke(document, ["search", "--format", "json"])

        assert result.exit_code == 0
        output_data = json.loads(result.output)
        assert output_data["total"] == 1


# Test Document List Command


class TestDocumentListCLI:
    def test_list_default(self, runner, mock_document_api, mock_graph):
        """Test listing documents with defaults."""
        mock_document_api.search.return_value = {"total": 5, "documents": []}

        result = runner.invoke(document, ["list"])

        assert result.exit_code == 0
        assert "Found 5 document(s)" in result.output
        mock_document_api.search.assert_called_once()

    def test_list_with_state(self, runner, mock_document_api, mock_graph):
        """Test listing with state filter."""
        mock_document_api.search.return_value = {"total": 3, "documents": []}

        result = runner.invoke(document, ["list", "--state", "published"])

        assert result.exit_code == 0
        call_args = mock_document_api.search.call_args
        assert call_args[1]["states"] == ["PUBLISHED"]


# Test Document Publish/Unpublish Commands


class TestDocumentPublishCLI:
    def test_publish(self, runner, mock_document_api, mock_graph):
        """Test publishing a document."""
        mock_document_api.publish.return_value = True

        result = runner.invoke(document, ["publish", "urn:li:document:test-123"])

        assert result.exit_code == 0
        assert "Successfully published document" in result.output
        mock_document_api.publish.assert_called_once_with(
            urn="urn:li:document:test-123"
        )

    def test_unpublish(self, runner, mock_document_api, mock_graph):
        """Test unpublishing a document."""
        mock_document_api.unpublish.return_value = True

        result = runner.invoke(document, ["unpublish", "urn:li:document:test-123"])

        assert result.exit_code == 0
        assert "Successfully unpublished document" in result.output
        mock_document_api.unpublish.assert_called_once_with(
            urn="urn:li:document:test-123"
        )


# Test Document Delete Command


class TestDocumentDeleteCLI:
    def test_delete_with_force(self, runner, mock_document_api, mock_graph):
        """Test deleting with --force flag."""
        mock_document_api.delete.return_value = True

        result = runner.invoke(
            document, ["delete", "urn:li:document:test-123", "--force"]
        )

        assert result.exit_code == 0
        assert "Successfully deleted document" in result.output
        mock_document_api.delete.assert_called_once()

    def test_delete_with_confirmation(self, runner, mock_document_api, mock_graph):
        """Test deleting with confirmation prompt."""
        mock_document_api.delete.return_value = True

        # Confirm deletion
        result = runner.invoke(
            document, ["delete", "urn:li:document:test-123"], input="y\n"
        )

        assert result.exit_code == 0
        assert "Successfully deleted document" in result.output

    def test_delete_cancelled(self, runner, mock_document_api, mock_graph):
        """Test cancelling deletion."""
        # Cancel deletion
        result = runner.invoke(
            document, ["delete", "urn:li:document:test-123"], input="n\n"
        )

        assert result.exit_code == 0
        assert "Cancelled" in result.output
        mock_document_api.delete.assert_not_called()


# Test Error Handling


class TestCLIErrorHandling:
    def test_graph_initialization_error(self, runner, mock_graph):
        """Test handling of graph initialization errors."""
        mock_graph.side_effect = Exception("Connection failed")

        result = runner.invoke(document, ["create", "--text", "Content"])

        assert result.exit_code != 0
        assert "Failed to initialize DataHub client" in result.output

    def test_validation_error_in_cli(self, runner, mock_document_api, mock_graph):
        """Test that validation errors are properly displayed."""
        mock_document_api.create.side_effect = DocumentValidationError(
            "Empty text not allowed"
        )

        result = runner.invoke(document, ["create", "--text", "Content"])

        assert result.exit_code == 1
        assert "Validation error" in result.output
        assert "Empty text not allowed" in result.output
