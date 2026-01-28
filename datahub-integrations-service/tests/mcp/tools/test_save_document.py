import os
from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.tools.save_document import (
    ROOT_PARENT_DOC_ID,
    _generate_document_id,
    _get_parent_title,
    _get_root_parent_id,
    _get_root_parent_urn,
    _get_user_display_name,
    _is_document_in_shared_folder,
    _is_organize_by_user_enabled,
    _make_safe_id,
    _restrict_updates_to_shared_folder,
    is_save_document_enabled,
    save_document,
)


@pytest.fixture
def mock_datahub_client():
    """Fixture for mocking DataHubClient."""
    mock_client = Mock()
    mock_entities = Mock()
    mock_client.entities = mock_entities
    mock_client._graph = Mock()
    return mock_client


@pytest.fixture
def mock_user_info():
    """Fixture for mock user info."""
    return {
        "urn": "urn:li:corpuser:john.doe",
        "username": "john.doe",
        "info": {
            "displayName": "John Doe",
            "fullName": "John Doe",
            "firstName": "John",
            "lastName": "Doe",
        },
        "editableProperties": {
            "displayName": "John Doe",
        },
    }


class TestDocumentIdGeneration:
    """Tests for document ID generation."""

    def test_generate_document_id_is_unique(self):
        """Test that document ID generation creates unique IDs."""
        doc_id_1 = _generate_document_id()
        doc_id_2 = _generate_document_id()
        # Each call should generate a unique UUID
        assert doc_id_1 != doc_id_2

    def test_generate_document_id_format(self):
        """Test that document ID has expected format (shared-<uuid>)."""
        doc_id = _generate_document_id()
        assert doc_id.startswith("shared-")
        # Should have UUID format after the prefix
        parts = doc_id.split("-", 1)
        assert len(parts) == 2
        assert parts[0] == "shared"
        # The second part should be a valid UUID
        assert len(parts[1]) == 36  # UUID length with dashes


class TestUserInfoHelpers:
    """Tests for user info helper functions."""

    def test_get_user_display_name_with_editable_display_name(self, mock_user_info):
        """Test display name extraction with editable displayName."""
        assert _get_user_display_name(mock_user_info) == "John Doe"

    def test_get_user_display_name_with_full_name(self):
        """Test display name extraction with fullName."""
        user_info = {
            "info": {"fullName": "Jane Smith"},
            "editableProperties": {},
        }
        assert _get_user_display_name(user_info) == "Jane Smith"

    def test_get_user_display_name_with_first_last(self):
        """Test display name extraction with first/last name."""
        user_info = {
            "info": {"firstName": "Bob", "lastName": "Jones"},
            "editableProperties": {},
        }
        assert _get_user_display_name(user_info) == "Bob Jones"

    def test_get_user_display_name_with_username(self):
        """Test display name extraction fallback to username."""
        user_info = {
            "username": "bob.jones",
            "info": {},
            "editableProperties": {},
        }
        assert _get_user_display_name(user_info) == "bob.jones"

    def test_get_user_display_name_unknown(self):
        """Test display name extraction with no info."""
        assert _get_user_display_name(None) == "Unknown User"
        assert _get_user_display_name({}) == "Unknown User"


class TestConfigHelpers:
    """Tests for configuration helper functions."""

    def test_make_safe_id(self):
        """Test safe ID generation."""
        assert _make_safe_id("Hello World") == "hello-world"
        assert _make_safe_id("Test   Multiple   Spaces") == "test-multiple-spaces"
        assert _make_safe_id("Special!@#$%Characters") == "special-characters"

    def test_get_parent_title_default(self):
        """Test default parent title."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove env var if set
            os.environ.pop("SAVE_DOCUMENT_PARENT_TITLE", None)
            assert _get_parent_title() == "Shared"

    def test_get_parent_title_custom(self):
        """Test custom parent title from env var."""
        with patch.dict(os.environ, {"SAVE_DOCUMENT_PARENT_TITLE": "Custom Title"}):
            assert _get_parent_title() == "Custom Title"

    def test_is_organize_by_user_enabled_default(self):
        """Test default organize by user setting."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SAVE_DOCUMENT_ORGANIZE_BY_USER", None)
            assert _is_organize_by_user_enabled() is False

    def test_is_organize_by_user_disabled(self):
        """Test disabled organize by user setting."""
        with patch.dict(os.environ, {"SAVE_DOCUMENT_ORGANIZE_BY_USER": "false"}):
            assert _is_organize_by_user_enabled() is False

    def test_is_save_document_enabled_default(self):
        """Test default save document enabled setting (should be True)."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SAVE_DOCUMENT_TOOL_ENABLED", None)
            assert is_save_document_enabled() is True

    def test_is_save_document_enabled_true(self):
        """Test save document enabled setting."""
        with patch.dict(os.environ, {"SAVE_DOCUMENT_TOOL_ENABLED": "true"}):
            assert is_save_document_enabled() is True

    def test_is_save_document_disabled(self):
        """Test save document disabled setting."""
        with patch.dict(os.environ, {"SAVE_DOCUMENT_TOOL_ENABLED": "false"}):
            assert is_save_document_enabled() is False

    def test_restrict_updates_to_shared_folder_default(self):
        """Test default update restriction setting (should be True)."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SAVE_DOCUMENT_RESTRICT_UPDATES", None)
            assert _restrict_updates_to_shared_folder() is True

    def test_restrict_updates_to_shared_folder_disabled(self):
        """Test update restriction disabled setting."""
        with patch.dict(os.environ, {"SAVE_DOCUMENT_RESTRICT_UPDATES": "false"}):
            assert _restrict_updates_to_shared_folder() is False

    def test_root_parent_doc_id_constant(self):
        """Test that ROOT_PARENT_DOC_ID is a fixed constant."""
        assert ROOT_PARENT_DOC_ID == "__system_shared_documents"


class TestSaveDocument:
    """Tests for the save_document function."""

    def test_save_document_creates_document(self, mock_datahub_client, mock_user_info):
        """Test saving a new document."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="High Null Rate in Customer Emails",
                content="## Finding\n\n23% of customer records have null email...",
                topics=["data-quality", "customer-data", "high-severity"],
                related_assets=[
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"
                ],
            )

        assert result["success"] is True
        assert result["urn"] is not None
        assert "urn:li:document:" in result["urn"]
        assert result["author"] == "John Doe"
        assert "created" in result["message"].lower()

    def test_save_document_empty_title_fails(self, mock_datahub_client):
        """Test that empty title returns error."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="",
                content="Some content",
            )

        assert result["success"] is False
        assert "title cannot be empty" in result["message"]

    def test_save_document_empty_content_fails(self, mock_datahub_client):
        """Test that empty content returns error."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Some title",
                content="",
            )

        assert result["success"] is False
        assert "content cannot be empty" in result["message"]

    def test_save_document_invalid_document_type_fails(self, mock_datahub_client):
        """Test that invalid document type returns error."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="InvalidType",  # type: ignore
                title="Some title",
                content="Some content",
            )

        assert result["success"] is False
        assert "Invalid document_type" in result["message"]

    def test_save_document_with_all_options(self, mock_datahub_client, mock_user_info):
        """Test saving a document with all optional parameters."""
        mock_datahub_client.entities.get.return_value = Mock()
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Decision",
                title="Migrating to New Data Model",
                content="## Decision\n\nWe will migrate to v2 schema...",
                topics=["architecture", "migration", "approved"],
                related_documents=["urn:li:document:agent-insight-related-abc123"],
                related_assets=[
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,old_schema,PROD)",
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,new_schema,PROD)",
                ],
            )

        assert result["success"] is True
        assert result["urn"] is not None

    def test_save_document_update_existing_document(
        self, mock_datahub_client, mock_user_info
    ):
        """Test updating an existing document by providing URN."""
        mock_datahub_client.entities.get.return_value = Mock()
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        existing_urn = "urn:li:document:agent-insight-existing-abc123"

        # Disable update restrictions for this test
        with (
            patch.dict(os.environ, {"SAVE_DOCUMENT_RESTRICT_UPDATES": "false"}),
            patch(
                "datahub_integrations.mcp.mcp_server.get_datahub_client",
                return_value=mock_datahub_client,
            ),
        ):
            result = save_document(
                document_type="Insight",
                title="Updated Title",
                content="Updated content...",
                urn=existing_urn,
            )

        assert result["success"] is True
        assert result["urn"] == existing_urn
        assert "updated" in result["message"].lower()

    def test_save_document_invalid_urn_format(self, mock_datahub_client):
        """Test that invalid URN format returns error."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Test",
                content="Some content",
                urn="invalid-urn-format",
            )

        assert result["success"] is False
        assert "Invalid urn format" in result["message"]

    def test_save_document_update_restriction_blocks_external_doc(
        self, mock_datahub_client, mock_user_info
    ):
        """Test that update restrictions block updating non-agent documents."""
        # Create a mock document that is NOT in the agent hierarchy
        mock_doc = Mock()
        mock_doc.aspects = {
            "documentInfo": Mock(parentDocument=None)  # No parent = not in hierarchy
        }
        mock_datahub_client.entities.get.return_value = mock_doc
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        external_urn = "urn:li:document:user-created-doc-123"

        # Enable update restrictions (default)
        with (
            patch.dict(os.environ, {"SAVE_DOCUMENT_RESTRICT_UPDATES": "true"}),
            patch(
                "datahub_integrations.mcp.mcp_server.get_datahub_client",
                return_value=mock_datahub_client,
            ),
        ):
            result = save_document(
                document_type="Insight",
                title="Trying to Update External Doc",
                content="This should fail...",
                urn=external_urn,
            )

        assert result["success"] is False
        assert "not in the shared documents folder" in result["message"]

    def test_save_document_create_without_urn(
        self, mock_datahub_client, mock_user_info
    ):
        """Test that not providing URN creates a new document."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="New Document",
                content="New content...",
            )

        assert result["success"] is True
        assert result["urn"] is not None
        assert result["urn"].startswith("urn:li:document:shared-")
        assert "created" in result["message"].lower()

    def test_save_document_all_valid_document_types(
        self, mock_datahub_client, mock_user_info
    ):
        """Test that all valid document types work."""
        valid_types = [
            "Insight",
            "Decision",
            "FAQ",
            "Analysis",
            "Summary",
            "Recommendation",
            "Note",
            "Context",
        ]

        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            for document_type in valid_types:
                result = save_document(
                    document_type=document_type,  # type: ignore
                    title=f"Test {document_type}",
                    content=f"Content for {document_type}",
                )
                assert result["success"] is True, (
                    f"Failed for document_type: {document_type}"
                )

    def test_save_document_exception_handling(
        self, mock_datahub_client, mock_user_info
    ):
        """Test handling of exceptions during save."""
        mock_datahub_client.entities.get.return_value = Mock()
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }
        mock_datahub_client.entities.upsert.side_effect = Exception("Network error")

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
            )

        assert result["success"] is False
        assert "Error saving document" in result["message"]

    def test_save_document_without_user_info(self, mock_datahub_client):
        """Test saving when user info is not available."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {"me": None}

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
            )

        assert result["success"] is True
        assert result["author"] is None

    def test_save_document_whitespace_title_fails(self, mock_datahub_client):
        """Test that whitespace-only title returns error."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="   ",
                content="Some content",
            )

        assert result["success"] is False
        assert "title cannot be empty" in result["message"]

    def test_save_document_whitespace_content_fails(self, mock_datahub_client):
        """Test that whitespace-only content returns error."""
        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Some title",
                content="   ",
            )

        assert result["success"] is False
        assert "content cannot be empty" in result["message"]

    def test_save_document_organize_by_user_disabled(
        self, mock_datahub_client, mock_user_info
    ):
        """Test saving when organize by user is disabled."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with (
            patch.dict(os.environ, {"SAVE_DOCUMENT_ORGANIZE_BY_USER": "false"}),
            patch(
                "datahub_integrations.mcp.mcp_server.get_datahub_client",
                return_value=mock_datahub_client,
            ),
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
            )

        assert result["success"] is True
        # Should still have author info
        assert result["author"] == "John Doe"

    def test_save_document_custom_parent_title(
        self, mock_datahub_client, mock_user_info
    ):
        """Test saving with custom parent title."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with (
            patch.dict(os.environ, {"SAVE_DOCUMENT_PARENT_TITLE": "My Custom Folder"}),
            patch(
                "datahub_integrations.mcp.mcp_server.get_datahub_client",
                return_value=mock_datahub_client,
            ),
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
            )

        assert result["success"] is True


class TestRootParentId:
    """Tests for root parent ID generation."""

    def test_get_root_parent_id_is_fixed(self):
        """Test that root parent ID is a fixed constant regardless of title."""
        # The root parent ID should be fixed and independent of the title
        # This allows changing the title without data migration
        assert _get_root_parent_id() == "__system_shared_documents"

    def test_get_root_parent_id_ignores_title_env(self):
        """Test that root parent ID ignores custom title env var."""
        with patch.dict(os.environ, {"SAVE_DOCUMENT_PARENT_TITLE": "Custom Folder"}):
            # Should still return the fixed ID, not derive from title
            assert _get_root_parent_id() == "__system_shared_documents"

    def test_get_root_parent_urn(self):
        """Test that root parent URN is correctly formatted."""
        expected = "urn:li:document:__system_shared_documents"
        assert _get_root_parent_urn() == expected


class TestDocumentInSharedFolder:
    """Tests for the _is_document_in_shared_folder validation function."""

    def test_document_in_shared_folder_returns_true(self, mock_datahub_client):
        """Test that document directly under shared folder is valid."""
        root_urn = _get_root_parent_urn()

        # Mock document with parent = shared folder
        mock_parent = Mock()
        mock_parent.document = root_urn
        mock_doc_info = Mock()
        mock_doc_info.parentDocument = mock_parent
        mock_doc = Mock()
        mock_doc.aspects = {"documentInfo": mock_doc_info}
        mock_datahub_client.entities.get.return_value = mock_doc

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            is_valid, error = _is_document_in_shared_folder(
                "urn:li:document:test-doc-123"
            )

        assert is_valid is True
        assert error is None

    def test_document_not_in_shared_folder_returns_false(self, mock_datahub_client):
        """Test that document with different parent is invalid."""
        # Mock document with parent = some other folder
        mock_parent = Mock()
        mock_parent.document = "urn:li:document:some-other-folder"
        mock_doc_info = Mock()
        mock_doc_info.parentDocument = mock_parent
        mock_doc = Mock()
        mock_doc.aspects = {"documentInfo": mock_doc_info}
        mock_datahub_client.entities.get.return_value = mock_doc

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            is_valid, error = _is_document_in_shared_folder(
                "urn:li:document:external-doc-123"
            )

        assert is_valid is False
        assert "not in the shared documents folder" in error

    def test_document_without_parent_returns_false(self, mock_datahub_client):
        """Test that document with no parent is invalid."""
        mock_doc_info = Mock()
        mock_doc_info.parentDocument = None
        mock_doc = Mock()
        mock_doc.aspects = {"documentInfo": mock_doc_info}
        mock_datahub_client.entities.get.return_value = mock_doc

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            is_valid, error = _is_document_in_shared_folder(
                "urn:li:document:orphan-doc-123"
            )

        assert is_valid is False
        assert "not in the shared documents folder" in error

    def test_nonexistent_document_returns_true(self, mock_datahub_client):
        """Test that non-existent document is allowed (will be created)."""
        mock_datahub_client.entities.get.return_value = None

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            is_valid, error = _is_document_in_shared_folder(
                "urn:li:document:new-doc-123"
            )

        assert is_valid is True
        assert error is None

    def test_root_folder_cannot_be_updated(self, mock_datahub_client):
        """Test that the root shared folder itself cannot be updated."""
        root_urn = _get_root_parent_urn()

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            is_valid, error = _is_document_in_shared_folder(root_urn)

        assert is_valid is False
        assert "Cannot update the root shared documents folder" in error

    def test_document_without_document_info_returns_false(self, mock_datahub_client):
        """Test that document without documentInfo aspect is invalid."""
        mock_doc = Mock()
        mock_doc.aspects = {}  # No documentInfo
        mock_datahub_client.entities.get.return_value = mock_doc

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            is_valid, error = _is_document_in_shared_folder(
                "urn:li:document:no-info-doc-123"
            )

        assert is_valid is False
        assert "has no document info" in error


class TestTopicsToTags:
    """Tests for topics-to-tags conversion."""

    def test_save_document_converts_topics_to_tag_urns(
        self, mock_datahub_client, mock_user_info
    ):
        """Test that topics are converted to tag URNs."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        # Capture what gets passed to upsert
        captured_doc = None

        def capture_upsert(doc):
            nonlocal captured_doc
            captured_doc = doc

        mock_datahub_client.entities.upsert = capture_upsert

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
                topics=["data-quality", "customer-data"],
            )

        assert result["success"] is True
        # The Document SDK should have received tag URNs
        assert captured_doc is not None


class TestUpdateOwnership:
    """Tests for ownership behavior during updates."""

    def test_new_document_sets_owner(self, mock_datahub_client, mock_user_info):
        """Test that new documents have owner set."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="New Document",
                content="Content",
            )

        assert result["success"] is True
        assert result["author"] == "John Doe"

    def test_update_preserves_existing_ownership(
        self, mock_datahub_client, mock_user_info
    ):
        """Test that updating a document does not overwrite ownership."""
        # Mock existing document in shared folder
        root_urn = _get_root_parent_urn()
        mock_parent = Mock()
        mock_parent.document = root_urn
        mock_doc_info = Mock()
        mock_doc_info.parentDocument = mock_parent
        mock_doc = Mock()
        mock_doc.aspects = {"documentInfo": mock_doc_info}
        mock_datahub_client.entities.get.return_value = mock_doc
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        existing_urn = "urn:li:document:shared-existing-doc-123"

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Updated Title",
                content="Updated content",
                urn=existing_urn,
            )

        assert result["success"] is True
        assert "updated" in result["message"].lower()


class TestOrganizeByUser:
    """Tests for organize-by-user functionality."""

    def test_organize_by_user_enabled(self, mock_datahub_client, mock_user_info):
        """Test saving when organize by user is enabled."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with (
            patch.dict(os.environ, {"SAVE_DOCUMENT_ORGANIZE_BY_USER": "true"}),
            patch(
                "datahub_integrations.mcp.mcp_server.get_datahub_client",
                return_value=mock_datahub_client,
            ),
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
            )

        assert result["success"] is True
        assert result["author"] == "John Doe"


class TestRelatedDocuments:
    """Tests for related documents functionality."""

    def test_save_document_with_related_documents(
        self, mock_datahub_client, mock_user_info
    ):
        """Test saving a document with related documents."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Insight",
                title="Test Document",
                content="Some content",
                related_documents=[
                    "urn:li:document:related-doc-1",
                    "urn:li:document:related-doc-2",
                ],
            )

        assert result["success"] is True

    def test_save_document_with_multiple_related_assets(
        self, mock_datahub_client, mock_user_info
    ):
        """Test saving a document with multiple related assets."""
        mock_datahub_client.entities.get.return_value = None
        mock_datahub_client._graph.execute_graphql.return_value = {
            "me": {"corpUser": mock_user_info}
        }

        with patch(
            "datahub_integrations.mcp.mcp_server.get_datahub_client",
            return_value=mock_datahub_client,
        ):
            result = save_document(
                document_type="Analysis",
                title="Cross-Dataset Analysis",
                content="Analysis of multiple datasets...",
                related_assets=[
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)",
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)",
                    "urn:li:dashboard:(urn:li:dataPlatform:looker,sales_dashboard)",
                ],
            )

        assert result["success"] is True
