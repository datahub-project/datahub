"""Unit tests for the Document SDK.

These tests verify the functionality described in the Document SDK tutorial
and example scripts. They cover:
- Native and external document creation
- Document visibility (global context settings)
- Document lifecycle (publish/unpublish)
- Related assets and documents
- All property getters and setters
"""

import pytest

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.metadata.urns import CorpUserUrn, DocumentUrn
from datahub.sdk.document import Document


class TestDocumentCreation:
    """Tests for document creation methods."""

    def test_create_document_minimal(self):
        """Test creating a native document with minimal required fields."""
        doc = Document.create_document(
            id="test-doc-1",
            title="Test Document",
            text="This is test content.",
        )

        assert isinstance(doc, Document)
        assert doc.id == "test-doc-1"
        assert doc.title == "Test Document"
        assert doc.text == "This is test content."
        # Native docs default to PUBLISHED
        assert doc.status == models.DocumentStateClass.PUBLISHED
        assert doc.is_native
        assert not doc.is_external
        assert doc.source_type == models.DocumentSourceTypeClass.NATIVE

    def test_create_document_unpublished(self):
        """Test creating a document with unpublished status."""
        doc = Document.create_document(
            id="unpublished-doc",
            title="Unpublished Document",
            text="Unpublished content",
            status="UNPUBLISHED",
        )

        assert doc.status == models.DocumentStateClass.UNPUBLISHED

    def test_create_document_with_all_fields(self):
        """Test creating a native document with all optional fields."""
        doc = Document.create_document(
            id="test-doc-full",
            title="Full Document",
            text="Complete document content.",
            status=models.DocumentStateClass.PUBLISHED,
            custom_properties={"file_type": "pdf", "page_count": "10"},
            parent_document="urn:li:document:parent-doc",
            related_assets=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
            ],
            related_documents=["urn:li:document:related-doc"],
            show_in_global_context=True,
            subtype="Tutorial",
            tags=["urn:li:tag:Finance"],
            owners=[CorpUserUrn("jdoe")],
            domain="urn:li:domain:engineering",
        )

        assert doc.id == "test-doc-full"
        assert doc.title == "Full Document"
        assert doc.text == "Complete document content."
        assert doc.status == models.DocumentStateClass.PUBLISHED
        assert doc.custom_properties == {"file_type": "pdf", "page_count": "10"}
        assert doc.parent_document == "urn:li:document:parent-doc"
        assert doc.related_assets == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        ]
        assert doc.related_documents == ["urn:li:document:related-doc"]
        assert doc.show_in_global_context is True
        assert doc.subtype == "Tutorial"

    def test_create_external_document(self):
        """Test creating an external document."""
        doc = Document.create_external_document(
            id="notion-doc-123",
            title="Notion Document",
            platform="urn:li:dataPlatform:notion",
            external_url="https://notion.so/team/handbook",
            external_id="notion-page-abc123",
            text="Content synced from Notion",
        )

        assert doc.id == "notion-doc-123"
        assert doc.title == "Notion Document"
        assert doc.text == "Content synced from Notion"
        assert doc.is_external
        assert not doc.is_native
        assert doc.source_type == models.DocumentSourceTypeClass.EXTERNAL
        assert doc.external_url == "https://notion.so/team/handbook"
        assert doc.external_id == "notion-page-abc123"
        # External docs default to PUBLISHED
        assert doc.status == models.DocumentStateClass.PUBLISHED

    def test_create_external_document_platform_shorthand(self):
        """Test creating an external document with platform shorthand."""
        doc = Document.create_external_document(
            id="confluence-doc",
            title="Confluence Doc",
            platform="confluence",  # Shorthand, not full URN
            external_url="https://company.atlassian.net/wiki/123",
        )

        assert doc.is_external
        assert doc.external_url == "https://company.atlassian.net/wiki/123"

    def test_create_external_document_without_text(self):
        """Test creating an external document without text (optional)."""
        doc = Document.create_external_document(
            id="external-no-text",
            title="External Doc",
            platform="urn:li:dataPlatform:notion",
            external_url="https://notion.so/page",
        )

        assert doc.text == ""  # Empty string when not provided


class TestDocumentVisibility:
    """Tests for document visibility (global context) settings."""

    def test_show_in_global_context_default(self):
        """Test that documents show in global context by default."""
        doc = Document.create_document(
            id="visible-doc",
            title="Visible Document",
            text="Content",
        )
        # Default is True, but no settings aspect is created unless explicitly False
        assert doc.show_in_global_context is True

    def test_hide_from_global_context_on_create(self):
        """Test creating a document hidden from global context."""
        doc = Document.create_document(
            id="hidden-doc",
            title="Hidden Document",
            text="Content",
            show_in_global_context=False,
        )
        assert doc.show_in_global_context is False

    def test_hide_from_global_context_method(self):
        """Test hiding document from global context via method."""
        doc = Document.create_document(
            id="doc-to-hide",
            title="Document",
            text="Content",
        )
        assert doc.show_in_global_context is True

        doc.hide_from_global_context()
        assert doc.show_in_global_context is False

    def test_show_in_global_search_method(self):
        """Test showing document in global context via method."""
        doc = Document.create_document(
            id="doc-to-show",
            title="Document",
            text="Content",
            show_in_global_context=False,
        )
        assert doc.show_in_global_context is False

        doc.show_in_global_search()
        assert doc.show_in_global_context is True

    def test_set_show_in_global_context(self):
        """Test setting global context visibility explicitly."""
        doc = Document.create_document(
            id="toggle-doc",
            title="Document",
            text="Content",
        )

        doc.set_show_in_global_context(False)
        assert doc.show_in_global_context is False

        doc.set_show_in_global_context(True)
        assert doc.show_in_global_context is True


class TestDocumentLifecycle:
    """Tests for document lifecycle (publish/unpublish)."""

    def test_publish(self):
        """Test publishing a document."""
        doc = Document.create_document(
            id="unpublished-doc",
            title="Unpublished",
            text="Content",
            status="UNPUBLISHED",
        )
        assert doc.status == models.DocumentStateClass.UNPUBLISHED

        doc.publish()
        assert doc.status == models.DocumentStateClass.PUBLISHED

    def test_unpublish(self):
        """Test unpublishing a document."""
        doc = Document.create_document(
            id="published-doc",
            title="Published",
            text="Content",
            status="PUBLISHED",
        )
        assert doc.status == models.DocumentStateClass.PUBLISHED

        doc.unpublish()
        assert doc.status == models.DocumentStateClass.UNPUBLISHED

    def test_set_status(self):
        """Test setting status explicitly."""
        doc = Document.create_document(
            id="status-doc",
            title="Document",
            text="Content",
        )

        doc.set_status(models.DocumentStateClass.UNPUBLISHED)
        assert doc.status == models.DocumentStateClass.UNPUBLISHED

        doc.set_status(models.DocumentStateClass.PUBLISHED)
        assert doc.status == models.DocumentStateClass.PUBLISHED


class TestDocumentRelationships:
    """Tests for document relationships (related assets, documents, parent)."""

    def test_related_assets(self):
        """Test managing related assets."""
        doc = Document.create_document(
            id="asset-doc",
            title="Document",
            text="Content",
        )

        # Initially no related assets
        assert doc.related_assets is None

        # Add related asset
        doc.add_related_asset(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        assert doc.related_assets == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        ]

        # Add another asset
        doc.add_related_asset("urn:li:dashboard:(looker,dashboard1)")
        assets = doc.related_assets
        assert assets is not None
        assert len(assets) == 2

        # Adding duplicate should not duplicate
        doc.add_related_asset(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        assets = doc.related_assets
        assert assets is not None
        assert len(assets) == 2

        # Remove asset
        doc.remove_related_asset("urn:li:dashboard:(looker,dashboard1)")
        assets = doc.related_assets
        assert assets is not None
        assert len(assets) == 1

    def test_set_related_assets(self):
        """Test setting related assets (replaces existing)."""
        doc = Document.create_document(
            id="set-assets-doc",
            title="Document",
            text="Content",
            related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,old,PROD)"],
        )

        doc.set_related_assets(
            [
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,new1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,new2,PROD)",
            ]
        )
        related_assets = doc.related_assets
        assert related_assets is not None
        assert len(related_assets) == 2
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,old,PROD)"
            not in related_assets
        )

    def test_related_documents(self):
        """Test managing related documents."""
        doc = Document.create_document(
            id="related-doc",
            title="Document",
            text="Content",
        )

        # Initially no related documents
        assert doc.related_documents is None

        # Add related document
        doc.add_related_document("urn:li:document:other-doc")
        assert doc.related_documents == ["urn:li:document:other-doc"]

        # Remove related document
        doc.remove_related_document("urn:li:document:other-doc")
        # After removing all, list is empty or None
        assert doc.related_documents == [] or doc.related_documents is None

    def test_parent_document(self):
        """Test parent document relationships."""
        doc = Document.create_document(
            id="child-doc",
            title="Child Document",
            text="Content",
        )

        # Initially no parent
        assert doc.parent_document is None

        # Set parent
        doc.set_parent_document("urn:li:document:parent-doc")
        assert doc.parent_document == "urn:li:document:parent-doc"

        # Clear parent
        doc.set_parent_document(None)
        assert doc.parent_document is None


class TestDocumentProperties:
    """Tests for document property getters and setters."""

    def test_urn(self):
        """Test document URN creation and handling."""
        doc = Document.create_document(
            id="urn-test-doc",
            title="URN Test",
            text="Content",
        )

        assert isinstance(doc.urn, DocumentUrn)
        assert str(doc.urn) == "urn:li:document:urn-test-doc"
        assert doc.id == "urn-test-doc"

    def test_set_title(self):
        """Test updating document title."""
        doc = Document.create_document(
            id="title-doc",
            title="Original Title",
            text="Content",
        )

        result = doc.set_title("Updated Title")
        assert doc.title == "Updated Title"
        # Should return self for chaining
        assert result is doc

    def test_set_text(self):
        """Test updating document text."""
        doc = Document.create_document(
            id="text-doc",
            title="Title",
            text="Original content",
        )

        result = doc.set_text("Updated content")
        assert doc.text == "Updated content"
        assert result is doc

    def test_custom_properties(self):
        """Test custom properties management."""
        doc = Document.create_document(
            id="props-doc",
            title="Props Test",
            text="Content",
        )

        # Initially empty
        assert doc.custom_properties == {}

        # Set single property
        doc.set_custom_property("key1", "value1")
        assert doc.custom_properties == {"key1": "value1"}

        # Set multiple properties
        doc.set_custom_properties({"key2": "value2", "key3": "value3"})
        assert doc.custom_properties == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }

    def test_set_source(self):
        """Test setting source information."""
        doc = Document.create_document(
            id="source-doc",
            title="Source Test",
            text="Content",
        )

        # Initially native
        assert doc.source_type == models.DocumentSourceTypeClass.NATIVE

        # Set to external
        doc.set_source(
            source_type=models.DocumentSourceTypeClass.EXTERNAL,
            external_url="https://example.com/doc",
            external_id="ext-123",
        )
        assert doc.source_type == models.DocumentSourceTypeClass.EXTERNAL
        assert doc.external_url == "https://example.com/doc"
        assert doc.external_id == "ext-123"


class TestDocumentMethodChaining:
    """Tests for method chaining support."""

    def test_method_chaining(self):
        """Test that setter methods return self for chaining."""
        doc = Document.create_document(
            id="chain-doc",
            title="Original",
            text="Original",
        )

        # Chain multiple operations
        result = (
            doc.set_title("Updated Title")
            .set_text("Updated content")
            .set_custom_property("key", "value")
            .publish()
            .add_related_asset(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
            )
        )

        # Should return the same document
        assert result is doc
        assert doc.title == "Updated Title"
        assert doc.text == "Updated content"
        assert doc.custom_properties == {"key": "value"}
        assert doc.status == models.DocumentStateClass.PUBLISHED
        assets = doc.related_assets
        assert assets is not None
        assert len(assets) == 1


class TestDocumentAspects:
    """Tests for aspect generation and handling."""

    def test_document_info_aspect(self):
        """Test that DocumentInfo aspect is properly set."""
        doc = Document.create_document(
            id="aspect-doc",
            title="Aspect Test",
            text="Content",
        )

        doc_info = doc._get_aspect(models.DocumentInfoClass)
        assert isinstance(doc_info, models.DocumentInfoClass)
        assert doc_info.title == "Aspect Test"
        assert doc_info.contents is not None
        assert doc_info.contents.text == "Content"
        assert doc_info.created is not None
        assert doc_info.lastModified is not None

    def test_document_settings_aspect(self):
        """Test that DocumentSettings aspect is created when needed."""
        doc = Document.create_document(
            id="settings-doc",
            title="Settings Test",
            text="Content",
            show_in_global_context=False,
        )

        settings = doc._get_aspect(models.DocumentSettingsClass)
        assert isinstance(settings, models.DocumentSettingsClass)
        assert settings.showInGlobalContext is False

    def test_as_workunits(self):
        """Test converting document to work units."""
        doc = Document.create_document(
            id="workunit-doc",
            title="Work Unit Test",
            text="Content",
        )

        work_units = doc.as_workunits()

        # Should have at least one work unit for DocumentInfo
        assert len(work_units) >= 1

        # First work unit should contain document URN
        first_wu = work_units[0]
        assert "urn:li:document:workunit-doc" in str(first_wu)

    def test_as_mcps(self):
        """Test converting document to MCPs."""
        doc = Document.create_document(
            id="mcp-doc",
            title="MCP Test",
            text="Content",
            tags=["urn:li:tag:TestTag"],
            owners=[CorpUserUrn("testuser")],
        )

        mcps = doc.as_mcps()
        aspect_names = [mcp.aspectName for mcp in mcps]

        # Should have documentInfo aspect
        assert "documentInfo" in aspect_names
        # Should have tags and ownership from mixins
        assert "globalTags" in aspect_names
        assert "ownership" in aspect_names


class TestDocumentErrors:
    """Tests for error handling."""

    def test_ensure_document_info_raises_on_empty(self):
        """Test that _ensure_document_info raises when aspect not set."""
        # Create a bare document without going through create methods
        doc = Document(DocumentUrn("bare-doc"))

        with pytest.raises(ValueError, match="DocumentInfo aspect must be set"):
            doc._ensure_document_info()

    def test_title_on_empty_document(self):
        """Test accessing title on document without info aspect."""
        doc = Document(DocumentUrn("bare-doc"))
        # Should return None rather than raising
        assert doc.title is None

    def test_text_on_empty_document(self):
        """Test accessing text on document without info aspect."""
        doc = Document(DocumentUrn("bare-doc"))
        # Should return None rather than raising
        assert doc.text is None


class TestDocumentSubtype:
    """Tests for document subtype functionality."""

    def test_set_subtype(self):
        """Test setting document subtype."""
        doc = Document.create_document(
            id="subtype-doc",
            title="Document",
            text="Content",
        )

        # Initially no subtype
        assert doc.subtype is None

        # Set subtype
        doc.set_subtype("FAQ")
        assert doc.subtype == "FAQ"

        # Change subtype
        doc.set_subtype("Tutorial")
        assert doc.subtype == "Tutorial"

    def test_subtype_on_create(self):
        """Test setting subtype during creation."""
        doc = Document.create_document(
            id="subtype-create-doc",
            title="Document",
            text="Content",
            subtype="Runbook",
        )
        assert doc.subtype == "Runbook"


class TestDocumentMoveParent:
    """Tests for moving documents between parents."""

    def test_move_document_to_new_parent(self):
        """Test moving a document from one parent to another."""
        doc = Document.create_document(
            id="child-doc",
            title="Child Document",
            text="Content",
            parent_document="urn:li:document:old-parent",
        )
        assert doc.parent_document == "urn:li:document:old-parent"

        # Move to new parent
        doc.set_parent_document("urn:li:document:new-parent")
        assert doc.parent_document == "urn:li:document:new-parent"

    def test_move_document_to_top_level(self):
        """Test making a child document a top-level document."""
        doc = Document.create_document(
            id="nested-doc",
            title="Nested Document",
            text="Content",
            parent_document="urn:li:document:some-parent",
        )
        assert doc.parent_document == "urn:li:document:some-parent"

        # Remove parent (make top-level)
        doc.set_parent_document(None)
        assert doc.parent_document is None


class TestDocumentSetRelatedDocuments:
    """Tests for set_related_documents functionality."""

    def test_set_related_documents_replaces_existing(self):
        """Test that set_related_documents replaces all existing relations."""
        doc = Document.create_document(
            id="set-docs-doc",
            title="Document",
            text="Content",
            related_documents=["urn:li:document:old-doc"],
        )
        assert doc.related_documents == ["urn:li:document:old-doc"]

        # Replace with new list
        doc.set_related_documents(
            [
                "urn:li:document:new-doc-1",
                "urn:li:document:new-doc-2",
            ]
        )
        assert len(doc.related_documents) == 2
        assert "urn:li:document:old-doc" not in doc.related_documents
        assert "urn:li:document:new-doc-1" in doc.related_documents


class TestDocumentTutorialExamples:
    """Tests that verify the examples from the tutorial work correctly."""

    def test_tutorial_create_native_document(self):
        """Test creating a native document as shown in the tutorial."""
        doc = Document.create_document(
            id="getting-started-tutorial",
            title="Getting Started with DataHub",
            text="# Getting Started with DataHub\n\nThis tutorial will help you get started...",
            subtype="Tutorial",
        )

        assert doc.id == "getting-started-tutorial"
        assert doc.title == "Getting Started with DataHub"
        assert doc.status == "PUBLISHED"
        assert doc.subtype == "Tutorial"
        assert doc.is_native

    def test_tutorial_create_external_document(self):
        """Test creating an external document as shown in the tutorial."""
        doc = Document.create_external_document(
            id="notion-engineering-handbook",
            title="Engineering Handbook",
            platform="urn:li:dataPlatform:notion",
            external_url="https://notion.so/team/engineering-handbook",
            external_id="notion-page-abc123",
            text="Summary of the handbook for search...",
        )

        assert doc.is_external
        assert doc.external_url == "https://notion.so/team/engineering-handbook"
        assert doc.external_id == "notion-page-abc123"

    def test_tutorial_hidden_document(self):
        """Test creating a document hidden from global context."""
        doc = Document.create_document(
            id="orders-dataset-context",
            title="Orders Dataset Context",
            text="# Context for AI Agents\n\nThe orders dataset contains daily summaries...",
            show_in_global_context=False,
            related_assets=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
            ],
        )

        assert doc.show_in_global_context is False
        assert doc.related_assets == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        ]

    def test_tutorial_document_with_metadata(self):
        """Test creating a document with full metadata as shown in the tutorial."""
        doc = Document.create_document(
            id="faq-data-quality",
            title="Data Quality FAQ",
            text="# Data Quality FAQ\n\n## Q: How do we measure data quality?\n\nA: We use...",
            subtype="FAQ",
            related_assets=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
            ],
            owners=[CorpUserUrn("john")],
            domain="urn:li:domain:engineering",
            tags=["urn:li:tag:important"],
            custom_properties={"team": "data-platform", "version": "1.0"},
        )

        assert doc.id == "faq-data-quality"
        assert doc.related_assets == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
        ]
        assert doc.custom_properties == {"team": "data-platform", "version": "1.0"}

    def test_tutorial_update_document(self):
        """Test updating a document as shown in the tutorial examples."""
        doc = Document.create_document(
            id="update-test-doc",
            title="Original Title",
            text="Original content",
        )

        # Update content and title
        doc.set_text(
            "# Updated Getting Started Guide\n\nThis is the updated content..."
        )
        doc.set_title("Updated Tutorial Title")

        assert doc.title == "Updated Tutorial Title"
        text = doc.text
        assert text is not None
        assert "Updated Getting Started Guide" in text

    def test_tutorial_publish_unpublish(self):
        """Test publish/unpublish as shown in the tutorial."""
        doc = Document.create_document(
            id="lifecycle-doc",
            title="Lifecycle Test",
            text="Content",
            status="UNPUBLISHED",
        )

        # Publish
        doc.publish()
        assert doc.status == "PUBLISHED"

        # Unpublish
        doc.unpublish()
        assert doc.status == "UNPUBLISHED"

    def test_tutorial_visibility(self):
        """Test visibility control as shown in the tutorial."""
        doc = Document.create_document(
            id="visibility-doc",
            title="Visibility Test",
            text="Content",
        )

        # Hide from global context
        doc.hide_from_global_context()
        assert doc.show_in_global_context is False

        # Show in global context
        doc.show_in_global_search()
        assert doc.show_in_global_context is True

    def test_tutorial_related_entities(self):
        """Test adding related entities as shown in the tutorial."""
        doc = Document.create_document(
            id="relations-doc",
            title="Relations Test",
            text="Content",
        )

        # Add related assets
        doc.add_related_asset(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,users,PROD)"
        )
        doc.add_related_asset(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )

        # Add a related document
        doc.add_related_document("urn:li:document:related-guide")

        related_assets = doc.related_assets
        related_docs = doc.related_documents
        assert related_assets is not None
        assert related_docs is not None
        assert len(related_assets) == 2
        assert len(related_docs) == 1


class TestDocumentUrnValidation:
    """Tests for URN validation in document methods."""

    def test_add_related_document_validates_urn_type(self):
        """Test that add_related_document rejects non-document URNs."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        # Should reject dataset URN
        with pytest.raises(SdkUsageError, match="Expected a document URN"):
            doc.add_related_document(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"
            )

    def test_add_related_document_rejects_invalid_urn(self):
        """Test that add_related_document rejects malformed URNs."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="Invalid URN format"):
            doc.add_related_document("not-a-valid-urn")

    def test_add_related_document_accepts_valid_document_urn(self):
        """Test that add_related_document accepts valid document URNs."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        # Should accept document URN string
        doc.add_related_document("urn:li:document:other-doc")
        assert doc.related_documents == ["urn:li:document:other-doc"]

    def test_add_related_document_accepts_document_urn_object(self):
        """Test that add_related_document accepts DocumentUrn objects."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        # Should accept DocumentUrn object
        doc.add_related_document(DocumentUrn("typed-doc"))
        assert doc.related_documents == ["urn:li:document:typed-doc"]

    def test_set_parent_document_validates_urn_type(self):
        """Test that set_parent_document rejects non-document URNs."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="Expected a document URN"):
            doc.set_parent_document(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"
            )

    def test_set_parent_document_accepts_valid_urn(self):
        """Test that set_parent_document accepts valid document URNs."""
        doc = Document.create_document(
            id="child-doc",
            title="Child",
            text="Content",
        )

        doc.set_parent_document("urn:li:document:parent-doc")
        assert doc.parent_document == "urn:li:document:parent-doc"

    def test_set_parent_document_accepts_none(self):
        """Test that set_parent_document accepts None to clear parent."""
        doc = Document.create_document(
            id="child-doc",
            title="Child",
            text="Content",
            parent_document="urn:li:document:parent",
        )

        doc.set_parent_document(None)
        assert doc.parent_document is None

    def test_add_related_asset_validates_urn_format(self):
        """Test that add_related_asset rejects malformed URNs."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="Invalid URN format"):
            doc.add_related_asset("not-a-valid-urn")

    def test_add_related_asset_accepts_valid_urn(self):
        """Test that add_related_asset accepts valid URNs."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        doc.add_related_asset(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"
        )
        assert doc.related_assets == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"
        ]

    def test_set_related_documents_validates_all_urns(self):
        """Test that set_related_documents validates all URNs in the list."""
        doc = Document.create_document(
            id="validation-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="Expected a document URN"):
            doc.set_related_documents(
                [
                    "urn:li:document:valid-doc",
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)",  # Invalid
                ]
            )


class TestDocumentStatusValidation:
    """Tests for document status validation."""

    def test_set_status_validates_value(self):
        """Test that set_status rejects invalid status values."""
        doc = Document.create_document(
            id="status-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="Invalid document status"):
            doc.set_status("INVALID_STATUS")

    def test_set_status_accepts_published(self):
        """Test that set_status accepts PUBLISHED."""
        doc = Document.create_document(
            id="status-test",
            title="Test",
            text="Content",
            status="UNPUBLISHED",
        )

        doc.set_status("PUBLISHED")
        assert doc.status == "PUBLISHED"

    def test_set_status_accepts_unpublished(self):
        """Test that set_status accepts UNPUBLISHED."""
        doc = Document.create_document(
            id="status-test",
            title="Test",
            text="Content",
        )

        doc.set_status("UNPUBLISHED")
        assert doc.status == "UNPUBLISHED"

    def test_create_document_validates_status(self):
        """Test that create_document validates status parameter."""
        with pytest.raises(SdkUsageError, match="Invalid document status"):
            Document.create_document(
                id="status-test",
                title="Test",
                text="Content",
                status="DRAFT",  # Invalid
            )


class TestDocumentSelfReferenceValidation:
    """Tests for self-referential document validation."""

    def test_set_parent_document_rejects_self_reference(self):
        """Test that a document cannot be its own parent."""
        doc = Document.create_document(
            id="self-ref-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="cannot be its own parent"):
            doc.set_parent_document("urn:li:document:self-ref-test")

    def test_add_related_document_rejects_self_reference(self):
        """Test that a document cannot be related to itself."""
        doc = Document.create_document(
            id="self-ref-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="cannot be related to itself"):
            doc.add_related_document("urn:li:document:self-ref-test")

    def test_set_related_documents_rejects_self_reference(self):
        """Test that set_related_documents rejects self-reference."""
        doc = Document.create_document(
            id="self-ref-test",
            title="Test",
            text="Content",
        )

        with pytest.raises(SdkUsageError, match="cannot be related to itself"):
            doc.set_related_documents(
                [
                    "urn:li:document:other-doc",
                    "urn:li:document:self-ref-test",  # Self-reference
                ]
            )

    def test_create_document_rejects_self_parent(self):
        """Test that create_document rejects self as parent."""
        with pytest.raises(SdkUsageError, match="cannot be its own parent"):
            Document.create_document(
                id="self-parent-test",
                title="Test",
                text="Content",
                parent_document="urn:li:document:self-parent-test",
            )

    def test_create_document_rejects_self_related(self):
        """Test that create_document rejects self in related_documents."""
        with pytest.raises(SdkUsageError, match="cannot be related to itself"):
            Document.create_document(
                id="self-related-test",
                title="Test",
                text="Content",
                related_documents=["urn:li:document:self-related-test"],
            )


class TestDocumentExternalUrlValidation:
    """Tests for external URL validation."""

    def test_create_external_document_validates_url(self):
        """Test that external documents require valid URLs."""
        with pytest.raises(SdkUsageError, match="Invalid URL format"):
            Document.create_external_document(
                id="bad-url-test",
                title="Test",
                platform="notion",
                external_url="not-a-valid-url",
            )

    def test_create_external_document_accepts_valid_url(self):
        """Test that external documents accept valid URLs."""
        doc = Document.create_external_document(
            id="good-url-test",
            title="Test",
            platform="notion",
            external_url="https://notion.so/page/123",
        )
        assert doc.external_url == "https://notion.so/page/123"

    def test_create_external_document_rejects_missing_scheme(self):
        """Test that URLs without scheme are rejected."""
        with pytest.raises(SdkUsageError, match="Invalid URL format"):
            Document.create_external_document(
                id="no-scheme-test",
                title="Test",
                platform="notion",
                external_url="notion.so/page/123",  # Missing https://
            )
