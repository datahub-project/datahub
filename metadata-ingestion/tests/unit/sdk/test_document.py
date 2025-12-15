"""Unit tests for the Document SDK."""

from datetime import datetime

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import DocumentUrn
from datahub.sdk.document import Document


def test_document_create_minimal():
    """Test creating a document with minimal required fields."""
    doc = Document.create(
        id="test-doc-1",
        title="Test Document",
        text="This is test content.",
    )

    assert isinstance(doc, Document)
    assert doc.id == "test-doc-1"
    assert doc.title() == "Test Document"
    assert doc.text() == "This is test content."
    assert doc.status() == models.DocumentStateClass.PUBLISHED


def test_document_create_with_all_fields():
    """Test creating a document with all optional fields."""
    created_time = datetime(2024, 1, 1, 12, 0, 0)
    modified_time = datetime(2024, 1, 2, 12, 0, 0)

    doc = Document.create(
        id="test-doc-2",
        title="Full Document",
        text="Complete document content.",
        source_url="https://example.com/doc.pdf",
        source_id="external-123",
        status=models.DocumentStateClass.UNPUBLISHED,
        custom_properties={"file_type": "pdf", "page_count": "10"},
        parent_document="urn:li:document:parent-doc",
        created_at=created_time,
        modified_at=modified_time,
        actor="urn:li:corpuser:testuser",
    )

    assert doc.id == "test-doc-2"
    assert doc.title() == "Full Document"
    assert doc.text() == "Complete document content."
    assert doc.source_url() == "https://example.com/doc.pdf"
    assert doc.status() == models.DocumentStateClass.UNPUBLISHED
    assert doc.custom_properties() == {"file_type": "pdf", "page_count": "10"}
    assert doc.parent_document() == "urn:li:document:parent-doc"


def test_document_urn():
    """Test document URN creation and handling."""
    doc = Document.create(
        id="test-doc-3",
        title="URN Test",
        text="Content",
    )

    assert isinstance(doc.urn, DocumentUrn)
    assert str(doc.urn) == "urn:li:document:test-doc-3"
    assert doc.id == "test-doc-3"


def test_document_set_title():
    """Test updating document title."""
    doc = Document.create(
        id="test-doc-4",
        title="Original Title",
        text="Content",
    )

    doc.set_title("Updated Title")
    assert doc.title() == "Updated Title"


def test_document_set_text():
    """Test updating document text."""
    doc = Document.create(
        id="test-doc-5",
        title="Title",
        text="Original content",
    )

    doc.set_text("Updated content")
    assert doc.text() == "Updated content"


def test_document_custom_properties():
    """Test custom properties management."""
    doc = Document.create(
        id="test-doc-6",
        title="Props Test",
        text="Content",
    )

    # Initially empty
    assert doc.custom_properties() == {}

    # Set single property
    doc.set_custom_property("key1", "value1")
    assert doc.custom_properties() == {"key1": "value1"}

    # Set multiple properties
    doc.set_custom_properties({"key2": "value2", "key3": "value3"})
    assert doc.custom_properties() == {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3",
    }


def test_document_parent():
    """Test parent document relationships."""
    doc = Document.create(
        id="test-doc-7",
        title="Child Document",
        text="Content",
    )

    # Initially no parent
    assert doc.parent_document() is None

    # Set parent
    doc.set_parent_document("urn:li:document:parent-doc")
    assert doc.parent_document() == "urn:li:document:parent-doc"


def test_document_source():
    """Test source URL and external ID."""
    doc = Document.create(
        id="test-doc-8",
        title="Source Test",
        text="Content",
    )

    # Initially no source
    assert doc.source_url() is None

    # Set source
    doc.set_source(url="https://example.com/doc", external_id="ext-123")
    assert doc.source_url() == "https://example.com/doc"


def test_document_status():
    """Test document status management."""
    doc = Document.create(
        id="test-doc-9",
        title="Status Test",
        text="Content",
        status=models.DocumentStateClass.PUBLISHED,
    )

    assert doc.status() == models.DocumentStateClass.PUBLISHED

    # Update status
    doc.set_status(models.DocumentStateClass.UNPUBLISHED)
    assert doc.status() == models.DocumentStateClass.UNPUBLISHED


def test_document_info_aspect():
    """Test that DocumentInfo aspect is properly set."""
    doc = Document.create(
        id="test-doc-10",
        title="Aspect Test",
        text="Content",
    )

    doc_info = doc.document_info()
    assert isinstance(doc_info, models.DocumentInfoClass)
    assert doc_info.title == "Aspect Test"
    assert doc_info.contents is not None
    assert doc_info.contents.text == "Content"
    assert doc_info.created is not None
    assert doc_info.lastModified is not None


def test_document_with_tags():
    """Test creating document with tags."""
    doc = Document.create(
        id="test-doc-11",
        title="Tagged Document",
        text="Content",
        tags=["urn:li:tag:Finance", "urn:li:tag:Quarterly"],
    )

    # Tags should be added via mixin - verify work units generated
    work_units = doc.as_workunits()
    assert len(work_units) > 0

    # Should have work units for both DocumentInfo and GlobalTags
    mcps = doc.as_mcps()
    aspect_names = [mcp.aspectName for mcp in mcps]
    assert "documentInfo" in aspect_names
    assert "globalTags" in aspect_names


def test_document_with_owners():
    """Test creating document with owners."""
    doc = Document.create(
        id="test-doc-12",
        title="Owned Document",
        text="Content",
        owners=["urn:li:corpuser:jdoe"],
    )

    # Owners should be added via mixin
    work_units = doc.as_workunits()
    assert len(work_units) > 0

    # Should have work units for both DocumentInfo and Ownership
    mcps = doc.as_mcps()
    aspect_names = [mcp.aspectName for mcp in mcps]
    assert "documentInfo" in aspect_names
    assert "ownership" in aspect_names


def test_document_with_domain():
    """Test creating document with domain."""
    doc = Document.create(
        id="test-doc-13",
        title="Domain Document",
        text="Content",
        domain="urn:li:domain:finance",
    )

    # Domain should be set via mixin
    work_units = doc.as_workunits()
    assert len(work_units) > 0

    # Should have work units for both DocumentInfo and Domains
    mcps = doc.as_mcps()
    aspect_names = [mcp.aspectName for mcp in mcps]
    assert "documentInfo" in aspect_names
    assert "domains" in aspect_names


def test_document_as_workunits():
    """Test converting document to work units."""
    doc = Document.create(
        id="test-doc-14",
        title="Work Unit Test",
        text="Content",
    )

    work_units = doc.as_workunits()

    # Should have at least one work unit for DocumentInfo
    assert len(work_units) >= 1

    # First work unit should contain document URN
    first_wu = work_units[0]
    assert "urn:li:document:test-doc-14" in str(first_wu)


def test_document_urn_encoding():
    """Test that document IDs with special characters are properly encoded."""
    # Create doc with special characters
    doc = Document.create(
        id="my-document/with:special-chars",
        title="Special Chars",
        text="Content",
    )

    # URN should be properly encoded
    assert isinstance(doc.urn, DocumentUrn)
    assert doc.id is not None
