"""Document entity SDK for DataHub."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Dict, Optional, Type

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import DocumentUrn
from datahub.sdk._shared import (
    DomainInputType,
    HasDomain,
    HasOwnership,
    HasStructuredProperties,
    HasSubtype,
    HasTags,
    HasTerms,
    OwnerInputType,
    OwnersInputType,
    StructuredPropertyInputType,
    TagInputType,
    TagsInputType,
    TermInputType,
    TermsInputType,
    make_time_stamp,
)
from datahub.sdk.entity import Entity, ExtraAspectsType


class Document(
    HasDomain,
    HasOwnership,
    HasStructuredProperties,
    HasSubtype,
    HasTags,
    HasTerms,
    Entity,
):
    """Represents a Document entity in DataHub.

    Documents are used to represent unstructured content such as PDFs, Word documents,
    text files, and other document types. They can store full text content, metadata,
    and can be organized hierarchically.

    Example:
        Create a new document::

            doc = Document.create(
                id="my-document-123",
                title="Q4 Financial Report",
                text="Full document text content...",
                source_url="s3://my-bucket/reports/q4.pdf",
                custom_properties={"file_type": "pdf", "page_count": "45"}
            )

        Add tags and ownership::

            doc.add_tag("Financial").add_tag("Quarterly")
            doc.add_owner("urn:li:corpuser:jdoe")

        Convert to work units for ingestion::

            for wu in doc.to_work_units():
                emitter.emit(wu)
    """

    def __init__(self, urn: DocumentUrn):
        """Initialize a Document entity.

        Args:
            urn: The URN that uniquely identifies this document.
        """
        super().__init__(urn=urn)

    if TYPE_CHECKING:
        # Type stubs for mixin methods
        def add_tag(self, tag: TagInputType) -> None: ...
        def set_tags(self, tags: TagsInputType) -> None: ...
        def add_term(self, term: TermInputType) -> None: ...
        def set_terms(self, terms: TermsInputType) -> None: ...
        def add_owner(self, owner: OwnerInputType) -> None: ...
        def set_owners(self, owners: OwnersInputType) -> None: ...
        def set_domain(self, domain: DomainInputType) -> None: ...
        def set_structured_properties(
            self, properties: StructuredPropertyInputType
        ) -> None: ...

    @classmethod
    def get_urn_type(cls) -> Type[DocumentUrn]:
        return DocumentUrn

    @classmethod
    def create(
        cls,
        *,
        id: str,
        title: str,
        text: str,
        source_url: Optional[str] = None,
        source_id: Optional[str] = None,
        status: str = models.DocumentStateClass.PUBLISHED,
        custom_properties: Optional[Dict[str, str]] = None,
        parent_document: Optional[str] = None,
        created_at: Optional[datetime] = None,
        modified_at: Optional[datetime] = None,
        actor: str = "urn:li:corpuser:datahub",
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        owners: Optional[OwnersInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: Optional[ExtraAspectsType] = None,
    ) -> Self:
        """Create a new Document entity.

        Args:
            id: Unique identifier for the document
            title: Document title
            text: Full text content of the document
            source_url: External URL where the document is stored
            source_id: External identifier for the document
            status: Publication status (PUBLISHED or UNPUBLISHED)
            custom_properties: Custom key-value properties
            parent_document: URN of parent document (for hierarchies)
            created_at: Creation timestamp
            modified_at: Last modification timestamp
            actor: Actor URN for audit stamps
            tags: Tags to add
            terms: Glossary terms to add
            owners: Owners to add
            domain: Domain to assign
            structured_properties: Structured properties to add
            extra_aspects: Additional aspects to include

        Returns:
            A new Document entity instance
        """
        urn = DocumentUrn(id)
        doc = cls(urn=urn)

        # Create DocumentInfo aspect
        now = datetime.now()
        created_stamp = (
            make_time_stamp(created_at) if created_at else make_time_stamp(now)
        )
        modified_stamp = (
            make_time_stamp(modified_at) if modified_at else make_time_stamp(now)
        )

        # Build DocumentSource if URL or ID provided
        document_source = None
        if source_url or source_id:
            document_source = models.DocumentSourceClass(
                sourceType=models.DocumentSourceTypeClass.EXTERNAL,
                externalUrl=source_url,
                externalId=source_id,
            )

        # Build DocumentStatus
        document_status = models.DocumentStatusClass(state=status)

        # Build DocumentContents
        document_contents = models.DocumentContentsClass(text=text)

        # Build ParentDocument if specified
        parent_doc = None
        if parent_document:
            parent_doc = models.ParentDocumentClass(document=parent_document)

        # Build audit stamps
        if not created_stamp or not modified_stamp:
            raise ValueError("Failed to create timestamp")
        audit_stamp = models.AuditStampClass(time=created_stamp.time, actor=actor)

        # Create DocumentInfo
        document_info = models.DocumentInfoClass(
            title=title,
            source=document_source,
            status=document_status,
            contents=document_contents,
            customProperties=custom_properties or {},
            created=audit_stamp,
            lastModified=models.AuditStampClass(time=modified_stamp.time, actor=actor),
            parentDocument=parent_doc,
        )

        doc._set_aspect(document_info)

        # Add tags, terms, owners, domain, structured properties
        if tags:
            doc.set_tags(tags)
        if terms:
            doc.set_terms(terms)
        if owners:
            doc.set_owners(owners)
        if domain:
            doc.set_domain(domain)
        if structured_properties:
            doc.set_structured_properties(structured_properties)

        # Add extra aspects
        if extra_aspects:
            for aspect in extra_aspects:
                doc._set_aspect(aspect)

        return doc

    @property
    def id(self) -> str:
        """Get the document ID."""
        assert isinstance(self.urn, DocumentUrn)
        return self.urn.id

    def document_info(self) -> models.DocumentInfoClass:
        """Get the DocumentInfo aspect.

        Returns:
            The DocumentInfo aspect

        Raises:
            ValueError: If DocumentInfo aspect is not set
        """
        doc_info = self._get_aspect(models.DocumentInfoClass)
        if doc_info is None:
            raise ValueError("DocumentInfo aspect must be set")
        return doc_info

    def title(self) -> str:
        """Get the document title."""
        title = self.document_info().title
        assert title is not None, "Document title must be set"
        return title

    def set_title(self, title: str) -> Self:
        """Set the document title.

        Args:
            title: New title for the document

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        doc_info.title = title
        return self

    def text(self) -> str:
        """Get the document text content."""
        doc_info = self.document_info()
        if doc_info.contents:
            return doc_info.contents.text
        return ""

    def set_text(self, text: str) -> Self:
        """Set the document text content.

        Args:
            text: New text content

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        if doc_info.contents:
            doc_info.contents.text = text
        else:
            doc_info.contents = models.DocumentContentsClass(text=text)
        return self

    def custom_properties(self) -> Dict[str, str]:
        """Get the document's custom properties."""
        doc_info = self.document_info()
        return doc_info.customProperties or {}

    def set_custom_property(self, key: str, value: str) -> Self:
        """Set a custom property.

        Args:
            key: Property key
            value: Property value

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        if doc_info.customProperties is None:
            doc_info.customProperties = {}
        doc_info.customProperties[key] = value
        return self

    def set_custom_properties(self, properties: Dict[str, str]) -> Self:
        """Set multiple custom properties.

        Args:
            properties: Dictionary of properties to set

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        if doc_info.customProperties is None:
            doc_info.customProperties = {}
        doc_info.customProperties.update(properties)
        return self

    def parent_document(self) -> Optional[str]:
        """Get the parent document URN if set."""
        doc_info = self.document_info()
        if doc_info.parentDocument:
            return doc_info.parentDocument.document
        return None

    def set_parent_document(self, parent_urn: str) -> Self:
        """Set the parent document for hierarchy.

        Args:
            parent_urn: URN of the parent document

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        doc_info.parentDocument = models.ParentDocumentClass(document=parent_urn)
        return self

    def source_url(self) -> Optional[str]:
        """Get the external source URL."""
        doc_info = self.document_info()
        if doc_info.source:
            return doc_info.source.externalUrl
        return None

    def set_source(
        self, url: Optional[str] = None, external_id: Optional[str] = None
    ) -> Self:
        """Set the external source information.

        Args:
            url: External URL for the document
            external_id: External identifier for the document

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        if url or external_id:
            doc_info.source = models.DocumentSourceClass(
                sourceType=models.DocumentSourceTypeClass.EXTERNAL,
                externalUrl=url,
                externalId=external_id,
            )
        return self

    def status(self) -> str:
        """Get the document status."""
        doc_info = self.document_info()
        if doc_info.status:
            state = doc_info.status.state
            # state is typed as Union[str, DocumentStateClass] but DocumentStateClass
            # is just a container for string constants, so state is always a str
            assert isinstance(state, str)
            return state
        # DocumentStateClass.PUBLISHED is a string constant
        return models.DocumentStateClass.PUBLISHED

    def set_status(self, status: str) -> Self:
        """Set the document status.

        Args:
            status: New document status (PUBLISHED or UNPUBLISHED)

        Returns:
            Self for method chaining
        """
        doc_info = self.document_info()
        if doc_info.status:
            doc_info.status.state = status
        else:
            doc_info.status = models.DocumentStatusClass(state=status)
        return self
