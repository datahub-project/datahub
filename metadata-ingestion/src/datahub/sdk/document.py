"""Document entity SDK for DataHub.

This module provides a high-level API for creating and managing Document entities
in DataHub. Documents can be either native (created in DataHub) or external
(references to documents stored in external systems like Notion, Confluence, etc.).

Key Features:
- Create native documents with full text content stored in DataHub
- Create external document references that link to content in other platforms
- Control document visibility in global context (search, sidebar navigation)
- Organize documents hierarchically with parent-child relationships
- Link documents to related assets and other documents
- Manage document lifecycle (published/unpublished status)

Example - Native document::

    from datahub.sdk import Document

    doc = Document.create_document(
        id="getting-started-guide",
        title="Getting Started with DataHub",
        text="# Welcome\\n\\nThis guide will help you get started...",
    )

Example - External document (from Notion, Confluence, etc.)::

    from datahub.sdk import Document

    doc = Document.create_external_document(
        id="notion-team-handbook",
        title="Team Handbook",
        platform="urn:li:dataPlatform:notion",
        external_url="https://notion.so/team-handbook",
        external_id="abc123",
        text="Summary of the handbook content...",  # Optional
    )

Example - Document hidden from global context (AI-only access)::

    from datahub.sdk import Document

    # This document won't appear in search or sidebar navigation.
    # It's only accessible via related assets - useful for AI agent context.
    doc = Document.create_document(
        id="dataset-context-doc",
        title="Dataset Context for AI",
        text="This dataset contains customer orders...",
        show_in_global_context=False,
        related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
    )
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.metadata.urns import DataPlatformUrn, DocumentUrn, Urn
from datahub.sdk._shared import (
    ActorUrnOrStr,
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
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.entity import Entity, ExtraAspectsType
from datahub.utilities.urns.error import InvalidUrnError

# Type aliases for document-specific inputs
RelatedAssetInputType = Union[str, Urn]
RelatedAssetsInputType = Sequence[RelatedAssetInputType]
RelatedDocumentInputType = Union[str, DocumentUrn]
RelatedDocumentsInputType = Sequence[RelatedDocumentInputType]
PlatformInputType = Union[str, DataPlatformUrn]


def _validate_document_urn(urn: RelatedDocumentInputType) -> str:
    """Validate that a URN is a valid document URN.

    Args:
        urn: URN string or DocumentUrn to validate

    Returns:
        The validated URN as a string

    Raises:
        SdkUsageError: If the URN is not a valid document URN
    """
    if isinstance(urn, DocumentUrn):
        return str(urn)

    urn_str = str(urn)
    try:
        parsed = Urn.from_string(urn_str)
        if parsed.entity_type != DocumentUrn.ENTITY_TYPE:
            raise SdkUsageError(
                f"Expected a document URN but got '{parsed.entity_type}' URN: {urn_str}. "
                f"Document URNs should be in the format 'urn:li:document:<id>'."
            )
        return urn_str
    except InvalidUrnError as e:
        raise SdkUsageError(
            f"Invalid URN format: {urn_str}. "
            f"Document URNs should be in the format 'urn:li:document:<id>'. "
            f"Error: {e}"
        ) from e


def _validate_asset_urn(urn: RelatedAssetInputType) -> str:
    """Validate that a URN is a valid entity URN.

    Args:
        urn: URN string or Urn to validate

    Returns:
        The validated URN as a string

    Raises:
        SdkUsageError: If the URN is not valid
    """
    if isinstance(urn, Urn):
        return str(urn)

    urn_str = str(urn)
    try:
        Urn.from_string(urn_str)
        return urn_str
    except InvalidUrnError as e:
        raise SdkUsageError(f"Invalid URN format: {urn_str}. Error: {e}") from e


# Valid document states
_VALID_DOCUMENT_STATES = {
    models.DocumentStateClass.PUBLISHED,
    models.DocumentStateClass.UNPUBLISHED,
}


def _validate_status(status: str) -> str:
    """Validate that a status is a valid document state.

    Args:
        status: The status string to validate

    Returns:
        The validated status string

    Raises:
        SdkUsageError: If the status is not valid
    """
    if status not in _VALID_DOCUMENT_STATES:
        raise SdkUsageError(
            f"Invalid document status: '{status}'. "
            f"Must be one of: {', '.join(sorted(_VALID_DOCUMENT_STATES))}"
        )
    return status


def _validate_external_url(url: str) -> str:
    """Validate that a URL is well-formed.

    Args:
        url: The URL to validate

    Returns:
        The validated URL string

    Raises:
        SdkUsageError: If the URL is not valid
    """
    # Basic URL validation - must have scheme and netloc
    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise SdkUsageError(
                f"Invalid URL format: '{url}'. "
                f"URL must include scheme (e.g., 'https://') and domain."
            )
    except Exception as e:
        if isinstance(e, SdkUsageError):
            raise
        raise SdkUsageError(f"Invalid URL format: '{url}'. Error: {e}") from e
    return url


def _validate_related_documents_for_create(
    document_urns: Optional[RelatedDocumentsInputType],
    self_urn: DocumentUrn,
    doc_id: str,
) -> Optional[List[str]]:
    """Validate related documents for document creation.

    Args:
        document_urns: List of document URNs to validate
        self_urn: The URN of the document being created
        doc_id: The ID of the document being created (for error messages)

    Returns:
        List of validated URN strings, or None if no documents provided

    Raises:
        SdkUsageError: If any URN is invalid or self-referential
    """
    if not document_urns:
        return None

    validated = []
    for doc_urn in document_urns:
        validated_urn = _validate_document_urn(doc_urn)
        if validated_urn == str(self_urn):
            raise SdkUsageError(
                f"A document cannot be related to itself. "
                f"Document '{doc_id}' cannot reference itself in related_documents."
            )
        validated.append(validated_urn)
    return validated


def _validate_parent_document_for_create(
    parent_document: Optional[str],
    self_urn: DocumentUrn,
    doc_id: str,
) -> Optional[str]:
    """Validate parent document for document creation.

    Args:
        parent_document: Parent document URN to validate
        self_urn: The URN of the document being created
        doc_id: The ID of the document being created (for error messages)

    Returns:
        Validated parent URN string, or None if no parent provided

    Raises:
        SdkUsageError: If the URN is invalid or self-referential
    """
    if not parent_document:
        return None

    validated = _validate_document_urn(parent_document)
    if validated == str(self_urn):
        raise SdkUsageError(
            f"A document cannot be its own parent. "
            f"Document '{doc_id}' cannot have itself as parent."
        )
    return validated


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

    Documents are used to store knowledge content such as tutorials, FAQs,
    runbooks, and other documentation. They support full-text search and can be
    organized hierarchically.

    There are two types of documents:

    **Native Documents** (created with :meth:`create_document`):
        Created and stored directly in DataHub. Full content is indexed and searchable.
        Use these for documentation authored in DataHub.

    **External Documents** (created with :meth:`create_external_document`):
        References to documents stored in external systems like Notion, Confluence,
        or Google Docs. The external URL links to the original content.

    Both types support:
        - Visibility control (show/hide in global search and sidebar)
        - Related assets (link to datasets, dashboards, etc.)
        - Related documents (link to other documents)
        - Tags, owners, domains, and glossary terms

    Example::

        from datahub.sdk import DataHubClient, Document

        # Create a native document
        doc = Document.create_document(
            id="my-doc",
            title="My Document",
            text="Document content...",
        )

        # Emit to DataHub
        client = DataHubClient.from_env()
        client.entities.upsert(doc)
    """

    __slots__ = ()

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
        """Get the URN type for Document entities."""
        return DocumentUrn

    @classmethod
    def _create_base(
        cls,
        *,
        id: str,
        title: str,
        text: str,
        source: Optional[models.DocumentSourceClass],
        status: str = models.DocumentStateClass.UNPUBLISHED,
        custom_properties: Optional[Dict[str, str]] = None,
        parent_document: Optional[str] = None,
        related_assets: Optional[RelatedAssetsInputType] = None,
        related_documents: Optional[RelatedDocumentsInputType] = None,
        show_in_global_context: bool = True,
        actor: ActorUrnOrStr = DEFAULT_ACTOR_URN,
        subtype: Optional[str] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        owners: Optional[OwnersInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        created_time: Optional[datetime] = None,
        last_modified_time: Optional[datetime] = None,
        extra_aspects: Optional[ExtraAspectsType] = None,
    ) -> Self:
        """Internal method to create a Document with all options."""
        urn = DocumentUrn(id)
        doc = cls(urn=urn)

        # Validate inputs
        _validate_status(status)
        validated_parent = _validate_parent_document_for_create(
            parent_document, urn, id
        )
        validated_assets = (
            [_validate_asset_urn(a) for a in related_assets] if related_assets else None
        )
        validated_related_docs = _validate_related_documents_for_create(
            related_documents, urn, id
        )

        # Create timestamps - use provided times or default to now
        now = datetime.now()
        created_stamp = make_time_stamp(created_time if created_time else now)
        modified_stamp = make_time_stamp(
            last_modified_time if last_modified_time else now
        )

        if not created_stamp or not modified_stamp:
            raise ValueError("Failed to create timestamp")

        # Build audit stamps
        created_audit = models.AuditStampClass(
            time=created_stamp.time, actor=str(actor)
        )
        modified_audit = models.AuditStampClass(
            time=modified_stamp.time, actor=str(actor)
        )

        # Build DocumentStatus
        document_status = models.DocumentStatusClass(state=status)

        # Build DocumentContents
        document_contents = models.DocumentContentsClass(text=text)

        # Build ParentDocument if specified
        parent_doc = None
        if validated_parent:
            parent_doc = models.ParentDocumentClass(document=validated_parent)

        # Build RelatedAssets if specified
        related_assets_list = None
        if validated_assets:
            related_assets_list = [
                models.RelatedAssetClass(asset=asset) for asset in validated_assets
            ]

        # Build RelatedDocuments if specified
        related_documents_list = None
        if validated_related_docs:
            related_documents_list = [
                models.RelatedDocumentClass(document=doc_urn)
                for doc_urn in validated_related_docs
            ]

        # Create DocumentInfo
        document_info = models.DocumentInfoClass(
            title=title,
            source=source,
            status=document_status,
            contents=document_contents,
            customProperties=custom_properties or {},
            created=created_audit,
            lastModified=modified_audit,
            parentDocument=parent_doc,
            relatedAssets=related_assets_list,
            relatedDocuments=related_documents_list,
        )

        doc._set_aspect(document_info)

        # Create DocumentSettings - always set if show_in_global_context is False
        # This controls whether the document appears in global search and sidebar
        if not show_in_global_context:
            settings_audit = models.AuditStampClass(
                time=modified_stamp.time, actor=str(actor)
            )
            document_settings = models.DocumentSettingsClass(
                showInGlobalContext=show_in_global_context,
                lastModified=settings_audit,
            )
            doc._set_aspect(document_settings)

        # Set subtype if specified
        if subtype:
            doc.set_subtype(subtype)

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
            for prop_urn, values in structured_properties.items():
                doc.set_structured_property(prop_urn, values)

        # Add extra aspects
        if extra_aspects:
            for aspect in extra_aspects:
                doc._set_aspect(aspect)

        return doc

    @classmethod
    def create_document(
        cls,
        *,
        id: str,
        title: str,
        text: str,
        status: str = models.DocumentStateClass.PUBLISHED,
        show_in_global_context: bool = True,
        subtype: Optional[str] = None,
        parent_document: Optional[str] = None,
        related_assets: Optional[RelatedAssetsInputType] = None,
        related_documents: Optional[RelatedDocumentsInputType] = None,
        owners: Optional[OwnersInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        created_time: Optional[datetime] = None,
        last_modified_time: Optional[datetime] = None,
        extra_aspects: Optional[ExtraAspectsType] = None,
    ) -> Self:
        """Create a native document stored in DataHub.

        Native documents are created and managed directly in DataHub. The full text
        content is stored and indexed for search. These are ideal for documentation
        authored directly in DataHub's knowledge base.

        Args:
            id: Unique identifier for the document (used in URN)
            title: Document title (displayed in search results and navigation)
            text: Full text content of the document (supports markdown)
            status: Publication status - "PUBLISHED" or "UNPUBLISHED" (default: PUBLISHED)
            show_in_global_context: If True, document appears in global search and
                sidebar navigation. If False, document is only accessible via related
                assets - useful for AI-only context documents. (default: True)
            subtype: Document sub-type (e.g., "FAQ", "Tutorial", "Runbook")
            parent_document: URN of parent document (for hierarchical organization)
            related_assets: URNs of related data assets (datasets, dashboards, etc.)
            related_documents: URNs of related documents
            owners: Owners of the document
            tags: Tags to add to the document
            terms: Glossary terms to associate
            domain: Domain to assign
            custom_properties: Custom key-value metadata
            structured_properties: Structured properties to add
            extra_aspects: Additional aspects to include

        Returns:
            A new Document entity instance

        Example - Basic document::

            doc = Document.create_document(
                id="getting-started",
                title="Getting Started Guide",
                text="# Getting Started\\n\\nWelcome to DataHub!",
            )

        Example - Document with metadata::

            doc = Document.create_document(
                id="data-quality-faq",
                title="Data Quality FAQ",
                text="# FAQ\\n\\n## How do we measure quality?\\n...",
                subtype="FAQ",
                owners=["urn:li:corpuser:datateam"],
                tags=["urn:li:tag:DataQuality"],
                domain="urn:li:domain:data-governance",
            )

        Example - AI-only context document (hidden from global search)::

            doc = Document.create_document(
                id="orders-dataset-context",
                title="Orders Dataset Context",
                text="This dataset contains daily order summaries...",
                show_in_global_context=False,
                related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
            )
        """
        # Native documents have NATIVE source type
        source = models.DocumentSourceClass(
            sourceType=models.DocumentSourceTypeClass.NATIVE,
        )

        return cls._create_base(
            id=id,
            title=title,
            text=text,
            source=source,
            status=status,
            custom_properties=custom_properties,
            parent_document=parent_document,
            related_assets=related_assets,
            related_documents=related_documents,
            show_in_global_context=show_in_global_context,
            subtype=subtype,
            tags=tags,
            terms=terms,
            owners=owners,
            domain=domain,
            structured_properties=structured_properties,
            created_time=created_time,
            last_modified_time=last_modified_time,
            extra_aspects=extra_aspects,
        )

    @classmethod
    def create_external_document(
        cls,
        *,
        id: str,
        title: str,
        platform: PlatformInputType,
        external_url: str,
        external_id: Optional[str] = None,
        text: Optional[str] = None,
        status: str = models.DocumentStateClass.PUBLISHED,
        show_in_global_context: bool = True,
        subtype: Optional[str] = None,
        parent_document: Optional[str] = None,
        related_assets: Optional[RelatedAssetsInputType] = None,
        related_documents: Optional[RelatedDocumentsInputType] = None,
        owners: Optional[OwnersInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        created_time: Optional[datetime] = None,
        last_modified_time: Optional[datetime] = None,
        extra_aspects: Optional[ExtraAspectsType] = None,
    ) -> Self:
        """Create an external document reference.

        External documents are references to content stored in external systems like
        Notion, Confluence, Google Docs, or SharePoint. The external URL links to the
        original content. You can optionally include the text content for indexing
        and search.

        Args:
            id: Unique identifier for the document (used in URN)
            title: Document title
            platform: The data platform URN or name (e.g., "notion" or "urn:li:dataPlatform:notion")
            external_url: URL to the document in the external system
            external_id: Unique identifier in the external system (optional)
            text: Text content for indexing (optional - can be synced from source)
            status: Publication status (default: PUBLISHED)
            show_in_global_context: If True, document appears in global search and
                sidebar navigation. (default: True)
            subtype: Document sub-type (e.g., "Runbook", "Reference")
            parent_document: URN of parent document
            related_assets: URNs of related data assets
            related_documents: URNs of related documents
            owners: Owners of the document
            tags: Tags to add
            terms: Glossary terms to associate
            domain: Domain to assign
            custom_properties: Custom key-value metadata
            structured_properties: Structured properties to add
            created_time: Creation timestamp from external system (optional, defaults to now)
            last_modified_time: Last modified timestamp from external system (optional, defaults to now)
            extra_aspects: Additional aspects to include

        Returns:
            A new Document entity instance

        Example - Notion document::

            doc = Document.create_external_document(
                id="notion-team-handbook",
                title="Engineering Handbook",
                platform="notion",  # or "urn:li:dataPlatform:notion"
                external_url="https://notion.so/team/engineering-handbook",
                external_id="notion-page-abc123",
                text="Summary of the handbook for search...",
            )

        Example - Confluence document::

            doc = Document.create_external_document(
                id="confluence-runbook",
                title="Incident Runbook",
                platform="confluence",
                external_url="https://company.atlassian.net/wiki/spaces/ENG/pages/123",
                owners=["urn:li:corpuser:oncall-team"],
            )
        """
        # Validate external URL
        _validate_external_url(external_url)

        # Validate/parse platform - allow shorthand like "notion"
        if isinstance(platform, str):
            if not platform.startswith("urn:li:dataPlatform:"):
                platform = f"urn:li:dataPlatform:{platform}"
            # Validate the URN format
            DataPlatformUrn.from_string(platform)

        # External documents have EXTERNAL source type
        source = models.DocumentSourceClass(
            sourceType=models.DocumentSourceTypeClass.EXTERNAL,
            externalUrl=external_url,
            externalId=external_id,
        )

        # Text is optional for external documents - use empty string if not provided
        doc_text = text if text is not None else ""

        return cls._create_base(
            id=id,
            title=title,
            text=doc_text,
            source=source,
            status=status,
            custom_properties=custom_properties,
            parent_document=parent_document,
            related_assets=related_assets,
            related_documents=related_documents,
            show_in_global_context=show_in_global_context,
            subtype=subtype,
            tags=tags,
            terms=terms,
            owners=owners,
            domain=domain,
            structured_properties=structured_properties,
            created_time=created_time,
            last_modified_time=last_modified_time,
            extra_aspects=extra_aspects,
        )

    @property
    def id(self) -> str:
        """Get the document ID."""
        assert isinstance(self.urn, DocumentUrn)
        return self.urn.id

    def _get_document_info(self) -> Optional[models.DocumentInfoClass]:
        """Get the DocumentInfo aspect if it exists."""
        return self._get_aspect(models.DocumentInfoClass)

    def _ensure_document_info(self) -> models.DocumentInfoClass:
        """Get or create the DocumentInfo aspect.

        Returns:
            The DocumentInfo aspect

        Raises:
            ValueError: If DocumentInfo aspect doesn't exist and can't be created
        """
        doc_info = self._get_document_info()
        if doc_info is None:
            raise ValueError(
                "DocumentInfo aspect must be set. "
                "Use Document.create_document() or Document.create_external_document() to create a document."
            )
        return doc_info

    def _get_document_settings(self) -> Optional[models.DocumentSettingsClass]:
        """Get the DocumentSettings aspect if it exists."""
        return self._get_aspect(models.DocumentSettingsClass)

    def _ensure_document_settings(self) -> models.DocumentSettingsClass:
        """Get or create the DocumentSettings aspect."""
        settings = self._get_document_settings()
        if settings is None:
            settings = models.DocumentSettingsClass(showInGlobalContext=True)
            self._set_aspect(settings)
        return settings

    @property
    def title(self) -> Optional[str]:
        """Get the document title."""
        doc_info = self._get_document_info()
        return doc_info.title if doc_info else None

    def set_title(self, title: str) -> Self:
        """Set the document title.

        Args:
            title: New title for the document

        Returns:
            Self for method chaining
        """
        doc_info = self._ensure_document_info()
        doc_info.title = title
        return self

    @property
    def text(self) -> Optional[str]:
        """Get the document text content."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.contents:
            return doc_info.contents.text
        return None

    def set_text(self, text: str) -> Self:
        """Set the document text content.

        Args:
            text: New text content

        Returns:
            Self for method chaining
        """
        doc_info = self._ensure_document_info()
        if doc_info.contents:
            doc_info.contents.text = text
        else:
            doc_info.contents = models.DocumentContentsClass(text=text)
        return self

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the document's custom properties."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.customProperties:
            return doc_info.customProperties
        return {}

    def set_custom_property(self, key: str, value: str) -> Self:
        """Set a custom property.

        Args:
            key: Property key
            value: Property value

        Returns:
            Self for method chaining
        """
        doc_info = self._ensure_document_info()
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
        doc_info = self._ensure_document_info()
        if doc_info.customProperties is None:
            doc_info.customProperties = {}
        doc_info.customProperties.update(properties)
        return self

    @property
    def parent_document(self) -> Optional[str]:
        """Get the parent document URN if set."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.parentDocument:
            return doc_info.parentDocument.document
        return None

    def set_parent_document(
        self, parent_urn: Optional[RelatedDocumentInputType]
    ) -> Self:
        """Set the parent document for hierarchy.

        Args:
            parent_urn: URN of the parent document, or None to remove parent

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If the URN is not a valid document URN or is self-referential
        """
        doc_info = self._ensure_document_info()
        if parent_urn:
            validated_urn = _validate_document_urn(parent_urn)
            # Check for self-referential parent
            if validated_urn == str(self.urn):
                raise SdkUsageError(
                    f"A document cannot be its own parent. "
                    f"Document '{self.id}' cannot have itself as parent."
                )
            doc_info.parentDocument = models.ParentDocumentClass(document=validated_urn)
        else:
            doc_info.parentDocument = None
        return self

    @property
    def source_type(self) -> Optional[str]:
        """Get the document source type (NATIVE or EXTERNAL)."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.source and doc_info.source.sourceType:
            return str(doc_info.source.sourceType)
        return None

    @property
    def external_url(self) -> Optional[str]:
        """Get the external source URL (for external documents)."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.source:
            return doc_info.source.externalUrl
        return None

    @property
    def external_id(self) -> Optional[str]:
        """Get the external ID (for external documents)."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.source:
            return doc_info.source.externalId
        return None

    def set_source(
        self,
        source_type: str,
        external_url: Optional[str] = None,
        external_id: Optional[str] = None,
    ) -> Self:
        """Set the document source information.

        Args:
            source_type: Source type - NATIVE or EXTERNAL
            external_url: External URL (for EXTERNAL source type)
            external_id: External ID (for EXTERNAL source type)

        Returns:
            Self for method chaining
        """
        doc_info = self._ensure_document_info()
        doc_info.source = models.DocumentSourceClass(
            sourceType=source_type,
            externalUrl=external_url,
            externalId=external_id,
        )
        return self

    @property
    def status(self) -> Optional[str]:
        """Get the document status (PUBLISHED or UNPUBLISHED)."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.status:
            state = doc_info.status.state
            assert isinstance(state, str)
            return state
        return None

    def set_status(self, status: str) -> Self:
        """Set the document status.

        Args:
            status: New document status (PUBLISHED or UNPUBLISHED)

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If the status is not valid
        """
        _validate_status(status)
        doc_info = self._ensure_document_info()
        if doc_info.status:
            doc_info.status.state = status
        else:
            doc_info.status = models.DocumentStatusClass(state=status)
        return self

    def publish(self) -> Self:
        """Publish the document (make it visible).

        Returns:
            Self for method chaining
        """
        return self.set_status(models.DocumentStateClass.PUBLISHED)

    def unpublish(self) -> Self:
        """Unpublish the document (make it not visible).

        Returns:
            Self for method chaining
        """
        return self.set_status(models.DocumentStateClass.UNPUBLISHED)

    @property
    def show_in_global_context(self) -> bool:
        """Check if document appears in global context (search/sidebar)."""
        settings = self._get_document_settings()
        if settings:
            return settings.showInGlobalContext
        return True  # Default is to show in global context

    def set_show_in_global_context(self, show: bool) -> Self:
        """Set whether document appears in global context.

        When False, the document won't appear in search results or the
        sidebar navigation. It will only be accessible through related assets.
        This is useful for documents intended only for AI agent context.

        Args:
            show: True to show in global context, False to hide

        Returns:
            Self for method chaining
        """
        settings = self._ensure_document_settings()
        settings.showInGlobalContext = show
        return self

    def hide_from_global_context(self) -> Self:
        """Hide document from global search and sidebar.

        The document will only be accessible through related assets.
        Useful for AI-only context documents.

        Returns:
            Self for method chaining
        """
        return self.set_show_in_global_context(False)

    def show_in_global_search(self) -> Self:
        """Show document in global search and sidebar.

        Returns:
            Self for method chaining
        """
        return self.set_show_in_global_context(True)

    @property
    def related_assets(self) -> Optional[List[str]]:
        """Get the list of related asset URNs."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.relatedAssets:
            return [ra.asset for ra in doc_info.relatedAssets]
        return None

    def set_related_assets(self, asset_urns: RelatedAssetsInputType) -> Self:
        """Set the related assets (replaces existing).

        Args:
            asset_urns: List of asset URNs to relate to this document

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If any URN is not valid
        """
        doc_info = self._ensure_document_info()
        validated_urns = [_validate_asset_urn(urn) for urn in asset_urns]
        doc_info.relatedAssets = [
            models.RelatedAssetClass(asset=urn) for urn in validated_urns
        ]
        return self

    def add_related_asset(self, asset_urn: RelatedAssetInputType) -> Self:
        """Add a related asset.

        Args:
            asset_urn: URN of the asset to relate

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If the URN is not valid
        """
        validated_urn = _validate_asset_urn(asset_urn)
        doc_info = self._ensure_document_info()
        if doc_info.relatedAssets is None:
            doc_info.relatedAssets = []
        # Check if already exists
        if not any(ra.asset == validated_urn for ra in doc_info.relatedAssets):
            doc_info.relatedAssets.append(models.RelatedAssetClass(asset=validated_urn))
        return self

    def remove_related_asset(self, asset_urn: RelatedAssetInputType) -> Self:
        """Remove a related asset.

        Args:
            asset_urn: URN of the asset to remove

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If the URN is not valid
        """
        validated_urn = _validate_asset_urn(asset_urn)
        doc_info = self._ensure_document_info()
        if doc_info.relatedAssets:
            doc_info.relatedAssets = [
                ra for ra in doc_info.relatedAssets if ra.asset != validated_urn
            ]
        return self

    @property
    def related_documents(self) -> Optional[List[str]]:
        """Get the list of related document URNs."""
        doc_info = self._get_document_info()
        if doc_info and doc_info.relatedDocuments:
            return [rd.document for rd in doc_info.relatedDocuments]
        return None

    def set_related_documents(self, document_urns: RelatedDocumentsInputType) -> Self:
        """Set the related documents (replaces existing).

        Args:
            document_urns: List of document URNs to relate

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If any URN is not a valid document URN or is self-referential
        """
        doc_info = self._ensure_document_info()
        validated_urns = []
        for urn in document_urns:
            validated_urn = _validate_document_urn(urn)
            if validated_urn == str(self.urn):
                raise SdkUsageError(
                    f"A document cannot be related to itself. "
                    f"Document '{self.id}' cannot reference itself in related_documents."
                )
            validated_urns.append(validated_urn)
        doc_info.relatedDocuments = [
            models.RelatedDocumentClass(document=urn) for urn in validated_urns
        ]
        return self

    def add_related_document(self, document_urn: RelatedDocumentInputType) -> Self:
        """Add a related document.

        Args:
            document_urn: URN of the document to relate

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If the URN is not a valid document URN or is self-referential
        """
        validated_urn = _validate_document_urn(document_urn)
        # Check for self-referential related document
        if validated_urn == str(self.urn):
            raise SdkUsageError(
                f"A document cannot be related to itself. "
                f"Document '{self.id}' cannot reference itself in related_documents."
            )
        doc_info = self._ensure_document_info()
        if doc_info.relatedDocuments is None:
            doc_info.relatedDocuments = []
        # Check if already exists
        if not any(rd.document == validated_urn for rd in doc_info.relatedDocuments):
            doc_info.relatedDocuments.append(
                models.RelatedDocumentClass(document=validated_urn)
            )
        return self

    def remove_related_document(self, document_urn: RelatedDocumentInputType) -> Self:
        """Remove a related document.

        Args:
            document_urn: URN of the document to remove

        Returns:
            Self for method chaining

        Raises:
            SdkUsageError: If the URN is not a valid document URN
        """
        validated_urn = _validate_document_urn(document_urn)
        doc_info = self._ensure_document_info()
        if doc_info.relatedDocuments:
            doc_info.relatedDocuments = [
                rd for rd in doc_info.relatedDocuments if rd.document != validated_urn
            ]
        return self

    def set_semantic_content(
        self,
        *,
        chunks: List[Dict[str, str]],
        embeddings: List[List[float]],
        model_key: str,
        model_version: str,
        chunking_strategy: str,
        actor: ActorUrnOrStr = DEFAULT_ACTOR_URN,
    ) -> Self:
        """Set semantic content with text chunks and embeddings for semantic search.

        This enables the document to be searched semantically using vector similarity.
        The chunks and embeddings are typically generated by an embedding model like
        Cohere or Bedrock.

        Args:
            chunks: List of text chunks, each with a "text" key
            embeddings: List of embedding vectors (one per chunk)
            model_key: Identifier for the embedding model (e.g., "cohere_embed_v3")
            model_version: Full model version string (e.g., "bedrock/cohere.embed-english-v3")
            chunking_strategy: Strategy used for chunking (e.g., "by_title", "basic")
            actor: Actor URN for audit stamp (defaults to system)

        Returns:
            Self for method chaining

        Raises:
            ValueError: If chunks and embeddings lengths don't match

        Example::

            from datahub.sdk import Document

            doc = Document.create_document(
                id="my-doc",
                title="My Document",
                text="This is a long document...",
            )

            # Add semantic content for search
            doc.set_semantic_content(
                chunks=[
                    {"text": "This is a long document..."},
                    {"text": "With multiple chunks..."},
                ],
                embeddings=[
                    [0.1, 0.2, 0.3, ...],  # 1024-dim vector
                    [0.4, 0.5, 0.6, ...],  # 1024-dim vector
                ],
                model_key="cohere_embed_v3",
                model_version="bedrock/cohere.embed-english-v3",
                chunking_strategy="by_title",
            )
        """
        if len(chunks) != len(embeddings):
            raise ValueError(
                f"Chunks and embeddings must have the same length. "
                f"Got {len(chunks)} chunks and {len(embeddings)} embeddings."
            )

        # Build embedding chunks with position, offset, and text
        embedding_chunks = []
        current_offset = 0

        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings, strict=False)):
            chunk_text = chunk.get("text", "")
            chunk_length = len(chunk_text)

            embedding_chunk = models.EmbeddingChunkClass(
                position=i,
                vector=embedding,
                characterOffset=current_offset,
                characterLength=chunk_length,
                tokenCount=None,  # Optional field - not calculated since it's unused
                text=chunk_text,
            )
            embedding_chunks.append(embedding_chunk)
            current_offset += chunk_length

        # Build embedding model data with metadata
        embedding_model_data = models.EmbeddingModelDataClass(
            modelVersion=model_version,
            generatedAt=int(datetime.now().timestamp() * 1000),  # milliseconds
            chunkingStrategy=chunking_strategy,
            totalChunks=len(chunks),
            totalTokens=sum(c.tokenCount or 0 for c in embedding_chunks),
            chunks=embedding_chunks,
        )

        # Build SemanticContent aspect
        semantic_content = models.SemanticContentClass(
            embeddings={model_key: embedding_model_data}
        )

        self._set_aspect(semantic_content)
        return self

    @property
    def is_native(self) -> bool:
        """Check if this is a native document (created in DataHub)."""
        return self.source_type == models.DocumentSourceTypeClass.NATIVE

    @property
    def is_external(self) -> bool:
        """Check if this is an external document reference."""
        return self.source_type == models.DocumentSourceTypeClass.EXTERNAL
