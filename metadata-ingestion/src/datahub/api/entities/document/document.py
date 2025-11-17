"""
DataHub Document Entity API

This module provides a high-level API for managing documents in DataHub,
including creating, updating, searching, and managing document lifecycle.
"""

import logging
from typing import Any, Dict, List, Optional

from pydantic import Field, validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class DocumentError(Exception):
    """Base exception for document operations."""

    pass


class DocumentNotFoundError(DocumentError):
    """Raised when a document is not found."""

    pass


class DocumentValidationError(DocumentError):
    """Raised when document validation fails."""

    pass


class DocumentOperationError(DocumentError):
    """Raised when a document operation fails."""

    pass


# GraphQL query and mutation definitions
CREATE_DOCUMENT_MUTATION = """
mutation createDocument($input: CreateDocumentInput!) {
    createDocument(input: $input)
}
"""

UPDATE_DOCUMENT_CONTENTS_MUTATION = """
mutation updateDocumentContents($input: UpdateDocumentContentsInput!) {
    updateDocumentContents(input: $input)
}
"""

UPDATE_DOCUMENT_STATUS_MUTATION = """
mutation updateDocumentStatus($input: UpdateDocumentStatusInput!) {
    updateDocumentStatus(input: $input)
}
"""

DELETE_DOCUMENT_MUTATION = """
mutation deleteDocument($urn: String!) {
    deleteDocument(urn: $urn)
}
"""

GET_DOCUMENT_QUERY = """
query getDocument($urn: String!) {
    document(urn: $urn) {
        urn
        type
        subType
        info {
            title
            source {
                sourceType
                externalUrl
                externalId
            }
            status {
                state
            }
            contents {
                text
            }
            created {
                time
                actor
            }
            lastModified {
                time
                actor
            }
            relatedAssets {
                asset {
                    urn
                    type
                }
            }
            relatedDocuments {
                document {
                    urn
                    type
                    info {
                        title
                    }
                }
            }
            parentDocument {
                document {
                    urn
                    info {
                        title
                    }
                }
            }
            draftOf {
                document {
                    urn
                    info {
                        title
                    }
                }
            }
            customProperties {
                key
                value
            }
        }
        ownership {
            owners {
                owner {
                    ... on CorpUser {
                        urn
                        type
                    }
                    ... on CorpGroup {
                        urn
                        type
                    }
                }
                type
            }
        }
        domain {
            domain {
                urn
                type
            }
        }
        tags {
            tags {
                tag {
                    urn
                    name
                }
            }
        }
        exists
    }
}
"""

SEARCH_DOCUMENTS_QUERY = """
query searchDocuments($input: SearchDocumentsInput!) {
    searchDocuments(input: $input) {
        start
        count
        total
        documents {
            urn
            type
            subType
            info {
                title
                status {
                    state
                }
                contents {
                    text
                }
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
                }
                parentDocument {
                    document {
                        urn
                        info {
                            title
                        }
                    }
                }
            }
            ownership {
                owners {
                    owner {
                        ... on CorpUser {
                            urn
                            type
                        }
                        ... on CorpGroup {
                            urn
                            type
                        }
                    }
                    type
                }
            }
            domain {
                domain {
                    urn
                    type
                }
            }
        }
        facets {
            field
            displayName
            aggregations {
                value
                count
            }
        }
    }
}
"""

UPDATE_DOCUMENT_SUB_TYPE_MUTATION = """
mutation updateDocumentSubType($input: UpdateDocumentSubTypeInput!) {
    updateDocumentSubType(input: $input)
}
"""

UPDATE_DOCUMENT_RELATED_ENTITIES_MUTATION = """
mutation updateDocumentRelatedEntities($input: UpdateDocumentRelatedEntitiesInput!) {
    updateDocumentRelatedEntities(input: $input)
}
"""

MOVE_DOCUMENT_MUTATION = """
mutation moveDocument($input: MoveDocumentInput!) {
    moveDocument(input: $input)
}
"""


class OwnerInput(ConfigModel):
    """Owner input for document creation."""

    owner: str = Field(description="Owner URN")
    type: str = Field(
        description="Ownership type (e.g., TECHNICAL_OWNER, BUSINESS_OWNER)"
    )

    @validator("owner")
    def validate_owner_urn(cls, v):
        """Validate that owner is a proper URN."""
        if not v:
            raise DocumentValidationError("Owner URN cannot be empty")
        try:
            # Basic URN validation
            if not v.startswith("urn:li:"):
                raise DocumentValidationError(
                    f"Invalid owner URN format: {v}. Must start with 'urn:li:'"
                )
        except Exception as e:
            raise DocumentValidationError(f"Invalid owner URN: {e}") from e
        return v


class Document:
    """
    High-level API for managing DataHub documents.

    This class provides methods for document CRUD operations, search,
    and lifecycle management using the DataHub GraphQL API.

    Example:
        >>> from datahub.api.entities.document import Document
        >>> from datahub.ingestion.graph.client import DataHubGraph
        >>>
        >>> graph = DataHubGraph(config=...)
        >>> doc_api = Document(graph=graph)
        >>>
        >>> # Create a document
        >>> urn = doc_api.create(
        ...     text="# Getting Started\\n\\nWelcome!",
        ...     title="Getting Started",
        ...     state="PUBLISHED"
        ... )
        >>>
        >>> # Search documents
        >>> results = doc_api.search(query="getting started", states=["PUBLISHED"])
        >>>
        >>> # Update document
        >>> doc_api.update(urn=urn, text="# Updated Content")
        >>>
        >>> # Delete document
        >>> doc_api.delete(urn=urn)
    """

    def __init__(self, graph: DataHubGraph):
        """
        Initialize the Document API.

        Args:
            graph: DataHubGraph client instance for executing GraphQL operations

        Raises:
            DocumentValidationError: If graph is None
        """
        if graph is None:
            raise DocumentValidationError("DataHubGraph client cannot be None")
        self.graph = graph

    def _validate_urn(self, urn: str) -> None:
        """
        Validate that a URN is properly formatted.

        Args:
            urn: The URN to validate

        Raises:
            DocumentValidationError: If URN is invalid
        """
        if not urn:
            raise DocumentValidationError("Document URN cannot be empty")

        try:
            parsed_urn = Urn.from_string(urn)
            if parsed_urn.entity_type != "document":
                raise DocumentValidationError(
                    f"Expected document URN, got {parsed_urn.entity_type}"
                )
        except InvalidUrnError as e:
            raise DocumentValidationError(f"Invalid document URN: {e}") from e

    def _execute_graphql(
        self,
        query: str,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
    ) -> Dict:
        """
        Execute a GraphQL query with error handling.

        Args:
            query: GraphQL query string
            variables: Optional variables dictionary
            operation_name: Optional operation name for logging

        Returns:
            Dict containing the response data

        Raises:
            DocumentOperationError: If the GraphQL operation fails
        """
        try:
            return self.graph.execute_graphql(
                query=query, variables=variables, operation_name=operation_name
            )
        except Exception as e:
            error_msg = "GraphQL operation failed"
            if operation_name:
                error_msg += f" ({operation_name})"
            error_msg += f": {str(e)}"
            logger.error(error_msg)
            raise DocumentOperationError(error_msg) from e

    def create(  # noqa: C901
        self,
        text: str,
        title: Optional[str] = None,
        id: Optional[str] = None,
        sub_type: Optional[str] = None,
        state: str = "UNPUBLISHED",
        owners: Optional[List[Dict[str, str]]] = None,
        parent_document: Optional[str] = None,
        related_assets: Optional[List[str]] = None,
        related_documents: Optional[List[str]] = None,
        draft_for: Optional[str] = None,
    ) -> str:
        """
        Create a new document in DataHub.

        Args:
            text: The text content of the document (required, cannot be empty)
            title: Optional title for the document
            id: Optional custom id for the document (defaults to auto-generated UUID)
            sub_type: Optional sub-type (e.g., "FAQ", "Tutorial", "Reference")
            state: Document state - "PUBLISHED" or "UNPUBLISHED" (default: "UNPUBLISHED")
            owners: Optional list of owner dicts [{"owner": "urn:li:corpuser:user", "type": "TECHNICAL_OWNER"}]
            parent_document: Optional URN of the parent document
            related_assets: Optional list of related asset URNs
            related_documents: Optional list of related document URNs
            draft_for: Optional URN of document this is a draft for

        Returns:
            The URN of the newly created document

        Raises:
            DocumentValidationError: If validation fails
            DocumentOperationError: If creation fails

        Example:
            >>> urn = doc_api.create(
            ...     text="# Tutorial\\nContent here",
            ...     title="My Tutorial",
            ...     sub_type="Tutorial",
            ...     state="PUBLISHED"
            ... )
        """
        # Validation
        if not text or not text.strip():
            raise DocumentValidationError("Document text cannot be empty")

        if state not in ["PUBLISHED", "UNPUBLISHED"]:
            raise DocumentValidationError(
                f"Invalid state: {state}. Must be 'PUBLISHED' or 'UNPUBLISHED'"
            )

        # Validate parent document URN if provided
        if parent_document:
            self._validate_urn(parent_document)

        # Validate related URNs
        if related_assets:
            for asset_urn in related_assets:
                if not asset_urn or not asset_urn.startswith("urn:li:"):
                    raise DocumentValidationError(
                        f"Invalid related asset URN: {asset_urn}"
                    )

        if related_documents:
            for doc_urn in related_documents:
                self._validate_urn(doc_urn)

        if draft_for:
            self._validate_urn(draft_for)

        # Validate owners
        if owners:
            for owner in owners:
                if "owner" not in owner or "type" not in owner:
                    raise DocumentValidationError(
                        "Each owner must have 'owner' and 'type' fields"
                    )
                OwnerInput.parse_obj(owner)  # This validates the owner

        # Build input
        input_dict: Dict[str, Any] = {
            "contents": {"text": text},
            "state": state,
        }

        if id is not None:
            input_dict["id"] = id
        if title is not None:
            input_dict["title"] = title
        if sub_type is not None:
            input_dict["subType"] = sub_type
        if owners is not None:
            input_dict["owners"] = owners
        if parent_document is not None:
            input_dict["parentDocument"] = parent_document
        if related_assets is not None:
            input_dict["relatedAssets"] = related_assets
        if related_documents is not None:
            input_dict["relatedDocuments"] = related_documents
        if draft_for is not None:
            input_dict["draftFor"] = draft_for

        try:
            result = self._execute_graphql(
                CREATE_DOCUMENT_MUTATION,
                variables={"input": input_dict},
                operation_name="createDocument",
            )
            urn = result.get("createDocument")
            if not urn:
                raise DocumentOperationError(
                    "Failed to create document: No URN returned"
                )

            logger.info(f"Successfully created document: {urn}")
            return urn
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(f"Failed to create document: {e}") from e

    def update(
        self,
        urn: str,
        text: Optional[str] = None,
        title: Optional[str] = None,
        sub_type: Optional[str] = None,
    ) -> bool:
        """
        Update the contents and/or title of an existing document.

        Args:
            urn: The URN of the document to update (required)
            text: Optional new text content for the document
            title: Optional new title for the document
            sub_type: Optional new sub-type for the document

        Returns:
            True if the update was successful

        Raises:
            DocumentValidationError: If validation fails
            DocumentOperationError: If update fails

        Example:
            >>> success = doc_api.update(
            ...     urn="urn:li:document:abc-123",
            ...     text="# Updated Content",
            ...     title="Updated Title"
            ... )
        """
        # Validation
        self._validate_urn(urn)

        if text is None and title is None and sub_type is None:
            raise DocumentValidationError(
                "At least one of text, title, or sub_type must be provided"
            )

        if text is not None and not text.strip():
            raise DocumentValidationError("Document text cannot be empty")

        # Build input
        input_dict: Dict[str, Any] = {"urn": urn}

        if text is not None:
            input_dict["contents"] = {"text": text}
        if title is not None:
            input_dict["title"] = title
        if sub_type is not None:
            input_dict["subType"] = sub_type

        try:
            result = self._execute_graphql(
                UPDATE_DOCUMENT_CONTENTS_MUTATION,
                variables={"input": input_dict},
                operation_name="updateDocumentContents",
            )
            success = result.get("updateDocumentContents", False)
            if success:
                logger.info(f"Successfully updated document: {urn}")
            return success
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(f"Failed to update document: {e}") from e

    def get(self, urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a document by its URN.

        Args:
            urn: The URN of the document to retrieve

        Returns:
            A dictionary containing the document data, or None if not found

        Raises:
            DocumentValidationError: If URN is invalid
            DocumentOperationError: If retrieval fails

        Example:
            >>> document = doc_api.get(urn="urn:li:document:abc-123")
            >>> if document:
            ...     print(document["info"]["title"])
            ...     print(document["info"]["contents"]["text"])
        """
        self._validate_urn(urn)

        try:
            result = self._execute_graphql(
                GET_DOCUMENT_QUERY,
                variables={"urn": urn},
                operation_name="getDocument",
            )
            document = result.get("document")

            if document and not document.get("exists", True):
                logger.warning(
                    f"Document {urn} exists in metadata but is marked as not existing"
                )
                return None

            return document
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(f"Failed to get document: {e}") from e

    def search(  # noqa: C901
        self,
        query: Optional[str] = None,
        start: int = 0,
        count: int = 10,
        types: Optional[List[str]] = None,
        domains: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        parent_document: Optional[str] = None,
        root_only: bool = False,
        include_drafts: bool = False,
    ) -> Dict[str, Any]:
        """
        Search for documents with various filters.

        Args:
            query: Optional semantic search query
            start: Starting offset for pagination (default: 0)
            count: Number of results to return (default: 10, max: 10000)
            types: Optional list of document sub-types to filter by
            domains: Optional list of domain URNs to filter by
            states: Optional list of document states ("PUBLISHED", "UNPUBLISHED")
            parent_document: Optional parent document URN to filter by
            root_only: If True, only return root-level documents (no parent)
            include_drafts: If True, include draft documents in results

        Returns:
            A dictionary containing search results with documents, total count, and facets

        Raises:
            DocumentValidationError: If validation fails
            DocumentOperationError: If search fails

        Example:
            >>> results = doc_api.search(
            ...     query="machine learning",
            ...     states=["PUBLISHED"],
            ...     types=["Tutorial"],
            ...     count=20
            ... )
            >>> print(f"Found {results['total']} documents")
            >>> for doc in results["documents"]:
            ...     print(doc["info"]["title"])
        """
        # Validation
        if start < 0:
            raise DocumentValidationError("start must be non-negative")
        if count <= 0:
            raise DocumentValidationError("count must be positive")
        if count > 10000:
            raise DocumentValidationError("count cannot exceed 10000")

        if states:
            for state in states:
                if state not in ["PUBLISHED", "UNPUBLISHED"]:
                    raise DocumentValidationError(
                        f"Invalid state: {state}. Must be 'PUBLISHED' or 'UNPUBLISHED'"
                    )

        if parent_document:
            self._validate_urn(parent_document)

        if domains:
            for domain_urn in domains:
                if not domain_urn or not domain_urn.startswith("urn:li:domain:"):
                    raise DocumentValidationError(f"Invalid domain URN: {domain_urn}")

        # Build input
        input_dict: Dict[str, Any] = {
            "start": start,
            "count": count,
        }

        if query is not None:
            input_dict["query"] = query
        if types is not None:
            input_dict["types"] = types
        if domains is not None:
            input_dict["domains"] = domains
        if states is not None:
            input_dict["states"] = states
        if parent_document is not None:
            input_dict["parentDocument"] = parent_document
        if root_only:
            input_dict["rootOnly"] = True
        if include_drafts:
            input_dict["includeDrafts"] = True

        try:
            result = self._execute_graphql(
                SEARCH_DOCUMENTS_QUERY,
                variables={"input": input_dict},
                operation_name="searchDocuments",
            )
            search_results = result.get("searchDocuments", {})
            if not search_results:
                logger.warning("Search returned no results wrapper")
                return {"total": 0, "documents": [], "facets": []}

            return search_results
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(f"Failed to search documents: {e}") from e

    def publish(self, urn: str) -> bool:
        """
        Publish a document (make it visible).

        Args:
            urn: The URN of the document to publish

        Returns:
            True if the operation was successful

        Raises:
            DocumentValidationError: If URN is invalid
            DocumentOperationError: If publish fails

        Example:
            >>> success = doc_api.publish(urn="urn:li:document:abc-123")
        """
        return self._update_status(urn, "PUBLISHED")

    def unpublish(self, urn: str) -> bool:
        """
        Unpublish a document (make it not visible).

        Args:
            urn: The URN of the document to unpublish

        Returns:
            True if the operation was successful

        Raises:
            DocumentValidationError: If URN is invalid
            DocumentOperationError: If unpublish fails

        Example:
            >>> success = doc_api.unpublish(urn="urn:li:document:abc-123")
        """
        return self._update_status(urn, "UNPUBLISHED")

    def _update_status(self, urn: str, state: str) -> bool:
        """
        Update the status (published/unpublished) of a document.

        Args:
            urn: The URN of the document to update
            state: The new state - "PUBLISHED" or "UNPUBLISHED"

        Returns:
            True if the update was successful

        Raises:
            DocumentValidationError: If validation fails
            DocumentOperationError: If update fails
        """
        self._validate_urn(urn)

        if state not in ["PUBLISHED", "UNPUBLISHED"]:
            raise DocumentValidationError(
                f"Invalid state: {state}. Must be 'PUBLISHED' or 'UNPUBLISHED'"
            )

        try:
            result = self._execute_graphql(
                UPDATE_DOCUMENT_STATUS_MUTATION,
                variables={"input": {"urn": urn, "state": state}},
                operation_name="updateDocumentStatus",
            )
            success = result.get("updateDocumentStatus", False)
            if success:
                logger.info(f"Successfully updated document status to {state}: {urn}")
            return success
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(
                f"Failed to update document status: {e}"
            ) from e

    def delete(self, urn: str) -> bool:
        """
        Delete a document.

        Args:
            urn: The URN of the document to delete

        Returns:
            True if the deletion was successful

        Raises:
            DocumentValidationError: If URN is invalid
            DocumentOperationError: If deletion fails

        Example:
            >>> success = doc_api.delete(urn="urn:li:document:abc-123")
        """
        self._validate_urn(urn)

        try:
            result = self._execute_graphql(
                DELETE_DOCUMENT_MUTATION,
                variables={"urn": urn},
                operation_name="deleteDocument",
            )
            success = result.get("deleteDocument", False)
            if success:
                logger.info(f"Successfully deleted document: {urn}")
            return success
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(f"Failed to delete document: {e}") from e

    def update_sub_type(self, urn: str, sub_type: Optional[str]) -> bool:
        """
        Update the sub-type of a document.

        Args:
            urn: The URN of the document to update
            sub_type: The new sub-type (e.g., "FAQ", "Tutorial", "Runbook"), or None to clear

        Returns:
            True if the update was successful

        Raises:
            DocumentValidationError: If URN is invalid
            DocumentOperationError: If update fails

        Example:
            >>> success = doc_api.update_sub_type(
            ...     urn="urn:li:document:abc-123",
            ...     sub_type="Tutorial"
            ... )
        """
        self._validate_urn(urn)

        try:
            result = self._execute_graphql(
                UPDATE_DOCUMENT_SUB_TYPE_MUTATION,
                variables={"input": {"urn": urn, "subType": sub_type}},
                operation_name="updateDocumentSubType",
            )
            success = result.get("updateDocumentSubType", False)
            if success:
                logger.info(f"Successfully updated document sub-type: {urn}")
            return success
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(
                f"Failed to update document sub-type: {e}"
            ) from e

    def update_related_entities(
        self,
        urn: str,
        related_assets: Optional[List[str]] = None,
        related_documents: Optional[List[str]] = None,
    ) -> bool:
        """
        Update the related assets and documents for a document.

        Args:
            urn: The URN of the document to update
            related_assets: Optional list of related asset URNs (replaces existing)
            related_documents: Optional list of related document URNs (replaces existing)

        Returns:
            True if the update was successful

        Raises:
            DocumentValidationError: If validation fails
            DocumentOperationError: If update fails

        Example:
            >>> success = doc_api.update_related_entities(
            ...     urn="urn:li:document:abc-123",
            ...     related_assets=["urn:li:dataset:(...)"],
            ...     related_documents=["urn:li:document:other-doc"]
            ... )
        """
        self._validate_urn(urn)

        # Validate related URNs
        if related_assets:
            for asset_urn in related_assets:
                if not asset_urn or not asset_urn.startswith("urn:li:"):
                    raise DocumentValidationError(
                        f"Invalid related asset URN: {asset_urn}"
                    )

        if related_documents:
            for doc_urn in related_documents:
                self._validate_urn(doc_urn)

        input_dict: Dict[str, Any] = {"urn": urn}

        if related_assets is not None:
            input_dict["relatedAssets"] = related_assets
        if related_documents is not None:
            input_dict["relatedDocuments"] = related_documents

        try:
            result = self._execute_graphql(
                UPDATE_DOCUMENT_RELATED_ENTITIES_MUTATION,
                variables={"input": input_dict},
                operation_name="updateDocumentRelatedEntities",
            )
            success = result.get("updateDocumentRelatedEntities", False)
            if success:
                logger.info(f"Successfully updated document related entities: {urn}")
            return success
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(
                f"Failed to update document related entities: {e}"
            ) from e

    def move(self, urn: str, parent_document: Optional[str] = None) -> bool:
        """
        Move a document to a different parent or to root level.

        Args:
            urn: The URN of the document to move
            parent_document: The URN of the new parent document, or None to move to root

        Returns:
            True if the move was successful

        Raises:
            DocumentValidationError: If validation fails
            DocumentOperationError: If move fails

        Example:
            >>> # Move to root level
            >>> success = doc_api.move(urn="urn:li:document:abc-123")
            >>>
            >>> # Move to another parent
            >>> success = doc_api.move(
            ...     urn="urn:li:document:abc-123",
            ...     parent_document="urn:li:document:parent-doc"
            ... )
        """
        self._validate_urn(urn)

        if parent_document:
            self._validate_urn(parent_document)

        try:
            result = self._execute_graphql(
                MOVE_DOCUMENT_MUTATION,
                variables={"input": {"urn": urn, "parentDocument": parent_document}},
                operation_name="moveDocument",
            )
            success = result.get("moveDocument", False)
            if success:
                logger.info(f"Successfully moved document: {urn}")
            return success
        except DocumentOperationError:
            raise
        except Exception as e:
            raise DocumentOperationError(f"Failed to move document: {e}") from e

    def exists(self, urn: str) -> bool:
        """
        Check if a document exists.

        Args:
            urn: The URN of the document to check

        Returns:
            True if the document exists, False otherwise

        Raises:
            DocumentValidationError: If URN is invalid

        Example:
            >>> if doc_api.exists(urn="urn:li:document:abc-123"):
            ...     print("Document exists")
        """
        try:
            document = self.get(urn=urn)
            return document is not None
        except DocumentOperationError:
            # If we get an operation error, the document doesn't exist
            return False
