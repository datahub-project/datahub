"""
Base interfaces for entity processors.

Each entity type implements these interfaces to provide consistent
extraction, conversion, and MCP creation.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, Type, TypeVar

from rdflib import Graph, URIRef

if TYPE_CHECKING:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph

# Type variables for generic entity processing
RDFEntityT = TypeVar("RDFEntityT")  # RDF AST entity type
DataHubEntityT = TypeVar("DataHubEntityT")  # DataHub AST entity type

# Default processing order constant
DEFAULT_PROCESSING_ORDER = 100


class EntityExtractor(ABC, Generic[RDFEntityT]):
    """
    Base class for extracting entities from RDF graphs.

    Implementations extract specific entity types (glossary terms, datasets, etc.)
    from an RDF graph and return RDF AST objects.
    """

    @property
    @abstractmethod
    def entity_type(self) -> str:
        """Return the entity type name (e.g., 'glossary_term', 'dataset')."""
        pass

    @abstractmethod
    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """
        Check if this extractor can handle the given URI.

        Args:
            graph: The RDF graph
            uri: The URI to check

        Returns:
            True if this extractor can extract an entity from this URI
        """
        pass

    @abstractmethod
    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] | None = None
    ) -> Optional[RDFEntityT]:
        """
        Extract an entity from the RDF graph.

        Args:
            graph: The RDF graph
            uri: The URI of the entity to extract
            context: Optional context with shared state (entity cache, etc.)

        Returns:
            The extracted RDF AST entity, or None if extraction failed
        """
        pass

    @abstractmethod
    def extract_all(
        self, graph: Graph, context: Dict[str, Any] | None = None
    ) -> List[RDFEntityT]:
        """
        Extract all entities of this type from the RDF graph.

        Args:
            graph: The RDF graph
            context: Optional context with shared state

        Returns:
            List of extracted RDF AST entities
        """
        pass


class EntityConverter(ABC, Generic[RDFEntityT, DataHubEntityT]):
    """
    Base class for converting RDF AST entities to DataHub AST entities.

    Implementations convert specific entity types from the internal RDF
    representation to DataHub-specific representation.
    """

    @property
    @abstractmethod
    def entity_type(self) -> str:
        """Return the entity type name."""
        pass

    @abstractmethod
    def convert(
        self, rdf_entity: RDFEntityT, context: Dict[str, Any] | None = None
    ) -> Optional[DataHubEntityT]:
        """
        Convert an RDF AST entity to a DataHub AST entity.

        Args:
            rdf_entity: The RDF AST entity to convert
            context: Optional context with shared state (URN generator, etc.)

        Returns:
            The converted DataHub AST entity, or None if conversion failed
        """
        pass

    @abstractmethod
    def convert_all(
        self, rdf_entities: List[RDFEntityT], context: Dict[str, Any] | None = None
    ) -> List[DataHubEntityT]:
        """
        Convert all RDF AST entities to DataHub AST entities.

        Args:
            rdf_entities: List of RDF AST entities
            context: Optional context with shared state

        Returns:
            List of converted DataHub AST entities
        """
        pass


class EntityMCPBuilder(ABC, Generic[DataHubEntityT]):
    """
    Base class for building MCPs from DataHub AST entities.

    Implementations create MetadataChangeProposalWrapper objects for
    specific entity types.
    """

    @property
    @abstractmethod
    def entity_type(self) -> str:
        """Return the entity type name."""
        pass

    @abstractmethod
    def build_mcps(
        self, entity: DataHubEntityT, context: Dict[str, Any] | None = None
    ) -> List["MetadataChangeProposalWrapper"]:
        """
        Build MCPs for a DataHub AST entity.

        Args:
            entity: The DataHub AST entity
            context: Optional context with shared state

        Returns:
            List of MetadataChangeProposalWrapper objects
        """
        pass

    @abstractmethod
    def build_all_mcps(
        self, entities: List[DataHubEntityT], context: Dict[str, Any] | None = None
    ) -> List["MetadataChangeProposalWrapper"]:
        """
        Build MCPs for all DataHub AST entities of this type.

        Args:
            entities: List of DataHub AST entities
            context: Optional context with shared state

        Returns:
            List of MetadataChangeProposalWrapper objects
        """
        pass

    def build_post_processing_mcps(
        self, datahub_graph: "DataHubGraph", context: Dict[str, Any] | None = None
    ) -> List["MetadataChangeProposalWrapper"]:
        """
        Optional hook for building MCPs that depend on other entities.

        This method is called after all standard entities have been processed,
        allowing entities to handle cross-entity dependencies (e.g., dataset-domain
        associations, glossary nodes from domains, structured property value assignments).

        Args:
            datahub_graph: The complete DataHubGraph AST
            context: Optional context with shared state

        Returns:
            List of MetadataChangeProposalWrapper objects (empty list by default)
        """
        return []


@dataclass
class EntityProcessor(Generic[RDFEntityT, DataHubEntityT]):
    """
    A complete entity processor combining extractor, converter, and MCP builder.

    This is a convenience class that bundles all three components for an entity type.
    """

    extractor: EntityExtractor[RDFEntityT]
    converter: EntityConverter[RDFEntityT, DataHubEntityT]
    mcp_builder: EntityMCPBuilder[DataHubEntityT]

    @property
    def entity_type(self) -> str:
        """Return the entity type name."""
        return self.extractor.entity_type

    def process(
        self, graph: Graph, context: Dict[str, Any] | None = None
    ) -> List["MetadataChangeProposalWrapper"]:
        """
        Complete pipeline: extract → convert → build MCPs.

        Args:
            graph: The RDF graph
            context: Optional context with shared state

        Returns:
            List of MetadataChangeProposalWrapper objects
        """
        # Extract from RDF graph
        rdf_entities = self.extractor.extract_all(graph, context)

        # Convert to DataHub AST
        datahub_entities = self.converter.convert_all(rdf_entities, context)

        # Build MCPs
        mcps = self.mcp_builder.build_all_mcps(datahub_entities, context)

        return mcps


@dataclass
class EntityMetadata:
    """
    Metadata about an entity type for registration.

    Each entity type module should define an ENTITY_METADATA instance
    that describes its CLI names, AST classes, export capabilities, etc.
    """

    entity_type: str  # Internal type name (e.g., 'glossary_term')
    cli_names: List[str]  # CLI choice names (e.g., ['glossary', 'glossary_terms'])
    rdf_ast_class: Optional[
        Type
    ]  # RDF AST class (e.g., RDFGlossaryTerm), None if not extracted from RDF
    datahub_ast_class: Type  # DataHub AST class (e.g., DataHubGlossaryTerm)
    export_targets: List[str] = field(default_factory=list)  # Supported export targets
    validation_rules: Dict[str, Any] = field(
        default_factory=dict
    )  # Entity-specific validation rules
    dependencies: List[str] = field(
        default_factory=list
    )  # List of entity types this entity depends on (for MCP emission ordering)
    processing_order: int = field(
        default=DEFAULT_PROCESSING_ORDER
    )  # DEPRECATED: Use dependencies instead. Kept for backward compatibility.


__all__ = [
    "EntityExtractor",
    "EntityConverter",
    "EntityMCPBuilder",
    "EntityProcessor",
    "EntityMetadata",
    "DEFAULT_PROCESSING_ORDER",
]
