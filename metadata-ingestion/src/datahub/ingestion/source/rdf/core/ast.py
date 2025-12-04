#!/usr/bin/env python3
"""
Shared AST (Abstract Syntax Tree) representations for RDF-to-DataHub transpilation.

This module defines shared data structures that aggregate entity types.
Entity-specific AST classes are now in their respective entity modules.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.core.utils import entity_type_to_field_name

# Note: Entity fields are dynamically initialized from registry metadata at runtime.
# No hardcoded imports - all entity types are discovered automatically.

# Backward compatibility alias
_entity_type_to_field_name = entity_type_to_field_name


# Shared classes that are used across multiple entity types


@dataclass
class RDFOwnership:
    """Represents ownership information for domains and other entities."""

    owner_uri: str
    owner_type: str  # Owner type string (supports custom types defined in DataHub UI, e.g., "BUSINESS_OWNER", "CUSTOM_TYPE")
    entity_uri: str  # The entity being owned (domain, etc.)
    entity_type: str  # "domain", etc.
    owner_label: Optional[str] = None
    owner_description: Optional[str] = None
    owner_department: Optional[str] = None
    owner_responsibility: Optional[str] = None
    owner_approval_authority: Optional[bool] = None


@dataclass
class RDFOwnerGroup:
    """Internal representation of an owner group from RDF."""

    iri: str  # Owner group IRI
    name: str  # Display name (from rdfs:label)
    owner_type: str  # Owner type string (supports custom types defined in DataHub UI, e.g., "BUSINESS_OWNER", "CUSTOM_TYPE") (from dh:hasOwnerType or RDF type)
    description: Optional[str] = None  # From rdfs:comment


class RDFGraph:
    """
    Internal AST representation of the complete RDF graph.

    Entity fields are dynamically initialized from registered entity types.
    Special fields (owner_groups, ownership, metadata) are always present.
    """

    def __init__(self):
        # Initialize entity fields dynamically from registry
        from datahub.ingestion.source.rdf.entities.registry import (
            create_default_registry,
        )

        registry = create_default_registry()

        # Initialize entity fields dynamically
        for entity_type, _metadata in registry._metadata.items():
            field_name = _entity_type_to_field_name(entity_type)
            setattr(self, field_name, [])

        # Special sub-component fields (not separate entity types)
        # These are populated by their parent entity processors.
        # Field names are discovered from entity metadata if available, otherwise use defaults.
        # Check registry for entities that define sub-component fields
        for entity_type, _metadata in registry._metadata.items():
            # Check if metadata defines sub-component fields (future extensibility)
            # For now, use known sub-components based on entity type
            if entity_type == "structured_property":
                self.structured_property_values = []
            elif entity_type == "lineage":
                self.lineage_activities = []
            elif entity_type == "assertion":
                self.cross_field_constraints = []

        # Domains are built from other entities, not extracted
        self.domains: List[Any] = []

        # Special fields (not entity types, always present)
        self.owner_groups: List[RDFOwnerGroup] = []
        self.ownership: List["RDFOwnership"] = []
        self.metadata: Dict[str, Any] = {}


# DataHub AST Classes (Internal representation before SDK object creation)

# Aggregate classes that collect entity types


@dataclass
class DataHubOwnerGroup:
    """Internal representation of an owner group (corpGroup)."""

    iri: str  # Owner group IRI
    urn: str  # DataHub corpGroup URN
    name: str  # Display name (from rdfs:label)
    owner_type: str  # Owner type string (supports custom types defined in DataHub UI, e.g., "BUSINESS_OWNER", "CUSTOM_TYPE") (from dh:hasOwnerType or RDF type)
    description: Optional[str] = None  # From rdfs:comment


class DataHubGraph:
    """
    Internal AST representation of the complete DataHub graph.

    Entity fields are dynamically initialized from registered entity types.
    Special fields (owner_groups, metadata) are always present.

    Note: Converted from @dataclass to regular class to support dynamic fields.
    """

    def __init__(self):
        # Initialize entity fields dynamically from registry
        from datahub.ingestion.source.rdf.entities.registry import (
            create_default_registry,
        )

        registry = create_default_registry()

        # Initialize entity fields dynamically
        for entity_type, _metadata in registry._metadata.items():
            field_name = _entity_type_to_field_name(entity_type)
            setattr(self, field_name, [])

        # Special sub-component fields (not separate entity types)
        # These are populated by their parent entity processors.
        # Field names are discovered from entity metadata if available, otherwise use defaults.
        # Check registry for entities that define sub-component fields
        for entity_type, _metadata in registry._metadata.items():
            # Check if metadata defines sub-component fields (future extensibility)
            # For now, use known sub-components based on entity type
            if entity_type == "structured_property":
                self.structured_property_values = []
            elif entity_type == "lineage":
                self.lineage_activities = []
            elif entity_type == "assertion":
                self.cross_field_constraints = []

        # Domains are built from other entities, not extracted
        self.domains: List[Any] = []

        # Special fields (not entity types, always present)
        self.owner_groups: List[DataHubOwnerGroup] = []
        self.metadata: Dict[str, Any] = {}

    def get_summary(self) -> Dict[str, int]:
        """
        Get a summary of the DataHub graph contents.

        Returns:
            Dictionary mapping field names to entity counts
        """
        summary = {}
        from datahub.ingestion.source.rdf.entities.registry import (
            create_default_registry,
        )

        registry = create_default_registry()

        # Include all registered entity types
        for entity_type, _metadata in registry._metadata.items():
            field_name = _entity_type_to_field_name(entity_type)
            if hasattr(self, field_name):
                summary[field_name] = len(getattr(self, field_name))

        # Include special sub-component fields (not entity types)
        # None for MVP - removed dataset/lineage/assertion/structured_property support

        return summary
