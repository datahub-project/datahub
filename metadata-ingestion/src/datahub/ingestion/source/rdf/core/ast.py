#!/usr/bin/env python3
"""
Shared AST (Abstract Syntax Tree) representations for RDF-to-DataHub transpilation.

This module defines shared data structures that aggregate entity types.
Entity-specific AST classes are now in their respective entity modules.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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

    Simplified: Static fields for the 2-3 entity types we support.
    """

    def __init__(self):
        # Entity fields (explicit for the types we support)
        self.glossary_terms: List[Any] = []
        self.relationships: List[Any] = []
        self.domains: List[Any] = []

        # Special fields
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

    Simplified: Static fields for the 2-3 entity types we support.
    No dynamic initialization needed.
    """

    def __init__(self):
        # Entity fields (explicit for the 2-3 types we support)
        self.glossary_terms: List[Any] = []
        self.relationships: List[Any] = []
        self.domains: List[Any] = []

        # Special fields
        self.owner_groups: List[DataHubOwnerGroup] = []
        self.metadata: Dict[str, Any] = {}

    def get_summary(self) -> Dict[str, int]:
        """
        Get a summary of the DataHub graph contents.

        Returns:
            Dictionary mapping field names to entity counts
        """
        return {
            "glossary_terms": len(self.glossary_terms),
            "relationships": len(self.relationships),
            "domains": len(self.domains),
        }
