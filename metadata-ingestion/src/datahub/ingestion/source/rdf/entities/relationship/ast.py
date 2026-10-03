"""
AST classes for Relationship entity.

Defines RDF and DataHub AST representations for relationships.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class RelationshipType(Enum):
    """Legacy relationship types retained for backward compatibility in tests."""

    BROADER = "broader"
    NARROWER = "narrower"


class MappingClass(str, Enum):
    ALIGNED = "aligned"
    EXTENSION = "extension"


@dataclass
class RDFStatement:
    """A harvested object-property triple between URI nodes."""

    subject_iri: str
    predicate_iri: str
    object_iri: str


@dataclass
class RDFRelationship:
    """Legacy RDF relationship representation."""

    source_uri: str
    target_uri: str
    relationship_type: RelationshipType
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubNativeRelationship:
    """A relationship routed to a native @Relationship aspect field."""

    source_urn: str
    target_urn: str
    field: str
    aspect: str = "glossaryRelatedTerms"
    original_predicate_iri: str = ""
    mapping_class: MappingClass = MappingClass.ALIGNED
    maps_to: str = ""


@dataclass
class DataHubStructuredPropertyAssignment:
    """An extension triple stored as a structured property assignment."""

    subject_urn: str
    object_urn: str
    predicate_iri: str
    qualified_name: str
    mapping_class: MappingClass = MappingClass.EXTENSION


@dataclass
class DataHubStructuredPropertyDefinition:
    """Structured property definition for an extension predicate."""

    qualified_name: str
    predicate_iri: str
    display_name: str


@dataclass
class DataHubRelationship:
    """Legacy internal representation of a DataHub relationship."""

    source_urn: str
    target_urn: str
    relationship_type: RelationshipType
    properties: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_native(
        cls, rel: DataHubNativeRelationship
    ) -> Optional["DataHubRelationship"]:
        if rel.field != "isRelatedTerms":
            return None
        rel_type = RelationshipType.BROADER
        return cls(
            source_urn=rel.source_urn,
            target_urn=rel.target_urn,
            relationship_type=rel_type,
            properties={
                "original_predicate_iri": rel.original_predicate_iri,
                "maps_to": rel.maps_to,
            },
        )
