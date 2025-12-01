"""
AST classes for Glossary Term entity.

Defines RDF and DataHub AST representations for glossary terms.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from datahub.ingestion.source.rdf.entities.relationship.ast import RDFRelationship
else:
    # Import at runtime to avoid circular dependency issues
    from datahub.ingestion.source.rdf.entities.relationship.ast import RDFRelationship


@dataclass
class RDFGlossaryTerm:
    """Internal representation of a glossary term extracted from RDF."""

    uri: str
    name: str
    definition: Optional[str] = None
    source: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    relationships: List[RDFRelationship] = field(default_factory=list)
    custom_properties: Dict[str, Any] = field(default_factory=dict)

    # Additional RDF properties useful for exporting
    rdf_type: Optional[str] = None  # Original RDF type (e.g., skos:Concept, owl:Class)
    alternative_labels: List[str] = field(default_factory=list)  # skos:altLabel values
    hidden_labels: List[str] = field(default_factory=list)  # skos:hiddenLabel values
    notation: Optional[str] = None  # skos:notation value
    scope_note: Optional[str] = None  # skos:scopeNote value


@dataclass
class DataHubGlossaryTerm:
    """Internal representation of a DataHub glossary term."""

    urn: str  # Use string for now since GlossaryTermUrn doesn't exist
    name: str
    definition: Optional[str] = None
    source: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[str, List[str]] = field(
        default_factory=dict
    )  # Use strings for now
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    path_segments: List[str] = field(default_factory=list)  # Hierarchical path from IRI
