"""
AST classes for Structured Property entity.

Defines RDF and DataHub AST representations for structured properties.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# DataHub SDK imports
from datahub.utilities.urns.structured_properties_urn import StructuredPropertyUrn


@dataclass
class RDFStructuredProperty:
    """Internal representation of a structured property definition."""

    uri: str
    name: str
    description: Optional[str] = None
    value_type: str = "string"
    allowed_values: List[str] = field(default_factory=list)
    entity_types: List[str] = field(default_factory=list)
    cardinality: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RDFStructuredPropertyValue:
    """Internal representation of a structured property value assignment."""

    entity_uri: str
    property_uri: str
    property_name: str
    value: str
    entity_type: str  # 'dataset' or 'glossaryTerm'
    platform: Optional[str] = None  # Platform URN for datasets
    environment: Optional[str] = None  # Environment for the entity


@dataclass
class DataHubStructuredProperty:
    """Internal representation of a DataHub structured property."""

    urn: StructuredPropertyUrn
    name: str
    description: Optional[str] = None
    value_type: str = "urn:li:dataType:datahub.string"
    allowed_values: List[str] = field(default_factory=list)
    entity_types: List[str] = field(default_factory=list)
    cardinality: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubStructuredPropertyValue:
    """Internal representation of a DataHub structured property value assignment."""

    entity_urn: str  # URN of the entity (dataset or glossary term)
    property_urn: str  # URN of the structured property
    property_name: str
    value: str
    entity_type: str  # 'dataset' or 'glossaryTerm'
