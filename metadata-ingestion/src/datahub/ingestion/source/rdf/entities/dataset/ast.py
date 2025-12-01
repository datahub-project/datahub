"""
AST classes for Dataset entity.

Defines RDF and DataHub AST representations for datasets.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Import assertion types from assertion module
from datahub.ingestion.source.rdf.entities.assertion.ast import RDFAssertion

# DataHub SDK imports
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn


@dataclass
class RDFSchemaField:
    """Represents a schema field from RDF data."""

    name: str
    field_type: str
    description: Optional[str] = None
    nullable: bool = True
    glossary_term_urns: List[str] = field(default_factory=list)
    dataset: Optional["RDFDataset"] = None  # Pointer back to owning dataset
    properties: Dict[str, Any] = field(default_factory=dict)
    contextual_constraints: Dict[str, Any] = field(
        default_factory=dict
    )  # sh:minCount, sh:maxCount, etc.
    property_shape_uri: Optional[str] = None  # URI of the SHACL property shape


@dataclass
class RDFDataset:
    """Internal representation of a dataset extracted from RDF."""

    uri: str
    name: str
    platform: str
    description: Optional[str] = None
    environment: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    schema_fields: List[RDFSchemaField] = field(default_factory=list)
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    assertions: List[RDFAssertion] = field(default_factory=list)
    # SHACL support
    schema_shape_uri: Optional[str] = None  # Reference to sh:NodeShape


@dataclass
class DataHubDataset:
    """Internal representation of a DataHub dataset."""

    urn: DatasetUrn
    name: str
    environment: str
    description: Optional[str] = None
    platform: Optional[str] = None  # No defaulting - use actual value or None
    properties: Dict[str, Any] = field(default_factory=dict)
    schema_fields: List[SchemaFieldClass] = field(default_factory=list)
    structured_properties: List[StructuredPropertyValueAssignmentClass] = field(
        default_factory=list
    )
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    path_segments: List[str] = field(default_factory=list)  # Hierarchical path from IRI
    field_glossary_relationships: List[Dict[str, str]] = field(
        default_factory=list
    )  # field_name -> glossary_term_urn
