"""
AST classes for Lineage entity.

Defines RDF and DataHub AST representations for lineage.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

# DataHub SDK imports
from datahub.utilities.urns.data_job_urn import DataJobUrn


class LineageType(Enum):
    """Types of lineage relationships."""

    USED = "used"  # prov:used - upstream dependency
    GENERATED = "generated"  # prov:generated - downstream product
    WAS_DERIVED_FROM = "was_derived_from"  # prov:wasDerivedFrom - direct derivation
    WAS_GENERATED_BY = "was_generated_by"  # prov:wasGeneratedBy - activity-to-entity
    WAS_INFLUENCED_BY = (
        "was_influenced_by"  # prov:wasInfluencedBy - downstream influence
    )


@dataclass
class RDFLineageActivity:
    """Represents a PROV-O activity (data processing job)."""

    uri: str
    name: str
    platform: str
    description: Optional[str] = None
    environment: Optional[str] = None
    started_at_time: Optional[str] = None
    ended_at_time: Optional[str] = None
    was_associated_with: Optional[str] = None  # User/agent URI
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RDFLineageRelationship:
    """Represents a lineage relationship between entities."""

    source_uri: str
    target_uri: str
    lineage_type: LineageType
    activity_uri: Optional[str] = None  # For activity-mediated relationships
    source_platform: Optional[str] = None  # Platform URN for source entity
    target_platform: Optional[str] = None  # Platform URN for target entity
    activity_platform: Optional[str] = None  # Platform URN for activity
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubLineageActivity:
    """Internal representation of a DataHub data job."""

    urn: DataJobUrn
    name: str
    description: Optional[str] = None
    started_at_time: Optional[str] = None
    ended_at_time: Optional[str] = None
    was_associated_with: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubLineageRelationship:
    """Internal representation of a DataHub lineage relationship."""

    source_urn: str  # Can be DatasetUrn or SchemaFieldUrn
    target_urn: str  # Can be DatasetUrn or SchemaFieldUrn
    lineage_type: LineageType
    activity_urn: Optional[DataJobUrn] = None
    properties: Dict[str, Any] = field(default_factory=dict)
