"""
AST classes for Data Product entity.

Defines RDF and DataHub AST representations for data products.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RDFDataProductAsset:
    """Represents an asset (dataset) in a data product with platform information."""

    uri: str
    platform: Optional[str] = None  # Platform URN for the dataset


@dataclass
class RDFDataProduct:
    """Internal representation of a DataHub Data Product from RDF."""

    uri: str
    name: str
    description: Optional[str] = None
    domain: Optional[str] = None
    owner: Optional[str] = None  # Owner IRI from dprod:dataOwner
    owner_type: Optional[str] = (
        None  # Owner type string (supports custom types, from dh:hasOwnerType or RDF type)
    )
    sla: Optional[str] = None
    quality_score: Optional[float] = None
    assets: List[RDFDataProductAsset] = field(
        default_factory=list
    )  # List of dataset assets with platform info
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubDataProduct:
    """Internal representation of a DataHub Data Product."""

    urn: str
    name: str
    description: Optional[str] = None
    domain: Optional[str] = None
    owner: Optional[str] = None  # Owner URN
    owner_type: Optional[str] = (
        None  # Owner type string (supports custom types defined in DataHub UI)
    )
    sla: Optional[str] = None
    quality_score: Optional[float] = None
    assets: List[str] = field(default_factory=list)  # List of dataset URNs
    properties: Dict[str, Any] = field(default_factory=dict)
