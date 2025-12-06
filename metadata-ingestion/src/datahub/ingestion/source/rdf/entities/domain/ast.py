"""
AST classes for Domain entity.

Defines DataHub AST representation for domains.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

# DataHub SDK imports
from datahub.utilities.urns.domain_urn import DomainUrn

# Forward references to avoid circular imports
if TYPE_CHECKING:
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )


@dataclass
class DataHubDomain:
    """Internal representation of a DataHub domain (for glossary terms)."""

    path_segments: List[str]  # Hierarchical path segments from IRI
    urn: DomainUrn  # DataHub domain URN
    name: str  # Domain name (last segment)
    parent_domain_urn: Optional[DomainUrn] = None  # Parent domain URN for hierarchy
    glossary_terms: List["DataHubGlossaryTerm"] = field(default_factory=list)
    subdomains: List["DataHubDomain"] = field(default_factory=list)
