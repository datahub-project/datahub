from enum import Enum
from typing import Dict, List, NewType, Optional, Union

from datahub.configuration.common import ConfigModel
from datahub.ingestion.graph.filters import SearchFilterRule


class LookupType(str, Enum):
    ASPECT = "aspect"
    RELATIONSHIP = "relationship"


class PropagationRelationships(str, Enum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


class AspectReference(ConfigModel):
    name: str
    field: str


class AspectLookup(ConfigModel):
    lookup_type: str = LookupType.ASPECT.value
    aspect_name: str
    field: str  # Can denote a chain of fields, separated by periods
    # Whether to look at the MCL's previous or new aspect value.
    # Will usually be new aspect value.
    use_previous_aspect: Optional[bool] = False


class RelationshipReference(ConfigModel):
    name: str
    is_source: bool = False


class RelationshipLookup(ConfigModel):
    lookup_type: str = LookupType.RELATIONSHIP.value
    type: PropagationRelationships
    relationshipNames: List[str] = ["DownstreamOf"]
    # Whether to search for urn as search or destination.
    # Will usually be destination.
    isSource: Optional[bool] = False


class MCLLookup(ConfigModel):
    type: str
    field: str
    use_previous_value: bool = False


EntityLookup = Union[AspectLookup, RelationshipLookup]
Filter = NewType("Filter", str)


class PropagatedMetadata(str, Enum):
    TAGS = ("tags",)
    TERMS = ("terms",)
    DOCUMENTATION = ("documentation",)
    DOMAIN = ("domain",)
    STRUCTURED_PROPERTIES = ("structuredProperties",)


class MclTriggerRule(ConfigModel):
    trigger: AspectLookup
    sourceUrnResolution: List[EntityLookup]
    targetUrnResolution: List[EntityLookup]


class PropagationRule(ConfigModel):
    metadataPropagated: Dict[PropagatedMetadata, Dict] = {}
    entityTypes: List[str] = ["dataset", "schemaField"]
    targetUrnResolution: List[EntityLookup]
    bootstrap: Optional[List[SearchFilterRule]] = None
