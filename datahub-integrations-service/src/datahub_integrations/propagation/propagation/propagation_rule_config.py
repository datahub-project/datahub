from enum import Enum
from typing import Dict, List, Literal, Optional, Union

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
    lookup_type: Literal["aspect"]
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
    relationship_names: List[str] = ["DownstreamOf"]
    # Whether to search for urn as search or destination.
    # Will usually be destination.
    is_source: Optional[bool] = False


EntityLookup = Union[AspectLookup, RelationshipLookup]


class PropagatedMetadata(str, Enum):
    TAGS = ("tags",)
    TERMS = ("terms",)
    DOCUMENTATION = ("documentation",)
    DOMAIN = ("domain",)
    STRUCTURED_PROPERTIES = ("structuredProperties",)


class MclTriggerRule(ConfigModel):
    trigger: AspectLookup
    source_urn_resolution: List[EntityLookup]
    target_urn_resolution: List[EntityLookup]


class PropagationRule(ConfigModel):
    metadata_propagated: Dict[PropagatedMetadata, Dict] = {}
    entity_types: List[str] = ["dataset", "schemaField"]
    target_urn_resolution: List[EntityLookup]
    bootstrap: Optional[List[SearchFilterRule]] = None
