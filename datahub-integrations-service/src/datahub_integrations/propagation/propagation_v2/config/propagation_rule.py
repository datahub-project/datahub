from enum import Enum
from typing import Annotated, Literal

from datahub.configuration.common import ConfigModel
from pydantic import Field

SourceType = str
DestinationType = str
RelationshipType = str


class LookupType(str, Enum):
    ENTITY = "entity"
    ASPECT = "aspect"
    RELATIONSHIP = "relationship"
    SCHEMA_FIELD = "schema_field"


class EntityLookupConfig(ConfigModel):
    lookup_type: Literal["entity"]
    entity_type: str

    query: str | None = Field(
        None, description="Elasticsearch query to filter entities"
    )


class AspectLookup(ConfigModel):
    lookup_type: Literal["aspect"]
    aspect_name: str
    field: str  # Can denote a chain of fields, separated by periods

    # Whether to look at the MCL's previous or new aspect value; will usually be new aspect value
    use_previous_aspect: bool = False

    # If true, the field referenced denotes a schema field path, not an urn.
    # A schema field urn can be constructed from the aspect's urn + this field path.
    is_schema_field_path: bool = False


class RelationshipLookup(ConfigModel):
    lookup_type: Literal["relationship"]
    relationship_type: RelationshipType

    # Whether the *origin* is the source in the relationship edge
    origin_is_source: bool = False


TargetUrnResolutionLookup = Annotated[
    AspectLookup | RelationshipLookup,
    Field(discriminator="lookup_type"),
]

OriginUrnResolutionLookup = Annotated[
    EntityLookupConfig | RelationshipLookup, Field(discriminator="lookup_type")
]


class MetadataToPropagate(str, Enum):
    TAGS = "tags"
    TERMS = "terms"
    DOCUMENTATION = "documentation"
    OWNERSHIP = "ownership"
    STRUCTURED_PROPERTIES = "structured_properties"


class PropagationRule(ConfigModel):
    # Describes what metadata is propagated and what events to respond to
    # Value is passed to appropriate propagator for its own config
    metadata_propagated: dict[MetadataToPropagate, dict] = {}

    # Describes how to fetch origin urns during bootstrap
    origin_urn_resolution: OriginUrnResolutionLookup

    # Describes function origin urn -> target urns, the propagation destinations
    target_urn_resolution: list[TargetUrnResolutionLookup] | Literal["schema_field"]

    # TODO: Support entity_types filter. Requires updating relationships scroll.
    # Additional entity_type filter for ease of use
    # entity_types: list[str] = ["dataset", "schemaField"]
