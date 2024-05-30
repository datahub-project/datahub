import json
import time
from enum import Enum
from typing import Optional, Tuple

from datahub.metadata.schema_classes import MetadataAttributionClass
from pydantic.fields import Field
from pydantic.main import BaseModel

SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"


class RelationshipType(Enum):
    LINEAGE = "lineage"  # signifies all types of lineage
    HIERARCHY = "hierarchy"  # signifies all types of hierarchy


class DirectionType(Enum):
    UP = "up"  # signifies upstream or parent (depending on relationship type)
    DOWN = "down"  # signifies downstream or child (depending on relationship type)
    ALL = "all"  # signifies all directions


class PropagationDirective(BaseModel):
    propagate: bool
    operation: str
    relationship: RelationshipType = RelationshipType.LINEAGE
    direction: DirectionType = DirectionType.UP
    entity: str = Field(
        description="Entity that currently triggered the propagation directive",
    )
    origin: str = Field(
        description="Origin entity for the association. This is the entity that triggered the propagation.",
    )
    via: Optional[str] = Field(
        None,
        description="Via entity for the association. This is the direct entity that the propagation came through.",
    )
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the propagation through the original association.",
    )


def get_attribution_and_context_from_directive(
    action_urn: str,
    propagation_directive: PropagationDirective,
    actor: str = SYSTEM_ACTOR,
    time: int = int(time.time() * 1000.0),
) -> Tuple[MetadataAttributionClass, str]:
    """
    Given a propagation directive, return the attribution and context for
    the directive.
    Attribution is the official way to track the source of metadata in
    DataHub.
    Context is the older way to track the source of metadata in DataHub.
    We populate both to ensure compatibility with older versions of DataHub.
    """
    source_detail: dict[str, str] = {"origin": propagation_directive.origin}
    if propagation_directive.actor:
        source_detail["actor"] = propagation_directive.actor
    else:
        source_detail["actor"] = actor
    if propagation_directive.via:
        source_detail["via"] = propagation_directive.via
    context_dict: dict[str, str] = {"propagated": "true"}
    context_dict.update(source_detail)
    return (
        MetadataAttributionClass(
            time=time,
            actor=actor,
            source=action_urn,
            sourceDetail=source_detail,
        ),
        json.dumps(context_dict),
    )
