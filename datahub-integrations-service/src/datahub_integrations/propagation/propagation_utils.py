import logging
import time
from abc import abstractmethod
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.metadata.schema_classes import MetadataAttributionClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from pydantic.fields import Field
from pydantic.main import BaseModel

logger = logging.getLogger(__name__)

SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"


class RelationshipType(Enum):
    LINEAGE = "lineage"  # signifies all types of lineage
    HIERARCHY = "hierarchy"  # signifies all types of hierarchy
    SIBLINGS = "siblings"  # signifies sibling relationships


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
) -> Tuple[MetadataAttributionClass, dict[str, str]]:
    """
    Given a propagation directive, return the attribution and context for
    the directive.
    Attribution is the official way to track the source of metadata in
    DataHub.
    Context is the older way to track the source of metadata in DataHub.
    We populate both to ensure compatibility with older versions of DataHub.
    """
    source_detail: dict[str, str] = {
        "origin": propagation_directive.origin,
        "propagated": "true",
    }
    if propagation_directive.actor:
        source_detail["actor"] = propagation_directive.actor
    else:
        source_detail["actor"] = actor
    if propagation_directive.via:
        source_detail["via"] = propagation_directive.via
    context_dict: dict[str, str] = {}
    context_dict.update(source_detail)
    return (
        MetadataAttributionClass(
            time=time,
            actor=actor,
            source=action_urn,
            sourceDetail=source_detail,
        ),
        context_dict,
    )


class SelectedAsset(BaseModel):
    """
    A selected asset is a data structure that represents an asset that has been
    selected for processing by a propagator.
    """

    urn: str  # URN of the asset that has been selected
    target_entity_type: str  # entity type that is being targeted by the propagator. e.g. schemaField even if asset is of type dataset


class ComposablePropagator:
    @abstractmethod
    def asset_filters(self) -> Dict[str, Dict[str, List[RawSearchFilterRule]]]:
        """
        Returns a dictionary of asset filters that are used to filter the assets
        based on the configuration of the action.
        """
        pass

    @abstractmethod
    def process_one_asset(
        self, asset: SelectedAsset, operation: str
    ) -> Iterable[PropagationDirective]:
        """
        Given an asset, returns a list of propagation directives

        :param asset_urn: URN of the asset
        :param target_entity_type: The entity type of the target entity (Note:
            this can be different from the entity type of the asset. e.g. we
            might process a dataset while the target entity_type is a column
            (schemaField))
        :param operation: The operation that triggered the propagation (ADD /
            REMOVE)
        :return: A list of PropagationDirective objects
        """
        pass


def get_unique_siblings(graph: AcrylDataHubGraph, entity_urn: str) -> list[str]:
    """
    Get unique siblings for the entity urn
    """

    if entity_urn.startswith("urn:li:schemaField"):
        try:
            parent_urn = Urn.create_from_string(entity_urn).get_entity_id()[0]
            entity_field_path = Urn.create_from_string(entity_urn).get_entity_id()[1]
        except Exception:
            logger.warning(
                f"Failed to extract parent urn and field path from {entity_urn}. Cannot find siblings.."
            )
            return []

        # Does my parent have siblings?
        parent_siblings: Optional[models.SiblingsClass] = graph.graph.get_aspect(
            parent_urn,
            models.SiblingsClass,
        )
        if parent_siblings and parent_siblings.siblings:
            other_siblings = [x for x in parent_siblings.siblings if x != parent_urn]
            sibling_urns = []
            for target_sibling in other_siblings:
                # now we need to find the schema field in this sibling that
                # matches us
                if target_sibling.startswith("urn:li:dataset"):
                    schema_fields = graph.graph.get_aspect(
                        target_sibling, models.SchemaMetadataClass
                    )
                    if schema_fields:
                        for schema_field in schema_fields.fields:
                            if schema_field.fieldPath == entity_field_path:
                                # we found the sibling field
                                schema_field_urn = make_schema_field_urn(
                                    target_sibling, schema_field.fieldPath
                                )
                                sibling_urns.append(schema_field_urn)
            return sibling_urns

    elif entity_urn.startswith("urn:li:dataset"):
        siblings: Optional[models.SiblingsClass] = graph.graph.get_aspect(
            entity_urn,
            models.SiblingsClass,
        )
        if siblings and siblings.siblings:
            other_siblings = [x for x in siblings.siblings if x != entity_urn]
            sibling_urns = []
            for sibling_urn in other_siblings:
                sibling_urns.append(sibling_urn)
            return sibling_urns

    return []


def filter_downstreams_by_entity_type(
    entity_urn: str, downstreams: list[str]
) -> list[str]:
    """
    Filters downstreams based on the entity type of the provided entity_urn.
    If the entity_urn is a schemaField, it filters downstreams to only include
    schemaFields. If the entity_urn is not a schemaField, it filters downstreams
    to exclude schemaFields. If the entity_urn is neither, it returns all downstreams.
    """
    if entity_urn.startswith("urn:li:schemaField"):
        return [
            downstream
            for downstream in downstreams
            if downstream.startswith("urn:li:schemaField")
        ]
    elif not entity_urn.startswith("urn:li:schemaField"):
        return [
            downstream
            for downstream in downstreams
            if not downstream.startswith("urn:li:schemaField")
        ]
    else:
        return downstreams  # If it's neither case (other asset-asset propagation), return all downstreams.
