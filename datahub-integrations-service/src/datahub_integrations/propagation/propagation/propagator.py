import logging
import time
from abc import abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

from datahub._codegen.aspect import _Aspect
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from pydantic.fields import Field
from pydantic.main import BaseModel

from datahub_integrations.propagation.propagation.propagation_rule_config import (
    PropagationRelationships,
    PropagationRule,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationDirective,
    PropertyPropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.utils.ece_utils import ECEProcessor

logger = logging.getLogger(__name__)

PropagationOutput = Iterable[
    Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
]


class EntityPropagatorConfig(BaseModel):
    class Config:
        extra = "forbid"

    enabled: bool = Field(
        default=True, description="Indicates whether entity propagation is enabled."
    )

    propagation_rule: PropagationRule = Field(
        description="Rule to determine if a property should be propagated.",
    )

    entity_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Pattern for entities urns to propagate.",
    )

    propagation_relationships: List[PropagationRelationships] = Field(
        default=[
            PropagationRelationships.DOWNSTREAM,
            PropagationRelationships.UPSTREAM,
        ],
        description="Allowed propagation relationships.",
    )

    max_propagation_time_millis: int = 1000 * 60 * 60 * 1  # 1 hour
    max_propagation_depth: int = 5


class EntityPropagator:
    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: EntityPropagatorConfig
    ):
        self.graph = graph
        self.config = config
        self.actor_urn = "urn:li:corpuser:__datahub_system"
        self.action_urn = action_urn
        self.mcl_processor = MCLProcessor()
        self.ece_processor = ECEProcessor()
        self.only_one_upstream_allowed = False

    @abstractmethod
    def aspects(self) -> List[Type[_Aspect]]:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def get_metadata_from_aspect(self, aspect: Aspect) -> List[Any]:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def boostrap_asset(self, urn: Urn) -> Iterable[PropagationDirective]:
        raise NotImplementedError("Method not implemented")

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[PropertyPropagationDirective]:
        """Determine if the event should trigger property propagation."""
        if self.mcl_processor.is_mcl(event):
            return self.mcl_processor.process(event)

        if self.ece_processor.is_ece(event):
            return self.ece_processor.process(event)

        return None

    def get_supported_entity_types(self) -> List[str]:
        properties = set(self.ece_processor.entity_aspect_processors.keys())
        return list(properties)

    def get_supported_aspects(self) -> List[str]:
        aspects: List[str] = []
        for key in self.ece_processor.entity_aspect_processors.keys():
            aspects.extend(self.ece_processor.entity_aspect_processors[key].keys())
        for key in self.mcl_processor.entity_aspect_processors.keys():
            aspects.extend(self.mcl_processor.entity_aspect_processors[key].keys())

        return list(set(aspects))

    def should_stop_propagation(
        self, source_details: SourceDetails
    ) -> Tuple[bool, str]:
        """
        Check if the propagation should be stopped based on the source details.
        Return result and reason.
        """
        if source_details.propagation_started_at and (
            int(time.time() * 1000.0) - source_details.propagation_started_at
            >= self.config.max_propagation_time_millis
        ):
            return (True, "Propagation time exceeded.")
        if (
            source_details.propagation_depth
            and source_details.propagation_depth >= self.config.max_propagation_depth
        ):
            return (True, "Propagation depth exceeded.")
        return False, ""

    @abstractmethod
    def create_property_change_proposal(
        self,
        propagation_directive: PropagationDirective,
        entity_urn: Urn,
        context: SourceDetails,
    ) -> PropagationOutput:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def asset_filters(self) -> Dict[str, Dict[str, List[SearchFilterRule]]]:
        # It returns a dictionary of asset filters.
        # The key is the target entity type.
        # The value is a dictionary of index filters.
        # The key is the index name.
        # The value is a list of search filters.
        # Example:
        # {
        #     "dataset": {
        #         "dataset_index": [
        #             SearchFilterRule(
        #                 field="dataset_urn",
        #                 operator="=",
        #                 value=dataset_urn,
        #             )
        # target_entity_type, index_filters in asset_filters.items():
        #             for index_name, filters in index_filters.items():
        pass

    @abstractmethod
    def rollbackable_assets(self) -> Iterable[Urn]:
        pass

    @abstractmethod
    def rollback_asset(self, asset: Urn) -> None:
        pass
