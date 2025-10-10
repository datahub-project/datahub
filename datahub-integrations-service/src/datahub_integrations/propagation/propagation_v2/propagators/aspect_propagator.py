import logging
import time
from abc import ABC, abstractmethod
from typing import Generic, Iterator, Self, Sequence, TypeVar, TypeVarTuple, cast

# Import _Aspect directly from codegen - importing from schema_classes does not work for mypy
from datahub._codegen.aspect import _Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    MetadataAttributionClass,
    MetadataChangeProposalClass,
)
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic.fields import Field
from pydantic.main import BaseModel

from datahub_integrations.actions.bulk_bootstrap_action import EntityWithData
from datahub_integrations.propagation.propagation_v2.types.ece_enums import (
    ChangeCategory,
    ChangeOperation,
)
from datahub_integrations.propagation.propagation_v2.types.source_details import (
    SourceDetails,
)

logger = logging.getLogger(__name__)

ChangeEventDict = dict[str, dict[tuple[str, str], list[EntityChangeEvent]]]
PropagationOutput = Iterator[
    MetadataChangeProposalWrapper | MetadataChangeProposalClass
]


class AspectPropagatorConfig(BaseModel):
    class Config:
        extra = "forbid"

    enabled: bool = Field(
        True,
        description="Indicates whether propagation for relevant aspect is enabled.",
    )

    # Used for dataset -> schema field propagation
    # Users do not think of this data as propagated, so we shouldn't represent it as such
    omit_attribution_is_propagated: bool = Field(
        False,
        description="If true, propagated aspects will not include attribution information.",
    )

    max_propagation_depth: int = 5


OriginAspects = TypeVarTuple("OriginAspects")
TargetAspect = TypeVar("TargetAspect", bound=_Aspect)


class AspectPropagator(Generic[TargetAspect, *OriginAspects], ABC):
    actor_urn = "urn:li:corpuser:__datahub_system"

    def __init__(
        self, action_urn: str, ctx: PipelineContext, config: AspectPropagatorConfig
    ):
        self.ctx = ctx
        self.config = config
        self.action_urn = action_urn

    @classmethod
    def create(cls, action_urn: str, config_dict: dict, ctx: PipelineContext) -> Self:
        action_config = AspectPropagatorConfig.model_validate(config_dict or {})
        return cls(action_urn, ctx, action_config)

    @staticmethod
    def _now() -> int:
        """Return the current time in milliseconds since epoch."""
        return int(time.time() * 1000)

    @classmethod
    def _propagation_audit_stamp(cls) -> AuditStampClass:
        return AuditStampClass(actor=cls.actor_urn, time=cls._now())

    @abstractmethod
    def origin_aspects(self) -> Sequence[type[_Aspect]]:
        """Returns the Aspects that this propagator needs to fetch for the origin entity."""

    @abstractmethod
    def target_aspect(self) -> type[TargetAspect]:
        """Returns the Aspect that this propagator handles."""

    @abstractmethod
    def empty_aspects(self) -> Sequence[_Aspect]:
        """Returns an instance of the empty aspect for each aspect that this propagator handles.
        The length and order must match that of `aspects()`.

        Used to rollback propagated aspects.
        """

    def empty_entity(self, urn: str) -> EntityWithData:
        return EntityWithData(
            urn=urn,
            aspects={aspect.ASPECT_NAME: aspect for aspect in self.empty_aspects()},
        )

    @abstractmethod
    def category(self) -> ChangeCategory:
        """Returns the category of changes that this propagator handles."""

    def supported_change_operations(self) -> set[str]:
        """Returns the set of change operations that this propagator supports, as strings."""
        return {op.value for op in self._supported_change_operations()}

    @abstractmethod
    def _supported_change_operations(self) -> set[ChangeOperation]:
        """Returns the set of change operations that this propagator supports."""

    def compute_diff_eces(
        self, *, origin: EntityWithData, target: EntityWithData
    ) -> Iterator[EntityChangeEvent]:
        """Calculate the difference between two entities and return MCPs that would propagate
        the relevant aspect's data from origin to target.

        MCPs are created based on and with appropriate attribution information.
        """

        origin_aspect = tuple(
            [origin.get_aspect(aspect) for aspect in self.origin_aspects()]
        )
        target_aspect = target.get_aspect(self.target_aspect())
        if any(origin_aspect):
            # Don't think the Python type system can figure out typing here without cast
            # Implementers need to remember that only one of the aspects is guaranteed to be non-null
            yield from self._compute_diff_eces_internal(
                origin_urn=origin.urn,
                target_urn=target.urn,
                origin_aspects=cast(tuple[*OriginAspects], origin_aspect),
                target_aspect=target_aspect,
            )

    @abstractmethod
    def _compute_diff_eces_internal(
        self,
        *,
        origin_urn: str,
        target_urn: str,
        origin_aspects: tuple[*OriginAspects],
        target_aspect: TargetAspect | None,
    ) -> Iterator[EntityChangeEvent]:
        """Implements the logic for `compute_diff_eces`, with a cleaner type signature."""

    def compute_propagation_mcps(
        self, change_events: ChangeEventDict
    ) -> PropagationOutput:
        for mcp in self._compute_propagation_mcps(change_events):
            logger.info(mcp)
            yield mcp

    @abstractmethod
    def _compute_propagation_mcps(
        self, change_events: ChangeEventDict
    ) -> PropagationOutput:
        """Produce MCPs that propagate Entity Change Events.

        Args:
            change_events: Mapping target urn -> change operation -> via (origin) urn -> ece
        """

    def _compute_attribution(
        self, old_source_details: SourceDetails, via_urn: str
    ) -> MetadataAttributionClass | None:
        origin = old_source_details.origin or via_urn
        new_source_details = SourceDetails(
            propagated=not self.config.omit_attribution_is_propagated,
            origin=origin,
            via=via_urn if origin != via_urn else None,
            propagation_depth=(old_source_details.propagation_depth or 0) + 1,
            actor=old_source_details.actor or self.actor_urn,
            propagation_started_at=old_source_details.propagation_started_at
            or self._now(),
        )
        return MetadataAttributionClass(
            source=self.action_urn,
            time=self._now(),
            actor=self.actor_urn
            if not new_source_details.actor
            else new_source_details.actor,
            sourceDetail=new_source_details.for_metadata_attribution(),
        )

    def should_fetch_schema_field_parent_schema_metadata(self) -> bool:
        """If, when processing a schema field urn, we need to fetch
        its parent's SchemaMetadata and EditableSchemaMetadata aspects.

        Needed for docs propagation to perform a full diff.
        """
        return False
