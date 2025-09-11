import logging
from typing import Iterator

from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.utilities.urns.urn import Urn
from datahub_actions.event.event_registry import EntityChangeEvent
from pydantic import ValidationError

from datahub_integrations.propagation.propagation_v2.propagators.aspect_propagator import (
    AspectPropagator,
    ChangeEventDict,
    PropagationOutput,
)
from datahub_integrations.propagation.propagation_v2.types.ece_enums import (
    ChangeCategory,
    ChangeOperation,
)
from datahub_integrations.propagation.propagation_v2.types.source_details import (
    SourceDetails,
)

logger = logging.getLogger(__name__)


class TagPropagator(AspectPropagator[GlobalTagsClass]):
    # TODO: Support custom config, init, and create to customize propagator

    def aspects(self) -> tuple[type[GlobalTagsClass]]:
        return (GlobalTagsClass,)

    def empty_aspects(self) -> tuple[GlobalTagsClass]:
        return (GlobalTagsClass(tags=[]),)

    def category(self) -> ChangeCategory:
        return ChangeCategory.TAG

    def _supported_change_operations(self) -> set[ChangeOperation]:
        return {ChangeOperation.ADD, ChangeOperation.REMOVE}

    def _compute_diff_eces_internal(
        self,
        origin_urn: str,
        target_urn: str,
        origin_aspects: tuple[GlobalTagsClass],
        target_aspects: tuple[GlobalTagsClass | None],
    ) -> Iterator[EntityChangeEvent]:
        origin_aspect = origin_aspects[0]
        target_aspect = target_aspects[0]
        if target_aspect:
            # Filter to only tags attributed to this propagation action
            relevant_tags = []
            for tag in target_aspect.tags:
                if not tag.attribution:
                    continue
                try:
                    source_details = SourceDetails.model_validate(
                        tag.attribution.sourceDetail
                    )
                except ValidationError:
                    logger.warning(f"Invalid tag attribution for tag {tag}. Skipping.")
                    continue

                if tag.attribution.source == self.action_urn and (
                    source_details.origin == origin_urn
                    or source_details.via == origin_urn
                ):
                    relevant_tags.append(tag)

            target_aspect = GlobalTagsClass(tags=relevant_tags)

        return compute_tag_diff_eces(
            entity_urn=origin_urn,
            old_aspect=target_aspect,
            new_aspect=origin_aspect,
            audit_stamp=self._propagation_audit_stamp(),
        )

    def _compute_propagation_mcps(
        self, change_events: ChangeEventDict
    ) -> PropagationOutput:
        for target_urn, ece_map in change_events.items():
            patch_builder = HasTagsPatch(target_urn)
            for (operation, via_urn), eces in ece_map.items():
                for ece in eces:
                    if ece.category == ChangeCategory.TAG.value:
                        self._process_ece(via_urn, operation, ece, patch_builder)
            yield from patch_builder.build()

    def _process_ece(
        self,
        via_urn: str,
        operation: str,
        ece: EntityChangeEvent,
        patch_builder: HasTagsPatch,
    ) -> None:
        tag_urn = ece.modifier or ece.safe_parameters.get("tagUrn")
        if not tag_urn:
            logger.warning(f"Could not determine tag urn for ECE: {ece}.")
            return

        if operation == ChangeOperation.REMOVE.value:
            # TODO: Remote tag based on attribution
            patch_builder.remove_tag(tag_urn)
            return

        if operation == ChangeOperation.ADD.value:
            context = ece.safe_parameters.get("context")
            try:
                old_source_details = SourceDetails.model_validate(context)
            except ValidationError:
                old_source_details = SourceDetails()

            attribution = self._compute_attribution(old_source_details, via_urn)
            patch_builder.add_tag(
                TagAssociationClass(
                    tag=tag_urn,
                    context=context if context and context != "{}" else None,
                    attribution=attribution,
                )
            )
            return


def compute_tag_diff_eces(
    entity_urn: str,
    old_aspect: GlobalTagsClass | None,
    new_aspect: GlobalTagsClass,
    audit_stamp: AuditStampClass,
) -> Iterator[EntityChangeEvent]:
    """
    Compute differences between two GlobalTagsClass aspects and return ChangeEvent objects.

    This is the Python equivalent of GlobalTagsChangeEventGenerator.computeDiffs() from Java,
    although logic is significantly different, using Python's set operations for clarity.

    Args:
        entity_urn: URN of the entity being changed
        old_aspect: The original global tags aspect
        new_aspect: The new global tags aspect
        audit_stamp: Audit stamp with information about how the ECEs were created

    Returns:
        List of EntityChangeEventClass objects representing the differences
    """

    old_tag_association_map = (
        {tag_assoc.tag: tag_assoc for tag_assoc in old_aspect.tags}
        if old_aspect
        else {}
    )
    new_tag_association_map = (
        {tag_assoc.tag: tag_assoc for tag_assoc in new_aspect.tags}
        if new_aspect
        else {}
    )

    tags_added = new_tag_association_map.keys() - old_tag_association_map.keys()
    tags_removed = old_tag_association_map.keys() - new_tag_association_map.keys()

    for tag in tags_added:
        tag_assoc = new_tag_association_map[tag]
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.TAG.value,
            operation=ChangeOperation.ADD.value,
            parameters={"context": tag_assoc.context},  # type: ignore
            auditStamp=audit_stamp,
            modifier=tag,
            version=0,
        )
    for tag in tags_removed:
        tag_assoc = old_tag_association_map[tag]
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.TAG.value,
            operation=ChangeOperation.REMOVE.value,
            parameters={"context": tag_assoc.context},  # type: ignore
            auditStamp=audit_stamp,
            modifier=tag,
            version=0,
        )
