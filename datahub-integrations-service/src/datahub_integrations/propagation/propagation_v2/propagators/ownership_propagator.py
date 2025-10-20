import logging
from collections import defaultdict
from typing import Iterator

from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
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


class OwnershipPropagator(AspectPropagator[OwnershipClass, OwnershipClass]):
    # TODO: Support custom config, init, and create to customize propagator

    def origin_aspects(self) -> tuple[type[OwnershipClass]]:
        return (OwnershipClass,)

    def target_aspect(self) -> type[OwnershipClass]:
        return OwnershipClass

    def empty_aspects(self) -> tuple[OwnershipClass]:
        return (OwnershipClass(owners=[], lastModified=None),)

    def category(self) -> ChangeCategory:
        return ChangeCategory.OWNER

    def _supported_change_operations(self) -> set[ChangeOperation]:
        return {ChangeOperation.ADD, ChangeOperation.REMOVE}

    def _compute_diff_eces_internal(
        self,
        *,
        origin_urn: str,
        target_urn: str,
        origin_aspects: tuple[OwnershipClass],
        target_aspect: OwnershipClass | None,
    ) -> Iterator[EntityChangeEvent]:
        origin_aspect = origin_aspects[0]
        if target_aspect:
            # Filter to only owners attributed to this propagation action
            relevant_owners = []
            for owner in target_aspect.owners:
                if not owner.attribution:
                    continue
                try:
                    source_details = SourceDetails.model_validate(
                        owner.attribution.sourceDetail
                    )
                except ValidationError:
                    logger.warning(f"Invalid attribution for owner {owner}. Skipping.")
                    continue

                if owner.attribution.source == self.action_urn and (
                    source_details.origin == origin_urn
                    or source_details.via == origin_urn
                ):
                    relevant_owners.append(owner)

            target_aspect = OwnershipClass(
                relevant_owners, lastModified=target_aspect.lastModified
            )

        return compute_ownership_diff_eces(
            origin_urn=origin_urn,
            old_aspect=target_aspect,
            new_aspect=origin_aspect,
            audit_stamp=self._propagation_audit_stamp(),
        )

    def _compute_propagation_mcps(
        self, change_events: ChangeEventDict
    ) -> PropagationOutput:
        for target_urn, ece_map in change_events.items():
            patch_builder = HasOwnershipPatch(target_urn)
            for (operation, via_urn), eces in ece_map.items():
                for ece in eces:
                    if ece.category == ChangeCategory.OWNER.value:
                        self._process_ece(via_urn, operation, ece, patch_builder)
            yield from patch_builder.build()

    def _process_ece(
        self,
        via_urn: str,
        operation: str,
        ece: EntityChangeEvent,
        patch_builder: HasOwnershipPatch,
    ) -> None:
        owner_type = ece.safe_parameters.get("ownerType")
        owner_urn = ece.safe_parameters.get("ownerUrn")
        owner_type_urn = ece.safe_parameters.get("ownerTypeUrn")
        if not owner_type or not owner_urn:
            logger.warning(
                f"Could not determine owner type or owner urn for ECE: {ece}. Skipping."
            )
            return

        if operation == ChangeOperation.REMOVE.value:
            # TODO: Remove owner based on attribution
            patch_builder.remove_owner(owner_urn, owner_type)
            return
        elif operation == ChangeOperation.ADD.value:
            # TODO: Update to support passing source details in ECE
            old_source_details = SourceDetails()
            attribution = self._compute_attribution(old_source_details, via_urn)
            # TODO: Support passing ownership source?
            patch_builder.add_owner(
                OwnerClass(
                    owner=owner_urn,
                    type=owner_type,
                    typeUrn=owner_type_urn,
                    attribution=attribution,
                )
            )
            return

        # TODO: Handle MODIFY... probably need to pass more info in ECE


def compute_ownership_diff_eces(
    origin_urn: str,
    old_aspect: OwnershipClass | None,
    new_aspect: OwnershipClass,
    audit_stamp: AuditStampClass,
) -> Iterator[EntityChangeEvent]:
    """
    Compute differences between two OwnershipClass aspects and return ChangeEvent objects.

    This is the Python equivalent of OwnershipChangeEventGenerator.computeDiffs() from Java,
    although logic is significantly different, using Python's set operations for clarity.

    Args:
        origin_urn: URN of the entity being changed
        old_aspect: The original ownership aspect
        new_aspect: The new ownership aspect
        audit_stamp: Audit stamp with information about how the ECEs were created

    Returns:
        List of EntityChangeEventClass objects representing the differences
    """

    # Maps owner type -> owner urn -> OwnerClass
    MapType = dict[tuple[str | OwnershipTypeClass, str | None], dict[str, OwnerClass]]
    old_owner_type_map: MapType = defaultdict(dict)
    if old_aspect:
        for owner_assoc in old_aspect.owners:
            typ = owner_assoc.type, owner_assoc.typeUrn
            old_owner_type_map[typ][owner_assoc.owner] = owner_assoc
    new_owner_type_map: MapType = defaultdict(dict)
    if new_aspect:
        for owner_assoc in new_aspect.owners:
            typ = owner_assoc.type, owner_assoc.typeUrn
            new_owner_type_map[typ][owner_assoc.owner] = owner_assoc

    for typ in old_owner_type_map.keys() | new_owner_type_map.keys():
        owner_type, owner_type_urn = typ
        new_owner_map = new_owner_type_map[typ]
        old_owner_map = old_owner_type_map[typ]
        owners_added = new_owner_map.keys() - old_owner_map.keys()
        owners_removed = old_owner_map.keys() - new_owner_map.keys()

        # TODO: Support passing attribution in ECE, to support multi-hop owner propagation
        for owner in owners_added:
            owner_assoc = new_owner_map[owner]
            yield EntityChangeEvent(
                entityUrn=origin_urn,
                entityType=Urn.from_string(origin_urn).entity_type,
                category=ChangeCategory.OWNER.value,
                operation=ChangeOperation.ADD.value,
                parameters={  # type: ignore
                    "ownerUrn": owner_assoc.owner,
                    "ownerType": owner_type,
                    "ownerTypeUrn": owner_type_urn,
                },
                auditStamp=audit_stamp,
                modifier=owner_type,
                version=0,
            )
        for owner in owners_removed:
            owner_assoc = old_owner_map[owner]
            yield EntityChangeEvent(
                entityUrn=origin_urn,
                entityType=Urn.from_string(origin_urn).entity_type,
                category=ChangeCategory.OWNER.value,
                operation=ChangeOperation.REMOVE.value,
                parameters={  # type: ignore
                    "ownerUrn": owner_assoc.owner,
                    "ownerType": owner_type,
                    "ownerTypeUrn": owner_type_urn,
                },
                auditStamp=audit_stamp,
                modifier=owner_type,
                version=0,
            )
