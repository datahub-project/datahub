import logging
from typing import Iterator

from datahub.metadata.schema_classes import (
    AuditStampClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)
from datahub.utilities.urns.urn import Urn
from datahub_actions.event.event_registry import EntityChangeEvent
from pydantic import ValidationError

from datahub_integrations.propagation.propagation_v2.propagators.aspect_propagator import (
    AspectPropagator,
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


class StructuredPropertyPropagator(AspectPropagator[StructuredPropertiesClass]):
    # TODO: Support custom config, init, and create to customize propagator

    def aspects(self) -> tuple[type[StructuredPropertiesClass]]:
        return (StructuredPropertiesClass,)

    def supported_change_operations(self) -> set[ChangeOperation]:
        return {ChangeOperation.ADD, ChangeOperation.REMOVE, ChangeOperation.MODIFY}

    def _compute_diff_eces_internal(
        self,
        *,
        origin_urn: str,
        target_urn: str,
        origin_aspects: tuple[StructuredPropertiesClass],
        target_aspects: tuple[StructuredPropertiesClass | None],
    ) -> Iterator[EntityChangeEvent]:
        origin_aspect = origin_aspects[0]
        target_aspect = target_aspects[0]
        if target_aspect:
            # Filter to only structured properties attributed to this propagation action
            relevant_property_value_assignments = []
            for property in target_aspect.properties:
                if not property.attribution:
                    continue
                try:
                    source_details = SourceDetails.model_validate(
                        property.attribution.sourceDetail
                    )
                except ValidationError:
                    logger.warning(
                        f"Invalid attribution for property {property}. Skipping."
                    )
                    continue

                if property.attribution.source == self.action_urn and (
                    source_details.origin == origin_urn
                    or source_details.via == origin_urn
                ):
                    relevant_property_value_assignments.append(property)

            target_aspect = StructuredPropertiesClass(
                relevant_property_value_assignments
            )

        return compute_structured_property_diff_mcps(
            entity_urn=origin_urn,
            old_aspect=target_aspect,
            new_aspect=origin_aspect,
            audit_stamp=self._propagation_audit_stamp(),
        )

    def compute_propagation_mcps(
        self, change_events: dict[str, dict[str, dict[str, EntityChangeEvent]]]
    ) -> PropagationOutput:
        for target_urn, ece_map in change_events.items():
            patch_builder = HasStructuredPropertiesPatch(target_urn)
            for operation, eces in ece_map.items():
                for via_urn, ece in eces.items():
                    if ece.category == ChangeCategory.STRUCTURED_PROPERTY.value:
                        self._process_ece(via_urn, operation, ece, patch_builder)
            yield from patch_builder.build()

    def _process_ece(
        self,
        via_urn: str,
        operation: str,
        ece: EntityChangeEvent,
        patch_builder: HasStructuredPropertiesPatch,
    ) -> None:
        property_urn = ece.modifier or ece.parameters.get("propertyUrn")
        property_values = ece.parameters.get("propertyValues")
        if not property_urn or property_values is None:
            logger.warning(
                f"Could not find structured property urn or property values for ECE: {ece}. Skipping."
            )
            return

        if operation == ChangeOperation.REMOVE.value:
            # TODO: Remove structured property based on attribution
            patch_builder.remove_structured_property(property_urn)
            return
        elif operation == ChangeOperation.ADD.value:
            # TODO: Update to support passing source details in ECE
            old_source_details = SourceDetails()  # Allows all nulls
            attribution = self._compute_attribution(old_source_details, via_urn)
            patch_builder.add_structured_property_manual(
                StructuredPropertyValueAssignmentClass(
                    propertyUrn=property_urn,
                    values=property_values,
                    created=self._propagation_audit_stamp(),
                    lastModified=self._propagation_audit_stamp(),
                    attribution=attribution,
                )
            )
            return
        elif operation == ChangeOperation.MODIFY.value:
            # TODO: Update to support passing source details in ECE
            old_source_details = SourceDetails()  # Allows all nulls
            attribution = self._compute_attribution(old_source_details, via_urn)
            patch_builder.set_structured_property_manual(
                StructuredPropertyValueAssignmentClass(
                    propertyUrn=property_urn,
                    values=property_values,
                    created=self._propagation_audit_stamp(),
                    lastModified=self._propagation_audit_stamp(),
                    attribution=attribution,
                )
            )


def compute_structured_property_diff_mcps(
    entity_urn: str,
    old_aspect: StructuredPropertiesClass | None,
    new_aspect: StructuredPropertiesClass,
    audit_stamp: AuditStampClass,
) -> Iterator[EntityChangeEvent]:
    """
    Compute differences between two StructuredPropertiesClass aspects and return ChangeEvent objects.

    This is the Python equivalent of StructuredPropertyChangeEventGenerator.computeDiffs() from Java,
    although logic is significantly different, using Python's set operations for clarity.

    Args:
        entity_urn: URN of the entity being changed
        old_aspect: The original structured properties aspect
        new_aspect: The new structured properties aspect
        audit_stamp: Audit stamp with information about how the ECEs were created

    Returns:
        List of EntityChangeEventClass objects representing the differences
    """

    old_property_association_map = (
        {
            property_assoc.propertyUrn: property_assoc
            for property_assoc in old_aspect.properties
        }
        if old_aspect
        else {}
    )
    new_property_association_map = (
        {
            property_assoc.propertyUrn: property_assoc
            for property_assoc in new_aspect.properties
        }
        if new_aspect
        else {}
    )

    properties_added = (
        new_property_association_map.keys() - old_property_association_map.keys()
    )
    properties_removed = (
        old_property_association_map.keys() - new_property_association_map.keys()
    )
    properties_potentially_modified = (
        new_property_association_map.keys() & old_property_association_map.keys()
    )

    # TODO: Support passing attribution in ECE, to support multi-hop structured property propagation
    for property in properties_added:
        property_assoc = new_property_association_map[property]
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.STRUCTURED_PROPERTY.value,
            operation=ChangeOperation.ADD.value,
            parameters={  # type: ignore
                "propertyUrn": property,
                "propertyValues": property_assoc.values,
            },
            auditStamp=audit_stamp,
            modifier=property,
            version=0,
        )
    for property in properties_removed:
        property_assoc = old_property_association_map[property]
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.STRUCTURED_PROPERTY.value,
            operation=ChangeOperation.REMOVE.value,
            parameters={  # type: ignore
                "propertyUrn": property,
                "propertyValues": property_assoc.values,
            },
            auditStamp=audit_stamp,
            modifier=property,
            version=0,
        )
    for property in properties_potentially_modified:
        old_property_assoc = old_property_association_map[property]
        new_property_assoc = new_property_association_map[property]
        if old_property_assoc.values != new_property_assoc.values:
            yield EntityChangeEvent(
                entityUrn=entity_urn,
                entityType=Urn.from_string(entity_urn).entity_type,
                category=ChangeCategory.STRUCTURED_PROPERTY.value,
                operation=ChangeOperation.MODIFY.value,
                parameters={  # type: ignore
                    "propertyUrn": property,
                    "propertyValues": new_property_assoc.values,
                },
                auditStamp=audit_stamp,
                modifier=property,
                version=0,
            )
