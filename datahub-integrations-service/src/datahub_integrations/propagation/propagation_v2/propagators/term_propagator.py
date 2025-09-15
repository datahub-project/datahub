import logging
from typing import Iterator

from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)
from datahub.specific.aspect_helpers.terms import HasTermsPatch
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


class GlossaryTermPropagator(AspectPropagator[GlossaryTermsClass]):
    # TODO: Support custom config, init, and create to customize propagator

    def aspects(self) -> tuple[type[GlossaryTermsClass]]:
        return (GlossaryTermsClass,)

    def empty_aspects(self) -> tuple[GlossaryTermsClass]:
        return (
            GlossaryTermsClass(terms=[], auditStamp=self._propagation_audit_stamp()),
        )

    def category(self) -> ChangeCategory:
        return ChangeCategory.GLOSSARY_TERM

    def _supported_change_operations(self) -> set[ChangeOperation]:
        return {ChangeOperation.ADD, ChangeOperation.REMOVE}

    def _compute_diff_eces_internal(
        self,
        *,
        origin_urn: str,
        target_urn: str,
        origin_aspects: tuple[GlossaryTermsClass],
        target_aspects: tuple[GlossaryTermsClass | None],
    ) -> Iterator[EntityChangeEvent]:
        origin_aspect = origin_aspects[0]
        target_aspect = target_aspects[0]
        if target_aspect:
            # Filter to only terms attributed to this propagation action
            relevant_terms = []
            for term in target_aspect.terms:
                if not term.attribution:
                    continue
                try:
                    source_details = SourceDetails.model_validate(
                        term.attribution.sourceDetail
                    )
                except ValidationError:
                    logger.warning(f"Invalid attribution for term {term}. Skipping.")
                    continue

                if term.attribution.source == self.action_urn and (
                    source_details.origin == origin_urn
                    or source_details.via == origin_urn
                ):
                    relevant_terms.append(term)

            target_aspect = GlossaryTermsClass(relevant_terms, target_aspect.auditStamp)

        return compute_term_diff_eces(
            entity_urn=origin_urn,
            old_aspect=target_aspect,
            new_aspect=origin_aspect,
            audit_stamp=self._propagation_audit_stamp(),
        )

    def _compute_propagation_mcps(
        self, change_events: ChangeEventDict
    ) -> PropagationOutput:
        for target_urn, ece_map in change_events.items():
            patch_builder = HasTermsPatch(target_urn)
            for (operation, via_urn), eces in ece_map.items():
                for ece in eces:
                    if ece.category == ChangeCategory.GLOSSARY_TERM.value:
                        self._process_ece(via_urn, operation, ece, patch_builder)
            yield from patch_builder.build()

    def _process_ece(
        self,
        via_urn: str,
        operation: str,
        ece: EntityChangeEvent,
        patch_builder: HasTermsPatch,
    ) -> None:
        term_urn = ece.modifier or ece.safe_parameters.get("termUrn")
        if not term_urn:
            logger.warning(f"Could not determine term urn for ECE: {ece}.")
            return

        if operation == ChangeOperation.REMOVE.value:
            # TODO: Remove term based on attribution
            patch_builder.remove_term(term_urn)
            return

        if operation == ChangeOperation.ADD.value:
            context = ece.safe_parameters.get("context")
            try:
                old_source_details = SourceDetails.model_validate(context)
            except ValidationError:
                old_source_details = SourceDetails()

            attribution = self._compute_attribution(old_source_details, via_urn)
            patch_builder.add_term(
                GlossaryTermAssociationClass(
                    urn=term_urn,
                    actor=old_source_details.actor or self.actor_urn,
                    context=context,
                    attribution=attribution,
                )
            )
            return


def compute_term_diff_eces(
    entity_urn: str,
    old_aspect: GlossaryTermsClass | None,
    new_aspect: GlossaryTermsClass,
    audit_stamp: AuditStampClass,
) -> Iterator[EntityChangeEvent]:
    """
    Compute differences between two GlossaryTermsClass aspects and return ChangeEvent objects.

    This is the Python equivalent of GlossaryTermsChangeEventGenerator.computeDiffs() from Java,
    although logic is significantly different, using Python's set operations for clarity.

    Args:
        entity_urn: URN of the entity being changed
        old_aspect: The original glossary terms aspect
        new_aspect: The new glossary terms aspect
        audit_stamp: Audit stamp with information about how the ECEs were created

    Returns:
        List of EntityChangeEventClass objects representing the differences
    """

    old_term_association_map = (
        {term_assoc.urn: term_assoc for term_assoc in old_aspect.terms}
        if old_aspect
        else {}
    )
    new_term_association_map = (
        {term_assoc.urn: term_assoc for term_assoc in new_aspect.terms}
        if new_aspect
        else {}
    )

    terms_added = new_term_association_map.keys() - old_term_association_map.keys()
    terms_removed = old_term_association_map.keys() - new_term_association_map.keys()

    for term in terms_added:
        term_assoc = new_term_association_map[term]
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.GLOSSARY_TERM.value,
            operation=ChangeOperation.ADD.value,
            parameters={"context": term_assoc.context},  # type: ignore
            auditStamp=audit_stamp,
            modifier=term,
            version=0,
        )
    for term in terms_removed:
        term_assoc = old_term_association_map[term]
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.GLOSSARY_TERM.value,
            operation=ChangeOperation.REMOVE.value,
            parameters={"context": term_assoc.context},  # type: ignore
            auditStamp=audit_stamp,
            modifier=term,
            version=0,
        )
