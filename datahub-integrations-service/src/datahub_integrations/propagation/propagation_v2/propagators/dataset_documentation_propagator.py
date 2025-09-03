import logging
from typing import Iterator

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableDatasetPropertiesClass,
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


# TODO: Make generic documentation propagator that only operates on DocumentationClass
# Requires propagating DatasetPropertiesClass and EditableDatasetPropertiesClass to DocumentationClass
class DatasetDocumentationPropagator(
    AspectPropagator[
        DatasetPropertiesClass, EditableDatasetPropertiesClass, DocumentationClass
    ]
):
    # TODO: Support custom config, init, and create to customize propagator

    def aspects(
        self,
    ) -> tuple[
        type[DatasetPropertiesClass],
        type[EditableDatasetPropertiesClass],
        type[DocumentationClass],
    ]:
        return (
            DatasetPropertiesClass,
            EditableDatasetPropertiesClass,
            DocumentationClass,
        )

    def supported_change_operations(self) -> set[ChangeOperation]:
        return {ChangeOperation.ADD, ChangeOperation.REMOVE, ChangeOperation.MODIFY}

    def _compute_diff_eces_internal(
        self,
        origin_urn: str,
        target_urn: str,
        origin_aspects: tuple[
            DatasetPropertiesClass | None,
            EditableDatasetPropertiesClass | None,
            DocumentationClass | None,
        ],
        target_aspects: tuple[
            DatasetPropertiesClass | None,
            EditableDatasetPropertiesClass | None,
            DocumentationClass | None,
        ]
        | None,
    ) -> Iterator[EntityChangeEvent]:
        origin_dp, origin_edp, origin_doc = origin_aspects
        target_dp, target_edp, target_doc = None, None, None

        if target_aspects:
            target_dp, target_edp, target_doc = target_aspects
            if target_doc:
                # Filter to only docs attributed to this propagation action
                relevant_docs = []
                for doc in target_doc.documentations:
                    if not doc.attribution:
                        continue
                    try:
                        source_details = SourceDetails.model_validate(
                            doc.attribution.sourceDetail
                        )
                    except ValidationError:
                        logger.warning(
                            f"Invalid doc attribution for doc {doc}. Skipping."
                        )
                        continue

                    if doc.attribution.source == self.action_urn and (
                        source_details.origin == origin_urn
                        or source_details.via == origin_urn
                    ):
                        relevant_docs.append(doc)

                target_doc = DocumentationClass(
                    documentations=relevant_docs, lastModified=target_doc.lastModified
                )

        return compute_doc_diff_mcps(
            entity_urn=origin_urn,
            old_dp_aspect=target_dp,
            new_dp_aspect=origin_dp,
            old_edp_aspect=target_edp,
            new_edp_aspect=origin_edp,
            old_doc_aspect=target_doc,
            new_doc_aspect=origin_doc,
            audit_stamp=self._propagation_audit_stamp(),
        )

    def compute_propagation_mcps(
        self, change_events: dict[str, dict[str, dict[str, EntityChangeEvent]]]
    ) -> PropagationOutput:
        for target_urn, ece_map in change_events.items():
            for operation, eces in ece_map.items():
                for via_urn, ece in eces.items():
                    if ece.category == ChangeCategory.DOCUMENTATION.value:
                        old_aspect = ece.parameters.get("old_aspect")
                        if "old_aspect" not in ece.parameters:
                            old_aspect = self.ctx.graph.graph.get_aspect(
                                target_urn, DocumentationClass
                            )
                        yield from self._process_ece(
                            target_urn, via_urn, operation, ece, old_aspect
                        )

    def _process_ece(
        self,
        target_urn: str,
        via_urn: str,
        operation: str,
        ece: EntityChangeEvent,
        old_aspect: DocumentationClass | None,
    ) -> PropagationOutput:
        # TODO: Support documentation patch; allows removing old_aspect logic
        context = ece.parameters.get("context")
        try:
            old_source_details = SourceDetails.model_validate_json(context)
        except ValidationError:
            old_source_details = SourceDetails()  # Allows all nulls

        attribution = self._compute_attribution(old_source_details, via_urn)

        if operation == ChangeOperation.REMOVE.value:
            if not old_aspect:
                return

            new_documentations = []
            for doc in old_aspect.documentations:
                if doc.attribution and doc.attribution.source == self.action_urn:
                    should_remove = False
                    try:
                        source_details = SourceDetails.model_validate(
                            doc.attribution.sourceDetail
                        )
                        if (
                            source_details.via == via_urn
                            and source_details.origin == old_source_details.origin
                        ):
                            should_remove = True
                    except ValidationError:
                        logger.warning(f"Invalid attribution for doc {doc}. Skipping.")

                    if not should_remove:
                        new_documentations.append(doc)

            if old_aspect.documentations != new_documentations:
                yield MetadataChangeProposalWrapper(
                    entityUrn=target_urn,
                    aspect=DocumentationClass(
                        documentations=new_documentations, lastModified=ece.auditStamp
                    ),
                )
            return

        elif operation == ChangeOperation.ADD.value:
            documentations = old_aspect.documentations if old_aspect else []
            documentations.append(
                DocumentationAssociationClass(
                    documentation=ece.parameters.get("description"),
                    attribution=attribution,
                )
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=DocumentationClass(
                    documentations=documentations,
                    lastModified=ece.auditStamp,
                ),
            )
            return

        elif operation == ChangeOperation.MODIFY.value:
            if not old_aspect or not ece.parameters.get("description"):
                logger.warning(f"Invalid MODIFY documentation ECE {ece}. Skipping.")
                return

            should_emit = False
            for doc in old_aspect.documentations:
                if doc.attribution and doc.attribution.source == self.action_urn:
                    try:
                        source_details = SourceDetails.model_validate(
                            doc.attribution.sourceDetail
                        )
                    except ValidationError:
                        logger.warning(f"Invalid attribution for doc {doc}. Skipping.")
                        continue
                    if (
                        source_details.via == via_urn
                        and source_details.origin == old_source_details.origin
                    ):
                        # Replace existing doc from this source and origin
                        should_emit = True
                        doc.documentation = ece.parameters.get("description")
                        doc.attribution = attribution
            if should_emit:
                yield MetadataChangeProposalWrapper(
                    entityUrn=target_urn, aspect=old_aspect
                )
            return


def compute_doc_diff_mcps(
    entity_urn: str,
    old_dp_aspect: DatasetPropertiesClass | None,
    new_dp_aspect: DatasetPropertiesClass | None,
    old_edp_aspect: EditableDatasetPropertiesClass | None,
    new_edp_aspect: EditableDatasetPropertiesClass | None,
    old_doc_aspect: DocumentationClass | None,
    new_doc_aspect: DocumentationClass | None,
    audit_stamp: AuditStampClass,
) -> Iterator[EntityChangeEvent]:
    """
    Compute differences between two datasets and return ChangeEvent objects.

    Based on the logic from the UI, in entityV2/shared/tabs/Documentation/utils.ts::getAssetDescriptionDetails:
      EditableDatasetProperties > DatasetProperties > Documentation
    And based on the ECEs generated in DatasetPropertiesChangeEventGenerator and EditableDatasetPropertiesChangeEventGenerator.

    Args:
        entity_urn: URN of the entity being changed
        old_dp_aspect: The original dataset properties aspect
        new_dp_aspect: The new dataset properties aspect
        old_edp_aspect: The original editable dataset properties aspect
        new_edp_aspect: The new editable dataset properties aspect
        old_doc_aspect: The original documentation aspect
        new_doc_aspect: The new documentation aspect
        audit_stamp: Audit stamp with information about how the ECEs were created

    Returns:
        List of EntityChangeEventClass objects representing the differences
    """

    old_edp_docs = old_edp_aspect.description if old_edp_aspect else None
    new_edp_docs = new_edp_aspect.description if new_edp_aspect else None

    old_dp_docs = old_dp_aspect.description if old_dp_aspect else None
    new_dp_docs = new_dp_aspect.description if new_dp_aspect else None

    # Warning: always counts inferred docs, even if they are not enabled in the UI
    old_documentation_docs = (
        sorted(
            old_doc_aspect.documentations,
            key=lambda doc: doc.attribution.time if doc.attribution else 0,
            reverse=True,
        )[0]
        if old_doc_aspect and old_doc_aspect.documentations
        else None
    )
    new_documentation_docs = (
        sorted(
            new_doc_aspect.documentations,
            key=lambda doc: doc.attribution.time if doc.attribution else 0,
            reverse=True,
        )[0]
        if new_doc_aspect and new_doc_aspect.documentations
        else None
    )

    old_docs = old_edp_docs or old_dp_docs or old_documentation_docs
    new_docs = new_edp_docs or new_dp_docs or new_documentation_docs

    # Note: code should change if we support documentation patch
    if new_docs and not old_doc_aspect:
        context = None
        if isinstance(new_docs, DocumentationAssociationClass):
            context = new_docs.attribution
            new_docs = new_docs.documentation
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.DOCUMENTATION.value,
            operation=ChangeOperation.ADD.value,
            parameters={  # type: ignore
                "description": new_docs,
                "context": context,
                "old_aspect": None,
            },
            auditStamp=audit_stamp,
            version=0,
        )
    elif not new_docs and old_doc_aspect:
        context = None
        if isinstance(old_docs, DocumentationAssociationClass):
            context = old_docs.attribution
            old_docs = old_docs.documentation
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.DOCUMENTATION.value,
            operation=ChangeOperation.REMOVE.value,
            parameters={  # type: ignore
                "description": old_docs,
                "context": context,
                "old_aspect": old_doc_aspect,
            },
            auditStamp=audit_stamp,
            version=0,
        )
    elif new_docs and old_doc_aspect:
        context = None
        if isinstance(new_docs, DocumentationAssociationClass):
            context = new_docs.attribution
            new_docs = new_docs.documentation
        if isinstance(old_docs, DocumentationAssociationClass):
            old_docs = old_docs.documentation
        if new_docs != old_docs:
            yield EntityChangeEvent(
                entityUrn=entity_urn,
                entityType=Urn.from_string(entity_urn).entity_type,
                category=ChangeCategory.DOCUMENTATION.value,
                operation=ChangeOperation.MODIFY.value,
                parameters={  # type: ignore
                    "description": new_docs,
                    "context": context,
                    "old_aspect": old_doc_aspect,
                },
                auditStamp=audit_stamp,
                version=0,
            )
