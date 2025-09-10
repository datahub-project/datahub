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

from datahub_integrations.actions.bulk_bootstrap_action import EntityWithData
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

    def should_fetch_schema_field_parent_schema_metadata(
        self,
    ) -> bool:
        return True

    def category(self) -> ChangeCategory:
        return ChangeCategory.DOCUMENTATION

    def _supported_change_operations(self) -> set[ChangeOperation]:
        return {ChangeOperation.ADD, ChangeOperation.REMOVE, ChangeOperation.MODIFY}

    # Override because this requires special logic, due to forced diff
    def compute_diff_eces(
        self, *, origin: EntityWithData, target: EntityWithData
    ) -> Iterator[EntityChangeEvent]:
        """Calculate the difference between two entities and return MCPs that would propagate
        the relevant aspect's data from origin to target.

        MCPs are created based on and with appropriate attribution information.
        """

        origin_dp = origin.get_aspect(DatasetPropertiesClass)
        origin_edp = origin.get_aspect(EditableDatasetPropertiesClass)
        new_ingestion_description = origin.ingestion_description or (
            origin_dp.description if origin_dp else None
        )
        new_editable_description = origin.editable_description or (
            origin_edp.description if origin_edp else None
        )

        origin_doc = origin.get_aspect(DocumentationClass)
        target_doc = target.get_aspect(DocumentationClass)

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
                    logger.warning(f"Invalid doc attribution for doc {doc}. Skipping.")
                    continue

                if doc.attribution.source == self.action_urn and (
                    source_details.origin == origin.urn
                    or source_details.via == origin.urn
                ):
                    relevant_docs.append(doc)

            target_doc = DocumentationClass(
                documentations=relevant_docs, lastModified=target_doc.lastModified
            )

        return compute_doc_diff_mcps(
            entity_urn=origin.urn,
            new_ingestion_description=new_ingestion_description,
            new_editable_description=new_editable_description,
            old_doc_aspect=target_doc,
            new_doc_aspect=origin_doc,
            audit_stamp=self._propagation_audit_stamp(),
        )

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
        ],
    ) -> Iterator[EntityChangeEvent]:
        raise NotImplementedError(
            "Should not be called as `compute_diff_eces` is overridden."
        )

    def _compute_propagation_mcps(
        self, change_events: dict[str, dict[str, dict[str, EntityChangeEvent]]]
    ) -> PropagationOutput:
        for target_urn, ece_map in change_events.items():
            for operation, eces in ece_map.items():
                for via_urn, ece in eces.items():
                    if ece.category == ChangeCategory.DOCUMENTATION.value:
                        old_aspect = ece.safe_parameters.get("old_aspect")
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
        context = ece.safe_parameters.get("context")
        try:
            old_source_details = SourceDetails.model_validate(context)
        except ValidationError:
            old_source_details = SourceDetails()

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
                            source_details.origin == via_urn
                            or source_details.origin == old_source_details.origin
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

        elif (
            operation == ChangeOperation.ADD.value
            or operation == ChangeOperation.MODIFY.value
        ):
            if not (description := ece.safe_parameters.get("description")):
                logger.warning(
                    f"Invalid ADD / MODIFY documentation ECE {ece}. Skipping."
                )
                return

            should_append = True
            documentations = old_aspect.documentations if old_aspect else []
            for doc in documentations:
                if doc.attribution and doc.attribution.source == self.action_urn:
                    try:
                        source_details = SourceDetails.model_validate(
                            doc.attribution.sourceDetail
                        )
                    except ValidationError:
                        logger.warning(f"Invalid attribution for doc {doc}. Skipping.")
                        continue
                    if (
                        source_details.origin == via_urn
                        or source_details.origin == old_source_details.origin
                    ):
                        # Replace existing doc from this source and origin
                        should_append = False
                        doc.documentation = description
                        doc.attribution = attribution

            if should_append:
                documentations.append(
                    DocumentationAssociationClass(
                        documentation=description,
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


def compute_doc_diff_mcps(
    entity_urn: str,
    new_ingestion_description: str | None,
    new_editable_description: str | None,
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
        new_ingestion_description: The new ingestion description (DatasetProperties or SchemaMetadata aspect)
        new_editable_description: The new editable description (EditableDatasetProperties or EditableSchemaMetadata aspect)
        old_doc_aspect: The original documentation aspect
        new_doc_aspect: The new documentation aspect
        audit_stamp: Audit stamp with information about how the ECEs were created

    Returns:
        List of EntityChangeEventClass objects representing the differences
    """

    # Warning: always counts inferred docs, even if they are not enabled in the UI
    old_docs = (
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

    new_docs = (
        new_editable_description or new_ingestion_description or new_documentation_docs
    )

    # Note: code should change if we support documentation patch
    if new_docs and not old_docs:
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
    elif not new_docs and old_docs:
        yield EntityChangeEvent(
            entityUrn=entity_urn,
            entityType=Urn.from_string(entity_urn).entity_type,
            category=ChangeCategory.DOCUMENTATION.value,
            operation=ChangeOperation.REMOVE.value,
            parameters={  # type: ignore
                "description": old_docs.documentation,
                "context": old_docs.attribution,
                "old_aspect": old_doc_aspect,
            },
            auditStamp=audit_stamp,
            version=0,
        )
    elif new_docs and old_docs:
        context = None
        if isinstance(new_docs, DocumentationAssociationClass):
            context = new_docs.attribution
            new_docs = new_docs.documentation
        if new_docs != old_docs.documentation:
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
