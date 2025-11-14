import json
import logging
import time
from typing import Dict, Iterable, List, Optional, Type

# Import _Aspect directly from codegen - importing from schema_classes does not work for mypy
from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import (
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaMetadataClass,
    GenericAspectClass,
    MetadataAttributionClass,
    SchemaMetadataClass,
)
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from pydantic import Field

from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    EntityPropagatorConfig,
)

logger = logging.getLogger(__name__)


class DocPropagationDirective(PropagationDirective):
    """Directive for documentation propagation."""

    doc_string: Optional[str] = Field(
        default=None, description="Documentation string to be propagated."
    )


class DocsPropagatorConfig(EntityPropagatorConfig):
    """Configuration for the Documentation Propagator."""

    columns_enabled: bool = True


class DocsPropagator(EntityPropagator):
    """
    A propagator that propagates documentation across entities based on relationships.
    """

    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: DocsPropagatorConfig
    ) -> None:
        """Initialize the DocsPropagator."""

        super().__init__(action_urn, graph, config)
        self.config = config
        self.graph = graph
        self.only_one_upstream_allowed = True

        self._register_processors()

    def _register_processors(self) -> None:
        """Register processors for different aspects and entity types."""

        # Register MCL processors for schema field documentation
        for aspect in ["documentation", "editableSchemaMetadata", "schemaMetadata"]:
            self.mcl_processor.register_processor(
                entity_type="schemaField",
                aspect=aspect,
                processor=self.process_schema_field_documentation_mcl,
            )

        # Register MCE processor for documentation changes
        self.ece_processor.register_processor(
            category="DOCUMENTATION",
            entity_type="schemaField",
            processor=self.process_ece,
        )

    def aspects(self) -> List[Type[_Aspect]]:
        """Return the aspects this propagator handles."""

        return [DocumentationClass, SchemaMetadataClass, EditableSchemaMetadataClass]

    def get_metadata_from_aspect(
        self, aspect: Aspect
    ) -> List[DocumentationAssociationClass]:
        """Extract metadata from aspect."""

        assert isinstance(aspect, DocumentationClass)
        return aspect.documentations

    def boostrap_asset(self, urn: Urn) -> Iterable[DocPropagationDirective]:
        """Bootstrap documentation for an asset."""

        assert urn.entity_type == "dataset"

        field_doc_map = self._collect_field_documentation(urn)

        for field_path, field_info in field_doc_map.items():
            aspect = self._create_documentation_aspect(field_info)
            directive = self.generate_directive(
                SchemaFieldUrn(urn, field_path).urn(), None, aspect
            )

            if directive:
                yield directive

    def _collect_field_documentation(self, urn: Urn) -> Dict:
        """Collect field documentation from schema metadata and editable schema metadata."""

        edited_field_docs = self.graph.graph.get_aspect(
            urn.urn(), EditableSchemaMetadataClass
        )
        base_field_docs = self.graph.graph.get_aspect(urn.urn(), SchemaMetadataClass)

        field_doc_map = {}

        # Collect documentation from base schema metadata
        if base_field_docs is not None:
            for field_info in base_field_docs.fields:
                if field_info.description:
                    field_doc_map[field_info.fieldPath] = {
                        "description": field_info.description,
                        "auditStamp": field_info.lastModified,
                    }

        # Collect documentation from editable schema metadata (overrides base)
        if edited_field_docs is not None:
            for field_info in edited_field_docs.editableSchemaFieldInfo:
                if field_info.description:
                    field_doc_map[field_info.fieldPath] = {
                        "description": field_info.description,
                        "auditStamp": edited_field_docs.lastModified,
                    }

        return field_doc_map

    def _create_documentation_aspect(self, field_info: Dict) -> DocumentationClass:
        """Create a documentation aspect from field info."""

        return DocumentationClass(
            documentations=[
                DocumentationAssociationClass(
                    documentation=field_info["description"],
                    attribution=MetadataAttributionClass(
                        source=self.action_urn,
                        time=int(field_info["auditStamp"].time)
                        if field_info["auditStamp"]
                        else int(time.time() * 1000),
                        actor=self.actor_urn,
                    ),
                )
            ]
        )

    def generate_directive(
        self,
        entity_urn: str,
        old_docs: Optional[DocumentationClass],
        current_docs: DocumentationClass,
        source_details: Optional[SourceDetails] = None,
    ) -> Optional[DocPropagationDirective]:
        """Generate a propagation directive for documentation."""

        # Source details comes from the aspect therefore we don't need to pass it
        if not current_docs.documentations:
            return None

        # Get the most recently updated documentation with attribution
        documentation_instances = sorted(
            [doc for doc in current_docs.documentations if doc.attribution],
            key=lambda x: x.attribution.time if x.attribution else 0,
        )

        if not documentation_instances:
            logger.warning(
                f"Documentation doesn't have any documentation attribution. Propagation will be skipped for {entity_urn} for this mcl."
            )
            return None

        current_documentation_instance = documentation_instances[-1]
        assert current_documentation_instance.attribution

        # Check if documentation is from this action
        if (
            current_documentation_instance.attribution.source is None
            or current_documentation_instance.attribution.source != self.action_urn
        ):
            logger.warning(
                f"Documentation is not sourced by this action which is unexpected. Will be propagating for {entity_urn}"
            )

        # Extract source details
        source_details_dict = (
            current_documentation_instance.attribution.sourceDetail
            if current_documentation_instance.attribution
            else {}
        )
        source_details_parsed: SourceDetails = SourceDetails.model_validate(
            source_details_dict
        )

        # Check if propagation should stop
        should_stop_propagation, reason = self.should_stop_propagation(
            source_details_parsed
        )
        if should_stop_propagation:
            logger.warning(f"Stopping propagation for {entity_urn}. {reason}")
            return None

        logger.debug(f"Propagating documentation for {entity_urn}")

        # Determine origin entity
        origin_entity = (
            source_details_parsed.origin if source_details_parsed.origin else entity_urn
        )

        # Create directive for new documentation or modified documentation
        if old_docs is None or not old_docs.documentations:
            return self._create_add_directive(
                entity_urn,
                current_documentation_instance,
                origin_entity,
                source_details_parsed,
            )
        else:
            old_docs_instance = sorted(
                old_docs.documentations,
                key=lambda x: x.attribution.time if x.attribution else 0,
            )[-1]

            if (
                current_documentation_instance.documentation
                != old_docs_instance.documentation
            ):
                return self._create_modify_directive(
                    entity_urn,
                    current_documentation_instance,
                    origin_entity,
                    source_details_parsed,
                )

        return None

    def _create_add_directive(
        self,
        entity_urn: str,
        doc_instance: DocumentationAssociationClass,
        origin_entity: str,
        source_details: SourceDetails,
    ) -> DocPropagationDirective:
        """Create a directive to add documentation."""

        return DocPropagationDirective(
            propagate=True,
            doc_string=doc_instance.documentation,
            operation="ADD",
            entity=entity_urn,
            origin=origin_entity,
            via=entity_urn if origin_entity != entity_urn else None,
            actor=self.actor_urn,
            propagation_started_at=source_details.propagation_started_at,
            propagation_depth=(
                source_details.propagation_depth + 1
                if source_details.propagation_depth
                else 1
            ),
        )

    def _create_modify_directive(
        self,
        entity_urn: str,
        doc_instance: DocumentationAssociationClass,
        origin_entity: str,
        source_details: SourceDetails,
    ) -> DocPropagationDirective:
        """Create a directive to modify documentation."""

        return DocPropagationDirective(
            propagate=True,
            doc_string=doc_instance.documentation,
            operation="MODIFY",
            entity=entity_urn,
            origin=origin_entity,
            via=entity_urn,
            actor=self.actor_urn,
            propagation_started_at=source_details.propagation_started_at,
            propagation_depth=(
                source_details.propagation_depth + 1
                if source_details.propagation_depth
                else 1
            ),
        )

    def process_schema_field_documentation_mcl(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: GenericAspectClass,
        previous_aspect_value: Optional[GenericAspectClass],
    ) -> Optional[DocPropagationDirective]:
        """
        Process changes in the documentation aspect of schemaField entities.
        Produce a directive to propagate the documentation.

        Business Logic checks:
            - If the documentation is sourced by this action, then we propagate it.
            - If the documentation is not sourced by this action, then we log a warning and propagate it.
            - If we have exceeded the maximum depth of propagation or maximum time for propagation,
              then we stop propagation and don't return a directive.
        """
        assert isinstance(self.config, DocsPropagatorConfig)
        logger.debug("Process MCL from DocPropagation")

        if (
            aspect_name != "documentation"
            or guess_entity_type(entity_urn) != "schemaField"
        ):
            # not a documentation aspect or not a schemaField entity
            return None

        logger.debug("Processing 'documentation' MCL")
        if self.config.columns_enabled:
            current_docs = DocumentationClass.from_obj(json.loads(aspect_value.value))
            old_docs = (
                None
                if previous_aspect_value is None
                else DocumentationClass.from_obj(
                    json.loads(previous_aspect_value.value)
                )
            )
            return self.generate_directive(entity_urn, old_docs, current_docs)

        return None

    def process_ece(self, event: EventEnvelope) -> Optional[DocPropagationDirective]:
        """Process metadata change events for documentation."""

        assert isinstance(event.event, EntityChangeEvent)
        assert self.graph is not None
        assert isinstance(self.config, DocsPropagatorConfig)

        semantic_event = event.event
        if not (semantic_event.category == "DOCUMENTATION" and self.config.enabled):
            return None

        logger.info(f"MCE from DocPropagation: {event}")

        if not (
            self.config.columns_enabled and semantic_event.entityType == "schemaField"
        ):
            return None

        # Extract parameters
        if semantic_event.parameters:
            parameters = semantic_event.parameters
        else:
            parameters = semantic_event._inner_dict.get("__parameters_json", {})

        doc_string = parameters.get("description")
        if not doc_string:
            return None

        # Determine origin and via
        origin = parameters.get("origin")
        origin = origin or semantic_event.entityUrn
        via = semantic_event.entityUrn if origin != semantic_event.entityUrn else None

        logger.debug(f"Origin: {origin}")
        logger.debug(f"Via: {via}")
        logger.debug(f"Doc string: {doc_string}")
        logger.debug(f"Semantic event {semantic_event}")

        # Create directive
        return DocPropagationDirective(
            propagate=True,
            doc_string=doc_string,
            operation=semantic_event.operation,
            entity=semantic_event.entityUrn,
            origin=origin,
            via=via,  # if origin is set, then via is the entity itself
            actor=(
                semantic_event.auditStamp.actor
                if semantic_event.auditStamp
                else self.actor_urn
            ),
            propagation_started_at=int(time.time() * 1000.0),
            propagation_depth=1,  # we start at 1 because this is the first propagation
        )

    def create_property_change_proposal(
        self,
        propagation_directive: PropagationDirective,
        entity_urn: Urn,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create property change proposals based on the directive."""

        assert isinstance(propagation_directive, DocPropagationDirective)
        parent_urn = entity_urn.entity_ids[0]

        yield from self.modify_docs_on_columns(
            graph=self.graph,
            operation=propagation_directive.operation,
            schema_field_urn=str(entity_urn),
            dataset_urn=parent_urn,
            field_doc=propagation_directive.doc_string,
            context=context,
        )

    def modify_docs_on_columns(
        self,
        graph: AcrylDataHubGraph,
        operation: str,
        schema_field_urn: str,
        dataset_urn: str,
        field_doc: Optional[str],
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Modify documentation on columns."""

        # Skip propagation to self
        if context.origin == schema_field_urn:
            logger.info("Skipping documentation propagation to self")
            return None

        logger.info(
            f"Modifying docs on urn: {dataset_urn} schema field urn: {schema_field_urn} field doc: {field_doc}"
        )

        # Validate dataset URN
        try:
            DatasetUrn.from_string(dataset_urn)
        except Exception as e:
            logger.error(
                f"Invalid dataset urn {dataset_urn}. {e}. Skipping documentation propagation."
            )
            return None

        # Create attribution
        source_details = context.for_metadata_attribution()
        attribution = MetadataAttributionClass(
            source=self.action_urn,
            time=int(time.time() * 1000.0),
            actor=self.actor_urn if not context.actor else context.actor,
            sourceDetail=source_details,
        )

        # Get existing documentation
        documentations = graph.graph.get_aspect(schema_field_urn, DocumentationClass)

        if documentations:
            yield from self._update_existing_documentation(
                documentations,
                schema_field_urn,
                field_doc,
                attribution,
                operation,
                context,
            )
        else:
            # No docs found, create a new one
            yield from self._create_new_documentation(
                schema_field_urn, field_doc, attribution
            )

    def _update_existing_documentation(
        self,
        documentations: DocumentationClass,
        schema_field_urn: str,
        field_doc: Optional[str],
        attribution: MetadataAttributionClass,
        operation: str,
        context: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Update existing documentation."""

        logger.info(
            f"Found existing documentation {documentations} for {schema_field_urn}"
        )
        mutation_needed = False
        action_sourced = False

        # Check if there are any existing documentations generated by this action
        # and sourced from the same origin; if so, update them
        for doc_association in documentations.documentations[:]:
            if doc_association.attribution and doc_association.attribution.source:
                source_details_parsed = SourceDetails.model_validate(
                    doc_association.attribution.sourceDetail
                )
                if (
                    doc_association.attribution.source == self.action_urn
                    and source_details_parsed.origin == context.origin
                ):
                    action_sourced = True
                    if doc_association.documentation != field_doc:
                        mutation_needed = True
                        if operation in ["ADD", "MODIFY"]:
                            doc_association.documentation = field_doc or ""
                            doc_association.attribution = attribution
                        elif operation == "REMOVE":
                            documentations.documentations.remove(doc_association)

        # Add new documentation if none exists from this action
        if not action_sourced:
            documentations.documentations.append(
                DocumentationAssociationClass(
                    documentation=field_doc or "",
                    attribution=attribution,
                )
            )
            mutation_needed = True

        # Emit the change if needed
        if mutation_needed:
            logger.info(
                f"Will emit documentation {documentations} change proposal for {schema_field_urn} with {field_doc}"
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=schema_field_urn,
                aspect=documentations,
            )
        else:
            logger.info(f"No mutation needed for {schema_field_urn}")

    def _create_new_documentation(
        self,
        schema_field_urn: str,
        field_doc: Optional[str],
        attribution: MetadataAttributionClass,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create new documentation."""

        # We don't check editableSchemaMetadata because our goal is to
        # propagate documentation to downstream entities.
        # UI will handle resolving priorities and conflicts
        documentations = DocumentationClass(
            documentations=[
                DocumentationAssociationClass(
                    documentation=field_doc or "",
                    attribution=attribution,
                )
            ]
        )

        logger.info(
            f"Will emit new documentation {documentations} for {schema_field_urn} with {field_doc}"
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=schema_field_urn,
            aspect=documentations,
        )

    def asset_filters(self) -> Dict[str, Dict[str, List[SearchFilterRule]]]:
        """Get filters for asset types."""

        return {
            "dataset": {
                "dataset": [
                    SearchFilterRule(
                        field="fieldDescriptions",
                        condition="EXISTS",
                        values=[],
                        negated=False,
                    ),
                    SearchFilterRule(
                        field="editedFieldDescriptions",
                        condition="EXISTS",
                        values=[],
                        negated=False,
                    ),
                ]
            }
        }

    def rollbackable_assets(self) -> Iterable[Urn]:
        """Get assets that can be rolled back."""

        urns = self.graph.graph.get_urns_by_filter(
            entity_types=["schemaField"],
            extra_or_filters=[
                {
                    "field": "documentationAttributionSources.keyword",
                    "condition": "IN",
                    "values": [self.action_urn],
                    "negated": "false",
                }
            ],
        )
        for urn in urns:
            yield Urn.from_string(urn)

    def rollback_asset(self, asset: Urn) -> None:
        """Rollback documentation from an asset."""

        try:
            assert self.graph
            logger.info(f"Rolling back documentation for asset {asset}")

            documentation = self.graph.graph.get_aspect(asset.urn(), DocumentationClass)
            if documentation is not None:
                # Find documentation sourced by this action
                exists = [
                    doc
                    for doc in documentation.documentations
                    if doc.attribution.source == self.action_urn
                ]

                if exists:
                    # Remove documentation sourced by this action
                    documentation.documentations = [
                        doc
                        for doc in documentation.documentations
                        if doc.attribution.source != self.action_urn
                    ]

                    # Emit the change
                    self.graph.graph.emit(
                        MetadataChangeProposalWrapper(
                            entityUrn=asset.urn(), aspect=documentation
                        )
                    )
            else:
                logger.warning(f"Documentation aspect not found for asset {asset}")
        except Exception as e:
            logger.error(f"Error rolling back documentation: {e}")
