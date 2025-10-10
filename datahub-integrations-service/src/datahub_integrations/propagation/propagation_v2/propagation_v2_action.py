import logging
from collections import defaultdict
from typing import Self

# Import _Aspect directly from codegen - importing from schema_classes does not work for mypy
from datahub._codegen.aspect import _Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.api.common import (
    PipelineContext as IngestionPipelineContext,
    RecordEnvelope,
)
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.datahub_rest import DatahubRestSink, DatahubRestSinkConfig
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    MetadataChangeProposalClass,
    RelationshipChangeEventClass,
    RelationshipChangeOperationClass,
    SchemaMetadataClass,
)
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    RelationshipChangeEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.bulk_bootstrap_action import (
    BootstrapEndpoint,
    BootstrapUrnsEndpoint,
    BulkBootstrapAction,
    EntityWithData,
)
from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)
from datahub_integrations.propagation.propagation_v2.config.propagation_rule import (
    LookupType,
    MetadataToPropagate,
    RelationshipLookup,
    TargetUrnResolutionLookup,
)
from datahub_integrations.propagation.propagation_v2.config.propagation_v2_config import (
    PropagationV2Config,
)
from datahub_integrations.propagation.propagation_v2.propagators.aspect_propagator import (
    AspectPropagator,
    ChangeEventDict,
)
from datahub_integrations.propagation.propagation_v2.propagators.dataset_documentation_propagator import (
    DatasetDocumentationPropagator,
)
from datahub_integrations.propagation.propagation_v2.propagators.ownership_propagator import (
    OwnershipPropagator,
)
from datahub_integrations.propagation.propagation_v2.propagators.structured_property_propagator import (
    StructuredPropertyPropagator,
)
from datahub_integrations.propagation.propagation_v2.propagators.tag_propagator import (
    TagPropagator,
)
from datahub_integrations.propagation.propagation_v2.propagators.term_propagator import (
    GlossaryTermPropagator,
)

logger = logging.getLogger(__name__)


SUPPORTED_PROPAGATORS: dict[MetadataToPropagate, type[AspectPropagator]] = {
    MetadataToPropagate.TAGS: TagPropagator,
    MetadataToPropagate.TERMS: GlossaryTermPropagator,
    MetadataToPropagate.DOCUMENTATION: DatasetDocumentationPropagator,
    MetadataToPropagate.OWNERSHIP: OwnershipPropagator,
    MetadataToPropagate.STRUCTURED_PROPERTIES: StructuredPropertyPropagator,
}


class PropagationV2Action(BulkBootstrapAction):
    def __init__(self, config: PropagationV2Config, ctx: PipelineContext):
        super(BulkBootstrapAction, self).__init__(ctx)  # Set self.action_urn
        self.config: PropagationV2Config = config
        if not self.config.propagation_rule.target_urn_resolution:
            raise ValueError(
                "PropagationV2Action requires at least one target urn resolution lookup."
            )

        if self.config.sink:
            logger.warning(
                "Using custom sink for propagation action. This should only be used for testing bootstrap."
            )
            sink_config = self.config.sink.model_dump().get("config") or {}
            sink_class = sink_registry.get(self.config.sink.type)
            self.sink = sink_class.create(
                sink_config,
                IngestionPipelineContext(
                    run_id=f"{ctx.pipeline_name}-text",
                    graph=ctx.graph.graph if ctx.graph else None,
                    pipeline_name=ctx.pipeline_name,
                ),
            )
        else:
            self.sink = DatahubRestSink(
                ctx,
                DatahubRestSinkConfig(
                    **ctx.graph.graph.config.model_dump() if ctx.graph else {}
                ),
            )

        propagator_classes = {
            SUPPORTED_PROPAGATORS.get(metadata_type): config
            for metadata_type, config in self.config.propagation_rule.metadata_propagated.items()
        }
        self.propagators = [
            propagator_cls.create(self.action_urn, config, ctx)
            for propagator_cls, config in propagator_classes.items()
            if propagator_cls
        ]

        super().__init__(config, ctx)
        self.noop_callback = NoopWriteCallback()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        action_config = PropagationV2Config.model_validate(config_dict or {})
        return cls(action_config, ctx)

    def bootstrap(self) -> None:
        super().bootstrap()
        self.sink.close()

    def act(self, event: EventEnvelope) -> None:
        if not self.config.enabled:
            logger.debug("Propagation is disabled. Skipping event")
            return

        # Initialize event processing stats if needed
        if not self.report.event_processing_stats:
            self.report.event_processing_stats = EventProcessingStats()

        stats = self.report.event_processing_stats
        if isinstance(event.event, EntityChangeEvent):
            ece = event.event
            if not self._should_process_ece(ece):
                logger.debug(f"Skipping {ece}")
                return

            ece_entity = EntityWithData(ece.entityUrn)
            logger.debug(f"Received {ece.category} {ece.operation} on {ece.entityUrn}")

            origin_entities = [ece_entity]
            origin_entities = self._preprocess_origin_entities(origin_entities)
            if not self._should_process_urn(ece_entity):
                logger.debug(f"Skipping {ece_entity}")
                return

            logger.info(f"Processing {ece}")
            stats.start(event)
            self._hydrate_origin_entities(origin_entities)
            target_to_origin = self._get_target_urns(origin_entities, ece=ece)
            for target_urn, origin_entity in target_to_origin.items():
                try:
                    self._process_ece(ece, origin_entity, target_urn)
                    stats.end(event, success=True)
                except Exception:
                    logger.warning(f"Error processing {event}:", exc_info=True)
                    stats.end(event, success=False)
        elif isinstance(event.event, RelationshipChangeEvent):
            rce = event.event
            if not (resolution := self._should_process_rce(rce)):
                logger.debug(f"Skipping {rce}")
                return

            logger.info(f"Processing {rce}")
            stats.start(event)
            try:
                self._process_rce(rce, origin_is_source=resolution.origin_is_source)
                stats.end(event, success=True)
            except Exception:
                logger.warning(f"Error processing {event}:", exc_info=True)
                stats.end(event, success=False)

    def _process_rce(
        self, rce: RelationshipChangeEventClass, *, origin_is_source: bool
    ) -> None:
        target_urn = rce.destinationUrn if origin_is_source else rce.sourceUrn
        origin_urn = rce.sourceUrn if origin_is_source else rce.destinationUrn

        if (
            rce.operation == RelationshipChangeOperationClass.ADD
            or rce.operation == RelationshipChangeOperationClass.RESTATE
        ):
            target_entity = EntityWithData(target_urn)
            origin_entity = EntityWithData(origin_urn)
            origin_entities = [origin_entity]
            origin_entities = self._preprocess_origin_entities(origin_entities)
            self._hydrate_origin_entities(origin_entities)
            self._batch_augment_entities_with_aspects(
                [target_entity], list(self.bootstrap_aspects())
            )
            self._bootstrap_single_urn(
                origin_entity=origin_entity, target_entity=target_entity
            )
        else:
            # TODO: Handle bulk deletion via lifecycle owner and entity hard delete events
            target_entity = EntityWithData(target_urn)
            self._batch_augment_entities_with_aspects(
                [target_entity], list(self.target_propagator_aspects())
            )
            self._rollback_single_urn(
                origin_urn=origin_urn, target_entity=target_entity
            )

    def _process_ece(
        self, ece: EntityChangeEvent, origin_entity: EntityWithData, target_urn: str
    ) -> None:
        for propagator in self.propagators:
            if ece.category != propagator.category().value:
                continue
            if ece.operation not in propagator.supported_change_operations():
                logger.debug(
                    f"Skipping propagator {propagator} for ece {ece} due to unsupported operation {ece.operation}"
                )
                continue

            # Special DatasetDocumentationPropagator case
            if isinstance(propagator, DatasetDocumentationPropagator):
                self._propagate_dataset_documentation_via_diff(
                    propagator, origin_entity, target_urn
                )
                continue

            for mcp in propagator.compute_propagation_mcps(
                {target_urn: {(ece.operation, ece.entityUrn): [ece]}}
            ):
                self._emit(mcp)

    def get_report(self) -> ActionStageReport:
        return self.report

    def close(self) -> None:
        return super().close()

    def bootstrap_urns_query(self) -> str:
        if (
            self.config.propagation_rule.origin_urn_resolution.lookup_type
            == LookupType.ENTITY.value
        ):
            return self.config.propagation_rule.origin_urn_resolution.query or ""
        return ""

    def bootstrap_urns_endpoint(self) -> BootstrapUrnsEndpoint:
        origin_urn_resolution = self.config.propagation_rule.origin_urn_resolution
        if origin_urn_resolution.lookup_type == LookupType.ENTITY.value:
            return (
                BootstrapEndpoint.ENTITY_SCROLL,
                origin_urn_resolution.entity_type,
            )
        elif origin_urn_resolution.lookup_type == LookupType.RELATIONSHIP.value:
            return (
                BootstrapEndpoint.RELATIONSHIP_SCROLL,
                origin_urn_resolution.relationship_type,
                origin_urn_resolution.origin_is_source,
            )
        else:
            logger.error(
                f"Unsupported origin urn resolution lookup: {origin_urn_resolution}"
            )
            raise NotImplementedError(
                f"Unsupported origin urn resolution lookup: {origin_urn_resolution.lookup_type}"
            )

    def origin_propagator_aspects(self) -> set[type[_Aspect]]:
        return set(
            aspect
            for propagator in self.propagators
            for aspect in propagator.origin_aspects()
        )

    def target_propagator_aspects(self) -> set[type[_Aspect]]:
        return set(propagator.target_aspect() for propagator in self.propagators)

    def bootstrap_aspects(self) -> set[type[_Aspect]]:
        return self.origin_propagator_aspects() | self.target_propagator_aspects()

    def _preprocess_origin_entities(
        self, origin_entities: list[EntityWithData]
    ) -> list[EntityWithData]:
        """Prepares a list of origin entities for propagation analysis.

        1. Combines duplicate entities.
        2. Converts schema field urns to their parent dataset urns for schema field propagation.

        Mutates the origin entities in place and returns a new list of origin_entities.
        """

        # Combine duplicate entities
        origin_entity_map: dict[str, EntityWithData] = {}
        for entity in origin_entities:
            if entity.urn in origin_entity_map:
                origin_entity_map[entity.urn].merge_in_place(entity)
            else:
                origin_entity_map[entity.urn] = entity
        origin_entities = list(origin_entity_map.values())

        if (
            self.config.propagation_rule.target_urn_resolution
            == LookupType.SCHEMA_FIELD.value
        ):
            # ECEs on schema fields will have schema field urns
            # We still need to fetch the parent dataset urn to get its docs, to perform the docs diff
            # Swap the entity urn to its dataset urn, but remember the field path for later
            for entity in origin_entities:
                urn_obj = Urn.create_from_string(entity.urn)
                if isinstance(urn_obj, SchemaFieldUrn):
                    entity.urn = str(urn_obj.parent)
                    entity.field_path = urn_obj.field_path

        return origin_entities

    def _hydrate_origin_entities(self, origin_entities: list[EntityWithData]) -> None:
        entities_needing_resolution = [
            entity
            for entity in origin_entities
            if set(self._bootstrap_aspect_names) - entity.aspects.keys()
        ]
        self._batch_augment_entities_with_aspects(
            entities_needing_resolution, list(self.origin_propagator_aspects())
        )

        self._attach_descriptions_from_schema_metadata(origin_entities)

    def _get_target_urns(
        self,
        origin_entities: list[EntityWithData],
        *,
        ece: EntityChangeEvent | None = None,
    ) -> dict[str, EntityWithData]:
        """
        Given a list of origin entities, resolve the target urns based on target_urn_resolution.

        Args:
            origin_entities: List of original entities.

        Returns:
            A dictionary mapping target urns to their corresponding origin entities.
        """

        # Define `target_to_origin` which maps destination urn -> origin entity
        # And `batch` which contains each destination urn as an EntityWithData
        if (
            self.config.propagation_rule.target_urn_resolution
            == LookupType.SCHEMA_FIELD.value
        ):
            if ece and not ece.safe_parameters.get("parentUrn"):
                logger.debug(
                    f"Skipping schema field propagation: no parentUrn in {ece}"
                )
                return {}
            if ece:
                # Do not process ECEs on non-schema field urns
                origin_entities = [e for e in origin_entities if e.field_path]

            target_to_origin = self._get_schema_field_target_urns(origin_entities)
        else:
            batch = origin_entities
            target_to_origin = {entity.urn: entity for entity in origin_entities}
            for lookup in self.config.propagation_rule.target_urn_resolution:
                new_target_to_origin = {}
                for parent, children in self._follow_lookup(lookup, batch).items():
                    if parent in target_to_origin:
                        for child in children:
                            new_target_to_origin[child] = target_to_origin[parent]
                target_to_origin = new_target_to_origin
                batch = [EntityWithData(urn) for urn in target_to_origin]

        return target_to_origin

    def _get_schema_field_target_urns(
        self, origin_entities: list[EntityWithData]
    ) -> dict[str, EntityWithData]:
        target_to_origin = {}

        extra_aspects: set[type[_Aspect]] = {
            SchemaMetadataClass,
            EditableSchemaMetadataClass,
        }
        aspects_to_fetch = list(self.origin_propagator_aspects() | extra_aspects)
        entities_needing_resolution = [
            entity
            for entity in origin_entities
            if set(aspects_to_fetch) - entity.aspects.keys()
        ]
        self._batch_augment_entities_with_aspects(
            entities_needing_resolution, aspects_to_fetch
        )

        for entity in origin_entities:
            if not isinstance(Urn.from_string(entity.urn), DatasetUrn):
                logger.warning(
                    f"Schema field propagation received non-dataset entity: {entity}. Skipping."
                )
                continue

            schema_metadata = entity.get_aspect(SchemaMetadataClass)
            editable_schema_metadata = entity.get_aspect(EditableSchemaMetadataClass)
            if not schema_metadata and not editable_schema_metadata:
                continue

            base_field_map = (
                {field.fieldPath: field for field in schema_metadata.fields}
                if schema_metadata
                else {}
            )
            editable_field_map = (
                {
                    field.fieldPath: field
                    for field in editable_schema_metadata.editableSchemaFieldInfo
                }
                if editable_schema_metadata
                else {}
            )

            # For live ECE, only process specified field path
            # For bootstrap, process all field paths
            field_paths = (
                {entity.field_path}
                if entity.field_path
                else base_field_map.keys() | editable_field_map.keys()
            )
            for field_path in field_paths:
                target_urn = str(SchemaFieldUrn(entity.urn, field_path))
                base_field = base_field_map.get(
                    field_path, EditableSchemaFieldInfoClass(fieldPath=field_path)
                )
                editable_field = editable_field_map.get(
                    field_path, EditableSchemaFieldInfoClass(fieldPath=field_path)
                )

                editable_tags = (
                    {tag.tag: tag for tag in editable_field.globalTags.tags}
                    if editable_field.globalTags
                    else {}
                )
                base_tags = (
                    {
                        tag.tag: tag
                        for tag in base_field.globalTags.tags
                        if tag.tag not in editable_tags
                    }
                    if base_field.globalTags
                    else {}
                )

                editable_terms = (
                    {term.urn: term for term in editable_field.glossaryTerms.terms}
                    if editable_field.glossaryTerms
                    else {}
                )
                base_terms = (
                    {
                        term.urn: term
                        for term in base_field.glossaryTerms.terms
                        if term.urn not in editable_terms
                    }
                    if base_field.glossaryTerms
                    else {}
                )

                editable_terms_audit_stamp = (
                    editable_field.glossaryTerms.auditStamp
                    if editable_field.glossaryTerms
                    else None
                )
                base_terms_audit_stamp = (
                    base_field.glossaryTerms.auditStamp
                    if base_field.glossaryTerms
                    else None
                )

                target_to_origin[target_urn] = EntityWithData(
                    urn=SchemaFieldUrn(entity.urn, field_path).urn(),
                    aspects={
                        "globalTags": GlobalTagsClass(
                            tags=list(editable_tags.values()) + list(base_tags.values())
                        ),
                        "glossaryTerms": GlossaryTermsClass(
                            terms=list(editable_terms.values())
                            + list(base_terms.values()),
                            auditStamp=editable_terms_audit_stamp
                            or base_terms_audit_stamp
                            or AuditStampClass(
                                time=0, actor=AspectPropagator.actor_urn
                            ),
                        ),
                        "documentation": DocumentationClass(
                            documentations=[
                                DocumentationAssociationClass(
                                    documentation=editable_field.description
                                    or base_field.description
                                    or ""
                                )
                            ]
                        ),
                    },
                )

        return target_to_origin

    def _bootstrap_batch_internal(self, entities: list[EntityWithData]) -> None:
        entities = self._preprocess_origin_entities(entities)
        self._hydrate_origin_entities(entities)
        target_to_origin = self._get_target_urns(entities)
        target_urn_batch = [EntityWithData(urn) for urn in target_to_origin]
        self._batch_augment_entities_with_aspects(
            target_urn_batch, list(self.target_propagator_aspects())
        )
        target_entity_by_urn = {entity.urn: entity for entity in target_urn_batch}
        for propagator in self.propagators:
            change_events: ChangeEventDict = defaultdict(lambda: defaultdict(list))
            for target_urn, origin_entity in target_to_origin.items():
                target_entity = target_entity_by_urn.get(target_urn)
                if not target_entity:
                    logger.warning(
                        f"Target entity {target_urn} not found in batch while iterating propagators. Skipping."
                    )
                    continue

                for ece in propagator.compute_diff_eces(
                    origin=origin_entity, target=target_entity
                ):
                    if ece.entityUrn != origin_entity.urn:
                        logger.debug(
                            f"ECE generated for bootstrap does not match origin entity urn {origin_entity.urn}."
                        )
                    key = ece.operation, ece.entityUrn
                    change_events[target_entity.urn][key].append(ece)

            for mcp in propagator.compute_propagation_mcps(change_events):
                self.sink.write_record_async(
                    RecordEnvelope(mcp, metadata={}), self.noop_callback
                )

    def _rollback_batch_internal(self, entities: list[EntityWithData]) -> None:
        entities = self._preprocess_origin_entities(entities)
        target_to_origin = self._get_target_urns(entities)
        target_urn_batch = [EntityWithData(urn) for urn in target_to_origin]
        self._batch_augment_entities_with_aspects(
            target_urn_batch, list(self.target_propagator_aspects())
        )
        target_entity_by_urn = {entity.urn: entity for entity in target_urn_batch}
        for propagator in self.propagators:
            change_events: ChangeEventDict = defaultdict(lambda: defaultdict(list))
            for target_urn, origin_entity in target_to_origin.items():
                target_entity = target_entity_by_urn.get(target_urn)
                if not target_entity:
                    logger.warning(
                        f"Target entity {target_urn} not found in batch while iterating propagators. Skipping."
                    )
                    continue

                for ece in propagator.compute_diff_eces(
                    origin=propagator.empty_entity(origin_entity.urn),
                    target=target_entity,
                ):
                    if ece.entityUrn != origin_entity.urn:
                        logger.debug(
                            f"ECE generated for bootstrap does not match origin entity urn {origin_entity.urn}."
                        )
                    key = ece.operation, ece.entityUrn
                    change_events[target_entity.urn][key].append(ece)

            for mcp in propagator.compute_propagation_mcps(change_events):
                self.sink.write_record_async(
                    RecordEnvelope(mcp, metadata={}), self.noop_callback
                )

    def _bootstrap_single_urn(
        self, *, origin_entity: EntityWithData, target_entity: EntityWithData
    ) -> None:
        # Should match logic in _bootstrap_batch_internal
        for propagator in self.propagators:
            change_events: ChangeEventDict = defaultdict(lambda: defaultdict(list))
            for ece in propagator.compute_diff_eces(
                origin=origin_entity, target=target_entity
            ):
                if ece.entityUrn != origin_entity.urn:
                    logger.debug(
                        f"ECE generated for bootstrap does not match origin entity urn {origin_entity.urn}."
                    )
                key = ece.operation, ece.entityUrn
                change_events[target_entity.urn][key].append(ece)

            for mcp in propagator.compute_propagation_mcps(change_events):
                self._emit(mcp)

    def _rollback_single_urn(
        self, *, origin_urn: str, target_entity: EntityWithData
    ) -> None:
        # Should match logic in _rollback_batch_internal
        for propagator in self.propagators:
            change_events: ChangeEventDict = defaultdict(lambda: defaultdict(list))
            for ece in propagator.compute_diff_eces(
                origin=propagator.empty_entity(origin_urn), target=target_entity
            ):
                if ece.entityUrn != origin_urn:
                    logger.debug(
                        f"ECE generated for bootstrap does not match origin entity urn {origin_urn}."
                    )
                key = ece.operation, ece.entityUrn
                change_events[target_entity.urn][key].append(ece)

            for mcp in propagator.compute_propagation_mcps(change_events):
                # Synchronous to handle when a relationship is modified
                # In that scenario, we get a REMOVE rce followed by an ADD rce
                # I don't anticipate too many REMOVE rces so not worried about performance
                self._emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)

    def _follow_lookup(
        self, lookup: TargetUrnResolutionLookup, entities: list[EntityWithData]
    ) -> dict[str, set[str]]:
        """Follow the lookup to resolve the target urns of each entity.

        Returns:
            A dictionary mapping each entity's urn to a set of target urns.
        """

        if lookup.lookup_type == LookupType.RELATIONSHIP.value:
            entities_needing_resolution = [
                entity
                for entity in entities
                if lookup.relationship_type
                not in entity.get_relationships(lookup.origin_is_source)
            ]
            if entities_needing_resolution:
                self._batch_augment_entities_with_relationship(
                    entities_needing_resolution,
                    lookup.relationship_type,
                    lookup.origin_is_source,
                )
            return {
                entity.urn: set(
                    entity.get_relationships(lookup.origin_is_source)[
                        lookup.relationship_type
                    ]
                )
                for entity in entities
            }
        elif lookup.lookup_type == LookupType.ASPECT.value:
            raise NotImplementedError("Aspect lookups not implemented yet")
        else:
            logger.error(
                f"Unsupported target urn resolution lookup: {lookup.lookup_type}"
            )
            return {}

    def _should_process_ece(self, ece: EntityChangeEvent) -> bool:
        return ece.category in {
            propagator.category().value for propagator in self.propagators
        }

    def _should_process_rce(
        self, rce: RelationshipChangeEventClass
    ) -> RelationshipLookup | None:
        if len(self.config.propagation_rule.target_urn_resolution) != 1:
            return None

        resolution = self.config.propagation_rule.target_urn_resolution[0]
        if (
            isinstance(resolution, RelationshipLookup)
            and resolution.relationship_type == rce.relationshipType
        ):
            return resolution
        else:
            return None

    def _should_process_urn(self, entity: EntityWithData) -> bool:
        entity_type = Urn.from_string(entity.urn).entity_type
        endpoint = self.bootstrap_urns_endpoint()
        match endpoint:
            case BootstrapEndpoint.ENTITY_SCROLL, bootstrap_entity_type:
                if entity_type != bootstrap_entity_type:
                    return False
                query = f'/q urn:"{entity.urn}"'
                if bootstrap_query := self.bootstrap_urns_query():
                    query += f" AND {bootstrap_query}"
                urns = self.ctx.graph.graph.get_urns_by_filter(
                    entity_types=[entity_type], query=query
                )
                return bool(urns)
            case (
                BootstrapEndpoint.RELATIONSHIP_SCROLL,
                relationship_type,
                is_source,
            ):
                response = self.ctx.graph.graph.get_related_entities(
                    entity.urn,
                    [relationship_type],
                    direction=DataHubGraph.RelationshipDirection.OUTGOING
                    if is_source
                    else DataHubGraph.RelationshipDirection.INCOMING,
                )
                return bool(response)
            case _:
                logger.error(f"Bootstrap endpoint and args {endpoint} not supported.")
                return False

    def _attach_descriptions_from_schema_metadata(
        self, origin_entities: list[EntityWithData]
    ) -> None:
        """Special handling for docs propagation :'(

        Sets `ingestion_description` and `editable_description` on origin schema field entities,
        so they can later be used by `DatasetDocumentationPropagator.compute_diff_eces`.
        """
        if not any(
            p.should_fetch_schema_field_parent_schema_metadata()
            for p in self.propagators
        ):
            return

        parent_entities_to_fetch = []
        schema_field_to_parent_map = {}
        for origin_entity in origin_entities:
            urn_obj = Urn.from_string(origin_entity.urn)
            if isinstance(urn_obj, SchemaFieldUrn):
                origin_entity.field_path = urn_obj.field_path
                parent_entity = EntityWithData(urn=urn_obj.parent)
                parent_entities_to_fetch.append(parent_entity)
                schema_field_to_parent_map[origin_entity] = parent_entity

        self._batch_augment_entities_with_aspects(
            parent_entities_to_fetch,
            [SchemaMetadataClass, EditableSchemaMetadataClass],
        )

        for origin_entity, parent in schema_field_to_parent_map.items():
            schema_metadata = parent.get_aspect(SchemaMetadataClass)
            if schema_metadata:
                field = next(
                    (
                        f
                        for f in schema_metadata.fields
                        if f.fieldPath == origin_entity.field_path
                    ),
                    None,
                )
                if field and field.description:
                    origin_entity.ingestion_description = field.description

            editable_schema_metadata = parent.get_aspect(EditableSchemaMetadataClass)
            if editable_schema_metadata:
                editable_field = next(
                    (
                        f
                        for f in editable_schema_metadata.editableSchemaFieldInfo
                        if f.fieldPath == origin_entity.field_path
                    ),
                    None,
                )
                if editable_field and editable_field.description:
                    origin_entity.editable_description = editable_field.description

    def _propagate_dataset_documentation_via_diff(
        self,
        propagator: DatasetDocumentationPropagator,
        origin_entity: EntityWithData,
        target_urn: str,
    ) -> None:
        """Override the normal propagation flow, computing a diff as in the
        bootstrap scenario, because we can't propagate on the backend-generated ECE alone.
        """

        # Super awkward we have to compute diff every time
        # Ideally, we only use Documentation aspect, we propagate all
        # documentations rather than just the primary one,
        # and the UI can prioritize which one to show not based on recency
        target_entity = EntityWithData(target_urn)
        target_aspect_cls = propagator.target_aspect()
        target_entity.aspects[target_aspect_cls.ASPECT_NAME] = (
            self.ctx.graph.graph.get_aspect(target_urn, propagator.target_aspect())
        )
        for new_ece in propagator.compute_diff_eces(
            origin=origin_entity, target=target_entity
        ):
            for mcp in propagator.compute_propagation_mcps(
                {target_urn: {(new_ece.operation, new_ece.entityUrn): [new_ece]}}
            ):
                self._emit(mcp)

    def _emit(
        self,
        mcp: MetadataChangeProposalClass | MetadataChangeProposalWrapper,
        emit_mode: EmitMode = EmitMode.ASYNC,
    ) -> None:
        """For one-off MCP emission outside of sink, e.g. during live event processing.

        Sink with async batching has a delay which is undesirable for live events.
        """
        try:
            self.ctx.graph.graph.emit(mcp, emit_mode=emit_mode)
        except Exception as e:
            logger.warning(f"Failed to emit {mcp}", exc_info=e)
