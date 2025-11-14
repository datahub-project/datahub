import json
import logging
import time
from typing import Dict, Iterable, List, Optional, Type

from datahub._codegen.aspect import _Aspect
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    MetadataAttributionClass,
    SchemaMetadataClass,
    TagAssociationClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from pydantic import Field, field_validator

from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    EntityPropagatorConfig,
    PropagationOutput,
)

logger = logging.getLogger(__name__)


class TagPropagatorConfig(EntityPropagatorConfig):
    """
    Configuration model for tag propagation.

    Attributes:
        enabled (bool): Indicates whether tag propagation is enabled or not. Default is True.
        tag_prefixes (Optional[List[str]]): Optional list of tag prefixes to restrict tag propagation.
            If provided, only tags with prefixes in this list will be propagated. Default is None,
            meaning all tags will be propagated.

    Note:
        Tag propagation allows tags to be automatically propagated to downstream entities.
        Enabling tag propagation can help maintain consistent metadata across connected entities.
        The `enabled` attribute controls whether tag propagation is enabled or disabled.
        The `tag_prefixes` attribute can be used to specify a list of tag prefixes that define which tags
        should be propagated. If no prefixes are specified (default), all tags will be propagated.

    Example:
        config = TagPropagationConfig(enabled=True, tag_prefixes=["urn:li:tag:classification"])
    """

    tag_prefixes: Optional[List[str]] = Field(
        default=None,
        description="Optional list of tag prefixes to restrict tag propagation.",
        examples=["urn:li:tag:classification"],
    )

    @field_validator("tag_prefixes")
    @classmethod
    def tag_prefix_should_start_with_urn(
        cls, v: Optional[List[str]]
    ) -> Optional[List[str]]:
        """Validate that tag prefixes start with URN format."""
        if v is None:
            return v
        return [make_tag_urn(item) if item else item for item in v]


class TagPropagationDirective(PropagationDirective):
    """Directive for tag propagation."""

    tags: List[str]


class TagPropagator(EntityPropagator):
    """
    A propagator that propagates tags across entities based on relationships.
    """

    def __init__(
        self, action_urn: str, graph: AcrylDataHubGraph, config: TagPropagatorConfig
    ) -> None:
        """Initialize the TagPropagator."""

        super().__init__(action_urn, graph, config)
        self.config = config
        self.graph = graph
        self.action_urn = action_urn

        self._register_processors()

    def _register_processors(self) -> None:
        """Register processors for different entity types."""

        entity_categories = [
            ("dataset", "TAG"),
            ("schemaField", "TAG"),
        ]

        for entity_type, category in entity_categories:
            self.ece_processor.register_processor(
                entity_type=entity_type,
                category=category,
                processor=self.process_ece,
            )

    def aspects(self) -> List[Type[_Aspect]]:
        """Return the aspects this propagator handles."""

        return [GlobalTagsClass]

    def get_metadata_from_aspect(self, aspect: _Aspect) -> List[TagAssociationClass]:
        """Extract metadata from aspect."""

        assert isinstance(aspect, GlobalTagsClass)
        return aspect.tags

    def boostrap_asset(self, asset_urn: Urn) -> Iterable[TagPropagationDirective]:
        """Bootstrap tags for an asset."""

        logger.info(
            f"Bootstrapping tags for {asset_urn} with config {self.config} and entity types {self.config.propagation_rule.entity_types}"
        )

        if (
            asset_urn.entity_type == "dataset"
            and "schemaField" in self.config.propagation_rule.entity_types
        ):
            yield from self._bootstrap_dataset_fields(asset_urn)

        if (
            asset_urn.entity_type == "dataset"
            and "dataset" in self.config.propagation_rule.entity_types
        ):
            yield from self._bootstrap_dataset(asset_urn)
        else:
            logger.error(f"Unsupported target entity type {asset_urn.entity_type}")

    def _bootstrap_dataset_fields(
        self, asset_urn: Urn
    ) -> Iterable[TagPropagationDirective]:
        """Bootstrap tags for dataset fields."""

        tags_map: Dict[str, List[TagAssociationClass]] = {}

        # Get tags from schema metadata
        schema_metadata = self.graph.graph.get_aspect(
            asset_urn.urn(), SchemaMetadataClass
        )
        if schema_metadata and schema_metadata.fields:
            tags_map = {
                field.fieldPath: [tag for tag in field.globalTags.tags]
                for field in schema_metadata.fields
                if field.globalTags
            }
            logger.debug(f"Field tags map: {tags_map}")

        # Get tags from editable schema metadata
        editable_schema_metadata = self.graph.graph.get_aspect(
            asset_urn.urn(), EditableSchemaMetadataClass
        )
        if (
            editable_schema_metadata
            and editable_schema_metadata.editableSchemaFieldInfo
        ):
            tags_map.update(
                {
                    field.fieldPath: [tag for tag in field.globalTags.tags]
                    for field in editable_schema_metadata.editableSchemaFieldInfo
                    if field.globalTags
                }
            )
            logger.debug(f"Post update: Field tags map: {tags_map}")

        # Create directives for each field and tag
        for field_path, field_tags in tags_map.items():
            field_urn = SchemaFieldUrn(asset_urn, field_path)
            for tag in field_tags:
                logger.debug(
                    f"Bootstrapping tags for schema field {tag} in {asset_urn}"
                )
                source_details = SourceDetails(
                    actor=self.actor_urn,
                    propagation_started_at=int(time.time() * 1000.0),
                )

                directive = self.generate_directive(
                    field_urn.urn(), None, GlobalTagsClass(tags=[tag]), source_details
                )
                if directive:
                    yield directive

    def _bootstrap_dataset(self, asset_urn: Urn) -> Iterable[TagPropagationDirective]:
        """Bootstrap tags for dataset."""

        global_tags = self.graph.graph.get_aspect(asset_urn.urn(), GlobalTagsClass)
        if not global_tags:
            return

        asset_tags = [tag for tag in global_tags.tags]

        if not asset_tags:
            return

        # for tag in asset_tags:
        source_details = SourceDetails(
            actor=self.actor_urn,
            propagation_started_at=int(time.time() * 1000.0),
        )
        directive = self.generate_directive(
            asset_urn.urn(), None, GlobalTagsClass(tags=asset_tags), source_details
        )
        if directive:
            yield directive

    def generate_directive(
        self,
        entity_urn: str,
        old_global_tags: Optional[GlobalTagsClass],
        current_global_tags: GlobalTagsClass,
        source_details: Optional[SourceDetails] = None,
    ) -> Optional[TagPropagationDirective]:
        """Generate a propagation directive for tags."""

        if source_details:
            origin = source_details.origin or entity_urn
            via = entity_urn if source_details.origin != entity_urn else None
            propagation_depth = (
                source_details.propagation_depth + 1
                if source_details.propagation_depth is not None
                else 1
            )
            actor = source_details.actor if source_details.actor else self.actor_urn
            propagation_started_at = source_details.propagation_started_at or int(
                time.time() * 1000.0
            )
            propagation_direction = source_details.propagation_direction
        else:
            origin = entity_urn
            via = None
            propagation_depth = 0
            actor = self.actor_urn
            propagation_started_at = int(time.time() * 1000.0)
            propagation_direction = None

        return TagPropagationDirective(
            propagate=True,
            tags=[tag.tag for tag in current_global_tags.tags],
            operation="ADD",
            entity=entity_urn,
            via=via,
            origin=origin,
            propagation_depth=propagation_depth,
            actor=actor,
            propagation_started_at=propagation_started_at,
            propagation_direction=propagation_direction,
        )

    def process_ece(self, event: EventEnvelope) -> Optional[TagPropagationDirective]:
        """
        Process metadata change events.
        Return a tag propagation directive or None if no propagation is desired.
        """
        if not (event.event_type == "EntityChangeEvent_v1" and self.config.enabled):
            return None

        assert isinstance(event.event, EntityChangeEvent)
        assert self.graph is not None
        assert isinstance(self.config, TagPropagatorConfig)

        semantic_event = event.event
        if not (
            semantic_event.category == "TAG"
            and semantic_event.operation in ["ADD", "REMOVE"]
        ):
            return None

        logger.info(f"Processing TAG event {event}")
        assert semantic_event.modifier, "tag urn should be present"

        # Check if tag should be propagated based on prefixes
        if self.config.tag_prefixes:
            propagate = any(
                [
                    True
                    for prefix in self.config.tag_prefixes
                    if semantic_event.modifier.startswith(prefix)
                ]
            )
            if not propagate:
                logger.debug(f"Not propagating {semantic_event.modifier}")
                return None

        # Extract parameters and context
        parameters = (
            semantic_event.parameters
            if semantic_event.parameters is not None
            else semantic_event._inner_dict.get("__parameters_json", {})
        )
        context_str = (
            parameters.get("context") if parameters.get("context") is not None else "{}"
        )

        tag = semantic_event.modifier

        # Parse source details from context
        source_details_parsed = SourceDetails.model_validate(json.loads(context_str))
        if not source_details_parsed.actor:
            source_details_parsed.actor = self.actor_urn

        return self.generate_directive(
            semantic_event.entityUrn,
            None,
            GlobalTagsClass(tags=[TagAssociationClass(tag=tag)]),
            source_details_parsed,
        )

    def create_property_change_proposal(
        self,
        propagation_directive: PropagationDirective,
        entity_urn: Urn,
        context: SourceDetails,
    ) -> PropagationOutput:
        """Create property change proposals based on the directive."""

        assert isinstance(propagation_directive, TagPropagationDirective)
        logger.info(
            f"Creating tag propagation proposal for {propagation_directive} for entity {entity_urn} with context {context}"
        )

        if entity_urn.entity_type == "dataset":
            self.graph.add_tags_to_dataset(
                str(entity_urn),
                propagation_directive.tags,
                context=context.for_metadata_attribution(),
                action_urn=self.action_urn,
            )
        elif entity_urn.entity_type == "schemaField":
            schema_field_urn: SchemaFieldUrn = SchemaFieldUrn.from_string(
                entity_urn.urn()
            )
            yield from self._add_tag_to_dataset_field_slow(
                Urn.from_string(schema_field_urn.parent),
                {schema_field_urn.field_path: propagation_directive.tags},
                propagation_directive,
                context,
            )

    def _create_attribution(
        self, source_details: SourceDetails
    ) -> MetadataAttributionClass:
        """Create a metadata attribution object."""

        source_details_dict = source_details.for_metadata_attribution()
        return MetadataAttributionClass(
            source=self.action_urn,
            time=int(time.time() * 1000.0),
            actor=self.actor_urn if not source_details.actor else source_details.actor,
            sourceDetail=source_details_dict,
        )

    def _create_audit_stamp(self, source_details: SourceDetails) -> AuditStampClass:
        """Create an audit stamp object."""

        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor=source_details.actor if source_details.actor else self.actor_urn,
        )

    def _deduplicate_field_paths(
        self, editable_schema_metadata: EditableSchemaMetadataClass
    ) -> None:
        """Deduplicate field paths in editable schema metadata."""

        field_paths = [
            x.fieldPath for x in editable_schema_metadata.editableSchemaFieldInfo
        ]
        field_paths_deduped = set(field_paths)

        if len(field_paths) != len(field_paths_deduped):
            logger.error("Duplicate field paths found in EditableSchemaMetadata")

            # Deduplicate fields
            editable_schema_field_info_deduped = []

            for field in editable_schema_metadata.editableSchemaFieldInfo:
                if field.fieldPath in field_paths_deduped:
                    editable_schema_field_info_deduped.append(field)
                    field_paths_deduped.remove(field.fieldPath)

            editable_schema_metadata.editableSchemaFieldInfo = (
                editable_schema_field_info_deduped
            )

    def _add_tag_to_dataset_field_slow(
        self,
        dataset_urn: Urn,
        field_tags: Dict[str, List[str]],
        tag_propagation_directive: TagPropagationDirective,
        source_details: SourceDetails,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Add tags to dataset fields."""

        if not dataset_urn.entity_type == "dataset":
            logger.error(
                f"Invalid dataset urn {dataset_urn}. Must start with urn:li:dataset"
            )
            return

        audit_stamp = self._create_audit_stamp(source_details)
        attribution = self._create_attribution(source_details)
        source_details_dict = source_details.for_metadata_attribution()

        editableSchemaMetadata = self.graph.graph.get_aspect(
            dataset_urn.urn(), EditableSchemaMetadataClass
        )

        # Handle case when editable schema metadata doesn't exist
        if editableSchemaMetadata is None:
            if tag_propagation_directive.operation == "ADD":
                editableSchemaMetadata = self._create_new_editable_schema_metadata(
                    field_tags, attribution, source_details_dict
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn.urn(),
                    aspect=editableSchemaMetadata,
                )
            return

        # Handle existing editable schema metadata
        if editableSchemaMetadata.editableSchemaFieldInfo is None:
            editableSchemaMetadata.editableSchemaFieldInfo = []

        # Deduplicate field paths if needed
        self._deduplicate_field_paths(editableSchemaMetadata)

        mutation_needed = False
        # Process each field and its tags
        for field_path, tags in field_tags.items():
            field_mutation_needed = self._process_field_tags(
                editableSchemaMetadata,
                field_path,
                tags,
                tag_propagation_directive.operation,
                attribution,
                audit_stamp,
                source_details_dict,
            )
            mutation_needed = mutation_needed or field_mutation_needed

        if mutation_needed:
            if not editableSchemaMetadata.validate():
                raise ValueError(
                    f"EditableSchemaMetadata validation failed for {dataset_urn}. "
                    f"The failing metadata was: {editableSchemaMetadata} "
                )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn.urn(),
                aspect=editableSchemaMetadata,
            )

    def _create_new_editable_schema_metadata(
        self,
        field_tags: Dict[str, List[str]],
        attribution: MetadataAttributionClass,
        source_details_dict: Dict,
    ) -> EditableSchemaMetadataClass:
        """Create a new editable schema metadata object with field tags."""

        return EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath=field_path,
                    globalTags=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(
                                tag,
                                context=json.dumps(source_details_dict),
                                attribution=attribution,
                            )
                            for tag in tags
                        ],
                    ),
                )
                for field_path, tags in field_tags.items()
            ]
        )

    def _process_field_tags(
        self,
        editable_schema_metadata: EditableSchemaMetadataClass,
        field_path: str,
        tags: List[str],
        operation: str,
        attribution: MetadataAttributionClass,
        audit_stamp: AuditStampClass,
        source_details_dict: Dict,
    ) -> bool:
        """Process tags for a field, return True if mutation is needed."""

        existing_fields = [
            x.fieldPath.lower()
            for x in editable_schema_metadata.editableSchemaFieldInfo
        ]
        mutation_needed = False

        # Field doesn't exist, add it if operation is ADD
        if field_path.lower() not in existing_fields:
            if operation == "ADD":
                field_info = EditableSchemaFieldInfoClass(
                    fieldPath=field_path,
                    globalTags=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(
                                tag,
                                context=json.dumps(source_details_dict),
                                attribution=attribution,
                            )
                            for tag in tags
                        ],
                    ),
                )
                editable_schema_metadata.editableSchemaFieldInfo.append(field_info)
                mutation_needed = True
        else:
            # Field exists, update its tags
            for field_info in editable_schema_metadata.editableSchemaFieldInfo:
                if field_info.fieldPath.lower() == field_path.lower():
                    mutation_needed = (
                        self._update_field_global_tags(
                            field_info,
                            tags,
                            operation,
                            attribution,
                            audit_stamp,
                            source_details_dict,
                        )
                        or mutation_needed
                    )

        return mutation_needed

    def _update_field_global_tags(
        self,
        field_info: EditableSchemaFieldInfoClass,
        tags: List[str],
        operation: str,
        attribution: MetadataAttributionClass,
        audit_stamp: AuditStampClass,
        source_details_dict: Dict,
    ) -> bool:
        """Update global tags for a field, return True if mutation is needed."""

        mutation_needed = False

        # No global tags yet, create if adding
        if field_info.globalTags is None:
            if operation == "ADD":
                field_info.globalTags = GlobalTagsClass(
                    tags=[
                        TagAssociationClass(
                            tag,
                            context=json.dumps(source_details_dict),
                            attribution=attribution,
                        )
                        for tag in tags
                    ],
                )
                mutation_needed = True
        else:
            # Process each tag based on operation
            existing_tags = [x.tag for x in field_info.globalTags.tags]

            for tag in tags:
                if tag not in existing_tags:
                    if operation == "ADD":
                        field_info.globalTags.tags.append(
                            TagAssociationClass(
                                tag,
                                context=json.dumps(source_details_dict),
                                attribution=attribution,
                            )
                        )
                        mutation_needed = True
                else:
                    if operation == "REMOVE":
                        field_info.globalTags.tags = [
                            x for x in field_info.globalTags.tags if x.tag != tag
                        ]
                        mutation_needed = True

        return mutation_needed

    def asset_filters(self) -> Dict[str, Dict[str, List[SearchFilterRule]]]:
        """Get filters for asset types."""

        return {
            "dataset": {
                "dataset": [
                    SearchFilterRule(
                        field="tags",
                        condition="EXISTS",
                        values=[],
                        negated=False,
                    ),
                    SearchFilterRule(
                        field="fieldTags",
                        condition="EXISTS",
                        values=[],
                        negated=False,
                    ),
                    SearchFilterRule(
                        field="editedFieldTags",
                        condition="EXISTS",
                        values=[],
                        negated=False,
                    ),
                ]
            }
        }

    def identify_assets(self) -> Iterable[Urn]:
        """Identify assets for processing."""

        urns: Iterable[str] = self.graph.graph.get_urns_by_filter(
            entity_types=["dataset"],
            query="*",
            extra_or_filters=[
                {
                    "field": "tagAttributionSources",
                    "values": [self.action_urn],
                    "condition": "EQUAL",
                },
                {
                    "field": "fieldTagAttributionSources",
                    "values": [self.action_urn],
                    "condition": "EQUAL",
                },
                {
                    "field": "editedFieldTagAttributionSources",
                    "values": [self.action_urn],
                    "condition": "EQUAL",
                },
            ],
        )

        for urn in urns:
            yield Urn.from_string(urn)

    def rollbackable_assets(self) -> Iterable[Urn]:
        """Get assets that can be rolled back."""

        return self.identify_assets()

    def rollback_asset(self, asset: Urn) -> None:
        """Rollback tag associations from an asset."""

        # Rollback dataset-level tag associations
        self._rollback_dataset_tags(asset)

        # Rollback field-level tag associations in editable schema metadata
        self._rollback_editable_schema_tags(asset)

        # Rollback field-level tag associations in schema metadata
        self._rollback_schema_tags(asset)

    def _rollback_dataset_tags(self, asset: Urn) -> None:
        """Rollback tag associations from a dataset."""

        global_tags = self.graph.graph.get_aspect(asset.urn(), GlobalTagsClass)
        if global_tags:
            tags = []
            aspect_updated = False
            for tag in global_tags.tags:
                assert isinstance(tag, TagAssociationClass)
                if tag.attribution and tag.attribution.source == self.action_urn:
                    logger.info(f"Deleting tag association {tag.tag}")
                    aspect_updated = True
                else:
                    tags.append(tag)

            # Emit updated aspect if tags were removed
            if aspect_updated:
                global_tags.tags = tags
                self.graph.graph.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn=asset.urn(),
                        aspect=global_tags,
                    )
                )

    def _rollback_editable_schema_tags(self, asset: Urn) -> None:
        """Rollback tag associations from editable schema metadata."""

        logger.debug("Checking if there is any field tag to rollback")
        editable_schema_metadata = self.graph.graph.get_aspect(
            asset.urn(), EditableSchemaMetadataClass
        )
        logger.debug(f"metadata: {editable_schema_metadata}")

        if not editable_schema_metadata:
            return

        editableSchemaFiledInfo = []
        aspect_updated = False

        for field in editable_schema_metadata.editableSchemaFieldInfo:
            if field.globalTags:
                tags = []
                for tag in field.globalTags.tags:
                    if tag.attribution and tag.attribution.source == self.action_urn:
                        logger.info(f"Deleting tag association {tag}")
                        aspect_updated = True
                    else:
                        tags.append(tag)

                field.globalTags.tags = tags

            editableSchemaFiledInfo.append(field)

        editable_schema_metadata.editableSchemaFieldInfo = editableSchemaFiledInfo

        if aspect_updated:
            self.graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=asset.urn(),
                    aspect=editable_schema_metadata,
                )
            )

    def _rollback_schema_tags(self, asset: Urn) -> None:
        """Rollback tag associations from schema metadata."""

        logger.debug("Checking if there is any schema field tag to rollback")
        schema_metadata = self.graph.graph.get_aspect(asset.urn(), SchemaMetadataClass)
        logger.debug(f"Schema metadata info: {schema_metadata}")

        if not schema_metadata:
            return

        schemaFiledInfo = []
        aspect_updated = False

        for field in schema_metadata.fields:
            if field.globalTags:
                tags = []
                for tag in field.globalTags.tags:
                    if tag.attribution and tag.attribution.source == self.action_urn:
                        logger.info(f"Deleting tag association {tag.tag}")
                        aspect_updated = True
                    else:
                        tags.append(tag)

                field.globalTags.tags = tags

            schemaFiledInfo.append(field)

        schema_metadata.fields = schemaFiledInfo

        if aspect_updated:
            self.graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=asset.urn(),
                    aspect=schema_metadata,
                )
            )
