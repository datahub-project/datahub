# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import time
from typing import Iterable, List, Optional, Tuple

from pydantic import Field

from datahub.configuration.common import ConfigEnum
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EntityChangeEventClass as EntityChangeEvent,
    GenericAspectClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    MetadataChangeLogClass
)
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.plugin.action.mcl_utils import MCLProcessor
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.propagation.propagation_utils import (
    DirectionType,
    PropagationConfig,
    PropagationDirective,
    RelationshipType,
    SourceDetails,
)
from datahub_actions.plugin.action.stats_util import (
    ActionStageReport,
    EventProcessingStats,
)

logger = logging.getLogger(__name__)


class GlossaryTermPropagationDirective(PropagationDirective):
    term_urn: str = Field(
        description="Glossary term URN to be propagated."
    )


class ColumnPropagationRelationships(ConfigEnum):
    DOWNSTREAM = "downstream"


class GlossaryTermPropagationConfig(PropagationConfig):
    """
    Configuration model for glossary term propagation.

    Attributes:
        enabled (bool): Indicates whether glossary term propagation is enabled or not. Default is True.
        columns_enabled (bool): Indicates whether column glossary term propagation is enabled or not. Default is True.

    Example:
        config = GlossaryTermPropagationConfig(enabled=True)
    """

    enabled: bool = Field(
        True,
        description="Indicates whether glossary term propagation is enabled or not.",
    )
    columns_enabled: bool = Field(
        True,
        description="Indicates whether column glossary term propagation is enabled or not.",
    )
    column_propagation_relationships: List[ColumnPropagationRelationships] = Field(
        [
            ColumnPropagationRelationships.DOWNSTREAM,
        ],
        description="Relationships for column glossary term propagation.",
    )

    allowed_terms: Optional[List[str]] = Field(
        None,
        description="Optional allowed terms to restrict term propagation to",
        examples=[
            "urn:li:glossaryTerm:Sensitive",
        ],
    )

    one_upstream: bool = Field(
        True,
        description="allow propagation to downstream, only if downstream has only one upstream"
    )

class GlossaryTermPropagationAction(Action):
    def __init__(self, config: GlossaryTermPropagationConfig, ctx: PipelineContext):
        super().__init__()
        self.action_urn: str
        if not ctx.pipeline_name.startswith("urn:li:dataHubAction"):
            self.action_urn = f"urn:li:dataHubAction:{ctx.pipeline_name}"
        else:
            self.action_urn = ctx.pipeline_name
        self.config: GlossaryTermPropagationConfig = config

        self._resolved_allowed_terms()

        self.last_config_refresh: float = 0
        self.ctx = ctx
        self.mcl_processor = MCLProcessor()
        self.actor_urn = "urn:li:corpuser:__datahub_system"

        self.mcl_processor.register_processor(
            "schemaField",
            "glossaryTerms",
            self.process_schema_field_terms,
        )

        self._refresh_config()
        self._stats = ActionStageReport()
        self._stats.start()
        assert self.ctx.graph
        self._rate_limited_emit_mcp = self.config.get_rate_limited_emit_mcp(
            self.ctx.graph.graph
        )

    def name(self) -> str:
        return "GlossaryTermPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = GlossaryTermPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Glossary Term Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def _should_stop_propagation(
            self, source_details: SourceDetails
    ) -> Tuple[bool, str]:
        """
        Check if the propagation should be stopped based on the source details.
        Return result and reason.
        """
        if source_details.propagation_started_at and (
                int(time.time() * 1000.0) - source_details.propagation_started_at
                >= self.config.max_propagation_time_millis
        ):
            return (True, "Propagation time exceeded.")
        if (
                source_details.propagation_depth
                and source_details.propagation_depth >= self.config.max_propagation_depth
        ):
            return (True, "Propagation depth exceeded.")
        return False, ""

    def _get_propagation_relationships(
            self, entity_type: str, source_details: Optional[SourceDetails]
    ) -> List[Tuple[RelationshipType, DirectionType]]:
        possible_relationships = []
        if entity_type == "schemaField":
            if (source_details is not None) and (
                    source_details.propagation_relationship
                    and source_details.propagation_direction
            ):
                restricted_relationship = source_details.propagation_relationship
                restricted_direction = source_details.propagation_direction
                possible_relationships.append(
                    (restricted_relationship, restricted_direction)
                )
                logger.debug(f"Possible relationships: {possible_relationships}")
                return possible_relationships
            for relationship in self.config.column_propagation_relationships:
                if relationship == ColumnPropagationRelationships.DOWNSTREAM:
                    possible_relationships.append(
                        (RelationshipType.LINEAGE, DirectionType.DOWN)
                    )
        logger.debug(f"Possible relationships: {possible_relationships}")
        return possible_relationships

    def process_schema_field_terms(
            self,
            entity_urn: str,
            aspect_name: str,
            aspect_value: GenericAspectClass,
            previous_aspect_value: Optional[GenericAspectClass],
    ) -> Optional[GlossaryTermPropagationDirective]:
        """
        Process changes in the glossaryTerms aspect of schemaField entities.
        Produce a directive to propagate the glossary term.
        Business Logic checks:
            - If the term is sourced by this action, then we propagate it.
            - If the term is not sourced by this action, then we log a warning and propagate it.
            - If we have exceeded the maximum depth of propagation or maximum
              time for propagation, then we stop propagation and don't return a directive.
        """
        if (
                aspect_name != "glossaryTerms"
                or guess_entity_type(entity_urn) != "schemaField"
        ):
            # not a glossaryTerms aspect or not a schemaField entity
            return None

        logger.debug("Processing 'glossaryTerms' MCL")
        if self.config.columns_enabled:
            current_terms: GlossaryTermsClass  = GlossaryTermsClass.from_obj(json.loads(aspect_value.value))
            old_terms: GlossaryTermsClass = (
                None
                if previous_aspect_value is None
                else GlossaryTermsClass.from_obj(
                    json.loads(previous_aspect_value.value)
                )
            )

            if current_terms.terms and len(current_terms.terms) > 0:
                # get the most recently updated term with attribution
                terms_with_attribution = [term for term in current_terms.terms if term.attribution]
                if not terms_with_attribution:
                    logger.warning(f"No terms with attribution found for {entity_urn}")
                    return None

                current_term_instance: GlossaryTermAssociationClass  = sorted(
                    terms_with_attribution,
                    key=lambda x: x.attribution.time if x.attribution else 0,
                )[-1]

                assert current_term_instance.attribution
                if (
                        current_term_instance.attribution.source is None
                        or current_term_instance.attribution.source
                        != self.action_urn
                ):
                    logger.warning(
                        f"Term is not sourced by this action which is unexpected. Skipping propagation"
                    )
                    return None

                source_details = (
                    (current_term_instance.attribution.sourceDetail)
                    if current_term_instance.attribution
                    else {}
                )
                source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                    source_details
                )
                should_stop_propagation, reason = self._should_stop_propagation(
                    source_details_parsed
                )
                if should_stop_propagation:
                    logger.warning(f"Stopping propagation for {entity_urn}. {reason}")
                    return None
                else:
                    logger.debug(f"Propagating glossary term for {entity_urn}")

                propagation_relationships = self._get_propagation_relationships(
                    entity_type="schemaField", source_details=source_details_parsed
                )
                origin_entity = (
                    source_details_parsed.origin
                    if source_details_parsed.origin
                    else entity_urn
                )

                if not self._is_term_allowed(current_term_instance.urn):
                    logger.info(f"Term {current_term_instance.urn} is not in the allowed terms list. Skipping propagation.")
                    return None

                if old_terms is None or not old_terms.terms:
                    # No previous terms, this is an ADD operation
                    operation = "ADD"

                    return GlossaryTermPropagationDirective(
                        propagate=True,
                        term_urn=current_term_instance.urn,
                        operation=operation,
                        entity=entity_urn,
                        origin=origin_entity,
                        via=entity_urn,
                        actor=self.actor_urn,
                        propagation_started_at=source_details_parsed.propagation_started_at,
                        propagation_depth=(
                            source_details_parsed.propagation_depth + 1
                            if source_details_parsed.propagation_depth
                            else 1
                        ),
                        relationships=propagation_relationships,
                    )

                if old_terms is not None and old_terms.terms:
                    if len(current_terms.terms) > len(old_terms.terms):
                        operation = "ADD"

                        return GlossaryTermPropagationDirective(
                            propagate=True,
                            term_urn=current_term_instance.urn,
                            operation=operation,
                            entity=entity_urn,
                            origin=origin_entity,
                            via=entity_urn,
                            actor=self.actor_urn,
                            propagation_started_at=source_details_parsed.propagation_started_at,
                            propagation_depth=(
                                source_details_parsed.propagation_depth + 1
                                if source_details_parsed.propagation_depth
                                else 1
                            ),
                            relationships=propagation_relationships,
                        )
                    if len(current_terms.terms) < len(old_terms.terms) and len(old_terms.terms) - len(current_terms.terms) == 1:
                        operation = "REMOVE"

                        # Find the term that was removed
                        current_term_urns = {term.urn for term in current_terms.terms}
                        removed_term = next(
                            (term for term in old_terms.terms if term.urn not in current_term_urns),
                            None
                        )

                        if removed_term:
                            source_details = (
                                (removed_term.attribution.sourceDetail)
                                if removed_term.attribution
                                else {}
                            )
                            source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                                source_details
                            )

                            propagation_relationships = self._get_propagation_relationships(
                                entity_type="schemaField", source_details=source_details_parsed
                            )
                            origin_entity = (
                                source_details_parsed.origin
                                if source_details_parsed.origin
                                else entity_urn
                            )

                            return GlossaryTermPropagationDirective(
                                propagate=True,
                                term_urn=removed_term.urn,
                                operation=operation,
                                entity=entity_urn,
                                origin=origin_entity,
                                via=entity_urn,
                                actor=self.actor_urn,
                                propagation_started_at=source_details_parsed.propagation_started_at,
                                propagation_depth=(
                                    source_details_parsed.propagation_depth + 1
                                    if source_details_parsed.propagation_depth
                                    else 1
                                ),
                                relationships=propagation_relationships,
                            )
            elif old_terms is not None and old_terms.terms and len(old_terms.terms) > 0:
                # All terms were removed
                operation = "REMOVE"

                # Get the most recently updated term with attribution from old terms
                old_terms_with_attribution = [term for term in old_terms.terms if term.attribution]
                if not old_terms_with_attribution:
                    logger.warning(f"No terms with attribution found in old terms for {entity_urn}")
                    return None

                removed_term = sorted(
                    old_terms_with_attribution,
                    key=lambda x: x.attribution.time if x.attribution else 0,
                )[-1]

                source_details = (
                    (removed_term.attribution.sourceDetail)
                    if removed_term.attribution
                    else {}
                )
                source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                    source_details
                )

                propagation_relationships = self._get_propagation_relationships(
                    entity_type="schemaField", source_details=source_details_parsed
                )
                origin_entity = (
                    source_details_parsed.origin
                    if source_details_parsed.origin
                    else entity_urn
                )

                return GlossaryTermPropagationDirective(
                    propagate=True,
                    term_urn=removed_term.urn,
                    operation=operation,
                    entity=entity_urn,
                    origin=origin_entity,
                    via=entity_urn,
                    actor=self.actor_urn,
                    propagation_started_at=source_details_parsed.propagation_started_at,
                    propagation_depth=(
                        source_details_parsed.propagation_depth + 1
                        if source_details_parsed.propagation_depth
                        else 1
                    ),
                    relationships=propagation_relationships,
                )

        return None

    def should_propagate(
            self, event: EventEnvelope
    ) -> Optional[GlossaryTermPropagationDirective]:
        if self.mcl_processor.is_mcl(event):
            return self.mcl_processor.process(event)
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                    semantic_event.category == "GLOSSARY_TERM"
                    and self.config is not None
                    and self.config.enabled
            ):
                logger.debug("Processing EntityChangeEvent Glossary Term Change")
                if self.config.columns_enabled and (
                        semantic_event.entityType == "schemaField"
                ):
                    if semantic_event.parameters:
                        parameters = semantic_event.parameters
                    else:
                        parameters = semantic_event._inner_dict.get(
                            "__parameters_json", {}
                        )
                    term_urn = semantic_event.modifier
                    assert term_urn

                    origin = parameters.get("origin")
                    origin = origin or semantic_event.entityUrn
                    via = (
                        semantic_event.entityUrn
                        if origin != semantic_event.entityUrn
                        else None
                    )
                    logger.debug(f"Origin: {origin}")
                    logger.debug(f"Via: {via}")
                    logger.debug(f"Term URN: {term_urn}")
                    logger.debug(f"Semantic event {semantic_event}")

                    if not self._is_term_allowed(term_urn):
                        logger.info(f"Term {term_urn} is not in the allowed terms list. Skipping propagation.")
                        return None

                    return GlossaryTermPropagationDirective(
                        propagate=True,
                        term_urn=term_urn,
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
                        relationships=self._get_propagation_relationships(
                            entity_type="schemaField",
                            source_details=None,
                        ),
                    )
        return None

    def _apply_term_to_entity(
            self,
            graph: AcrylDataHubGraph,
            operation: str,
            entity_urn: str,
            term_urn: str,
            context: SourceDetails,
    ) -> Optional[MetadataChangeProposalWrapper]:

        try:
            auditStamp = AuditStampClass(
                time=int(time.time() * 1000.0), actor=self.actor_urn
            )

            source_details = context.for_metadata_attribution()
            attribution: MetadataAttributionClass = MetadataAttributionClass(
                source=self.action_urn,
                time=auditStamp.time,
                actor=self.actor_urn,
                sourceDetail=source_details,
            )

            # Get existing terms
            glossary_terms = graph.graph.get_aspect(entity_urn, GlossaryTermsClass)

            if operation == "ADD" or operation == "MODIFY":
                if glossary_terms:
                    # Check if term already exists
                    for term_association in glossary_terms.terms:
                        if term_association.urn == term_urn:
                            # Term already exists, update attribution if needed
                            term_association.attribution = attribution
                            return MetadataChangeProposalWrapper(
                                entityUrn=entity_urn,
                                aspect=glossary_terms,
                            )

                    # Term doesn't exist, add it
                    glossary_terms.terms.append(
                        GlossaryTermAssociationClass(
                            urn=term_urn,
                            attribution=attribution,
                        )
                    )
                else:
                    # No terms exist, create new glossary terms aspect
                    glossary_terms = GlossaryTermsClass(
                        terms=[
                            GlossaryTermAssociationClass(
                                urn=term_urn,
                                attribution=attribution,
                            )
                        ],
                        auditStamp=auditStamp,
                    )

                return MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=glossary_terms,
                )

            elif operation == "REMOVE":
                if glossary_terms and glossary_terms.terms:
                    # Filter out the term to remove
                    original_count = len(glossary_terms.terms)
                    glossary_terms.terms = [
                        term for term in glossary_terms.terms if term.urn != term_urn
                    ]

                    # If we removed a term, return the updated aspect
                    if len(glossary_terms.terms) < original_count:
                        return MetadataChangeProposalWrapper(
                            entityUrn=entity_urn,
                            aspect=glossary_terms,
                        )

            return None
        except Exception as e:
            logger.error(f"Error applying term to entity {entity_urn}: {e}")
            return None

    def _refresh_config(self, event: Optional[EventEnvelope] = None) -> None:
        """
        Fetches important configuration flags from the global settings entity to
        override client-side settings.
        If not found, it will use the client-side values.
        """
        now = time.time()
        try:
            if now - self.last_config_refresh > 60 or self._is_settings_change(event):
                assert self.ctx.graph
                entity_dict = self.ctx.graph.graph.get_entity_raw(
                    "urn:li:globalSettings:0", ["globalSettingsInfo"]
                )
                if entity_dict:
                    global_settings = entity_dict.get("aspects", {}).get(
                        "globalSettingsInfo"
                    )
                    if global_settings:
                        term_propagation_config = global_settings.get("value", {}).get(
                            "glossaryTermPropagation"
                        )
                        if term_propagation_config:
                            if term_propagation_config.get("enabled") is not None:
                                logger.info(
                                    "Overwriting the asset-level config using globalSettings"
                                )
                                self.config.enabled = term_propagation_config.get(
                                    "enabled"
                                )
                            if (
                                    term_propagation_config.get("columnPropagationEnabled")
                                    is not None
                            ):
                                logger.info(
                                    "Overwriting the column-level config using globalSettings"
                                )
                                self.config.columns_enabled = (
                                    term_propagation_config.get(
                                        "columnPropagationEnabled"
                                    )
                                )
        except Exception:
            # We don't want to fail the pipeline if we can't fetch the config
            logger.warning(
                "Error fetching global settings for term propagation. Will try again in 1 minute.",
                exc_info=True,
            )
        """
        TODO: need to be removed
        """
        logger.info(f"Glossary Term Propagation Config: {self.config}")
        self.last_config_refresh = now

    def _is_settings_change(self, event: Optional[EventEnvelope]) -> bool:
        if event and isinstance(event.event, MetadataChangeLogClass):
            entity_type = event.event.entityType
            if entity_type == "globalSettings":
                return True
        return False

    def act(self, event: EventEnvelope) -> None:
        assert self.ctx.graph
        for mcp in self.act_async(event):
            self._rate_limited_emit_mcp(mcp)

    def act_async(
            self, event: EventEnvelope
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Process the event asynchronously and return the change proposals
        """
        self._refresh_config(event)
        if not self.config.enabled or not self.config.columns_enabled:
            logger.warning("Glossary term propagation is disabled. Skipping event")
            return
        else:
            logger.debug(f"Processing event {event}")

        if not self._stats.event_processing_stats:
            self._stats.event_processing_stats = EventProcessingStats()

        stats = self._stats.event_processing_stats
        stats.start(event)

        try:
            term_propagation_directive = self.should_propagate(event)
            logger.debug(
                f"Glossary term propagation directive for {event} --> {term_propagation_directive}"
            )

            if (
                    term_propagation_directive is not None
                    and term_propagation_directive.propagate
            ):
                self._stats.increment_assets_processed(term_propagation_directive.entity)
                context = SourceDetails(
                    origin=term_propagation_directive.origin,
                    via=term_propagation_directive.via,
                    propagated=True,
                    actor=term_propagation_directive.actor,
                    propagation_started_at=term_propagation_directive.propagation_started_at,
                    propagation_depth=term_propagation_directive.propagation_depth,
                )
                assert self.ctx.graph

                lineage_downstream = (
                                         RelationshipType.LINEAGE,
                                         DirectionType.DOWN,
                                     ) in term_propagation_directive.relationships

                logger.debug(
                    f"Lineage Downstream: {lineage_downstream}"
                )

                if lineage_downstream:
                    yield from self._propagate_to_downstreams(
                        term_propagation_directive, context
                    )

            stats.end(event, success=True)

        except Exception:
            logger.error(f"Error processing event {event}:", exc_info=True)
            stats.end(event, success=False)

    def _only_one_upstream_field(
            self,
            graph: AcrylDataHubGraph,
            downstream_field: str,
            upstream_field: str,
    ) -> bool:
        """
        Check if there is only one upstream field for the downstream field. If upstream_field is provided,
        it will also check if the upstream field is the only upstream
        """
        upstreams = graph.get_upstreams(entity_urn=downstream_field)
        # Use a set here in case there are duplicated upstream edges
        upstream_fields = list(
            {x for x in upstreams if guess_entity_type(x) == "schemaField"}
        )

        logger.debug(
            f"Upstream fields for {downstream_field}: {upstream_fields} (expected: {upstream_field})"
        )

        # If we found no upstreams for the downstream field, simply skip.
        if not upstream_fields:
            logger.debug(
                f"No upstream fields found. Skipping propagation to downstream {downstream_field}"
            )
            return False

        # Convert the set to a list to access by index
        result = len(upstream_fields) == 1 and upstream_fields[0] == upstream_field
        if not result:
            logger.warning(
                f"Failed check for single upstream: Found upstream fields {upstream_fields} for downstream {downstream_field}. Expecting only one upstream field: {upstream_field}"
            )
        return result

    def _propagate_to_downstreams(
            self, term_propagation_directive: GlossaryTermPropagationDirective, context: SourceDetails
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Propagate the glossary term to downstream entities.
        """
        assert self.ctx.graph
        downstreams = self.ctx.graph.get_downstreams(
            entity_urn=term_propagation_directive.entity
        )
        logger.debug(
            f"Downstreams: {downstreams} for {term_propagation_directive.entity}"
        )
        entity_urn = term_propagation_directive.entity
        propagated_context = SourceDetails.parse_obj(context.dict())
        propagated_context.propagation_relationship = RelationshipType.LINEAGE
        propagated_context.propagation_direction = DirectionType.DOWN
        propagated_entities_this_hop_count = 0

        if guess_entity_type(entity_urn) == "schemaField":
            downstream_fields = {
                x for x in downstreams if guess_entity_type(x) == "schemaField"
            }
            for field in downstream_fields:
                schema_field_urn = Urn.from_string(field)
                parent_urn = schema_field_urn.get_entity_id()[0]
                field_path = schema_field_urn.get_entity_id()[1]

                logger.debug(
                    f"Will {term_propagation_directive.operation} glossary term {term_propagation_directive.term_urn} for {field_path} on {parent_urn}"
                )

                parent_entity_type = guess_entity_type(parent_urn)

                propagate_term: bool = False

                if parent_entity_type == "dataset":

                    if self.config.one_upstream is False:
                        propagate_term = True

                    elif self.config.one_upstream is True and self._only_one_upstream_field(
                            self.ctx.graph,
                            downstream_field=field,
                            upstream_field=entity_urn,
                    ):
                        propagate_term = True

                    if (
                            propagated_entities_this_hop_count
                            >= self.config.max_propagation_fanout
                    ):
                        logger.warning(
                            f"Exceeded max propagation fanout of {self.config.max_propagation_fanout}. Skipping propagation to downstream {field}"
                        )
                        # No need to propagate to more downstreams
                        return None

                    if propagate_term:
                        maybe_mcp = self._apply_term_to_entity(
                            self.ctx.graph,
                            term_propagation_directive.operation,
                            field,
                            term_propagation_directive.term_urn,
                            context=propagated_context,
                        )
                        if maybe_mcp:
                            propagated_entities_this_hop_count += 1
                            yield maybe_mcp

                self._stats.increment_assets_impacted(field)

    def close(self) -> None:
        return

    def _resolved_allowed_terms(self) -> None:
        if self.config.allowed_terms and len(self.config.allowed_terms) > 0:
            resolved_terms = []
            for term in self.config.allowed_terms:
                if term.startswith("urn:li:glossaryTerm:"):
                    resolved_terms.append(term)

            self.config.allowed_terms = resolved_terms
            logger.info(
                f"[Config] Will propagate terms {self.config.allowed_terms}"
            )

    def _is_term_allowed(self, term_urn: str) -> bool:
        if not self.config.allowed_terms or len(self.config.allowed_terms) == 0:
            return True

        return term_urn in self.config.allowed_terms
