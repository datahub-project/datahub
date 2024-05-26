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
from typing import Any, Callable, List, Optional

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaMetadataClass,
)
from datahub.metadata.schema_classes import EntityChangeEventClass as EntityChangeEvent
from datahub.metadata.schema_classes import GenericAspectClass, ParametersClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.action.action import Action
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel, Field, validator
from ratelimit import limits, sleep_and_retry

from datahub_integrations.actions.action_extended import ExtendedAction
from datahub_integrations.propagation.doc.mcl_utils import MCLProcessor

logger = logging.getLogger(__name__)


class DocPropagationDirective(BaseModel):
    propagate: bool = Field(
        description="Indicates whether the documentation should be propagated."
    )
    doc_string: Optional[str] = Field(
        default=None, description="Documentation string to be propagated."
    )
    operation: str = Field(
        description="Operation to be performed on the documentation. Can be ADD, MODIFY or REMOVE."
    )
    entity: str = Field(
        description="Entity URN from which the documentation is propagated. This will either be the same as the origin or the via entity, depending on the propagation path."
    )
    origin: str = Field(
        description="Origin entity for the documentation. This is the entity that triggered the documentation propagation.",
    )
    via: Optional[str] = Field(
        None,
        description="Via entity for the documentation. This is the direct entity that the documentation was propagated through.",
    )
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the documentation propagation.",
    )


class SourceDetails(BaseModel):
    origin: Optional[str] = Field(
        None,
        description="Origin entity for the documentation. This is the entity that triggered the documentation propagation.",
    )
    via: Optional[str] = Field(
        None,
        description="Via entity for the documentation. This is the direct entity that the documentation was propagated through.",
    )
    propagated: Optional[str] = Field(
        None,
        description="Indicates whether the documentation was propagated.",
    )
    actor: Optional[str] = Field(
        None,
        description="Actor that triggered the documentation propagation.",
    )

    @validator("propagated", pre=True)
    def convert_boolean_to_lowercase_string(cls, v: Any) -> Optional[str]:
        if isinstance(v, bool):
            return str(v).lower()
        return v


class DocPropagationConfig(ConfigModel):
    """
    Configuration model for documentation propagation.

    Attributes:
        enabled (bool): Indicates whether documentation propagation is enabled or not. Default is True.
        columns_enabled (bool): Indicates whether column documentation propagation is enabled or not. Default is True.
        datasets_enabled (bool): Indicates whether dataset level documentation propagation is enabled or not. Default is False.
        term_attachment_enabled (bool): Indicates whether term attachment is enabled or not. Default is False.
        target_term (Optional[str]): Optional target term to restrict documentation propagation.
            If provided, only documentation from this specific term and terms related to it via `isA` relationship will be propagated.
            Default is None, meaning that documentation from any term will be
            propagated as long as term_inheritance is enabled.
        term_groups (Optional[List[str]]): Optional list of term groups to
        restrict documentation propagation.
            If provided, only terms within these groups will be propagated. Default is None, meaning all term groups will be propagated.

    Note:
        Documentation propagation allows documentation to be automatically propagated to downstream entities.
        Enabling documentation propagation can help maintain consistent metadata across connected entities.
        The `enabled` attribute controls whether term propagation is enabled or disabled.
        The `target_terms` attribute can be used to specify a set of specific terms or all terms related to these specific terms that should be propagated.
        The `term_groups` attribute can be used to specify a list of term groups to restrict propagation to.

    Example:
        config = DocPropagationConfig(enabled=True, target_terms=["urn:li:glossaryTerm:Sensitive"])
    """

    enabled: bool = Field(
        True,
        description="Indicates whether documentation propagation is enabled or not.",
        example=True,
    )
    columns_enabled: bool = Field(
        True,
        description="Indicates whether column documentation propagation is enabled or not.",
        example=True,
    )
    datasets_enabled: bool = Field(
        False,
        description="Indicates whether dataset level documentation propagation is enabled or not.",
        example=False,
    )
    term_attachment_enabled: bool = Field(
        False,
        description="Indicates whether term attachment is enabled or not.",
        example=False,
    )

    target_term: Optional[str] = Field(
        None,
        description="Optional target term to restrict documentation propagation.",
        example="urn:li:glossaryTerm:Sensitive",
    )

    term_groups: Optional[List[str]] = Field(
        None,
        description="Optional list of term groups to restrict documentation propagation.",
        example=["urn:li:glossaryNode:Metrics", "urn:li:glossaryNode:Dimensions"],
    )

    bootstrap: bool = Field(
        False,
        description="Indicates whether to bootstrap the action. Default is False.",
    )

    event_processing_rate_limit: int = Field(
        1,
        description="Rate limit for processing events. Default is 1 event per rate period.",
    )
    event_processing_rate_period: int = Field(
        1,
        description="Rate limit period for processing events. Default is 1 second.",
    )


def get_field_path(schema_field_urn: str) -> str:
    urn = Urn.create_from_string(schema_field_urn)
    return urn.get_entity_id()[1]


def get_field_doc_from_dataset(
    graph: AcrylDataHubGraph, dataset_urn: str, schema_field_urn: str
) -> Optional[str]:
    editableSchemaMetadata = graph.graph.get_aspect(
        dataset_urn, EditableSchemaMetadataClass
    )
    if editableSchemaMetadata is not None:
        if editableSchemaMetadata.editableSchemaFieldInfo is not None:
            field_info = [
                x
                for x in editableSchemaMetadata.editableSchemaFieldInfo
                if x.fieldPath == get_field_path(schema_field_urn)
            ]
            if field_info:
                return field_info[0].description
    return None


ECE_EVENT_TYPE = "EntityChangeEvent_v1"


# TODO - move to utils class
# Define a factory function to create a rate-limited version of the function


def create_rate_limited_function(
    rate_limit: int, time_period: int, func: Callable
) -> Callable:
    @sleep_and_retry
    @limits(calls=rate_limit, period=time_period)
    def rate_limited_func(*args, **kwargs):  # type: ignore
        return func(*args, **kwargs)

    return rate_limited_func


class DocPropagationAction(ExtendedAction):
    def __init__(self, config: DocPropagationConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx
        self.mcl_processor = MCLProcessor()
        self.actor_urn = "urn:li:corpuser:__datahub_system"
        if ctx.pipeline_name:
            if not ctx.pipeline_name.startswith("urn:li:dataHubAction"):
                self.action_urn = f"urn:li:dataHubAction:{ctx.pipeline_name}"
            else:
                self.action_urn = ctx.pipeline_name
        else:
            self.action_urn = "urn:li:dataHubAction:doc_propagation"

        self.mcl_processor.register_processor(
            "schemaField",
            "documentation",
            self.process_schema_field_documentation,
        )

        if self.config.bootstrap:
            self.bootstrap()

        # self.term_resolver = GlossaryTermsResolver(graph=self.ctx.graph)
        # if self.config.target_terms:
        #     logger.info(
        #         f"[Config] Will propagate terms that inherit from terms {self.config.target_terms}"
        #     )
        #     resolved_terms = []
        #     for t in self.config.target_terms:
        #         if t.startswith("urn:li:glossaryTerm"):
        #             resolved_terms.append(t)
        #         else:
        #             resolved_term = self.term_resolver.get_glossary_term_urn(t)
        #             if not resolved_term:
        #                 raise Exception(f"Failed to resolve term by name {t}")
        #             resolved_terms.append(resolved_term)
        #     self.config.target_terms = resolved_terms
        #     logger.info(
        #         f"[Config] Will propagate terms that inherit from terms {self.config.target_terms}"
        #     )

        # if self.config.term_groups:
        #     resolved_nodes = []
        #     for node in self.config.term_groups:
        #         if node.startswith("urn:li:glossaryNode"):
        #             resolved_nodes.append(node)
        #         else:
        #             resolved_node = self.term_resolver.get_glossary_node_urn(node)
        #             if not resolved_node:
        #                 raise Exception(f"Failed to resolve node by name {node}")
        #             resolved_nodes.append(resolved_node)
        #     self.config.term_groups = resolved_nodes
        #     logger.info(
        #         f"[Config] Will propagate all terms in groups {self.config.term_groups}"
        #     )

    def name(self) -> str:
        return "DocPropagator"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = DocPropagationConfig.parse_obj(config_dict or {})
        logger.info(f"Doc Propagation Config action configured with {action_config}")
        return cls(action_config, ctx)

    def bootstrap(self) -> None:
        self.bootstrapping = True
        # start a thread to bootstrap the action
        # while bootstrap is running, the action will continue processing any
        # real time events
        # start a threadpool executor
        # Create a ThreadPoolExecutor
        import concurrent.futures

        # Create a rate-limited version of the function
        rate_limit = self.config.event_processing_rate_limit
        time_period = self.config.event_processing_rate_period
        rate_limited_bootstrap_dataset = create_rate_limited_function(
            rate_limit, time_period, self.bootstrap_dataset
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            datasets_with_field_docs = self.ctx.graph.graph.get_urns_by_filter(
                entity_types=["dataset"],
                extra_or_filters=[
                    {
                        "field": "fieldDescriptions",
                        "condition": "EXISTS",
                        "negated": "false",
                    },
                    {
                        "field": "editedFieldDescriptions",
                        "condition": "EXISTS",
                        "negated": "false",
                    },
                ],
            )
            # for dataset_urn in datasets_with_field_docs:
            #     self.bootstrap_dataset(dataset_urn)

            futures = [
                executor.submit(rate_limited_bootstrap_dataset, dataset_urn)
                for dataset_urn in datasets_with_field_docs
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error bootstrapping dataset: {e}")

        self.bootstrapping = False

    def bootstrap_dataset(self, dataset_urn: str) -> None:
        """
        Bootstrap the action by processing all the field descriptions on the
        dataset
        """
        assert self.ctx.graph
        from datahub.emitter.mce_builder import make_schema_field_urn
        from datahub.metadata.schema_classes import SchemaMetadataClass

        edited_field_docs = self.ctx.graph.graph.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )
        base_field_docs = self.ctx.graph.graph.get_aspect(
            dataset_urn, SchemaMetadataClass
        )
        field_doc_map = {}
        if base_field_docs is not None:
            for field_info in base_field_docs.fields:
                if field_info.description:
                    field_doc_map[field_info.fieldPath] = {
                        "description": field_info.description,
                        "auditStamp": field_info.lastModified,
                    }
        if edited_field_docs is not None:
            for field_info in edited_field_docs.editableSchemaFieldInfo:
                if field_info.description:
                    field_doc_map[field_info.fieldPath] = {
                        "description": field_info.description,
                        "auditStamp": edited_field_docs.lastModified,
                    }

        for field_path, field_dict in field_doc_map.items():
            schema_field_urn = make_schema_field_urn(dataset_urn, field_path)
            # make a fake ECE event
            params = ParametersClass()
            params.__dict__ = {"description": field_dict["description"]}
            event = EntityChangeEvent(
                entityType="schemaField",
                entityUrn=schema_field_urn,
                category="DOCUMENTATION",
                operation="ADD",  # ADD RESTATE later
                auditStamp=field_dict["auditStamp"],
                parameters=params,
                version=1,
            )
            self.act(EventEnvelope(event_type=ECE_EVENT_TYPE, event=event, meta={}))

    def rollback(self) -> None:
        assets_with_my_edits = self.ctx.graph.graph.get_urns_by_filter(
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
        for asset_urn in assets_with_my_edits:
            print(f"Rolling back documentation for {asset_urn}")
        pass

    def process_schema_field_documentation(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: GenericAspectClass,
        previous_aspect_value: Optional[GenericAspectClass],
    ) -> Optional[DocPropagationDirective]:
        if aspect_name == "documentation":
            if self.config.columns_enabled:
                current_docs = DocumentationClass.from_obj(
                    json.loads(aspect_value.value)
                )
                old_docs = (
                    None
                    if previous_aspect_value is None
                    else DocumentationClass.from_obj(
                        json.loads(previous_aspect_value.value)
                    )
                )
                if current_docs.documentations:
                    # we assume that the first documentation is the primary one
                    # we can change this later
                    current_documentation_instance = current_docs.documentations[0]
                    source_details = (
                        (current_documentation_instance.attribution.sourceDetail)
                        if current_documentation_instance.attribution
                        else {}
                    )
                    origin_entity = source_details.get("origin")
                    if old_docs is None:
                        return DocPropagationDirective(
                            propagate=True,
                            doc_string=current_documentation_instance.documentation,
                            operation="ADD",
                            entity=entity_urn,
                            origin=origin_entity,
                            via=entity_urn,
                            actor=self.actor_urn,
                        )
                    else:
                        if (
                            current_docs.documentations[0].documentation
                            != old_docs.documentations[0].documentation
                        ):
                            return DocPropagationDirective(
                                propagate=True,
                                doc_string=current_documentation_instance.documentation,
                                operation="MODIFY",
                                entity=entity_urn,
                                origin=origin_entity,
                                via=entity_urn,
                                actor=self.actor_urn,
                            )
        return None

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[DocPropagationDirective]:

        if self.mcl_processor.is_mcl(event):
            return self.mcl_processor.process(event)
        if event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            logger.info(f"Received event {event}")
            assert self.ctx.graph is not None
            semantic_event = event.event
            if (
                semantic_event.category == "DOCUMENTATION"
                and self.config is not None
                and self.config.enabled
            ):
                if (
                    self.config.columns_enabled
                    and semantic_event.entityType == "schemaField"
                ):
                    if semantic_event.parameters:
                        parameters = semantic_event.parameters
                    else:
                        parameters = semantic_event._inner_dict.get(
                            "__parameters_json", {}
                        )
                    doc_string = parameters.get("description")
                    origin = parameters.get("origin")
                    origin = origin or semantic_event.entityUrn
                    via = (
                        semantic_event.entityUrn
                        if origin != semantic_event.entityUrn
                        else None
                    )
                    print(f"Origin: {origin}")
                    print(f"Via: {via}")
                    if doc_string:
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
                        )
            # if (
            #     semantic_event.category == "GLOSSARY_TERM"
            #     and self.config is not None
            #     and self.config.enabled
            # ):
            #     assert semantic_event.modifier
            #     for target_term in self.config.target_terms or [
            #         semantic_event.modifier
            #     ]:
            #         # a cheap way to handle optionality and always propagate if config is not set
            #         # Check which terms have connectivity to the target term
            #         if (
            #             semantic_event.modifier == target_term
            #             or self.ctx.graph.check_relationship(  # term has been directly applied  # term is indirectly associated
            #                 target_term,
            #                 semantic_event.modifier,
            #                 "IsA",
            #             )
            #         ):
            #             return DocPropagationDirective(
            #                 propagate=True,
            #                 term=semantic_event.modifier,
            #                 operation=semantic_event.operation,
            #                 entity=semantic_event.entityUrn,
            #             )
        return None

    def modify_docs_on_columns(
        self,
        graph: AcrylDataHubGraph,
        operation: str,
        schema_field_urn: str,
        dataset_urn: str,
        field_doc: Optional[str],
        context: SourceDetails,
    ) -> None:
        if not dataset_urn.startswith("urn:li:dataset"):
            logger.error(
                f"Invalid dataset urn {dataset_urn}. Must start with urn:li:dataset"
            )
            return

        auditStamp = AuditStampClass(
            time=int(time.time() * 1000.0), actor=self.actor_urn
        )

        from datahub.metadata.schema_classes import (
            DocumentationClass,
            MetadataAttributionClass,
        )

        source_details = context.dict(exclude_none=True)
        attribution: MetadataAttributionClass = MetadataAttributionClass(
            source=self.action_urn,
            time=auditStamp.time,
            actor=self.actor_urn,
            sourceDetail=source_details,
        )
        documentations = graph.graph.get_aspect(schema_field_urn, DocumentationClass)
        if documentations:
            mutation_needed = False
            action_sourced = False
            # we check if there are any existing documentations generated by
            # this action, if so, we update them
            # otherwise, we add a new documentation entry sourced by this action
            for doc_association in documentations.documentations:
                if doc_association.attribution and doc_association.attribution.source:
                    if doc_association.attribution.source == self.action_urn:
                        action_sourced = True
                        if doc_association.documentation != field_doc:
                            mutation_needed = True
                            if operation == "ADD" or operation == "MODIFY":
                                doc_association.documentation = field_doc
                                doc_association.attribution = attribution
                            elif operation == "REMOVE":
                                # TODO : should we remove the documentation or just set it to empty string?
                                doc_association.documentation = ""
                                doc_association.attribution = attribution
            if not action_sourced:
                documentations.documentations.append(
                    DocumentationAssociationClass(
                        documentation=field_doc or "",
                        attribution=attribution,
                    )
                )
                mutation_needed = True
        else:
            # no docs found, create a new one
            # we don't check editableSchemaMetadata because our goal is to
            # propagate documentation to downstream entities
            # UI will handle resolving priorities and conflicts
            documentations = DocumentationClass(
                documentations=[
                    DocumentationAssociationClass(
                        documentation=field_doc or "",
                        attribution=attribution,
                    )
                ]
            )
            mutation_needed = True

        if mutation_needed:
            assert documentations.validate()
            logger.info(
                f"Will emit documentation change proposal for {schema_field_urn} with {field_doc}"
            )
            graph.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=schema_field_urn,
                    aspect=documentations,
                )
            )
            # validate the aspect
            read_documentation = graph.graph.get_aspect(
                schema_field_urn, DocumentationClass
            )
            assert read_documentation is not None
            logger.info(f"{documentations}")
            assert read_documentation == documentations

    def act(self, event: EventEnvelope) -> None:
        """This method responds to changes to documentation and propagates them to downstream entities"""

        logger.info(f"Received event {event}")
        doc_propagation_directive = self.should_propagate(event)
        logger.info(f"Doc propagation directive: {doc_propagation_directive}")

        if (
            doc_propagation_directive is not None
            and doc_propagation_directive.propagate is True
        ):
            context = SourceDetails(
                origin=doc_propagation_directive.origin,
                via=doc_propagation_directive.via,
                propagated=True,
                actor=doc_propagation_directive.actor,
            )
            assert self.ctx.graph
            # find downstream lineage
            downstreams = self.ctx.graph.get_downstreams(
                entity_urn=doc_propagation_directive.entity
            )
            logger.info(
                f"Downstreams: {downstreams} for {doc_propagation_directive.entity}"
            )
            entity_urn = doc_propagation_directive.entity
            if entity_urn.startswith("urn:li:schemaField"):
                downstream_fields = [
                    x for x in downstreams if x.startswith("urn:li:schemaField")
                ]
                for field in downstream_fields:

                    schema_field_urn = Urn.create_from_string(field)
                    parent_urn = schema_field_urn.get_entity_id()[0]
                    field_path = schema_field_urn.get_entity_id()[1]
                    logger.info(
                        f"Will {doc_propagation_directive.operation} documentation {doc_propagation_directive.doc_string} for {field_path} on {parent_urn}"
                    )
                    if parent_urn.startswith("urn:li:dataset"):
                        self.modify_docs_on_columns(
                            self.ctx.graph,
                            doc_propagation_directive.operation,
                            field,
                            parent_urn,
                            field_doc=doc_propagation_directive.doc_string,
                            context=context,
                        )
                    elif parent_urn.startswith("urn:li:chart"):
                        self.modify_docs_on_chart_fields(
                            self.ctx.graph,
                            doc_propagation_directive.operation,
                            field,
                            parent_urn,
                            field_doc=doc_propagation_directive.doc_string,
                            context=context,
                        )
            elif entity_urn.startswith("urn:li:dataset"):
                downstream_datasets = [
                    x for x in downstreams if x.startswith("urn:li:dataset")
                ]
                for dataset in downstream_datasets:
                    self.ctx.graph.add_docs_to_dataset(
                        dataset,
                        [doc_propagation_directive.doc_string],
                        context=context,
                    )
                    logger.info(
                        f"Will {doc_propagation_directive.operation} doc {doc_propagation_directive.doc_string} to {dataset}"
                    )
        else:
            print("No doc propagation directive")

    def close(self) -> None:
        return super().close()
