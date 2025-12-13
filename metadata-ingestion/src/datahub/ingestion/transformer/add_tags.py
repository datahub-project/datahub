# ABOUTME: Universal transformer that adds tags to datasets, charts, and dashboards.
# ABOUTME: Supports callback, simple list, and pattern-based tag assignment.

import logging
from typing import Callable, Dict, List, Optional, Union, cast

from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
)
from datahub.utilities.urns.tag_urn import TagUrn

logger = logging.getLogger(__name__)


class AddTagsConfig(TransformerSemanticsConfigModel):
    get_tags_to_add: Callable[[str], List[TagAssociationClass]]

    _resolve_tag_fn = pydantic_resolve_key("get_tags_to_add")


class AddTags(BaseTransformer, SingleAspectTransformer):
    """Universal transformer that adds tags to datasets, charts, and dashboards."""

    ctx: PipelineContext
    config: AddTagsConfig
    processed_tags: Dict[str, TagAssociationClass]

    def __init__(self, config: AddTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.processed_tags = {}

    def aspect_name(self) -> str:
        return "globalTags"

    def entity_types(self) -> List[str]:
        return ["dataset", "chart", "dashboard"]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddTags":
        config = AddTagsConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _merge_with_server_global_tags(
        graph: DataHubGraph, urn: str, global_tags_aspect: Optional[GlobalTagsClass]
    ) -> Optional[GlobalTagsClass]:
        if not global_tags_aspect or not global_tags_aspect.tags:
            return None

        server_global_tags_aspect = graph.get_tags(entity_urn=urn)
        if server_global_tags_aspect:
            global_tags_aspect.tags = list(
                {
                    **{tag.tag: tag for tag in server_global_tags_aspect.tags},
                    **{tag.tag: tag for tag in global_tags_aspect.tags},
                }.values()
            )

        return global_tags_aspect

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_global_tags_aspect: GlobalTagsClass = cast(GlobalTagsClass, aspect)
        out_global_tags_aspect: GlobalTagsClass = GlobalTagsClass(tags=[])

        # Check if user wants to keep existing tags
        if in_global_tags_aspect is not None and self.config.replace_existing is False:
            tags_seen = set()
            for item in in_global_tags_aspect.tags:
                if item.tag not in tags_seen:
                    out_global_tags_aspect.tags.append(item)
                    tags_seen.add(item.tag)

        tags_to_add = self.config.get_tags_to_add(entity_urn)
        if tags_to_add is not None:
            out_global_tags_aspect.tags.extend(tags_to_add)
            for tag in tags_to_add:
                self.processed_tags.setdefault(tag.tag, tag)

        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            return cast(
                Optional[Aspect],
                self._merge_with_server_global_tags(
                    self.ctx.graph, entity_urn, out_global_tags_aspect
                ),
            )

        return cast(Aspect, out_global_tags_aspect)

    def handle_end_of_stream(
        self,
    ) -> List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []

        logger.debug("Generating tags")

        for tag_association in self.processed_tags.values():
            tag_urn = TagUrn.from_string(tag_association.tag)
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=tag_urn.urn(),
                    aspect=tag_urn.to_key_aspect(),
                )
            )

        return mcps


class SimpleTagsConfig(TransformerSemanticsConfigModel):
    tag_urns: List[str]


class SimpleAddTags(AddTags):
    """Transformer that adds a specified set of tags to each entity."""

    def __init__(self, config: SimpleTagsConfig, ctx: PipelineContext):
        tags = [TagAssociationClass(tag=tag) for tag in config.tag_urns]

        generic_config = AddTagsConfig(
            get_tags_to_add=lambda _: tags,
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleAddTags":
        config = SimpleTagsConfig.model_validate(config_dict)
        return cls(config, ctx)


class PatternTagsConfig(TransformerSemanticsConfigModel):
    tag_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddTags(AddTags):
    """Transformer that adds tags based on pattern matching."""

    def __init__(self, config: PatternTagsConfig, ctx: PipelineContext):
        tag_pattern = config.tag_pattern
        generic_config = AddTagsConfig(
            get_tags_to_add=lambda _: [
                TagAssociationClass(tag=urn) for urn in tag_pattern.value(_)
            ],
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "PatternAddTags":
        config = PatternTagsConfig.model_validate(config_dict)
        return cls(config, ctx)
