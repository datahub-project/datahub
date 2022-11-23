from typing import Callable, List, Optional, cast

from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetTagsTransformer
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass


class AddDatasetTagsConfig(TransformerSemanticsConfigModel):
    get_tags_to_add: Callable[[str], List[TagAssociationClass]]

    _resolve_tag_fn = pydantic_resolve_key("get_tags_to_add")


class AddDatasetTags(DatasetTagsTransformer):
    """Transformer that adds tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetTagsConfig

    def __init__(self, config: AddDatasetTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetTags":
        config = AddDatasetTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _merge_with_server_global_tags(
        graph: DataHubGraph, urn: str, global_tags_aspect: Optional[GlobalTagsClass]
    ) -> Optional[GlobalTagsClass]:
        if not global_tags_aspect or not global_tags_aspect.tags:
            # nothing to add, no need to consult server
            return None

        # Merge the transformed tags with existing server tags.
        # The transformed tags takes precedence, which may change the tag context.
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
        # Check if user want to keep existing tags
        if in_global_tags_aspect is not None and self.config.replace_existing is False:
            out_global_tags_aspect.tags.extend(in_global_tags_aspect.tags)

        tags_to_add = self.config.get_tags_to_add(entity_urn)
        if tags_to_add is not None:
            out_global_tags_aspect.tags.extend(tags_to_add)

        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            return cast(
                Optional[Aspect],
                AddDatasetTags._merge_with_server_global_tags(
                    self.ctx.graph, entity_urn, out_global_tags_aspect
                ),
            )

        return cast(Aspect, out_global_tags_aspect)


class SimpleDatasetTagConfig(TransformerSemanticsConfigModel):
    tag_urns: List[str]


class SimpleAddDatasetTags(AddDatasetTags):
    """Transformer that adds a specified set of tags to each dataset."""

    def __init__(self, config: SimpleDatasetTagConfig, ctx: PipelineContext):
        tags = [TagAssociationClass(tag=tag) for tag in config.tag_urns]

        generic_config = AddDatasetTagsConfig(
            get_tags_to_add=lambda _: tags,
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleAddDatasetTags":
        config = SimpleDatasetTagConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternDatasetTagsConfig(TransformerSemanticsConfigModel):
    tag_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddDatasetTags(AddDatasetTags):
    """Transformer that adds a specified set of tags to each dataset."""

    def __init__(self, config: PatternDatasetTagsConfig, ctx: PipelineContext):
        tag_pattern = config.tag_pattern
        generic_config = AddDatasetTagsConfig(
            get_tags_to_add=lambda _: [
                TagAssociationClass(tag=urn) for urn in tag_pattern.value(_)
            ],
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "PatternAddDatasetTags":
        config = PatternDatasetTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)
