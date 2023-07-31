from typing import Callable, List, Optional, cast

from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
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

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_global_tags_aspect: GlobalTagsClass = cast(GlobalTagsClass, aspect)
        out_global_tags_aspect: GlobalTagsClass = GlobalTagsClass(tags=[])
        self.update_if_keep_existing(
            self.config, in_global_tags_aspect, out_global_tags_aspect
        )

        tags_to_add = self.config.get_tags_to_add(entity_urn)
        if tags_to_add is not None:
            out_global_tags_aspect.tags.extend(tags_to_add)

        return self.get_result_semantics(
            self.config, self.ctx.graph, entity_urn, out_global_tags_aspect
        )


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
