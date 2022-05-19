from typing import Callable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel, KeyValuePattern
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    TagAssociationClass,
)


class AddDatasetSchemaTagsConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    get_tags_to_add: Union[
        Callable[[str], List[TagAssociationClass]],
        Callable[[str], List[TagAssociationClass]],
    ]

    _resolve_tag_fn = pydantic_resolve_key("get_tags_to_add")


class AddDatasetSchemaTags(BaseTransformer, SingleAspectTransformer):
    """Transformer that adds glossary tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetSchemaTagsConfig

    def __init__(self, config: AddDatasetSchemaTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    def aspect_name(self) -> str:
        return "schemaMetadata"

    def entity_types(self) -> List[str]:
        return ["dataset"]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetSchemaTags":
        config = AddDatasetSchemaTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def extend_field(self, schema_field: SchemaFieldClass) -> SchemaFieldClass:
        tags_to_add = self.config.get_tags_to_add(schema_field.fieldPath)
        if len(tags_to_add) > 0:
            new_tags = (
                schema_field.globalTags
                if schema_field.globalTags is not None
                else GlobalTagsClass(
                    tags=[],
                )
            )
            new_tags.tags.extend(tags_to_add)
            schema_field.globalTags = new_tags

        return schema_field

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        assert aspect is None or isinstance(aspect, SchemaMetadataClass)

        if aspect is None:
            return aspect

        schema_metadata_aspect: SchemaMetadataClass = aspect

        schema_metadata_aspect.fields = [
            self.extend_field(field) for field in schema_metadata_aspect.fields
        ]

        return schema_metadata_aspect  # type: ignore


class PatternDatasetTagsConfig(ConfigModel):
    tag_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddDatasetSchemaTags(AddDatasetSchemaTags):
    """Transformer that adds a dynamic set of tags to each field in a dataset based on supplied patterns."""

    def __init__(self, config: PatternDatasetTagsConfig, ctx: PipelineContext):
        tag_pattern = config.tag_pattern
        generic_config = AddDatasetSchemaTagsConfig(
            get_tags_to_add=lambda path: [
                TagAssociationClass(tag=urn) for urn in tag_pattern.value(path)
            ],
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetSchemaTags":
        config = PatternDatasetTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)
