from typing import Callable, List, Optional, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetSchemaMetadataTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    TagAssociationClass,
)


class AddDatasetSchemaTagsConfig(TransformerSemanticsConfigModel):
    get_tags_to_add: Callable[[str], List[TagAssociationClass]]

    _resolve_tag_fn = pydantic_resolve_key("get_tags_to_add")


class AddDatasetSchemaTags(DatasetSchemaMetadataTransformer):
    """Transformer that adds glossary tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetSchemaTagsConfig

    def __init__(self, config: AddDatasetSchemaTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetSchemaTags":
        config = AddDatasetSchemaTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def extend_field(
        self, schema_field: SchemaFieldClass, server_field: Optional[SchemaFieldClass]
    ) -> SchemaFieldClass:
        all_tags = self.config.get_tags_to_add(schema_field.fieldPath)
        if len(all_tags) == 0:
            # return input schema field as there is no tags to add
            return schema_field

        # Add existing tags if user want to keep them
        if (
            schema_field.globalTags is not None
            and self.config.replace_existing is False
        ):
            all_tags.extend(schema_field.globalTags.tags)

        tags_to_add: List[TagAssociationClass] = []
        server_tags: List[TagAssociationClass] = []
        if server_field is not None and server_field.globalTags is not None:
            # Go for patch
            server_tags = server_field.globalTags.tags
            server_tag_urns = [tag_association.tag for tag_association in server_tags]
            for tag in all_tags:
                if tag.tag not in server_tag_urns:
                    tags_to_add.append(tag)

        # Set tags_to_add to all_tags if server tags were not present
        if len(tags_to_add) == 0:
            tags_to_add = all_tags

        new_global_tag_class: GlobalTagsClass = GlobalTagsClass(tags=[])
        new_global_tag_class.tags.extend(tags_to_add)
        new_global_tag_class.tags.extend(server_tags)

        schema_field.globalTags = new_global_tag_class

        return schema_field

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:

        schema_metadata_aspect: SchemaMetadataClass = cast(SchemaMetadataClass, aspect)
        assert (
            schema_metadata_aspect is None
            or isinstance(schema_metadata_aspect, SchemaMetadataClass)
            or schema_metadata_aspect.field is None
        )

        server_field_map: dict = {}
        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            server_schema_metadata_aspect: Optional[
                SchemaMetadataClass
            ] = self.ctx.graph.get_schema_metadata(entity_urn=entity_urn)
            if server_schema_metadata_aspect is not None:
                input_field_path = [
                    field.fieldPath
                    for field in schema_metadata_aspect.fields
                    if field is not None
                ]
                server_field_to_add: List[SchemaFieldClass] = []
                # cache the server field to use in patching the schema-field later
                for field in server_schema_metadata_aspect.fields:
                    server_field_map[field.fieldPath] = field
                    if field.fieldPath not in input_field_path:
                        # This field is present on server but not in input aspect
                        server_field_to_add.append(field)
                # Add field present on server
                schema_metadata_aspect.fields.extend(server_field_to_add)

        schema_metadata_aspect.fields = [
            self.extend_field(field, server_field=server_field_map.get(field.fieldPath))
            for field in schema_metadata_aspect.fields
        ]

        return schema_metadata_aspect  # type: ignore


class PatternDatasetTagsConfig(TransformerSemanticsConfigModel):
    tag_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddDatasetSchemaTags(AddDatasetSchemaTags):
    """Transformer that adds a dynamic set of tags to each field in a dataset based on supplied patterns."""

    def __init__(self, config: PatternDatasetTagsConfig, ctx: PipelineContext):
        tag_pattern = config.tag_pattern
        generic_config = AddDatasetSchemaTagsConfig(
            get_tags_to_add=lambda path: [
                TagAssociationClass(tag=urn) for urn in tag_pattern.value(path)
            ],
            semantics=config.semantics,
            replace_existing=config.replace_existing,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetSchemaTags":
        config = PatternDatasetTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)
