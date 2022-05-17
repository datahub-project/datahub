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
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
)


class AddDatasetSchemaTermsConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    get_terms_to_add: Union[
        Callable[[str], List[GlossaryTermAssociationClass]],
        Callable[[str], List[GlossaryTermAssociationClass]],
    ]

    _resolve_term_fn = pydantic_resolve_key("get_terms_to_add")


class AddDatasetSchemaTerms(BaseTransformer, SingleAspectTransformer):
    """Transformer that adds glossary terms to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetSchemaTermsConfig

    def __init__(self, config: AddDatasetSchemaTermsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    def aspect_name(self) -> str:
        return "schemaMetadata"

    def entity_types(self) -> List[str]:
        return ["dataset"]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetSchemaTerms":
        config = AddDatasetSchemaTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def extend_field(self, schema_field: SchemaFieldClass) -> SchemaFieldClass:
        terms_to_add = self.config.get_terms_to_add(schema_field.fieldPath)
        if len(terms_to_add) > 0:
            new_terms = (
                schema_field.glossaryTerms
                if schema_field.glossaryTerms is not None
                else GlossaryTermsClass(
                    terms=[],
                    auditStamp=AuditStampClass(
                        time=builder.get_sys_time(), actor="urn:li:corpUser:restEmitter"
                    ),
                )
            )
            new_terms.terms.extend(terms_to_add)
            schema_field.glossaryTerms = new_terms

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


class PatternDatasetTermsConfig(ConfigModel):
    term_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddDatasetSchemaTerms(AddDatasetSchemaTerms):
    """Transformer that adds a dynamic set of glossary terms to each field in a dataset based on supplied patterns."""

    def __init__(self, config: PatternDatasetTermsConfig, ctx: PipelineContext):
        term_pattern = config.term_pattern
        generic_config = AddDatasetSchemaTermsConfig(
            get_terms_to_add=lambda path: [
                GlossaryTermAssociationClass(urn=urn)
                for urn in term_pattern.value(path)
            ],
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetSchemaTerms":
        config = PatternDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)
