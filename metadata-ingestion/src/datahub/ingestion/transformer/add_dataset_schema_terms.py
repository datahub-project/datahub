from typing import Callable, Dict, List, Optional, cast

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
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
)


class AddDatasetSchemaTermsConfig(TransformerSemanticsConfigModel):
    get_terms_to_add: Callable[[str], List[GlossaryTermAssociationClass]]

    _resolve_term_fn = pydantic_resolve_key("get_terms_to_add")


class AddDatasetSchemaTerms(DatasetSchemaMetadataTransformer):
    """Transformer that adds glossary terms to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetSchemaTermsConfig

    def __init__(self, config: AddDatasetSchemaTermsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetSchemaTerms":
        config = AddDatasetSchemaTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def extend_field(
        self, schema_field: SchemaFieldClass, server_field: Optional[SchemaFieldClass]
    ) -> SchemaFieldClass:
        all_terms = self.config.get_terms_to_add(schema_field.fieldPath)
        if len(all_terms) == 0:
            # return input schema field as there is no terms to add
            return schema_field

        # Add existing terms if user want to keep them
        if (
            schema_field.glossaryTerms is not None
            and self.config.replace_existing is False
        ):
            all_terms.extend(schema_field.glossaryTerms.terms)

        terms_to_add: List[GlossaryTermAssociationClass] = []
        server_terms: List[GlossaryTermAssociationClass] = []
        if server_field is not None and server_field.glossaryTerms is not None:
            # Go for patch
            server_terms = server_field.glossaryTerms.terms
            server_term_urns = [term.urn for term in server_terms]
            for term in all_terms:
                if term.urn not in server_term_urns:
                    terms_to_add.append(term)

        # Set terms_to_add to all_terms if server terms were not present
        if len(terms_to_add) == 0:
            terms_to_add = all_terms

        new_glossary_terms = []
        new_glossary_terms.extend(server_terms)
        new_glossary_terms.extend(terms_to_add)

        unique_gloseary_terms = []
        for term in new_glossary_terms:
            if term not in unique_gloseary_terms:
                unique_gloseary_terms.append(term)

        new_glossary_term = GlossaryTermsClass(
            terms=[],
            auditStamp=(
                schema_field.glossaryTerms.auditStamp
                if schema_field.glossaryTerms is not None
                else AuditStampClass(
                    time=builder.get_sys_time(), actor="urn:li:corpUser:restEmitter"
                )
            ),
        )
        new_glossary_term.terms.extend(unique_gloseary_terms)

        schema_field.glossaryTerms = new_glossary_term
        return schema_field

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        schema_metadata_aspect: SchemaMetadataClass = cast(SchemaMetadataClass, aspect)
        assert schema_metadata_aspect is None or isinstance(
            schema_metadata_aspect, SchemaMetadataClass
        )

        server_field_map: Dict[
            str, SchemaFieldClass
        ] = {}  # Map to cache server field objects, where fieldPath is key
        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            server_schema_metadata_aspect: Optional[
                SchemaMetadataClass
            ] = self.ctx.graph.get_schema_metadata(entity_urn=entity_urn)
            if server_schema_metadata_aspect is not None:
                if not schema_metadata_aspect:
                    schema_metadata_aspect = server_schema_metadata_aspect
                else:
                    input_field_path = [
                        field.fieldPath
                        for field in schema_metadata_aspect.fields
                        if field is not None
                    ]
                    # cache the server field to use in patching the schema-field later
                    for field in server_schema_metadata_aspect.fields:
                        server_field_map[field.fieldPath] = field
                        if field.fieldPath not in input_field_path:
                            # This field is present on server but not in input aspect, so we add it.
                            schema_metadata_aspect.fields.append(field)

        if not schema_metadata_aspect:
            # We can't add terms to a schema that doesn't exist.
            return None

        schema_metadata_aspect.fields = [
            self.extend_field(field, server_field=server_field_map.get(field.fieldPath))
            for field in schema_metadata_aspect.fields
        ]

        return schema_metadata_aspect  # type: ignore


class PatternDatasetTermsConfig(TransformerSemanticsConfigModel):
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
            semantics=config.semantics,
            replace_existing=config.replace_existing,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetSchemaTerms":
        config = PatternDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)
