from typing import List, Optional, Set, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect, make_term_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import TagsToTermTransformer
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaMetadataClass,
)


class TagsToTermMapperConfig(TransformerSemanticsConfigModel):
    tags: List[str]


class TagsToTermMapper(TagsToTermTransformer):
    """This transformer maps specified tags to corresponding glossary terms for a dataset."""

    def __init__(self, config: TagsToTermMapperConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx: PipelineContext = ctx
        self.config: TagsToTermMapperConfig = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "TagsToTermMapper":
        config = TagsToTermMapperConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _merge_with_server_glossary_terms(
        graph: DataHubGraph,
        urn: str,
        glossary_terms_aspect: Optional[GlossaryTermsClass],
    ) -> Optional[GlossaryTermsClass]:
        if not glossary_terms_aspect or not glossary_terms_aspect.terms:
            # nothing to add, no need to consult server
            return None

        # Merge the transformed terms with existing server terms.
        # The transformed terms takes precedence, which may change the term context.
        server_glossary_terms_aspect = graph.get_glossary_terms(entity_urn=urn)
        if server_glossary_terms_aspect is not None:
            glossary_terms_aspect.terms = list(
                {
                    **{term.urn: term for term in server_glossary_terms_aspect.terms},
                    **{term.urn: term for term in glossary_terms_aspect.terms},
                }.values()
            )

        return glossary_terms_aspect

    @staticmethod
    def get_tags_from_global_tags(global_tags: Optional[GlobalTagsClass]) -> Set[str]:
        """Extracts tags urn from GlobalTagsClass."""
        if not global_tags or not global_tags.tags:
            return set()

        return {tag_assoc.tag for tag_assoc in global_tags.tags}

    @staticmethod
    def get_tags_from_schema_metadata(
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> Set[str]:
        """Extracts globalTags from all fields in SchemaMetadataClass."""
        if not schema_metadata or not schema_metadata.fields:
            return set()
        tags = set()
        for field in schema_metadata.fields:
            if field.globalTags:
                tags.update(
                    TagsToTermMapper.get_tags_from_global_tags(field.globalTags)
                )
        return tags

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_glossary_terms: Optional[GlossaryTermsClass] = cast(
            Optional[GlossaryTermsClass], aspect
        )

        assert self.ctx.graph
        in_global_tags_aspect: Optional[GlobalTagsClass] = self.ctx.graph.get_tags(
            entity_urn
        )
        in_schema_metadata_aspect: Optional[SchemaMetadataClass] = (
            self.ctx.graph.get_schema_metadata(entity_urn)
        )

        if in_global_tags_aspect is None and in_schema_metadata_aspect is None:
            return cast(Aspect, in_glossary_terms)

        global_tags = TagsToTermMapper.get_tags_from_global_tags(in_global_tags_aspect)
        schema_metadata_tags = TagsToTermMapper.get_tags_from_schema_metadata(
            in_schema_metadata_aspect
        )

        # Combine tags from both global and schema level
        combined_tags = global_tags.union(schema_metadata_tags)

        tag_set = set(self.config.tags)
        terms_to_add = set()
        tags_to_delete = set()

        # Check each global tag against the configured tag list and prepare terms
        for full_tag in combined_tags:
            tag_name = full_tag.split("urn:li:tag:")[-1].split(".")[-1].split(":")[0]
            if tag_name in tag_set:
                term_urn = make_term_urn(tag_name)
                terms_to_add.add(term_urn)
                tags_to_delete.add(full_tag)  # Full URN for deletion

        if not terms_to_add:
            return cast(Aspect, in_glossary_terms)  # No new terms to add

        for tag_urn in tags_to_delete:
            self.ctx.graph.remove_tag(tag_urn=tag_urn, resource_urn=entity_urn)

        # Initialize the Glossary Terms properly
        out_glossary_terms = GlossaryTermsClass(
            terms=[GlossaryTermAssociationClass(urn=term) for term in terms_to_add],
            auditStamp=AuditStampClass(
                time=builder.get_sys_time(), actor="urn:li:corpUser:restEmitter"
            ),
        )

        if self.config.semantics == TransformerSemantics.PATCH:
            patch_glossary_terms: Optional[GlossaryTermsClass] = (
                TagsToTermMapper._merge_with_server_glossary_terms(
                    self.ctx.graph, entity_urn, out_glossary_terms
                )
            )
            return cast(Optional[Aspect], patch_glossary_terms)
        else:
            return cast(Aspect, out_glossary_terms)
