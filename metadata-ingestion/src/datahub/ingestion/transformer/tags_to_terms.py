from typing import Dict, Iterable, List, Optional, Set, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect, make_term_urn
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetSchemaMetadataTransformer,
    TagsToTermTransformer,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaMetadataClass,
)

_TAG_URN_PREFIX = "urn:li:tag:"


class TagsToTermMapperConfig(TransformerSemanticsConfigModel):
    tags: List[str]


def _tag_name(tag_urn: str) -> str:
    """``urn:li:tag:pii.email`` -> ``pii.email``. The whole name is kept, so
    dotted and ``key:value`` tags match exactly as configured."""
    return (
        tag_urn[len(_TAG_URN_PREFIX) :]
        if tag_urn.startswith(_TAG_URN_PREFIX)
        else tag_urn
    )


def _merge_terms(
    existing: Optional[GlossaryTermsClass], term_urns: Iterable[str]
) -> GlossaryTermsClass:
    """Add ``term_urns`` to ``existing``, keeping its terms and their context."""
    terms: Dict[str, GlossaryTermAssociationClass] = {
        term.urn: term for term in (existing.terms if existing else [])
    }
    for urn in term_urns:
        terms.setdefault(urn, GlossaryTermAssociationClass(urn=urn))
    return GlossaryTermsClass(
        terms=list(terms.values()),
        auditStamp=(
            existing.auditStamp
            if existing
            else AuditStampClass(
                time=builder.get_sys_time(), actor="urn:li:corpUser:restEmitter"
            )
        ),
    )


class _ColumnTagsToTerms(DatasetSchemaMetadataTransformer):
    """Maps tags on columns to terms on the same columns.

    Works on the schemaMetadata the source emits in this run, so the column tag
    is dropped before it is written. The server cannot remove it afterwards:
    removeTag on a column only edits editableSchemaMetadata.
    """

    def __init__(self, tags: Set[str]):
        super().__init__()
        self.tags = tags

    @classmethod
    def create(  # pragma: no cover - required by the base class, never called
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "_ColumnTagsToTerms":
        return cls(set(TagsToTermMapperConfig.model_validate(config_dict).tags))

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        schema = cast(Optional[SchemaMetadataClass], aspect)
        if schema is None:
            return None
        for field in schema.fields:
            if not field.globalTags or not field.globalTags.tags:
                continue
            matched = [
                assoc
                for assoc in field.globalTags.tags
                if _tag_name(assoc.tag) in self.tags
            ]
            if not matched:
                continue
            field.glossaryTerms = _merge_terms(
                field.glossaryTerms,
                (make_term_urn(_tag_name(assoc.tag)) for assoc in matched),
            )
            remaining = [a for a in field.globalTags.tags if a not in matched]
            field.globalTags = GlobalTagsClass(tags=remaining) if remaining else None
        return cast(Aspect, schema)


class TagsToTermMapper(TagsToTermTransformer):
    """Maps configured tags to glossary terms with the same name.

    A tag on the dataset or container becomes a term on the dataset or container,
    and a tag on a column becomes a term on that column. Tag names are matched in
    full, so ``pii.email`` and ``sensitivity:high`` must be configured as written.
    """

    def __init__(self, config: TagsToTermMapperConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx: PipelineContext = ctx
        self.config: TagsToTermMapperConfig = config
        self._columns = _ColumnTagsToTerms(set(config.tags))

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "TagsToTermMapper":
        config = TagsToTermMapperConfig.model_validate(config_dict)
        return cls(config, ctx)

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        # Entity-level glossaryTerms first, then column terms in schemaMetadata.
        return self._columns.transform(super().transform(record_envelopes))

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

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_glossary_terms: Optional[GlossaryTermsClass] = cast(
            Optional[GlossaryTermsClass], aspect
        )

        assert self.ctx.graph
        entity_tags = TagsToTermMapper.get_tags_from_global_tags(
            self.ctx.graph.get_tags(entity_urn)
        )

        tag_set = set(self.config.tags)
        terms_to_add: Set[str] = set()
        tags_to_delete: Set[str] = set()
        for full_tag in entity_tags:
            name = _tag_name(full_tag)
            if name in tag_set:
                terms_to_add.add(make_term_urn(name))
                tags_to_delete.add(full_tag)

        if not terms_to_add:
            return cast(Aspect, in_glossary_terms)  # No new terms to add

        for tag_urn in tags_to_delete:
            self.ctx.graph.remove_tag(tag_urn=tag_urn, resource_urn=entity_urn)

        # Keep terms emitted earlier in this run, e.g. by dbt meta_mapping.
        out_glossary_terms = _merge_terms(in_glossary_terms, sorted(terms_to_add))

        if self.config.semantics == TransformerSemantics.PATCH:
            patch_glossary_terms: Optional[GlossaryTermsClass] = (
                TagsToTermMapper._merge_with_server_glossary_terms(
                    self.ctx.graph, entity_urn, out_glossary_terms
                )
            )
            return cast(Optional[Aspect], patch_glossary_terms)
        else:
            return cast(Aspect, out_glossary_terms)
