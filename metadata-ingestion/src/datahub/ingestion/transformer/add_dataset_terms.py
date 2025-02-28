import logging
from typing import Callable, List, Optional, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetTermsTransformer
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)


class AddDatasetTermsConfig(TransformerSemanticsConfigModel):
    get_terms_to_add: Callable[[str], List[GlossaryTermAssociationClass]]

    _resolve_term_fn = pydantic_resolve_key("get_terms_to_add")


class AddDatasetTerms(DatasetTermsTransformer):
    """Transformer that adds glossary terms to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetTermsConfig

    def __init__(self, config: AddDatasetTermsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.log = logging.getLogger(__name__)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetTerms":
        config = AddDatasetTermsConfig.parse_obj(config_dict)
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

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_glossary_terms: Optional[GlossaryTermsClass] = cast(
            Optional[GlossaryTermsClass], aspect
        )
        out_glossary_terms: GlossaryTermsClass = GlossaryTermsClass(
            terms=[],
            auditStamp=(
                in_glossary_terms.auditStamp
                if in_glossary_terms is not None
                else AuditStampClass(
                    time=builder.get_sys_time(), actor="urn:li:corpUser:restEmitter"
                )
            ),
        )
        # Check if user want to keep existing terms
        if in_glossary_terms is not None and self.config.replace_existing is False:
            out_glossary_terms.terms.extend(in_glossary_terms.terms)
            out_glossary_terms.auditStamp = in_glossary_terms.auditStamp

        terms_to_add = self.config.get_terms_to_add(entity_urn)
        if terms_to_add is not None:
            out_glossary_terms.terms.extend(terms_to_add)

        patch_glossary_terms: Optional[GlossaryTermsClass] = None
        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            patch_glossary_terms = AddDatasetTerms._merge_with_server_glossary_terms(
                self.ctx.graph, entity_urn, out_glossary_terms
            )
            return cast(Optional[Aspect], patch_glossary_terms)
        else:
            return cast(Aspect, out_glossary_terms)


class SimpleDatasetTermsConfig(TransformerSemanticsConfigModel):
    term_urns: List[str]


class SimpleAddDatasetTerms(AddDatasetTerms):
    """Transformer that adds a specified set of glossary terms to each dataset."""

    def __init__(self, config: SimpleDatasetTermsConfig, ctx: PipelineContext):
        terms = [GlossaryTermAssociationClass(urn=term) for term in config.term_urns]

        generic_config = AddDatasetTermsConfig(
            get_terms_to_add=lambda _: terms,
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleAddDatasetTerms":
        config = SimpleDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternDatasetTermsConfig(TransformerSemanticsConfigModel):
    term_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddDatasetTerms(AddDatasetTerms):
    """Transformer that adds a specified set of glossary terms to each dataset."""

    def __init__(self, config: PatternDatasetTermsConfig, ctx: PipelineContext):
        term_pattern = config.term_pattern
        generic_config = AddDatasetTermsConfig(
            get_terms_to_add=lambda entity_urn: [
                GlossaryTermAssociationClass(urn=term_urn)
                for term_urn in term_pattern.value(entity_urn)
            ],
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetTerms":
        config = PatternDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)
