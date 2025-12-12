# ABOUTME: Universal transformer that adds glossary terms to datasets, charts, and dashboards.
# ABOUTME: Supports callback, simple list, and pattern-based term assignment.

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
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)

logger = logging.getLogger(__name__)


class AddTermsConfig(TransformerSemanticsConfigModel):
    get_terms_to_add: Callable[[str], List[GlossaryTermAssociationClass]]

    _resolve_term_fn = pydantic_resolve_key("get_terms_to_add")


class AddTerms(BaseTransformer, SingleAspectTransformer):
    """Universal transformer that adds glossary terms to datasets, charts, and dashboards."""

    ctx: PipelineContext
    config: AddTermsConfig

    def __init__(self, config: AddTermsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    def aspect_name(self) -> str:
        return "glossaryTerms"

    def entity_types(self) -> List[str]:
        return ["dataset", "chart", "dashboard"]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddTerms":
        config = AddTermsConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _merge_with_server_glossary_terms(
        graph: DataHubGraph,
        urn: str,
        glossary_terms_aspect: Optional[GlossaryTermsClass],
    ) -> Optional[GlossaryTermsClass]:
        if not glossary_terms_aspect or not glossary_terms_aspect.terms:
            return None

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

        # Check if user wants to keep existing terms
        if in_glossary_terms is not None and self.config.replace_existing is False:
            out_glossary_terms.terms.extend(in_glossary_terms.terms)
            out_glossary_terms.auditStamp = in_glossary_terms.auditStamp

        terms_to_add = self.config.get_terms_to_add(entity_urn)
        if terms_to_add is not None:
            out_glossary_terms.terms.extend(terms_to_add)

        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            return cast(
                Optional[Aspect],
                self._merge_with_server_glossary_terms(
                    self.ctx.graph, entity_urn, out_glossary_terms
                ),
            )

        return cast(Aspect, out_glossary_terms)


class SimpleTermsConfig(TransformerSemanticsConfigModel):
    term_urns: List[str]


class SimpleAddTerms(AddTerms):
    """Transformer that adds a specified set of glossary terms to each entity."""

    def __init__(self, config: SimpleTermsConfig, ctx: PipelineContext):
        terms = [GlossaryTermAssociationClass(urn=term) for term in config.term_urns]

        generic_config = AddTermsConfig(
            get_terms_to_add=lambda _: terms,
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleAddTerms":
        config = SimpleTermsConfig.model_validate(config_dict)
        return cls(config, ctx)


class PatternTermsConfig(TransformerSemanticsConfigModel):
    term_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddTerms(AddTerms):
    """Transformer that adds glossary terms based on pattern matching."""

    def __init__(self, config: PatternTermsConfig, ctx: PipelineContext):
        term_pattern = config.term_pattern
        generic_config = AddTermsConfig(
            get_terms_to_add=lambda entity_urn: [
                GlossaryTermAssociationClass(urn=term_urn)
                for term_urn in term_pattern.value(entity_urn)
            ],
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "PatternAddTerms":
        config = PatternTermsConfig.model_validate(config_dict)
        return cls(config, ctx)
