from __future__ import annotations

from typing import Dict, List, Optional, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import GlossaryNodeUrn, GlossaryTermUrn, Urn
from datahub.sdk._shared import (
    DomainInputType,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasStructuredProperties,
    LinksInputType,
    OwnersInputType,
    StructuredPropertyInputType,
)
from datahub.sdk.entity import Entity, ExtraAspectsType
from datahub.sdk.glossary_node import GlossaryNode, GlossaryNodeUrnOrStr

GlossaryTermUrnOrStr = Union[str, GlossaryTermUrn]


class GlossaryTerm(
    HasOwnership,
    HasInstitutionalMemory,
    HasStructuredProperties,
    HasDomain,
    Entity,
):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[GlossaryTermUrn]:
        return GlossaryTermUrn

    def __init__(
        self,
        *,
        name: str,
        display_name: Optional[str] = None,
        definition: str = "",
        parent_node: Optional[Union[GlossaryNodeUrnOrStr, GlossaryNode]] = None,
        term_source: str = "INTERNAL",
        source_ref: Optional[str] = None,
        source_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        is_a: Optional[List[GlossaryTermUrnOrStr]] = None,
        has_a: Optional[List[GlossaryTermUrnOrStr]] = None,
        values: Optional[List[GlossaryTermUrnOrStr]] = None,
        related_terms: Optional[List[GlossaryTermUrnOrStr]] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = GlossaryTermUrn(name)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._ensure_term_info(term_source=term_source)
        if definition:
            self.set_definition(definition)
        if display_name is not None:
            self.set_display_name(display_name)
        if parent_node is not None:
            self.set_parent_node(parent_node)
        if source_ref is not None:
            self.set_source_ref(source_ref)
        if source_url is not None:
            self.set_source_url(source_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)

        if is_a is not None:
            self.set_is_a(is_a)
        if has_a is not None:
            self.set_has_a(has_a)
        if values is not None:
            self.set_values(values)
        if related_terms is not None:
            self.set_related_terms(related_terms)

        if owners is not None:
            self.set_owners(owners)
        if links is not None:
            self.set_links(links)
        if domain is not None:
            self.set_domain(domain)
        if structured_properties is not None:
            for key, value in structured_properties.items():
                self.set_structured_property(property_urn=key, values=value)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, GlossaryTermUrn)
        entity = cls(name=urn.name)
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> GlossaryTermUrn:
        assert isinstance(self._urn, GlossaryTermUrn)
        return self._urn

    def _ensure_term_info(
        self, term_source: str = "INTERNAL"
    ) -> models.GlossaryTermInfoClass:
        return self._setdefault_aspect(
            models.GlossaryTermInfoClass(definition="", termSource=term_source)
        )

    def _ensure_related_terms(self) -> models.GlossaryRelatedTermsClass:
        return self._setdefault_aspect(models.GlossaryRelatedTermsClass())

    @property
    def name(self) -> str:
        return self.urn.name

    @property
    def display_name(self) -> Optional[str]:
        return self._ensure_term_info().name

    def set_display_name(self, display_name: str) -> None:
        self._ensure_term_info().name = display_name

    @property
    def definition(self) -> str:
        return self._ensure_term_info().definition

    def set_definition(self, definition: str) -> None:
        self._ensure_term_info().definition = definition

    @property
    def parent_node(self) -> Optional[GlossaryNodeUrn]:
        raw = self._ensure_term_info().parentNode
        if raw is None:
            return None
        return GlossaryNodeUrn.from_string(raw)

    def set_parent_node(
        self, parent_node: Union[GlossaryNodeUrnOrStr, GlossaryNode]
    ) -> None:
        if isinstance(parent_node, GlossaryNode):
            parent_node = parent_node.urn
        self._ensure_term_info().parentNode = str(parent_node)

    @property
    def term_source(self) -> str:
        return self._ensure_term_info().termSource

    def set_term_source(self, term_source: str) -> None:
        self._ensure_term_info().termSource = term_source

    @property
    def source_ref(self) -> Optional[str]:
        return self._ensure_term_info().sourceRef

    def set_source_ref(self, source_ref: str) -> None:
        self._ensure_term_info().sourceRef = source_ref

    @property
    def source_url(self) -> Optional[str]:
        return self._ensure_term_info().sourceUrl

    def set_source_url(self, source_url: str) -> None:
        self._ensure_term_info().sourceUrl = source_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        return self._ensure_term_info().customProperties or {}

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        self._ensure_term_info().customProperties = custom_properties

    # --- Related terms: is_a ---

    @property
    def is_a(self) -> List[GlossaryTermUrn]:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.isRelatedTerms is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.isRelatedTerms]

    def set_is_a(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        self._ensure_related_terms().isRelatedTerms = [str(t) for t in terms]

    def add_is_a(self, term: GlossaryTermUrnOrStr) -> None:
        related = self._ensure_related_terms()
        if related.isRelatedTerms is None:
            related.isRelatedTerms = []
        term_str = str(term)
        if term_str not in related.isRelatedTerms:
            related.isRelatedTerms.append(term_str)

    def remove_is_a(self, term: GlossaryTermUrnOrStr) -> None:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.isRelatedTerms is not None:
            term_str = str(term)
            raw.isRelatedTerms = [t for t in raw.isRelatedTerms if t != term_str]

    # --- Related terms: has_a ---

    @property
    def has_a(self) -> List[GlossaryTermUrn]:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.hasRelatedTerms is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.hasRelatedTerms]

    def set_has_a(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        self._ensure_related_terms().hasRelatedTerms = [str(t) for t in terms]

    def add_has_a(self, term: GlossaryTermUrnOrStr) -> None:
        related = self._ensure_related_terms()
        if related.hasRelatedTerms is None:
            related.hasRelatedTerms = []
        term_str = str(term)
        if term_str not in related.hasRelatedTerms:
            related.hasRelatedTerms.append(term_str)

    def remove_has_a(self, term: GlossaryTermUrnOrStr) -> None:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.hasRelatedTerms is not None:
            term_str = str(term)
            raw.hasRelatedTerms = [t for t in raw.hasRelatedTerms if t != term_str]

    # --- Related terms: values (enum values) ---

    @property
    def values(self) -> List[GlossaryTermUrn]:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.values is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.values]

    def set_values(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        self._ensure_related_terms().values = [str(t) for t in terms]

    def add_value(self, term: GlossaryTermUrnOrStr) -> None:
        related = self._ensure_related_terms()
        if related.values is None:
            related.values = []
        term_str = str(term)
        if term_str not in related.values:
            related.values.append(term_str)

    def remove_value(self, term: GlossaryTermUrnOrStr) -> None:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.values is not None:
            term_str = str(term)
            raw.values = [t for t in raw.values if t != term_str]

    # --- Related terms: related_terms ---

    @property
    def related_terms(self) -> List[GlossaryTermUrn]:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.relatedTerms is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.relatedTerms]

    def set_related_terms(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        self._ensure_related_terms().relatedTerms = [str(t) for t in terms]

    def add_related_term(self, term: GlossaryTermUrnOrStr) -> None:
        related = self._ensure_related_terms()
        if related.relatedTerms is None:
            related.relatedTerms = []
        term_str = str(term)
        if term_str not in related.relatedTerms:
            related.relatedTerms.append(term_str)

    def remove_related_term(self, term: GlossaryTermUrnOrStr) -> None:
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.relatedTerms is not None:
            term_str = str(term)
            raw.relatedTerms = [t for t in raw.relatedTerms if t != term_str]
