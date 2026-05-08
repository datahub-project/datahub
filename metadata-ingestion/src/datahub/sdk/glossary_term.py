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
    """A business glossary term in DataHub.

    Glossary terms capture the authoritative definitions of business concepts
    and can be attached to datasets, columns, dashboards, and other entities
    to communicate their meaning.

    **Identity vs. display name**

    A glossary term has two distinct name fields:

    - ``id`` (the *identity*): forms the URN
      ``urn:li:glossaryTerm:<id>`` and must be unique and stable.
      Renaming it breaks every place the term is referenced (datasets,
      columns, policies, …), so choose an identifier that won't need to
      change.

    - ``display_name``: the human-readable label shown in the DataHub UI
      (e.g. ``"Personally Identifiable Information"``). It can be changed
      freely without affecting any references to the term.

    **Term source**

    - ``"INTERNAL"`` (default): the term is defined by your organisation.
    - ``"EXTERNAL"``: the term comes from an external standard such as
      FIBO, GDPR, or another vocabulary. Use ``source_ref`` and
      ``source_url`` to link to the canonical definition.

    **Semantic relationships**

    Relationships between terms are modelled via four lists:

    - ``is_a``: *IsA* — this term is a subtype/specialisation of the
      listed terms (e.g. ``Email`` is_a ``PII``).
    - ``has_a``: *HasA* — this term is composed of the listed terms
      (e.g. ``Address`` has_a ``ZipCode``, ``Street``, ``City``).
    - ``values``: *HasValue* — fixed allowed values for an enumeration
      (e.g. ``ColorEnum`` has values ``Red``, ``Green``, ``Blue``).
    - ``related_terms``: *IsRelatedTo* — loosely related terms with no
      directional semantic (e.g. ``Email`` is related to ``PhoneNumber``).

    Example::

        term = GlossaryTerm(
            id="3a4b5c6d",                      # stable URN key
            display_name="Personally Identifiable Information",
            definition="Data that can identify a person.",
            parent_node=GlossaryNodeUrn("2f3a4b5c"),
            is_a=[GlossaryTermUrn("1a2b3c4d")],
        )

        # External term sourced from FIBO
        fibo_term = GlossaryTerm(
            id="7f8a9b0c",
            display_name="Financial Instrument",
            definition="A financial instrument as defined by FIBO.",
            term_source="EXTERNAL",
            source_ref="FIBO",
            source_url="https://spec.edmcouncil.org/fibo/",
        )
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[GlossaryTermUrn]:
        return GlossaryTermUrn

    def __init__(
        self,
        *,
        id: str,
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
        """Initialize a new GlossaryTerm.

        Args:
            id: Stable identifier used in the URN
                (``urn:li:glossaryTerm:<id>``). This value must not change
                after the term is published — renaming it creates a new
                entity and orphans every reference (dataset columns,
                policies, …). Use ``display_name`` for the human-readable
                label shown in the UI.
            display_name: Human-readable label shown in the UI.
                Unlike ``id``, this can be updated freely at any time.
                Defaults to ``None`` (the UI falls back to ``id``).
            definition: Authoritative business definition of the term.
            parent_node: Optional parent glossary node for organising the
                term within the hierarchy. Accepted as a
                :class:`~datahub.sdk.glossary_node.GlossaryNode` object, a
                :class:`GlossaryNodeUrn`, or a raw URN string.
            term_source: ``"INTERNAL"`` (default) for terms defined by your
                organisation, or ``"EXTERNAL"`` for terms sourced from an
                external standard (e.g. FIBO, GDPR).
            source_ref: Short reference name for the external source, e.g.
                ``"FIBO"``. Only meaningful when ``term_source="EXTERNAL"``.
            source_url: URL to the canonical definition in the external
                source. Only meaningful when ``term_source="EXTERNAL"``.
            custom_properties: Arbitrary key/value metadata pairs.
            is_a: *IsA* relationships — terms that this term is a
                subtype/specialisation of (e.g. ``Email`` is_a ``PII``).
            has_a: *HasA* relationships — terms that this term is composed
                of (e.g. ``Address`` has_a ``ZipCode``, ``Street``).
            values: *HasValue* relationships — fixed allowed values when
                this term represents an enumeration (e.g. ``Red``,
                ``Green``, ``Blue`` for a ``ColorEnum`` term).
            related_terms: *IsRelatedTo* relationships — loosely related
                terms with no directional semantic.
            owners: Owners of this term.
            links: Documentation or reference links.
            domain: Domain this term belongs to.
            structured_properties: Structured property assignments.
            extra_aspects: Additional raw aspects to attach to the entity.
        """
        urn = GlossaryTermUrn(id)
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
        entity = cls(id=urn.name)
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
    def id(self) -> str:
        """Stable identifier used in the URN. Read-only after construction."""
        return self.urn.name

    @property
    def display_name(self) -> Optional[str]:
        """Human-readable label shown in the UI. ``None`` if not set."""
        return self._ensure_term_info().name

    def set_display_name(self, display_name: str) -> None:
        """Set the human-readable display label."""
        self._ensure_term_info().name = display_name

    @property
    def definition(self) -> str:
        """Authoritative business definition of the term."""
        return self._ensure_term_info().definition

    def set_definition(self, definition: str) -> None:
        """Set the business definition."""
        self._ensure_term_info().definition = definition

    @property
    def parent_node(self) -> Optional[GlossaryNodeUrn]:
        """URN of the parent glossary node, or ``None`` if unset."""
        raw = self._ensure_term_info().parentNode
        if raw is None:
            return None
        return GlossaryNodeUrn.from_string(raw)

    def set_parent_node(
        self, parent_node: Union[GlossaryNodeUrnOrStr, GlossaryNode]
    ) -> None:
        """Set the parent glossary node.

        Args:
            parent_node: Accepted as a :class:`GlossaryNode` object, a
                :class:`GlossaryNodeUrn`, or a raw URN string.
        """
        if isinstance(parent_node, GlossaryNode):
            parent_node = parent_node.urn
        self._ensure_term_info().parentNode = str(parent_node)

    @property
    def term_source(self) -> str:
        """``"INTERNAL"`` or ``"EXTERNAL"``."""
        return self._ensure_term_info().termSource

    def set_term_source(self, term_source: str) -> None:
        """Set the term source (``"INTERNAL"`` or ``"EXTERNAL"``)."""
        self._ensure_term_info().termSource = term_source

    @property
    def source_ref(self) -> Optional[str]:
        """Short name of the external source (e.g. ``"FIBO"``), or ``None``."""
        return self._ensure_term_info().sourceRef

    def set_source_ref(self, source_ref: str) -> None:
        """Set the external source reference name."""
        self._ensure_term_info().sourceRef = source_ref

    @property
    def source_url(self) -> Optional[str]:
        """URL to the canonical definition in the external source, or ``None``."""
        return self._ensure_term_info().sourceUrl

    def set_source_url(self, source_url: str) -> None:
        """Set the URL to the external source definition."""
        self._ensure_term_info().sourceUrl = source_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Arbitrary key/value metadata pairs."""
        return self._ensure_term_info().customProperties or {}

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Replace all custom properties."""
        self._ensure_term_info().customProperties = custom_properties

    # --- Related terms: is_a ---

    @property
    def is_a(self) -> List[GlossaryTermUrn]:
        """*IsA* — terms that this term is a subtype/specialisation of."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.isRelatedTerms is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.isRelatedTerms]

    def set_is_a(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        """Replace the full *IsA* relationship list."""
        self._ensure_related_terms().isRelatedTerms = [str(t) for t in terms]

    def add_is_a(self, term: GlossaryTermUrnOrStr) -> None:
        """Add a term to the *IsA* list (idempotent)."""
        related = self._ensure_related_terms()
        if related.isRelatedTerms is None:
            related.isRelatedTerms = []
        term_str = str(term)
        if term_str not in related.isRelatedTerms:
            related.isRelatedTerms.append(term_str)

    def remove_is_a(self, term: GlossaryTermUrnOrStr) -> None:
        """Remove a term from the *IsA* list."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.isRelatedTerms is not None:
            term_str = str(term)
            raw.isRelatedTerms = [t for t in raw.isRelatedTerms if t != term_str]

    # --- Related terms: has_a ---

    @property
    def has_a(self) -> List[GlossaryTermUrn]:
        """*HasA* — terms that this term is composed of."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.hasRelatedTerms is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.hasRelatedTerms]

    def set_has_a(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        """Replace the full *HasA* relationship list."""
        self._ensure_related_terms().hasRelatedTerms = [str(t) for t in terms]

    def add_has_a(self, term: GlossaryTermUrnOrStr) -> None:
        """Add a term to the *HasA* list (idempotent)."""
        related = self._ensure_related_terms()
        if related.hasRelatedTerms is None:
            related.hasRelatedTerms = []
        term_str = str(term)
        if term_str not in related.hasRelatedTerms:
            related.hasRelatedTerms.append(term_str)

    def remove_has_a(self, term: GlossaryTermUrnOrStr) -> None:
        """Remove a term from the *HasA* list."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.hasRelatedTerms is not None:
            term_str = str(term)
            raw.hasRelatedTerms = [t for t in raw.hasRelatedTerms if t != term_str]

    # --- Related terms: values (enum values) ---

    @property
    def values(self) -> List[GlossaryTermUrn]:
        """*HasValue* — fixed allowed values when this term is an enumeration."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.values is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.values]

    def set_values(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        """Replace the full *HasValue* list."""
        self._ensure_related_terms().values = [str(t) for t in terms]

    def add_value(self, term: GlossaryTermUrnOrStr) -> None:
        """Add a term to the *HasValue* list (idempotent)."""
        related = self._ensure_related_terms()
        if related.values is None:
            related.values = []
        term_str = str(term)
        if term_str not in related.values:
            related.values.append(term_str)

    def remove_value(self, term: GlossaryTermUrnOrStr) -> None:
        """Remove a term from the *HasValue* list."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.values is not None:
            term_str = str(term)
            raw.values = [t for t in raw.values if t != term_str]

    # --- Related terms: related_terms ---

    @property
    def related_terms(self) -> List[GlossaryTermUrn]:
        """*IsRelatedTo* — loosely related terms with no directional semantic."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is None or raw.relatedTerms is None:
            return []
        return [GlossaryTermUrn.from_string(t) for t in raw.relatedTerms]

    def set_related_terms(self, terms: List[GlossaryTermUrnOrStr]) -> None:
        """Replace the full *IsRelatedTo* list."""
        self._ensure_related_terms().relatedTerms = [str(t) for t in terms]

    def add_related_term(self, term: GlossaryTermUrnOrStr) -> None:
        """Add a term to the *IsRelatedTo* list (idempotent)."""
        related = self._ensure_related_terms()
        if related.relatedTerms is None:
            related.relatedTerms = []
        term_str = str(term)
        if term_str not in related.relatedTerms:
            related.relatedTerms.append(term_str)

    def remove_related_term(self, term: GlossaryTermUrnOrStr) -> None:
        """Remove a term from the *IsRelatedTo* list."""
        raw = self._get_aspect(models.GlossaryRelatedTermsClass)
        if raw is not None and raw.relatedTerms is not None:
            term_str = str(term)
            raw.relatedTerms = [t for t in raw.relatedTerms if t != term_str]
