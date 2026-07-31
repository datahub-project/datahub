from __future__ import annotations

from typing import Dict, Optional, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import GlossaryNodeUrn, Urn
from datahub.sdk._shared import (
    HasInstitutionalMemory,
    HasOwnership,
    HasStructuredProperties,
    LinksInputType,
    OwnersInputType,
    StructuredPropertyInputType,
)
from datahub.sdk.entity import Entity, ExtraAspectsType

GlossaryNodeUrnOrStr = Union[str, GlossaryNodeUrn]


class GlossaryNode(
    HasOwnership,
    HasInstitutionalMemory,
    HasStructuredProperties,
    Entity,
):
    """A glossary node (folder/category) in the DataHub business glossary.

    Glossary nodes are used to organise glossary terms into a hierarchy.
    Each node can contain child nodes and/or glossary terms, forming a tree
    that mirrors your organisation's taxonomy.

    **Identity vs. display name**

    A glossary node has two distinct name fields:

    - ``id`` (the *identity*): forms the URN
      ``urn:li:glossaryNode:<id>`` and must be unique and stable.
      Renaming it breaks existing references, so choose an identifier that
      won't need to change.

    - ``display_name``: the human-readable label shown in the DataHub UI
      (e.g. ``"Financial Metrics"``). It can be changed freely without
      affecting any references to the node.

    Example::

        # Stable opaque id; human-readable label as display_name.
        node = GlossaryNode(
            id="7f3d2c1a",
            display_name="Financial Metrics",
            definition="All financial and accounting-related business terms.",
        )

        # Nested node — child refers to parent by object or URN.
        child = GlossaryNode(
            id="4b5e6f7a",
            display_name="Revenue Metrics",
            definition="Terms related to revenue recognition.",
            parent_node=node,
        )
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[GlossaryNodeUrn]:
        return GlossaryNodeUrn

    def __init__(
        self,
        *,
        id: str,
        display_name: Optional[str] = None,
        definition: str = "",
        parent_node: Optional[Union[GlossaryNodeUrnOrStr, "GlossaryNode"]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        """Initialize a new GlossaryNode.

        Args:
            id: Stable identifier used in the URN
                (``urn:li:glossaryNode:<id>``). This value must not change
                after the node is published — renaming it creates a new
                entity and orphans existing references. Use ``display_name``
                for the human-readable label shown in the UI.
            display_name: Human-readable label shown in the UI.
                Unlike ``id``, this can be updated freely at any time.
                Defaults to ``None`` (the UI falls back to ``id``).
            definition: Business definition of this glossary category.
            parent_node: Optional parent node, accepted as a
                :class:`GlossaryNode` object, a :class:`GlossaryNodeUrn`, or
                a raw URN string.
            custom_properties: Arbitrary key/value metadata pairs.
            owners: Owners of this node.
            links: Documentation or reference links.
            structured_properties: Structured property assignments.
            extra_aspects: Additional raw aspects to attach to the entity.
        """
        urn = GlossaryNodeUrn(id)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._ensure_node_info()
        if definition:
            self.set_definition(definition)
        if display_name is not None:
            self.set_display_name(display_name)
        if parent_node is not None:
            self.set_parent_node(parent_node)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)

        if owners is not None:
            self.set_owners(owners)
        if links is not None:
            self.set_links(links)
        if structured_properties is not None:
            for key, value in structured_properties.items():
                self.set_structured_property(property_urn=key, values=value)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, GlossaryNodeUrn)
        entity = cls(id=urn.name)
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> GlossaryNodeUrn:
        assert isinstance(self._urn, GlossaryNodeUrn)
        return self._urn

    def _ensure_node_info(self) -> models.GlossaryNodeInfoClass:
        return self._setdefault_aspect(models.GlossaryNodeInfoClass(definition=""))

    @property
    def id(self) -> str:
        """Stable identifier used in the URN. Read-only after construction."""
        return self.urn.name

    @property
    def display_name(self) -> Optional[str]:
        """Human-readable label shown in the UI. ``None`` if not set."""
        return self._ensure_node_info().name

    def set_display_name(self, display_name: str) -> None:
        """Set the human-readable display label."""
        self._ensure_node_info().name = display_name

    @property
    def definition(self) -> str:
        """Business definition of this glossary category."""
        return self._ensure_node_info().definition

    def set_definition(self, definition: str) -> None:
        """Set the business definition."""
        self._ensure_node_info().definition = definition

    @property
    def parent_node(self) -> Optional[GlossaryNodeUrn]:
        """URN of the parent node, or ``None`` if this is a root node."""
        raw = self._ensure_node_info().parentNode
        if raw is None:
            return None
        return GlossaryNodeUrn.from_string(raw)

    def set_parent_node(
        self, parent_node: Union[GlossaryNodeUrnOrStr, "GlossaryNode"]
    ) -> None:
        """Set the parent node.

        Args:
            parent_node: Accepted as a :class:`GlossaryNode` object, a
                :class:`GlossaryNodeUrn`, or a raw URN string.
        """
        if isinstance(parent_node, GlossaryNode):
            parent_node = parent_node.urn
        self._ensure_node_info().parentNode = str(parent_node)

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Arbitrary key/value metadata pairs."""
        return self._ensure_node_info().customProperties or {}

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Replace all custom properties."""
        self._ensure_node_info().customProperties = custom_properties
