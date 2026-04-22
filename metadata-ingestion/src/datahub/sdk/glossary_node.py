from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional, Type, Union

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

if TYPE_CHECKING:
    pass

GlossaryNodeUrnOrStr = Union[str, GlossaryNodeUrn]


class GlossaryNode(
    HasOwnership,
    HasInstitutionalMemory,
    HasStructuredProperties,
    Entity,
):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[GlossaryNodeUrn]:
        return GlossaryNodeUrn

    def __init__(
        self,
        *,
        name: str,
        display_name: Optional[str] = None,
        definition: str = "",
        parent_node: Optional[Union[GlossaryNodeUrnOrStr, "GlossaryNode"]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = GlossaryNodeUrn(name)
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
        entity = cls(name=urn.name)
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> GlossaryNodeUrn:
        assert isinstance(self._urn, GlossaryNodeUrn)
        return self._urn

    def _ensure_node_info(self) -> models.GlossaryNodeInfoClass:
        return self._setdefault_aspect(models.GlossaryNodeInfoClass(definition=""))

    @property
    def name(self) -> str:
        return self.urn.name

    @property
    def display_name(self) -> Optional[str]:
        return self._ensure_node_info().name

    def set_display_name(self, display_name: str) -> None:
        self._ensure_node_info().name = display_name

    @property
    def definition(self) -> str:
        return self._ensure_node_info().definition

    def set_definition(self, definition: str) -> None:
        self._ensure_node_info().definition = definition

    @property
    def parent_node(self) -> Optional[GlossaryNodeUrn]:
        raw = self._ensure_node_info().parentNode
        if raw is None:
            return None
        return GlossaryNodeUrn.from_string(raw)

    def set_parent_node(
        self, parent_node: Union[GlossaryNodeUrnOrStr, "GlossaryNode"]
    ) -> None:
        if isinstance(parent_node, GlossaryNode):
            parent_node = parent_node.urn
        self._ensure_node_info().parentNode = str(parent_node)

    @property
    def custom_properties(self) -> Dict[str, str]:
        return self._ensure_node_info().customProperties or {}

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        self._ensure_node_info().customProperties = custom_properties
