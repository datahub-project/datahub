from __future__ import annotations

from typing import Optional, Type

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import TagUrn, Urn
from datahub.sdk._shared import (
    HasOwnership,
    OwnersInputType,
)
from datahub.sdk.entity import Entity, ExtraAspectsType


class Tag(
    HasOwnership,
    Entity,
):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[TagUrn]:
        return TagUrn

    def __init__(
        self,
        *,
        # Identity.
        name: str,
        # Tag properties.
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        color: Optional[str] = None,
        # Standard aspects.
        owners: Optional[OwnersInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        """Initialize a new Tag instance."""
        urn = TagUrn(name=name)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._ensure_tag_props(
            display_name=display_name or name,
            description=description,
            color=color,
        )

        if owners is not None:
            self.set_owners(owners)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, TagUrn)
        entity = cls(name=urn.name)
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> TagUrn:
        assert isinstance(self._urn, TagUrn)
        return self._urn

    def _ensure_tag_props(
        self,
        *,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        color: Optional[str] = None,
    ) -> models.TagPropertiesClass:
        existing_props = self._get_aspect(models.TagPropertiesClass)
        if existing_props is not None:
            if display_name is not None:
                existing_props.name = display_name
            if description is not None:
                existing_props.description = description
            if color is not None:
                existing_props.colorHex = color
            return existing_props

        return self._setdefault_aspect(
            models.TagPropertiesClass(
                name=display_name or self.urn.name,
                description=description,
                colorHex=color,
            )
        )

    @property
    def name(self) -> str:
        return self.urn.name

    @property
    def display_name(self) -> str:
        return self._ensure_tag_props().name

    def set_display_name(self, display_name: str) -> None:
        self._ensure_tag_props(display_name=display_name)

    @property
    def description(self) -> Optional[str]:
        return self._ensure_tag_props().description

    def set_description(self, description: str) -> None:
        self._ensure_tag_props(description=description)

    @property
    def color(self) -> Optional[str]:
        return self._ensure_tag_props().colorHex

    def set_color(self, color: str) -> None:
        self._ensure_tag_props(color=color)
