from __future__ import annotations

import abc
from typing import TYPE_CHECKING, List, Optional, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import Aspect as AspectTypeVar
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.metadata.urns import Urn
from datahub.utilities.urns._urn_base import _SpecificUrn

if TYPE_CHECKING:
    from datahub.ingestion.api.workunit import MetadataWorkUnit


ExtraAspectsType = Union[None, List[AspectTypeVar]]


class Entity:
    """Base class for all DataHub entities.

    This class provides the core functionality for working with DataHub entities,
    including aspect management and URN handling. It should not be instantiated directly;
    instead, use one of its subclasses like Dataset or Container.
    """

    __slots__ = ("_urn", "_prev_aspects", "_aspects")

    def __init__(self, /, urn: Urn):
        """Initialize a new Entity instance.

        Args:
            urn: The URN that uniquely identifies this entity.

        Raises:
            SdkUsageError: If this base class is instantiated directly.
        """
        # This method is not meant for direct usage.
        if type(self) is Entity:
            raise SdkUsageError(f"{Entity.__name__} cannot be instantiated directly.")

        assert isinstance(urn, self.get_urn_type())
        self._urn: _SpecificUrn = urn

        # prev_aspects is None means this was created from scratch
        self._prev_aspects: Optional[models.AspectBag] = None
        self._aspects: models.AspectBag = {}

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        """Create a new entity instance from graph data.

        Args:
            urn: The URN of the entity.
            current_aspects: The current aspects of the entity from the graph.

        Returns:
            A new entity instance initialized with the graph data.
        """
        # If an init method from a subclass adds required fields, it also needs to override this method.
        # An alternative approach would call cls.__new__() to bypass the init method, but it's a bit
        # too hacky for my taste.
        entity = cls(urn=urn)
        return entity._init_from_graph(current_aspects)

    def _init_from_graph(self, current_aspects: models.AspectBag) -> Self:
        """Initialize the entity with aspects from the graph.

        Args:
            current_aspects: The current aspects of the entity from the graph.

        Returns:
            The entity instance with initialized aspects.
        """
        self._prev_aspects = current_aspects

        self._aspects = {}
        aspect: models._Aspect
        for aspect_name, aspect in (current_aspects or {}).items():  # type: ignore
            aspect_copy = type(aspect).from_obj(aspect.to_obj())
            self._aspects[aspect_name] = aspect_copy  # type: ignore
        return self

    @classmethod
    @abc.abstractmethod
    def get_urn_type(cls) -> Type[_SpecificUrn]:
        """Get the URN type for this entity class.

        Returns:
            The URN type class that corresponds to this entity type.
        """
        ...

    @classmethod
    def entity_type_name(cls) -> str:
        """Get the entity type name.

        Returns:
            The string name of this entity type.
        """
        return cls.get_urn_type().ENTITY_TYPE

    @property
    def urn(self) -> _SpecificUrn:
        """Get the entity's URN.

        Returns:
            The URN that uniquely identifies this entity.
        """
        return self._urn

    def _get_aspect(
        self,
        aspect_type: Type[AspectTypeVar],
        /,
    ) -> Optional[AspectTypeVar]:
        """Get an aspect of the entity by its type.

        Args:
            aspect_type: The type of aspect to retrieve.

        Returns:
            The aspect if it exists, None otherwise.
        """
        return self._aspects.get(aspect_type.ASPECT_NAME)  # type: ignore

    def _set_aspect(self, value: AspectTypeVar, /) -> None:
        """Set an aspect of the entity.

        Args:
            value: The aspect to set.
        """
        self._aspects[value.ASPECT_NAME] = value  # type: ignore

    def _setdefault_aspect(self, default_aspect: AspectTypeVar, /) -> AspectTypeVar:
        """Set a default aspect if it doesn't exist.

        Args:
            default_aspect: The default aspect to set if none exists.

        Returns:
            The existing aspect if one exists, otherwise the default aspect.
        """
        # Similar semantics to dict.setdefault.
        if existing_aspect := self._get_aspect(type(default_aspect)):
            return existing_aspect
        self._set_aspect(default_aspect)
        return default_aspect

    def as_mcps(
        self,
        change_type: Union[str, models.ChangeTypeClass] = models.ChangeTypeClass.UPSERT,
    ) -> List[MetadataChangeProposalWrapper]:
        """Convert the entity's aspects to MetadataChangeProposals.

        Args:
            change_type: The type of change to apply (default: UPSERT).

        Returns:
            A list of MetadataChangeProposalWrapper objects.
        """
        urn_str = str(self.urn)

        mcps = []
        for aspect in self._aspects.values():
            assert isinstance(aspect, models._Aspect)
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=urn_str,
                    aspect=aspect,
                    changeType=change_type,
                )
            )
        return mcps

    def as_workunits(self) -> List[MetadataWorkUnit]:
        """Convert the entity's aspects to MetadataWorkUnits.

        Returns:
            A list of MetadataWorkUnit objects.
        """
        return [mcp.as_workunit() for mcp in self.as_mcps()]

    def _set_extra_aspects(self, extra_aspects: ExtraAspectsType) -> None:
        """Set additional aspects on the entity.

        Args:
            extra_aspects: List of additional aspects to set.

        Note:
            This method does not validate for conflicts between extra aspects
            and standard aspects.
        """
        # TODO: Add validation to ensure that an "extra aspect" does not conflict
        # with / get overridden by a standard aspect.
        for aspect in extra_aspects or []:
            self._set_aspect(aspect)

    def __repr__(self) -> str:
        """Get a string representation of the entity.

        Returns:
            A string in the format "EntityClass('urn')".
        """
        return f"{self.__class__.__name__}('{self.urn}')"
