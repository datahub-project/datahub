import abc
from typing import List, Optional, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import Aspect as AspectTypeVar
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.metadata.urns import Urn
from datahub.utilities.urns._urn_base import _SpecificUrn


class Entity:
    __slots__ = ("_urn", "_prev_aspects", "_aspects")

    def __init__(self, /, urn: Urn):
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
        # If an init method from a subclass adds required fields, it also needs to override this method.
        # An alternative approach would call cls.__new__() to bypass the init method, but it's a bit
        # too hacky for my taste.
        entity = cls(urn=urn)
        return entity._init_from_graph(current_aspects)

    def _init_from_graph(self, current_aspects: models.AspectBag) -> Self:
        self._prev_aspects = current_aspects

        self._aspects = {}
        aspect: models._Aspect
        for aspect_name, aspect in (current_aspects or {}).items():  # type: ignore
            aspect_copy = type(aspect).from_obj(aspect.to_obj())
            self._aspects[aspect_name] = aspect_copy  # type: ignore
        return self

    @classmethod
    @abc.abstractmethod
    def get_urn_type(cls) -> Type[_SpecificUrn]: ...

    @property
    def urn(self) -> _SpecificUrn:
        return self._urn

    def _get_aspect(
        self,
        aspect_type: Type[AspectTypeVar],
        /,
    ) -> Optional[AspectTypeVar]:
        return self._aspects.get(aspect_type.ASPECT_NAME)  # type: ignore

    def _set_aspect(self, value: AspectTypeVar, /) -> None:
        self._aspects[value.ASPECT_NAME] = value  # type: ignore

    def _setdefault_aspect(self, default_aspect: AspectTypeVar, /) -> AspectTypeVar:
        # Similar semantics to dict.setdefault.
        if existing_aspect := self._get_aspect(type(default_aspect)):
            return existing_aspect
        self._set_aspect(default_aspect)
        return default_aspect

    def _as_mcps(
        self,
        change_type: Union[str, models.ChangeTypeClass] = models.ChangeTypeClass.UPSERT,
    ) -> List[MetadataChangeProposalWrapper]:
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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.urn}')"
