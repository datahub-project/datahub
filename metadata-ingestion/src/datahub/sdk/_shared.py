import abc
from typing import List, Optional, Protocol, Tuple, Type, Union, runtime_checkable

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import (
    Aspect as AspectTypeVar,
    make_user_urn,
    validate_ownership_type,
)
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, OwnershipTypeUrn, Urn
from datahub.sdk.errors import SdkUsageError

UrnOrStr = Union[Urn, str]
ActorUrn = Union[CorpUserUrn, CorpGroupUrn]


@runtime_checkable
class HasUrn(Protocol):
    @property
    def urn(self) -> Urn:
        ...


class Entity(HasUrn):
    def __init__(self, /, urn: Urn):
        # This method is not meant for direct usage.
        if type(self) is Entity:
            raise SdkUsageError(f"{Entity.__name__} cannot be instantiated directly.")

        assert isinstance(urn, self.get_urn_type())
        self._urn: Urn = urn

        # prev_aspects is None means this was created from scratch
        self._prev_aspects: Optional[models.AspectBag] = None
        self._aspects: models.AspectBag = {}

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        entity = cls(urn=urn)

        entity._prev_aspects = current_aspects
        for aspect_name, aspect in (current_aspects or {}).items():
            entity._aspects[aspect_name] = aspect.copy()  # type: ignore
        return entity

    @classmethod
    @abc.abstractmethod
    def get_urn_type(cls) -> Type[Urn]:
        pass

    @property
    def urn(self) -> Urn:
        return self._urn

    def _get_aspect(
        self,
        aspect_type: Type[AspectTypeVar],
        /,
    ) -> Optional[AspectTypeVar]:
        return self._aspects.get(aspect_type.ASPECT_NAME)  # type: ignore

    def _set_aspect(self, value: AspectTypeVar, /) -> None:
        self._aspects[value.ASPECT_NAME] = value  # type: ignore


class HasSubtype(Entity):
    @property
    def subtype(self) -> Optional[str]:
        subtypes = self._get_aspect(models.SubTypesClass)
        if subtypes and subtypes.typeNames:
            # TODO: throw an error if there are multiple subtypes
            return subtypes.typeNames[0]
        # TODO: throw an error if there is no subtype? or default to None?
        return None

    @subtype.setter
    def subtype(self, subtype: str) -> None:
        self._set_aspect(models.SubTypesClass(typeNames=[subtype]))


OwnershipTypeType = Union[str, OwnershipTypeUrn]
OwnerInputType = Union[
    str,
    ActorUrn,
    Tuple[Union[str, ActorUrn], OwnershipTypeType],
    models.OwnerClass,
]
OwnersInputType = List[OwnerInputType]


class HasOwners(Entity):
    @staticmethod
    def _parse_owner_class(owner: OwnerInputType) -> models.OwnerClass:
        # TODO: better support for custom ownership types?
        # TODO: add the user auto-resolver here?

        if isinstance(owner, models.OwnerClass):
            return owner

        owner_type = models.OwnershipTypeClass.TECHNICAL_OWNER
        owner_type_urn = None

        if isinstance(owner, tuple):
            raw_owner, raw_owner_type = owner

            if isinstance(raw_owner_type, OwnershipTypeUrn):
                owner_type = models.OwnershipTypeClass.CUSTOM
                owner_type_urn = str(raw_owner_type)
            else:
                owner_type, owner_type_urn = validate_ownership_type(raw_owner_type)
        else:
            raw_owner = owner

        if isinstance(raw_owner, str):
            # Tricky: this will gracefully handle a user passing in a group urn as a string.
            return models.OwnerClass(
                owner=make_user_urn(raw_owner),
                type=owner_type,
                typeUrn=owner_type_urn,
            )
        elif isinstance(raw_owner, Urn):
            return models.OwnerClass(
                owner=str(raw_owner),
                type=owner_type,
                typeUrn=owner_type_urn,
            )
        else:
            raise SdkUsageError(
                f"Invalid owner {owner}: {type(owner)} is not a valid owner type"
            )

    @property
    def owners(self) -> Optional[List[models.OwnerClass]]:
        # TODO: Ideally we'd use first-class type urns here, not strings.
        if owners_aspect := self._get_aspect(models.OwnershipClass):
            return owners_aspect.owners
        return None

    @owners.setter
    def owners(self, owners: OwnersInputType) -> None:
        parsed_owners = [self._parse_owner_class(owner) for owner in owners]
        self._set_aspect(models.OwnershipClass(owners=parsed_owners))
