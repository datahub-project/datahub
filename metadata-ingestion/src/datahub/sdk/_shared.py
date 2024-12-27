import abc
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
    runtime_checkable,
)

from typing_extensions import Self, TypeAlias

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import (
    Aspect as AspectTypeVar,
    make_ts_millis,
    make_user_urn,
    parse_ts_millis,
    validate_ownership_type,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.errors import SdkUsageError
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, OwnershipTypeUrn, Urn

if TYPE_CHECKING:
    from datahub.sdk.container import Container

UrnOrStr: TypeAlias = Union[Urn, str]
ActorUrn: TypeAlias = Union[CorpUserUrn, CorpGroupUrn]


def make_time_stamp(ts: Optional[datetime]) -> Optional[models.TimeStampClass]:
    if ts is None:
        return None
    return models.TimeStampClass(time=make_ts_millis(ts))


def parse_time_stamp(ts: Optional[models.TimeStampClass]) -> Optional[datetime]:
    if ts is None:
        return None
    return parse_ts_millis(ts.time)


@runtime_checkable
class HasUrn(Protocol):
    __slots__ = ()

    @property
    def urn(self) -> Urn:
        ...


class Entity(HasUrn):
    __slots__ = ("_urn", "_prev_aspects", "_aspects")

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
    def _init_dummy_args(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        entity = cls(urn=urn, **cls._init_dummy_args())

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

    def _setdefault_aspect(self, default_aspect: AspectTypeVar, /) -> AspectTypeVar:
        # Similar semantics to dict.setdefault.
        if existing_aspect := self._get_aspect(type(default_aspect)):
            return existing_aspect
        self._set_aspect(default_aspect)
        return default_aspect


class HasSubtype(Entity):
    __slots__ = ()

    @property
    def subtype(self) -> Optional[str]:
        subtypes = self._get_aspect(models.SubTypesClass)
        if subtypes and subtypes.typeNames:
            # TODO: throw an error if there are multiple subtypes
            return subtypes.typeNames[0]
        # TODO: throw an error if there is no subtype? or default to None?
        return None

    def set_subtype(self, subtype: str) -> None:
        self._set_aspect(models.SubTypesClass(typeNames=[subtype]))


OwnershipTypeType: TypeAlias = Union[str, OwnershipTypeUrn]
OwnerInputType: TypeAlias = Union[
    str,
    ActorUrn,
    Tuple[Union[str, ActorUrn], OwnershipTypeType],
    models.OwnerClass,
]
OwnersInputType: TypeAlias = List[OwnerInputType]


class HasOwnership(Entity):
    __slots__ = ()

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

    # Due to https://github.com/python/mypy/issues/3004, we cannot use python setters directly.
    # Otherwise, we'll get a bunch of complaints about type annotations, since the getter
    # and setter would differ.
    def set_owners(self, owners: OwnersInputType) -> None:
        # TODO: add docs on the default parsing + default ownership type
        parsed_owners = [self._parse_owner_class(owner) for owner in owners]
        self._set_aspect(models.OwnershipClass(owners=parsed_owners))


ContainerInputType: TypeAlias = Union["Container", ContainerKey]


class HasContainer(Entity):
    __slots__ = ()

    def _set_container(self, container: Optional[ContainerInputType]) -> None:
        # We need to allow container to be None. It won't happen for datasets much, but
        # will be required for root containers.

        browse_path: List[Union[str, models.BrowsePathEntryClass]] = []
        if isinstance(container, Container):
            container_urn = container.urn.urn()

            parent_browse_path = container._get_aspect(models.BrowsePathsV2Class)
            if parent_browse_path is None:
                raise SdkUsageError(
                    "Parent container does not have a browse path, so cannot generate one for its children."
                )
            browse_path = [
                *parent_browse_path.path,
                models.BrowsePathEntryClass(
                    id=container_urn,
                    urn=container_urn,
                ),
            ]
        elif container is not None:
            container_urn = container.as_urn()

            browse_path_reversed = [container_urn]
            parent_key = container.parent_key()
            while parent_key is not None:
                browse_path_reversed.append(parent_key.as_urn())
                parent_key = parent_key.parent_key()
            browse_path = list(reversed(browse_path_reversed))
        else:
            container_urn = None
            browse_path = []

        if container_urn:
            self._set_aspect(models.ContainerClass(container=container_urn))

        self._set_aspect(
            models.BrowsePathsV2Class(
                path=[
                    entry
                    if isinstance(entry, models.BrowsePathEntryClass)
                    else models.BrowsePathEntryClass(
                        id=entry,
                        urn=entry,
                    )
                    for entry in browse_path
                ]
            )
        )
