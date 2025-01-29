import warnings
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Callable,
    Hashable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from typing_extensions import TypeAlias

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import (
    make_ts_millis,
    make_user_urn,
    parse_ts_millis,
    validate_ownership_type,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.errors import ItemNotFoundError, MultipleSubtypesWarning, SdkUsageError
from datahub.metadata.urns import (
    CorpGroupUrn,
    CorpUserUrn,
    DataJobUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    OwnershipTypeUrn,
    TagUrn,
    Urn,
)
from datahub.sdk._entity import Entity

if TYPE_CHECKING:
    from datahub.sdk.container import Container

UrnOrStr: TypeAlias = Union[Urn, str]
DatasetUrnOrStr: TypeAlias = Union[str, DatasetUrn]
DatajobUrnOrStr: TypeAlias = Union[str, DataJobUrn]

ActorUrn: TypeAlias = Union[CorpUserUrn, CorpGroupUrn]


def make_time_stamp(ts: Optional[datetime]) -> Optional[models.TimeStampClass]:
    if ts is None:
        return None
    return models.TimeStampClass(time=make_ts_millis(ts))


def parse_time_stamp(ts: Optional[models.TimeStampClass]) -> Optional[datetime]:
    if ts is None:
        return None
    return parse_ts_millis(ts.time)


class HasSubtype(Entity):
    __slots__ = ()

    @property
    def subtype(self) -> Optional[str]:
        subtypes = self._get_aspect(models.SubTypesClass)
        if subtypes and subtypes.typeNames:
            if len(subtypes.typeNames) > 1:
                warnings.warn(
                    f"The entity {self.urn} has multiple subtypes: {subtypes.typeNames}. "
                    "Only the first subtype will be considered.",
                    MultipleSubtypesWarning,
                    stacklevel=2,
                )
            return subtypes.typeNames[0]
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


T = TypeVar("T")
K = TypeVar("K", bound=Hashable)


def _add_list_unique(lst: List[T], key: Callable[[T], K], item: T) -> None:
    for i, existing in enumerate(lst):
        if key(existing) == key(item):
            lst[i] = item
            return
    lst.append(item)


def _remove_list_unique(lst: List[T], key: Callable[[T], K], item: T) -> None:
    # Poor man's patch implementation.
    # TODO: We require K to be hashable, but we actually need it to be comparable.
    item_key = key(item)
    removed = False
    for i, existing in enumerate(lst):
        if key(existing) == item_key:
            lst.pop(i)
            removed = True
            # Tricky: no break. In case there's already duplicates, we want to remove all of them.
    if not removed:
        raise ItemNotFoundError(f"Item {item} not found in list")


class HasOwnership(Entity):
    __slots__ = ()

    @staticmethod
    def _parse_owner_class(owner: OwnerInputType) -> models.OwnerClass:
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
            # TODO: is this the right behavior? or should we require a valid urn here?
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

    def _ensure_owners(self) -> List[models.OwnerClass]:
        owners = self._setdefault_aspect(models.OwnershipClass(owners=[])).owners
        return owners

    @property
    def owners(self) -> Optional[List[models.OwnerClass]]:
        # TODO: Ideally we'd use first-class ownership type urns here, not strings.
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

    @classmethod
    def _typed_owner_key(cls, owner: models.OwnerClass) -> Hashable:
        return (owner.owner, owner.typeUrn or owner.type)

    @classmethod
    def _simple_owner_key(cls, owner: models.OwnerClass) -> Hashable:
        return owner.owner

    def add_owner(self, owner: OwnerInputType) -> None:
        # TODO: document that if the owner is already present, ignore it.
        parsed_owner = self._parse_owner_class(owner)
        _add_list_unique(self._ensure_owners(), self._typed_owner_key, parsed_owner)

    def remove_owner(
        self,
        owner: Union[ActorUrn, str],
        owner_type: Optional[OwnershipTypeType] = None,
    ) -> None:
        # TODO: I'm not super happy with the interface here - ideally it'd be consistent
        # with OwnersInputType.
        if owner_type is None:
            parsed_owner = self._parse_owner_class(owner)
            key = self._simple_owner_key
        else:
            parsed_owner = self._parse_owner_class((owner, owner_type))
            key = self._typed_owner_key

        _remove_list_unique(self._ensure_owners(), key, parsed_owner)


ContainerInputType: TypeAlias = Union["Container", ContainerKey]


class HasContainer(Entity):
    __slots__ = ()

    def _set_container(self, container: Optional[ContainerInputType]) -> None:
        # We need to allow container to be None. It won't happen for datasets much, but
        # will be required for root containers.
        from datahub.sdk.container import Container

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
                    (
                        entry
                        if isinstance(entry, models.BrowsePathEntryClass)
                        else models.BrowsePathEntryClass(
                            id=entry,
                            urn=entry,
                        )
                    )
                    for entry in browse_path
                ]
            )
        )


TagInputType: TypeAlias = Union[str, TagUrn, models.TagAssociationClass]
TagsInputType: TypeAlias = List[TagInputType]


class HasTags(Entity):
    __slots__ = ()

    def _ensure_tags(self) -> List[models.TagAssociationClass]:
        tags = self._setdefault_aspect(models.GlobalTagsClass(tags=[])).tags
        return tags

    @property
    def tags(self) -> Optional[List[models.TagAssociationClass]]:
        if tags := self._get_aspect(models.GlobalTagsClass):
            return tags.tags
        return None

    @classmethod
    def _parse_tag_association_class(
        cls, tag: TagInputType
    ) -> models.TagAssociationClass:
        if isinstance(tag, models.TagAssociationClass):
            return tag
        elif isinstance(tag, str):
            assert TagUrn.from_string(tag)
        return models.TagAssociationClass(tag=str(tag))

    def set_tags(self, tags: TagsInputType) -> None:
        self._set_aspect(
            models.GlobalTagsClass(
                tags=[self._parse_tag_association_class(tag) for tag in tags]
            )
        )

    @classmethod
    def _tag_key(cls, tag: models.TagAssociationClass) -> Hashable:
        return (tag.tag, tag.attribution)

    def add_tag(self, tag: TagInputType) -> None:
        # TODO: mention that this is an idempotent operation e.g. will be a no-op if the tag is already present
        _add_list_unique(
            self._ensure_tags(), self._tag_key, self._parse_tag_association_class(tag)
        )

    def remove_tag(self, tag: TagInputType) -> None:
        # TODO: Similar to owners - it's unclear whether or not we should respect attribution.
        _remove_list_unique(
            self._ensure_tags(), self._tag_key, self._parse_tag_association_class(tag)
        )


TermInputType: TypeAlias = Union[
    str, GlossaryTermUrn, models.GlossaryTermAssociationClass
]
TermsInputType: TypeAlias = List[TermInputType]


class HasTerms(Entity):
    __slots__ = ()

    @property
    def terms(self) -> Optional[List[models.GlossaryTermAssociationClass]]:
        if glossary_terms := self._get_aspect(models.GlossaryTermsClass):
            return glossary_terms.terms
        return None

    @classmethod
    def _parse_glossary_term_association_class(
        cls, term: TermInputType
    ) -> models.GlossaryTermAssociationClass:
        if isinstance(term, models.GlossaryTermAssociationClass):
            return term
        elif isinstance(term, str):
            assert GlossaryTermUrn.from_string(term)
        return models.GlossaryTermAssociationClass(urn=str(term))

    @classmethod
    def _terms_audit_stamp(self) -> models.AuditStampClass:
        return models.AuditStampClass(
            time=0,
            # TODO figure out what to put here
            actor=CorpUserUrn("__ingestion").urn(),
        )

    def set_terms(self, terms: TermsInputType) -> None:
        self._set_aspect(
            models.GlossaryTermsClass(
                terms=[
                    self._parse_glossary_term_association_class(term) for term in terms
                ],
                auditStamp=self._terms_audit_stamp(),
            )
        )


DomainInputType: TypeAlias = Union[str, DomainUrn]


class HasDomain(Entity):
    __slots__ = ()

    @property
    def domain(self) -> Optional[DomainUrn]:
        if domains := self._get_aspect(models.DomainsClass):
            if len(domains.domains) > 1:
                raise SdkUsageError(
                    f"The entity has multiple domains set, but only one is supported: {domains.domains}"
                )
            elif domains.domains:
                domain_str = domains.domains[0]
                return DomainUrn.from_string(domain_str)

        return None

    def set_domain(self, domain: DomainInputType) -> None:
        domain_urn = DomainUrn.from_string(domain)  # basically a type assertion
        self._set_aspect(models.DomainsClass(domains=[str(domain_urn)]))
