from __future__ import annotations

import warnings
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from typing_extensions import TypeAlias, assert_never

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import (
    make_ts_millis,
    make_user_urn,
    parse_ts_millis,
    validate_ownership_type,
)
from datahub.emitter.mcp_builder import ContainerKey
from datahub.errors import MultipleSubtypesWarning, SdkUsageError
from datahub.metadata.urns import (
    ContainerUrn,
    CorpGroupUrn,
    CorpUserUrn,
    DataJobUrn,
    DataPlatformInstanceUrn,
    DataPlatformUrn,
    DataProcessInstanceUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    OwnershipTypeUrn,
    TagUrn,
    Urn,
    VersionSetUrn,
)
from datahub.sdk._utils import DEFAULT_ACTOR_URN, add_list_unique, remove_list_unique
from datahub.sdk.entity import Entity
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.sdk.container import Container

UrnOrStr: TypeAlias = Union[Urn, str]
DatasetUrnOrStr: TypeAlias = Union[str, DatasetUrn]
DatajobUrnOrStr: TypeAlias = Union[str, DataJobUrn]

ActorUrn: TypeAlias = Union[CorpUserUrn, CorpGroupUrn]

TrainingMetricsInputType: TypeAlias = Union[
    List[models.MLMetricClass], Dict[str, Optional[str]]
]
HyperParamsInputType: TypeAlias = Union[
    List[models.MLHyperParamClass], Dict[str, Optional[str]]
]
MLTrainingJobInputType: TypeAlias = Union[Sequence[Union[str, DataProcessInstanceUrn]]]


def convert_training_metrics(
    metrics: TrainingMetricsInputType,
) -> List[models.MLMetricClass]:
    if isinstance(metrics, dict):
        return [
            models.MLMetricClass(name=name, value=str(value))
            for name, value in metrics.items()
        ]
    return metrics


def convert_hyper_params(
    params: HyperParamsInputType,
) -> List[models.MLHyperParamClass]:
    if isinstance(params, dict):
        return [
            models.MLHyperParamClass(name=name, value=str(value))
            for name, value in params.items()
        ]
    return params


def make_time_stamp(ts: Optional[datetime]) -> Optional[models.TimeStampClass]:
    if ts is None:
        return None
    return models.TimeStampClass(time=make_ts_millis(ts))


def parse_time_stamp(ts: Optional[models.TimeStampClass]) -> Optional[datetime]:
    if ts is None:
        return None
    return parse_ts_millis(ts.time)


class HasPlatformInstance(Entity):
    __slots__ = ()

    def _set_platform_instance(
        self,
        platform: Union[str, DataPlatformUrn],
        instance: Union[None, str, DataPlatformInstanceUrn],
    ) -> None:
        platform = DataPlatformUrn(platform)
        if instance is not None:
            try:
                instance = DataPlatformInstanceUrn.from_string(instance)
            except InvalidUrnError:
                if not isinstance(
                    instance, DataPlatformInstanceUrn
                ):  # redundant check to make mypy happy
                    instance = DataPlatformInstanceUrn(platform, instance)
        # At this point, instance is either None or a DataPlatformInstanceUrn.

        self._set_aspect(
            models.DataPlatformInstanceClass(
                platform=platform.urn(),
                instance=instance.urn() if instance else None,
            )
        )

    @property
    def platform(self) -> Optional[DataPlatformUrn]:
        dataPlatform = self._get_aspect(models.DataPlatformInstanceClass)
        if dataPlatform and dataPlatform.platform:
            return DataPlatformUrn.from_string(dataPlatform.platform)
        return None

    @property
    def platform_instance(self) -> Optional[DataPlatformInstanceUrn]:
        dataPlatformInstance = self._get_aspect(models.DataPlatformInstanceClass)
        if dataPlatformInstance and dataPlatformInstance.instance:
            return DataPlatformInstanceUrn.from_string(dataPlatformInstance.instance)
        return None


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


# TODO: Reference OwnershipTypeClass as the valid ownership type enum.
OwnershipTypeType: TypeAlias = Union[str, OwnershipTypeUrn]
OwnerInputType: TypeAlias = Union[
    ActorUrn,
    Tuple[ActorUrn, OwnershipTypeType],
    models.OwnerClass,
]
OwnersInputType: TypeAlias = List[OwnerInputType]


class HasOwnership(Entity):
    __slots__ = ()

    @staticmethod
    def _parse_owner_class(owner: OwnerInputType) -> Tuple[models.OwnerClass, bool]:
        if isinstance(owner, models.OwnerClass):
            return owner, False

        was_type_specified = False
        owner_type = models.OwnershipTypeClass.TECHNICAL_OWNER
        owner_type_urn = None

        if isinstance(owner, tuple):
            raw_owner, raw_owner_type = owner
            was_type_specified = True

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
            ), was_type_specified
        elif isinstance(raw_owner, Urn):
            return models.OwnerClass(
                owner=str(raw_owner),
                type=owner_type,
                typeUrn=owner_type_urn,
            ), was_type_specified
        else:
            assert_never(raw_owner)

    # TODO: Return a custom type with deserialized urns, instead of the raw aspect.
    # Ideally we'd also use first-class ownership type urns here, not strings.
    @property
    def owners(self) -> Optional[List[models.OwnerClass]]:
        if owners_aspect := self._get_aspect(models.OwnershipClass):
            return owners_aspect.owners
        return None

    def set_owners(self, owners: OwnersInputType) -> None:
        # TODO: add docs on the default parsing + default ownership type
        parsed_owners = [self._parse_owner_class(owner)[0] for owner in owners]
        self._set_aspect(models.OwnershipClass(owners=parsed_owners))

    @classmethod
    def _owner_key_method(
        cls, consider_owner_type: bool
    ) -> Callable[[models.OwnerClass], Tuple[str, ...]]:
        if consider_owner_type:
            return cls._typed_owner_key
        else:
            return cls._simple_owner_key

    @classmethod
    def _typed_owner_key(cls, owner: models.OwnerClass) -> Tuple[str, str]:
        return (owner.owner, owner.typeUrn or str(owner.type))

    @classmethod
    def _simple_owner_key(cls, owner: models.OwnerClass) -> Tuple[str,]:
        return (owner.owner,)

    def _ensure_owners(self) -> List[models.OwnerClass]:
        owners = self._setdefault_aspect(models.OwnershipClass(owners=[])).owners
        return owners

    def add_owner(self, owner: OwnerInputType) -> None:
        # Tricky: when adding an owner, we always use the ownership type.
        # For removals, we only use it if it was explicitly specified.
        parsed_owner, _ = self._parse_owner_class(owner)
        add_list_unique(
            self._ensure_owners(),
            key=self._typed_owner_key,
            item=parsed_owner,
        )

    def remove_owner(self, owner: OwnerInputType) -> None:
        parsed_owner, was_type_specified = self._parse_owner_class(owner)
        remove_list_unique(
            self._ensure_owners(),
            key=self._owner_key_method(was_type_specified),
            item=parsed_owner,
        )


# If you pass in a container object, we can build on top of its browse path.
# If you pass in a ContainerKey, we can use parent_key() to build the browse path.
# If you pass in a list of urns, we'll use that as the browse path. Any non-urn strings
# will be treated as raw ids.
ParentContainerInputType: TypeAlias = Union["Container", ContainerKey, List[UrnOrStr]]


class HasContainer(Entity):
    __slots__ = ()

    @staticmethod
    def _maybe_parse_as_urn(urn: UrnOrStr) -> UrnOrStr:
        if isinstance(urn, Urn):
            return urn
        elif urn.startswith("urn:li:"):
            return Urn.from_string(urn)
        else:
            return urn

    def _set_container(self, container: Optional[ParentContainerInputType]) -> None:
        # We need to allow container to be None. It won't happen for datasets much, but
        # will be required for root containers.
        from datahub.sdk.container import Container

        container_urn: Optional[str]
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
        elif isinstance(container, list):
            parsed_path = [self._maybe_parse_as_urn(entry) for entry in container]

            # Use the last container in the path as the container urn.
            container_urns = [
                urn.urn() for urn in parsed_path if isinstance(urn, ContainerUrn)
            ]
            container_urn = container_urns[-1] if container_urns else None

            browse_path = [
                (
                    models.BrowsePathEntryClass(
                        id=str(entry),
                        urn=str(entry),
                    )
                    if isinstance(entry, Urn)
                    else models.BrowsePathEntryClass(
                        id=entry,
                        urn=None,
                    )
                )
                for entry in parsed_path
            ]
        elif container is not None:
            container_urn = container.as_urn()

            browse_path_reversed = [container_urn]
            parent_key = container.parent_key()
            while parent_key is not None:
                browse_path_reversed.append(parent_key.as_urn())
                parent_key = parent_key.parent_key()
            if container.instance is not None:
                browse_path_reversed.append(
                    DataPlatformInstanceUrn(
                        container.platform, container.instance
                    ).urn()
                )

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

    @property
    def parent_container(self) -> Optional[ContainerUrn]:
        if container := self._get_aspect(models.ContainerClass):
            return ContainerUrn.from_string(container.container)
        return None

    @property
    def browse_path(self) -> Optional[List[UrnOrStr]]:
        if browse_path := self._get_aspect(models.BrowsePathsV2Class):
            path: List[UrnOrStr] = []
            for entry in browse_path.path:
                if entry.urn:
                    path.append(Urn.from_string(entry.urn))
                else:
                    path.append(entry.id)
            return path
        return None


TagInputType: TypeAlias = Union[str, TagUrn, models.TagAssociationClass]
TagsInputType: TypeAlias = List[TagInputType]


class HasTags(Entity):
    __slots__ = ()

    def _ensure_tags(self) -> List[models.TagAssociationClass]:
        return self._setdefault_aspect(models.GlobalTagsClass(tags=[])).tags

    # TODO: Return a custom type with deserialized urns, instead of the raw aspect.
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
    def _tag_key(cls, tag: models.TagAssociationClass) -> str:
        return tag.tag

    def add_tag(self, tag: TagInputType) -> None:
        add_list_unique(
            self._ensure_tags(),
            self._tag_key,
            self._parse_tag_association_class(tag),
        )

    def remove_tag(self, tag: TagInputType) -> None:
        remove_list_unique(
            self._ensure_tags(),
            self._tag_key,
            self._parse_tag_association_class(tag),
        )


TermInputType: TypeAlias = Union[
    str, GlossaryTermUrn, models.GlossaryTermAssociationClass
]
TermsInputType: TypeAlias = List[TermInputType]


class HasTerms(Entity):
    __slots__ = ()

    def _ensure_terms(self) -> List[models.GlossaryTermAssociationClass]:
        return self._setdefault_aspect(
            models.GlossaryTermsClass(terms=[], auditStamp=self._terms_audit_stamp())
        ).terms

    # TODO: Return a custom type with deserialized urns, instead of the raw aspect.
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
            actor=DEFAULT_ACTOR_URN,
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

    @classmethod
    def _terms_key(self, term: models.GlossaryTermAssociationClass) -> str:
        return term.urn

    def add_term(self, term: TermInputType) -> None:
        add_list_unique(
            self._ensure_terms(),
            self._terms_key,
            self._parse_glossary_term_association_class(term),
        )

    def remove_term(self, term: TermInputType) -> None:
        remove_list_unique(
            self._ensure_terms(),
            self._terms_key,
            self._parse_glossary_term_association_class(term),
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


LinkInputType: TypeAlias = Union[
    str,
    Tuple[str, str],  # url, description
    models.InstitutionalMemoryMetadataClass,
]
LinksInputType: TypeAlias = Sequence[LinkInputType]


class HasInstitutionalMemory(Entity):
    __slots__ = ()

    # Internally the aspect is called institutionalMemory, and so much of the code
    # uses that name. However, the public-facing API is called "links", since
    # that's what we call these in the UI.

    def _ensure_institutional_memory(
        self,
    ) -> List[models.InstitutionalMemoryMetadataClass]:
        return self._setdefault_aspect(
            models.InstitutionalMemoryClass(elements=[])
        ).elements

    @property
    def links(self) -> Optional[List[models.InstitutionalMemoryMetadataClass]]:
        if institutional_memory := self._get_aspect(models.InstitutionalMemoryClass):
            return institutional_memory.elements
        return None

    @classmethod
    def _institutional_memory_audit_stamp(self) -> models.AuditStampClass:
        return models.AuditStampClass(
            time=0,
            actor=DEFAULT_ACTOR_URN,
        )

    @classmethod
    def _parse_link_association_class(
        cls, link: LinkInputType
    ) -> models.InstitutionalMemoryMetadataClass:
        if isinstance(link, models.InstitutionalMemoryMetadataClass):
            return link
        elif isinstance(link, str):
            return models.InstitutionalMemoryMetadataClass(
                url=link,
                description=link,
                createStamp=cls._institutional_memory_audit_stamp(),
            )
        elif isinstance(link, tuple) and len(link) == 2:
            url, description = link
            return models.InstitutionalMemoryMetadataClass(
                url=url,
                description=description,
                createStamp=cls._institutional_memory_audit_stamp(),
            )
        else:
            assert_never(link)

    def set_links(self, links: LinksInputType) -> None:
        self._set_aspect(
            models.InstitutionalMemoryClass(
                elements=[self._parse_link_association_class(link) for link in links]
            )
        )

    @classmethod
    def _link_key(self, link: models.InstitutionalMemoryMetadataClass) -> str:
        return link.url

    def add_link(self, link: LinkInputType) -> None:
        add_list_unique(
            self._ensure_institutional_memory(),
            self._link_key,
            self._parse_link_association_class(link),
        )

    def remove_link(self, link: LinkInputType) -> None:
        remove_list_unique(
            self._ensure_institutional_memory(),
            self._link_key,
            self._parse_link_association_class(link),
        )


class HasVersion(Entity):
    """Mixin for entities that have version properties."""

    def _get_version_props(self) -> Optional[models.VersionPropertiesClass]:
        return self._get_aspect(models.VersionPropertiesClass)

    def _ensure_version_props(self) -> models.VersionPropertiesClass:
        version_props = self._get_version_props()
        if version_props is None:
            guid_dict = {"urn": str(self.urn)}
            version_set_urn = VersionSetUrn(
                id=builder.datahub_guid(guid_dict), entity_type=self.urn.ENTITY_TYPE
            )

            version_props = models.VersionPropertiesClass(
                versionSet=str(version_set_urn),
                version=models.VersionTagClass(versionTag="0.1.0"),
                sortId="0000000.1.0",
            )
            self._set_aspect(version_props)
        return version_props

    @property
    def version(self) -> Optional[str]:
        version_props = self._get_version_props()
        if version_props and version_props.version:
            return version_props.version.versionTag
        return None

    def set_version(self, version: str) -> None:
        """Set the version of the entity."""
        guid_dict = {"urn": str(self.urn)}
        version_set_urn = VersionSetUrn(
            id=builder.datahub_guid(guid_dict), entity_type=self.urn.ENTITY_TYPE
        )

        version_props = self._get_version_props()
        if version_props is None:
            # If no version properties exist, create a new one
            version_props = models.VersionPropertiesClass(
                version=models.VersionTagClass(versionTag=version),
                versionSet=str(version_set_urn),
                sortId=version.zfill(10),  # Pad with zeros for sorting
            )
        else:
            # Update existing version properties
            version_props.version = models.VersionTagClass(versionTag=version)
            version_props.versionSet = str(version_set_urn)
            version_props.sortId = version.zfill(10)

        self._set_aspect(version_props)

    @property
    def version_aliases(self) -> List[str]:
        version_props = self._get_version_props()
        if version_props and version_props.aliases:
            return [
                alias.versionTag
                for alias in version_props.aliases
                if alias.versionTag is not None
            ]
        return []  # Return empty list instead of None

    def set_version_aliases(self, aliases: List[str]) -> None:
        version_props = self._get_aspect(models.VersionPropertiesClass)
        if version_props:
            version_props.aliases = [
                models.VersionTagClass(versionTag=alias) for alias in aliases
            ]
        else:
            # If no version properties exist, we need to create one with a default version
            guid_dict = {"urn": str(self.urn)}
            version_set_urn = VersionSetUrn(
                id=builder.datahub_guid(guid_dict), entity_type=self.urn.ENTITY_TYPE
            )
            self._set_aspect(
                models.VersionPropertiesClass(
                    version=models.VersionTagClass(
                        versionTag="0.1.0"
                    ),  # Default version
                    versionSet=str(version_set_urn),
                    sortId="0000000.1.0",
                    aliases=[
                        models.VersionTagClass(versionTag=alias) for alias in aliases
                    ],
                )
            )

    def add_version_alias(self, alias: str) -> None:
        if not alias:
            raise ValueError("Alias cannot be empty")
        version_props = self._ensure_version_props()
        if version_props.aliases is None:
            version_props.aliases = []
        version_props.aliases.append(models.VersionTagClass(versionTag=alias))
        self._set_aspect(version_props)

    def remove_version_alias(self, alias: str) -> None:
        version_props = self._get_version_props()
        if version_props and version_props.aliases:
            version_props.aliases = [
                a for a in version_props.aliases if a.versionTag != alias
            ]
            self._set_aspect(version_props)
