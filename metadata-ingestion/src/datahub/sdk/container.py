from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional, Type

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import ALL_ENV_TYPES
from datahub.emitter.mcp_builder import (
    _INCLUDE_ENV_IN_CONTAINER_PROPERTIES,
    ContainerKey,
)
from datahub.errors import SdkUsageError
from datahub.metadata.urns import (
    ContainerUrn,
    Urn,
)
from datahub.sdk._entity import Entity
from datahub.sdk._shared import (
    DomainInputType,
    HasContainer,
    HasDomain,
    HasOwnership,
    HasPlatformInstance,
    HasSubtype,
    HasTags,
    HasTerms,
    OwnersInputType,
    ParentContainerInputType,
    TagsInputType,
    TermsInputType,
    make_time_stamp,
    parse_time_stamp,
)
from datahub.utilities.sentinels import Auto, auto


class Container(
    HasPlatformInstance,
    HasSubtype,
    HasContainer,
    HasOwnership,
    HasTags,
    HasTerms,
    HasDomain,
    Entity,
):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[ContainerUrn]:
        return ContainerUrn

    def __init__(
        self,
        /,
        # Identity.
        container_key: ContainerKey,
        *,
        # Container attributes.
        display_name: str,
        qualified_name: Optional[str] = None,
        description: Optional[str] = None,
        external_url: Optional[str] = None,
        # TODO: call this custom properties?
        extra_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        # Standard aspects.
        parent_container: Auto | ParentContainerInputType | None = auto,
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
    ):
        # Hack: while the type annotations say container_key is always a ContainerKey,
        # we allow ContainerUrn to make the graph-based constructor work.
        if isinstance(container_key, ContainerUrn):
            urn = container_key
        else:
            urn = ContainerUrn.from_string(container_key.as_urn())
        super().__init__(urn)

        # This needs to come first to ensure that the display name is registered.
        self._ensure_container_props(name=display_name)

        # TODO: Normal usages should require container key. Only the graph init method can accept an urn.
        if isinstance(container_key, ContainerKey):
            self._set_platform_instance(container_key.platform, container_key.instance)

            self.set_custom_properties(
                {
                    **container_key.property_dict(),
                    **(extra_properties or {}),
                }
            )

            # Extra validation on the env field.
            # In certain cases (mainly for backwards compatibility), the env field will actually
            # have a platform instance name.
            env = container_key.env if container_key.env in ALL_ENV_TYPES else None
            if _INCLUDE_ENV_IN_CONTAINER_PROPERTIES and env is not None:
                self._ensure_container_props().env = env
        else:
            self.set_custom_properties(extra_properties or {})

        if parent_container is auto:
            if not isinstance(container_key, ContainerKey):
                raise SdkUsageError(
                    "Either a container_key or parent_container must be provided"
                )

            self._set_container(container_key.parent_key())
        else:
            self._set_container(parent_container)

        if description is not None:
            self.set_description(description)
        if external_url is not None:
            self.set_external_url(external_url)
        if qualified_name is not None:
            self.set_qualified_name(qualified_name)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)

        if subtype is not None:
            self.set_subtype(subtype)
        if owners is not None:
            self.set_owners(owners)
        if tags is not None:
            self.set_tags(tags)
        if terms is not None:
            self.set_terms(terms)
        if domain is not None:
            self.set_domain(domain)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, ContainerUrn)

        entity = cls(urn, display_name="__dummy_value__", parent_container=None)  # type: ignore[arg-type]
        return entity._init_from_graph(current_aspects)

    def _ensure_container_props(
        self, *, name: Optional[str] = None
    ) -> models.ContainerPropertiesClass:
        # TODO: Not super happy with this method's implementation, but it's
        # internal-only and enforces the constraints that we need.
        if name is not None:
            return self._setdefault_aspect(models.ContainerPropertiesClass(name=name))

        props = self._get_aspect(models.ContainerPropertiesClass)
        if props is None:
            raise SdkUsageError("Containers must have a name.")
        return props

    @property
    def display_name(self) -> str:
        return self._ensure_container_props().name

    def set_display_name(self, value: str) -> None:
        self._ensure_container_props(name=value).name = value

    @property
    def description(self) -> Optional[str]:
        return self._ensure_container_props().description

    def set_description(self, description: str) -> None:
        self._ensure_container_props().description = description

    @property
    def custom_properties(self) -> Optional[Dict[str, str]]:
        return self._ensure_container_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        # TODO: How do we ensure that the container key props are always retained?
        self._ensure_container_props().customProperties = custom_properties

    @property
    def external_url(self) -> Optional[str]:
        return self._ensure_container_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        self._ensure_container_props().externalUrl = external_url

    @property
    def qualified_name(self) -> Optional[str]:
        return self._ensure_container_props().qualifiedName

    def set_qualified_name(self, qualified_name: str) -> None:
        self._ensure_container_props().qualifiedName = qualified_name

    @property
    def created(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_container_props().created)

    def set_created(self, created: datetime) -> None:
        self._ensure_container_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_container_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_container_props().lastModified = make_time_stamp(last_modified)
