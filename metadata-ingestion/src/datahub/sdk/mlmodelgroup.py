from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Sequence, Type, Union

from typing_extensions import Self

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.metadata.schema_classes import (
    AspectBag,
    MLModelGroupPropertiesClass,
)
from datahub.metadata.urns import DataProcessInstanceUrn, MlModelGroupUrn, Urn
from datahub.sdk._shared import (
    DomainInputType,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasPlatformInstance,
    HasTags,
    HasTerms,
    LinksInputType,
    OwnersInputType,
    TagsInputType,
    TermsInputType,
    make_time_stamp,
    parse_time_stamp,
)
from datahub.sdk.entity import Entity, ExtraAspectsType


class MLModelGroup(
    HasPlatformInstance,
    HasOwnership,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    Entity,
):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[MlModelGroupUrn]:
        return MlModelGroupUrn

    def __init__(
        self,
        id: str,
        platform: str,
        name: Optional[str] = "",
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        # Model group properties
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        # Standard aspects
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        training_jobs: Optional[Sequence[Union[str, DataProcessInstanceUrn]]] = None,
        downstream_jobs: Optional[Sequence[Union[str, DataProcessInstanceUrn]]] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = MlModelGroupUrn(platform=platform, name=id, env=env)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(urn.platform, platform_instance)

        # Set MLModelGroupProperties aspect
        self._ensure_model_group_props(name=display_name or name)

        if description is not None:
            self.set_description(description)
        if external_url is not None:
            self.set_external_url(external_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)

        # Standard aspects
        if owners is not None:
            self.set_owners(owners)
        if links is not None:
            self.set_links(links)
        if tags is not None:
            self.set_tags(tags)
        if terms is not None:
            self.set_terms(terms)
        if domain is not None:
            self.set_domain(domain)

        # ML model group specific aspects
        if training_jobs is not None:
            self.set_training_jobs(training_jobs)
        if downstream_jobs is not None:
            self.set_downstream_jobs(downstream_jobs)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: AspectBag) -> Self:
        assert isinstance(urn, MlModelGroupUrn)
        entity = cls(
            platform=urn.platform,
            id=urn.name,
            env=urn.env,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> MlModelGroupUrn:
        return self._urn  # type: ignore

    def _ensure_model_group_props(
        self, *, name: Optional[str] = None
    ) -> MLModelGroupPropertiesClass:
        if name is not None:
            return self._setdefault_aspect(MLModelGroupPropertiesClass(name=name))

        props = self._get_aspect(MLModelGroupPropertiesClass)
        if props is None:
            # If we need properties but they don't exist and no name was provided
            return self._setdefault_aspect(
                MLModelGroupPropertiesClass(name=self.urn.name)
            )
        return props

    @property
    def name(self) -> Optional[str]:
        return self._ensure_model_group_props().name

    def set_name(self, display_name: str) -> None:
        self._ensure_model_group_props().name = display_name

    @property
    def description(self) -> Optional[str]:
        return self._ensure_model_group_props().description

    def set_description(self, description: str) -> None:
        self._ensure_model_group_props().description = description

    @property
    def external_url(self) -> Optional[str]:
        return self._ensure_model_group_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        self._ensure_model_group_props().externalUrl = external_url

    @property
    def custom_properties(self) -> Optional[Dict[str, str]]:
        return self._ensure_model_group_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        self._ensure_model_group_props().customProperties = custom_properties

    @property
    def created(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_model_group_props().created)

    def set_created(self, created: datetime) -> None:
        self._ensure_model_group_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_model_group_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_model_group_props().lastModified = make_time_stamp(last_modified)

    @property
    def training_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_group_props().trainingJobs

    def set_training_jobs(
        self, training_jobs: Sequence[Union[str, DataProcessInstanceUrn]]
    ) -> None:
        self._ensure_model_group_props().trainingJobs = [
            str(job) for job in training_jobs
        ]

    def add_training_job(
        self, training_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_group_props()
        if props.trainingJobs is None:
            props.trainingJobs = []
        props.trainingJobs.append(str(training_job))

    def remove_training_job(
        self, training_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_group_props()
        if props.trainingJobs is not None:
            props.trainingJobs = [
                job for job in props.trainingJobs if job != str(training_job)
            ]

    @property
    def downstream_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_group_props().downstreamJobs

    def set_downstream_jobs(
        self, downstream_jobs: Sequence[Union[str, DataProcessInstanceUrn]]
    ) -> None:
        self._ensure_model_group_props().downstreamJobs = [
            str(job) for job in downstream_jobs
        ]

    def add_downstream_job(
        self, downstream_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_group_props()
        if props.downstreamJobs is None:
            props.downstreamJobs = []
        props.downstreamJobs.append(str(downstream_job))

    def remove_downstream_job(
        self, downstream_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_group_props()
        if props.downstreamJobs is not None:
            props.downstreamJobs = [
                job for job in props.downstreamJobs if job != str(downstream_job)
            ]
