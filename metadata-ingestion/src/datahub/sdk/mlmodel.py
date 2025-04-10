from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from typing_extensions import Self

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.metadata.schema_classes import (
    AspectBag,
    MLHyperParamClass,
    MLMetricClass,
    MLModelPropertiesClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import (
    DataProcessInstanceUrn,
    MlModelGroupUrn,
    MlModelUrn,
    Urn,
)
from datahub.sdk._shared import (
    DomainInputType,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasPlatformInstance,
    HasTags,
    HasTerms,
    HasVersion,
    HyperParamsInputType,
    LinksInputType,
    OwnersInputType,
    TagsInputType,
    TermsInputType,
    TrainingMetricsInputType,
    convert_hyper_params,
    convert_training_metrics,
    make_time_stamp,
    parse_time_stamp,
)
from datahub.sdk.entity import Entity, ExtraAspectsType


class MLModel(
    HasPlatformInstance,
    HasOwnership,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    HasVersion,
    Entity,
):
    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[MlModelUrn]:
        return MlModelUrn

    def __init__(
        self,
        id: str,
        platform: str,
        version: Optional[str] = None,
        aliases: Optional[List[str]] = None,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        name: Optional[str] = None,
        description: Optional[str] = None,
        training_metrics: Optional[TrainingMetricsInputType] = None,
        hyper_params: Optional[HyperParamsInputType] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        group: Optional[str] = None,
        training_jobs: Optional[List[DataProcessInstanceUrn]] = None,
        downstream_jobs: Optional[List[DataProcessInstanceUrn]] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = MlModelUrn(platform=platform, name=id, env=env)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(urn.platform, platform_instance)

        self._ensure_model_props()

        if version is not None:
            self.set_version(version)
        if name is not None:
            self.set_name(name)
        if aliases is not None:
            self.set_aliases(aliases)
        if description is not None:
            self.set_description(description)
        if training_metrics is not None:
            self.set_training_metrics(training_metrics)
        if hyper_params is not None:
            self.set_hyper_params(hyper_params)
        if external_url is not None:
            self.set_external_url(external_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)

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

        if group is not None:
            self.set_group(group)
        if training_jobs is not None:
            self.set_training_jobs(training_jobs)
        if downstream_jobs is not None:
            self.set_downstream_jobs(downstream_jobs)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: AspectBag) -> Self:
        assert isinstance(urn, MlModelUrn)
        entity = cls(
            id=urn.name,
            platform=urn.platform,
            env=urn.env,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> MlModelUrn:
        return self._urn  # type: ignore

    def _ensure_model_props(
        self,
    ) -> MLModelPropertiesClass:
        return self._setdefault_aspect(MLModelPropertiesClass())

    @property
    def name(self) -> Optional[str]:
        return self._ensure_model_props().name

    def set_name(self, name: str) -> None:
        self._ensure_model_props().name = name

    @property
    def description(self) -> Optional[str]:
        return self._ensure_model_props().description

    def set_description(self, description: str) -> None:
        self._ensure_model_props().description = description

    @property
    def external_url(self) -> Optional[str]:
        return self._ensure_model_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        self._ensure_model_props().externalUrl = external_url

    @property
    def custom_properties(self) -> Optional[Dict[str, str]]:
        return self._ensure_model_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        self._ensure_model_props().customProperties = custom_properties

    @property
    def created(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_model_props().created)

    def set_created(self, created: datetime) -> None:
        self._ensure_model_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        return parse_time_stamp(self._ensure_model_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_model_props().lastModified = make_time_stamp(last_modified)

    @property
    def training_metrics(self) -> Optional[TrainingMetricsInputType]:
        return self._ensure_model_props().trainingMetrics

    def set_training_metrics(self, metrics: TrainingMetricsInputType) -> None:
        self._ensure_model_props().trainingMetrics = convert_training_metrics(metrics)

    def add_training_metric(self, name: str, value: Any) -> None:
        props = self._ensure_model_props()
        if props.trainingMetrics is None:
            props.trainingMetrics = []
        props.trainingMetrics.append(MLMetricClass(name=name, value=str(value)))

    @property
    def hyper_params(self) -> Optional[HyperParamsInputType]:
        return self._ensure_model_props().hyperParams

    def set_hyper_params(self, params: HyperParamsInputType) -> None:
        self._ensure_model_props().hyperParams = convert_hyper_params(params)

    def add_hyper_param(self, name: str, value: Any) -> None:
        props = self._ensure_model_props()
        if props.hyperParams is None:
            props.hyperParams = []
        props.hyperParams.append(MLHyperParamClass(name=name, value=str(value)))

    def set_aliases(self, aliases: Union[str, List[str]]) -> None:
        """Set aliases for the ML model."""
        # Convert string to list if needed
        aliases_list = [aliases] if isinstance(aliases, str) else aliases

        version_props = self._get_aspect(VersionPropertiesClass)
        if version_props:
            version_props.aliases = [
                VersionTagClass(versionTag=alias) for alias in aliases_list
            ]
        else:
            # If no version properties exist, we need to create one with a default version
            version_set_urn = f"mlmodel_{self.urn.name}_versions"
            self._set_aspect(
                VersionPropertiesClass(
                    version=VersionTagClass(versionTag="0.1.0"),  # Default version
                    versionSet=version_set_urn,
                    sortId="0000000.1.0",
                    aliases=[
                        VersionTagClass(versionTag=alias) for alias in aliases_list
                    ],
                )
            )

    @property
    def groups(self) -> Optional[List[str]]:
        return self._ensure_model_props().groups

    def set_group(self, group: str) -> None:
        self._ensure_model_props().groups = [group]

    def add_group(self, group_urn: MlModelGroupUrn) -> None:
        props = self._ensure_model_props()
        if props.groups is None:
            props.groups = []
        props.groups.append(str(group_urn))

    def remove_group(self, group_urn: MlModelGroupUrn) -> None:
        props = self._ensure_model_props()
        if props.groups is not None:
            props.groups = [group for group in props.groups if group != str(group_urn)]

    @property
    def training_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_props().trainingJobs

    def set_training_jobs(self, training_jobs: List[DataProcessInstanceUrn]) -> None:
        self._ensure_model_props().trainingJobs = [str(job) for job in training_jobs]

    def add_training_job(self, training_job: DataProcessInstanceUrn) -> None:
        props = self._ensure_model_props()
        if props.trainingJobs is None:
            props.trainingJobs = []
        props.trainingJobs.append(str(training_job))

    def remove_training_job(self, training_job: DataProcessInstanceUrn) -> None:
        props = self._ensure_model_props()
        if props.trainingJobs is not None:
            props.trainingJobs = [
                job for job in props.trainingJobs if job != str(training_job)
            ]

    @property
    def downstream_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_props().downstreamJobs

    def set_downstream_jobs(
        self, downstream_jobs: List[DataProcessInstanceUrn]
    ) -> None:
        self._ensure_model_props().downstreamJobs = [
            str(job) for job in downstream_jobs
        ]

    def add_downstream_job(self, downstream_job: DataProcessInstanceUrn) -> None:
        props = self._ensure_model_props()
        if props.downstreamJobs is None:
            props.downstreamJobs = []
        props.downstreamJobs.append(str(downstream_job))

    def remove_downstream_job(self, downstream_job: DataProcessInstanceUrn) -> None:
        props = self._ensure_model_props()
        if props.downstreamJobs is not None:
            props.downstreamJobs = [
                job for job in props.downstreamJobs if job != str(downstream_job)
            ]
