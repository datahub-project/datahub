from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Sequence, Type, Union

from typing_extensions import Self

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.metadata.schema_classes import (
    AspectBag,
    MLHyperParamClass,
    MLMetricClass,
    MLModelPropertiesClass,
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
    MLTrainingJobInputType,
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
        model_group: Optional[Union[str, MlModelGroupUrn]] = None,
        training_jobs: Optional[MLTrainingJobInputType] = None,
        downstream_jobs: Optional[MLTrainingJobInputType] = None,
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
            self.set_version_aliases(aliases)
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
        if model_group is not None:
            self.set_model_group(model_group)
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
    def training_metrics(self) -> Optional[List[MLMetricClass]]:
        return self._ensure_model_props().trainingMetrics

    def set_training_metrics(self, metrics: TrainingMetricsInputType) -> None:
        self._ensure_model_props().trainingMetrics = convert_training_metrics(metrics)

    def add_training_metrics(self, metrics: TrainingMetricsInputType) -> None:
        props = self._ensure_model_props()
        if props.trainingMetrics is None:
            props.trainingMetrics = []
        if isinstance(metrics, list):
            props.trainingMetrics.extend(
                [
                    MLMetricClass(name=metric.name, value=metric.value)
                    for metric in metrics
                ]
            )
        else:
            # For dictionary case, use the key as name and value as value
            for name, value in metrics.items():
                props.trainingMetrics.append(MLMetricClass(name=name, value=value))

    @property
    def hyper_params(self) -> Optional[List[MLHyperParamClass]]:
        return self._ensure_model_props().hyperParams

    def set_hyper_params(self, params: HyperParamsInputType) -> None:
        self._ensure_model_props().hyperParams = convert_hyper_params(params)

    def add_hyper_params(self, params: HyperParamsInputType) -> None:
        props = self._ensure_model_props()
        if props.hyperParams is None:
            props.hyperParams = []
        if isinstance(params, list):
            props.hyperParams.extend(
                [
                    MLHyperParamClass(name=param.name, value=param.value)
                    for param in params
                ]
            )
        else:
            # For dictionary case, iterate through key-value pairs
            for name, value in params.items():
                props.hyperParams.append(MLHyperParamClass(name=name, value=value))

    @property
    def model_group(self) -> Optional[str]:
        props = self._ensure_model_props()
        groups = props.groups
        if groups is None or len(groups) == 0:
            return None
        return groups[0]

    def set_model_group(self, group: Union[str, MlModelGroupUrn]) -> None:
        self._ensure_model_props().groups = [str(group)]

    @property
    def training_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_props().trainingJobs

    def set_training_jobs(self, training_jobs: MLTrainingJobInputType) -> None:
        self._ensure_model_props().trainingJobs = [str(job) for job in training_jobs]

    def add_training_job(
        self, training_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_props()
        if props.trainingJobs is None:
            props.trainingJobs = []
        props.trainingJobs.append(str(training_job))

    def remove_training_job(
        self, training_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_props()
        if props.trainingJobs is not None:
            job_str = str(training_job)
            props.trainingJobs = [job for job in props.trainingJobs if job != job_str]

    @property
    def downstream_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_props().downstreamJobs

    def set_downstream_jobs(
        self, downstream_jobs: Sequence[Union[str, DataProcessInstanceUrn]]
    ) -> None:
        self._ensure_model_props().downstreamJobs = [
            str(job) for job in downstream_jobs
        ]

    def add_downstream_job(
        self, downstream_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_props()
        if props.downstreamJobs is None:
            props.downstreamJobs = []
        props.downstreamJobs.append(str(downstream_job))

    def remove_downstream_job(
        self, downstream_job: Union[str, DataProcessInstanceUrn]
    ) -> None:
        props = self._ensure_model_props()
        if props.downstreamJobs is not None:
            job_str = str(downstream_job)
            props.downstreamJobs = [
                job for job in props.downstreamJobs if job != job_str
            ]
