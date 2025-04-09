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
from datahub.metadata.urns import MlModelUrn, Urn, VersionSetUrn
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


class MLModel(
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
        display_name: Optional[str] = None,
        training_metrics: Optional[Union[List[MLMetricClass], Dict[str, Any]]] = None,
        hyper_params: Optional[Union[List[MLHyperParamClass], Dict[str, Any]]] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        extra_aspects: ExtraAspectsType = None,
        group: Optional[str] = None,
        training_jobs: Optional[List[str]] = None,
    ):
        urn = MlModelUrn(platform=platform, name=id, env=env)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(urn.platform, platform_instance)

        self._ensure_model_props(name=display_name or name or id)

        if version is not None:
            version_set_urn = VersionSetUrn(
                id=f"mlmodel_{id}_versions", entity_type="mlModel"
            )
            self._set_aspect(
                VersionPropertiesClass(
                    version=VersionTagClass(versionTag=version),
                    versionSet=str(version_set_urn),
                    sortId=str(version).zfill(10),
                )
            )
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
        self, *, name: Optional[str] = None
    ) -> MLModelPropertiesClass:
        if name is not None:
            return self._setdefault_aspect(MLModelPropertiesClass(name=name))

        props = self._get_aspect(MLModelPropertiesClass)
        if props is None:
            return self._setdefault_aspect(MLModelPropertiesClass(name=self.urn.name))
        return props

    def _convert_training_metrics(
        self, metrics: Union[List[MLMetricClass], Dict[str, Any]]
    ) -> List[MLMetricClass]:
        if isinstance(metrics, list):
            return metrics
        return [
            MLMetricClass(name=name, value=str(value))
            for name, value in metrics.items()
        ]

    def _convert_hyper_params(
        self, params: Union[List[MLHyperParamClass], Dict[str, Any]]
    ) -> List[MLHyperParamClass]:
        if isinstance(params, list):
            return params
        return [
            MLHyperParamClass(name=name, value=str(value))
            for name, value in params.items()
        ]

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

    def set_training_metrics(
        self, metrics: Union[List[MLMetricClass], Dict[str, Any]]
    ) -> None:
        self._ensure_model_props().trainingMetrics = self._convert_training_metrics(
            metrics
        )

    def add_training_metric(self, name: str, value: Any) -> None:
        props = self._ensure_model_props()
        if props.trainingMetrics is None:
            props.trainingMetrics = []
        props.trainingMetrics.append(MLMetricClass(name=name, value=str(value)))

    @property
    def hyper_params(self) -> Optional[List[MLHyperParamClass]]:
        return self._ensure_model_props().hyperParams

    def set_hyper_params(
        self, params: Union[List[MLHyperParamClass], Dict[str, Any]]
    ) -> None:
        self._ensure_model_props().hyperParams = self._convert_hyper_params(params)

    def add_hyper_param(self, name: str, value: Any) -> None:
        props = self._ensure_model_props()
        if props.hyperParams is None:
            props.hyperParams = []
        props.hyperParams.append(MLHyperParamClass(name=name, value=str(value)))

    @property
    def version(self) -> Optional[str]:
        version_props = self._get_aspect(VersionPropertiesClass)
        if version_props and version_props.version:
            return version_props.version.versionTag
        return None

    @property
    def aliases(self) -> Optional[List[str]]:
        version_props = self._get_aspect(VersionPropertiesClass)
        if version_props and version_props.aliases:
            return [
                alias.versionTag
                for alias in version_props.aliases
                if alias.versionTag is not None
            ]
        return None

    def set_aliases(self, aliases: List[str]) -> None:
        version_props = self._get_aspect(VersionPropertiesClass)
        if version_props:
            version_props.aliases = [
                VersionTagClass(versionTag=alias) for alias in aliases
            ]
        else:
            # If no version properties exist, we need to create one with a default version
            version_set_urn = VersionSetUrn(
                id=f"mlmodel_{self.urn.name}_versions", entity_type="mlModel"
            )
            self._set_aspect(
                VersionPropertiesClass(
                    version=VersionTagClass(versionTag="0.1.0"),  # Default version
                    versionSet=str(version_set_urn),
                    sortId="0000000.1.0",
                    aliases=[VersionTagClass(versionTag=alias) for alias in aliases],
                )
            )

    def add_aliases(self, aliases: List[str]) -> None:
        version_props = self._get_aspect(VersionPropertiesClass)
        if version_props:
            if version_props.aliases is None:
                version_props.aliases = []
            # Check if alias already exists
            for alias in aliases:
                if not any(a.versionTag == alias for a in version_props.aliases):
                    version_props.aliases.append(VersionTagClass(versionTag=alias))
        else:
            # If no version properties exist, we need to create one with the alias
            self.set_aliases(aliases)

    @property
    def groups(self) -> Optional[List[str]]:
        return self._ensure_model_props().groups

    def set_group(self, group: str) -> None:
        self._ensure_model_props().groups = [group]

    def add_to_group(self, group_urn: str) -> None:
        props = self._ensure_model_props()
        if props.groups is None:
            props.groups = []
        props.groups.append(group_urn)

    def remove_from_group(self, group_urn: str) -> None:
        props = self._ensure_model_props()
        if props.groups is not None:
            props.groups = [group for group in props.groups if group != group_urn]

    @property
    def training_jobs(self) -> Optional[List[str]]:
        return self._ensure_model_props().trainingJobs

    def set_training_jobs(self, training_jobs: List[str]) -> None:
        self._ensure_model_props().trainingJobs = training_jobs

    def add_training_jobs(self, training_jobs: List[str]) -> None:
        props = self._ensure_model_props()
        if props.trainingJobs is None:
            props.trainingJobs = []
        props.trainingJobs.extend(training_jobs)
