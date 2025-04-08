from typing import List, Optional

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import MlModelUrn, VersionSetUrn
from datahub.sdk.entity import Entity


class MLModel(Entity):
    """A class representing an ML model in DataHub."""

    def __init__(
        self,
        id: str,
        version: str,
        platform: str = "mlflow",
        name: Optional[str] = None,
        description: Optional[str] = None,
        trainingMetrics: Optional[List[models.MLMetricClass]] = None,
        hyperParams: Optional[List[models.MLHyperParamClass]] = None,
        externalUrl: Optional[str] = None,
        created: Optional[models.TimeStampClass] = None,
        lastModified: Optional[models.TimeStampClass] = None,
    ) -> None:
        urn = MlModelUrn(platform=platform, name=id)
        super().__init__(urn=urn)

        self.id = id
        self.version = version
        self.platform = platform
        self.name = name
        self.description = description
        self.trainingMetrics = trainingMetrics
        self.hyperParams = hyperParams
        self.externalUrl = externalUrl
        self.created = created
        self.lastModified = lastModified

        self.version_set_urn = VersionSetUrn(
            id=f"mlmodel_{self.id}_versions", entity_type="mlModel"
        )

        self._set_aspect(
            models.MLModelPropertiesClass(
                name=self.name or self.id,
                description=self.description,
                trainingMetrics=self.trainingMetrics,
                hyperParams=self.hyperParams,
                externalUrl=self.externalUrl,
                created=self.created,
                lastModified=self.lastModified,
                tags=[],
                groups=[],
                trainingJobs=[],
            )
        )

        self._set_aspect(
            models.VersionPropertiesClass(
                version=models.VersionTagClass(versionTag=self.version),
                versionSet=str(self.version_set_urn),
                sortId=str(self.version).zfill(10),
            )
        )

    @classmethod
    def get_urn_type(cls) -> type[MlModelUrn]:
        return MlModelUrn

    @property
    def properties(self) -> Optional[models.MLModelPropertiesClass]:
        return self._get_aspect(models.MLModelPropertiesClass)

    @property
    def version_properties(self) -> Optional[models.VersionPropertiesClass]:
        return self._get_aspect(models.VersionPropertiesClass)

    @property
    def tags(self) -> List[str]:
        if props := self.properties:
            return props.tags or []
        return []

    @property
    def groups(self) -> List[str]:
        if props := self.properties:
            return props.groups or []
        return []

    @property
    def trainingJobs(self) -> List[str]:
        if props := self.properties:
            return props.trainingJobs or []
        return []

    def add_to_group(self, group_urn: str) -> Self:
        """Add this model to a group."""
        if props := self.properties:
            props.groups = props.groups or []
            props.groups.append(group_urn)
            self._set_aspect(props)
        return self

    def add_training_job(self, job_urn: str) -> Self:
        """Add a training job to this model."""
        if props := self.properties:
            props.trainingJobs = props.trainingJobs or []
            props.trainingJobs.append(job_urn)
            self._set_aspect(props)
        return self
