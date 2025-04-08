from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk.entity import Entity


class MLModelGroup(Entity):
    """A class representing an ML model group in DataHub."""

    def __init__(
        self,
        id: str,
        platform: str = "mlflow",
        name: Optional[str] = None,
        description: Optional[str] = None,
        created: Optional[models.TimeStampClass] = None,
        lastModified: Optional[models.TimeStampClass] = None,
        customProperties: Optional[dict] = None,
    ) -> None:
        urn = MlModelGroupUrn(platform=platform, name=id)
        super().__init__(urn=urn)

        self.id = id
        self.platform = platform
        self.name = name
        self.description = description
        self.created = created
        self.lastModified = lastModified
        self.customProperties = customProperties

        self._set_aspect(
            models.MLModelGroupPropertiesClass(
                name=self.name or self.id,
                description=self.description,
                created=self.created,
                lastModified=self.lastModified,
                customProperties=self.customProperties,
            )
        )

    @classmethod
    def get_urn_type(cls) -> type[MlModelGroupUrn]:
        return MlModelGroupUrn

    @property
    def properties(self) -> Optional[models.MLModelGroupPropertiesClass]:
        return self._get_aspect(models.MLModelGroupPropertiesClass)
