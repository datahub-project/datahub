from typing import Optional

from pydantic import BaseModel

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)


class DataPlatformInstance(BaseModel):
    platform: str
    platform_instance: Optional[str] = None

    @classmethod
    def from_data_platform_instance(
        cls,
        platform_instance: models.DataPlatformInstanceClass,
    ) -> "DataPlatformInstance":
        return cls(
            platform=platform_instance.platform,
            platform_instance=platform_instance.instance,
        )

    def to_data_platform_instance(self) -> models.DataPlatformInstanceClass:
        return models.DataPlatformInstanceClass(
            platform=make_data_platform_urn(self.platform),
            instance=(
                make_dataplatform_instance_urn(
                    platform=self.platform, instance=self.platform_instance
                )
                if self.platform_instance
                else None
            ),
        )
