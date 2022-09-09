from typing import Dict, Optional, Set

from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.metadata.schema_classes import FabricTypeClass

DEFAULT_ENV = FabricTypeClass.PROD

# Get all the constants from the FabricTypeClass. It's not an enum, so this is a bit hacky but works.
ALL_ENV_TYPES: Set[str] = set(
    [value for name, value in vars(FabricTypeClass).items() if not name.startswith("_")]
)


class PlatformSourceConfigBase(ConfigModel):
    """
    Any source that connects to a platform should inherit this class
    """

    platform: Optional[str] = Field(
        default=None, description="The platform that this source connects to"
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )


class EnvBasedSourceConfigBase(ConfigModel):
    """
    Any source that produces dataset urns in a single environment should inherit this class
    """

    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by this connector belong to",
    )

    @validator("env")
    def env_must_be_one_of(cls, v: str) -> str:
        if v.upper() not in ALL_ENV_TYPES:
            raise ConfigurationError(f"env must be one of {ALL_ENV_TYPES}, found {v}")
        return v.upper()


class DatasetSourceConfigBase(PlatformSourceConfigBase, EnvBasedSourceConfigBase):
    """
    Any source that is a primary producer of Dataset metadata should inherit this class
    """


class DatasetLineageProviderConfigBase(EnvBasedSourceConfigBase):
    """
    Any non-Dataset source that produces lineage to Datasets should inherit this class.
    e.g. Orchestrators, Pipelines, BI Tools etc.
    """

    platform_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="A holder for platform -> platform_instance mappings to generate correct dataset urns",
    )
