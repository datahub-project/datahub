from typing import Dict, Optional

from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import ALL_ENV_TYPES, DEFAULT_ENV


class PlatformInstanceConfigMixin(ConfigModel):
    """
    Any source that connects to a platform should inherit this class
    """

    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to. "
        "This should be unique within the platform. "
        "See https://docs.datahub.com/docs/platform-instances/ for more details.",
    )


class EnvConfigMixin(ConfigModel):
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
            raise ValueError(f"env must be one of {ALL_ENV_TYPES}, found {v}")
        return v.upper()


class DatasetSourceConfigMixin(PlatformInstanceConfigMixin, EnvConfigMixin):
    """
    Any source that is a primary producer of Dataset metadata should inherit this class
    """

    # TODO: Deprecate this in favor of the more granular config mixins in order
    # to flatten our config inheritance hierarchies.


class LowerCaseDatasetUrnConfigMixin(ConfigModel):
    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Whether to convert dataset urns to lowercase.",
    )


class DatasetLineageProviderConfigBase(EnvConfigMixin):
    """
    Any non-Dataset source that produces lineage to Datasets should inherit this class.
    e.g. Orchestrators, Pipelines, BI Tools etc.
    """

    platform_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="A holder for platform -> platform_instance mappings to generate correct dataset urns",
    )


class PlatformDetail(ConfigModel):
    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform instance name. To generate correct urn for upstream dataset, this should match "
        "with platform instance name used in ingestion "
        "recipe of other datahub sources.",
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )
