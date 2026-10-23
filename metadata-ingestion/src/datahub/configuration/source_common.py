from typing import Dict, Optional

import pydantic
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

    @pydantic.field_validator("env", mode="after")
    @classmethod
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
        description="Whether to convert dataset urns to lowercase. "
        "This value is part of each dataset's URN identity, so it must stay fixed for "
        "the life of a deployment. Changing it after data has been ingested re-keys "
        "every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful "
        "ingestion enabled the old-cased URNs are then soft-deleted as stale while the "
        "new-cased ones are created, producing duplicate or orphaned entities. Pick one "
        "value before the first run and leave it unchanged.",
    )


class TransformOperationLengthConfigMixin(ConfigModel):
    max_transform_operation_length: Optional[int] = Field(
        default=None,
        ge=0,
        description="Applies to dataset upstreamLineage only; data-job lineage is not affected. "
        "When emitting incremental lineage patches, cuts each column's "
        "transformation logic to this many characters followed by `...`, when that "
        "makes it shorter. The `SQL:` or `COPY:` label is not counted. `0` keeps "
        "only the label, and removes logic that has no label. Unset keeps the full "
        "logic in patches. For non-incremental upserts, oversized upstreamLineage "
        "aspects shorten transform logic only as far as needed to fit the payload "
        "limit, then drop excess lineage edges. Changing this value with "
        "`incremental_lineage` leaves previously stored full-text entries alongside "
        "the new ones until a run with `incremental_lineage: false`.",
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
