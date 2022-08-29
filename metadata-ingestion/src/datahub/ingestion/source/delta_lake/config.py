import logging
from typing import Any, Dict, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    ConfigModel,
    EnvBasedSourceConfigBase,
    PlatformSourceConfigBase,
)
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class S3(ConfigModel):
    aws_config: AwsSourceConfig = Field(default=None, description="AWS configuration")

    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = Field(
        False, description="Whether or not to create tags in datahub from the s3 bucket"
    )
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = Field(
        False,
        description="# Whether or not to create tags in datahub from the s3 object",
    )


class DeltaLakeSourceConfig(PlatformSourceConfigBase, EnvBasedSourceConfigBase):
    class Config:
        arbitrary_types_allowed = True

    base_path: str = Field(
        description="Path to table (s3 or local file system). If path is not a delta table path "
        "then all subfolders will be scanned to detect and ingest delta tables."
    )
    relative_path: str = Field(
        default=None,
        description="If set, delta-tables will be searched at location "
        "'<base_path>/<relative_path>' and URNs will be created using "
        "relative_path only.",
    )
    platform: str = Field(
        default="delta-lake",
        description="The platform that this source connects to",
        const=True,
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to filter in ingestion.",
    )
    version_history_lookback: Optional[int] = Field(
        default=1,
        description="Number of previous version histories to be ingested. Defaults to 1. If set to -1 all version history will be ingested.",
    )

    s3: Optional[S3] = Field()

    # to be set internally
    _is_s3: bool
    _complete_path: str

    def is_s3(self):
        return self._is_s3

    def get_complete_path(self):
        return self._complete_path

    @pydantic.validator("version_history_lookback")
    def negative_version_history_implies_no_limit(cls, v):
        if v and v < 0:
            return None
        return v

    @pydantic.root_validator()
    def validate_config(cls, values: Dict) -> Dict[str, Any]:
        values["_is_s3"] = is_s3_uri(values.get("base_path", ""))
        if values["_is_s3"]:
            if values.get("s3") is None:
                raise ValueError("s3 config must be set for s3 path")
        values["_complete_path"] = values.get("base_path")
        if values.get("relative_path") is not None:
            values[
                "_complete_path"
            ] = f"{values['_complete_path'].rstrip('/')}/{values['relative_path'].lstrip('/')}"
        return values
