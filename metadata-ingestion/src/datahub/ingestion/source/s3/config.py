import logging
from typing import Any, Dict, List, Optional

import pydantic
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvBasedSourceConfigBase,
    PlatformSourceConfigBase,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.path_spec import PathSpec
from datahub.ingestion.source.aws.s3_util import get_bucket_name
from datahub.ingestion.source.s3.profiling import DataLakeProfilerConfig

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class DataLakeSourceConfig(PlatformSourceConfigBase, EnvBasedSourceConfigBase):
    path_specs: Optional[List[PathSpec]] = Field(
        description="List of PathSpec. See below the details about PathSpec"
    )
    path_spec: Optional[PathSpec] = Field(
        description="Path spec will be deprecated in favour of path_specs option."
    )
    platform: str = Field(
        default="", description="The platform that this source connects to"
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None, description="AWS configuration"
    )

    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = Field(
        None, description="Whether or not to create tags in datahub from the s3 bucket"
    )
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = Field(
        None,
        description="# Whether or not to create tags in datahub from the s3 object",
    )

    profile_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to profile ",
    )
    profiling: DataLakeProfilerConfig = Field(
        default=DataLakeProfilerConfig(), description="Data profiling configuration"
    )

    spark_driver_memory: str = Field(
        default="4g", description="Max amount of memory to grant Spark."
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

    @pydantic.root_validator(pre=False)
    def validate_platform(cls, values: Dict) -> Dict:
        value = values.get("platform")
        if value is not None and value != "":
            return values

        if not values.get("path_specs") and not values.get("path_spec"):
            raise ValueError("Either path_specs or path_spec needs to be specified")

        if values.get("path_specs") and values.get("path_spec"):
            raise ValueError(
                "Either path_specs or path_spec needs to be specified but not both"
            )

        if values.get("path_spec"):
            logger.warning(
                "path_spec config property is deprecated, please use path_specs instead of it."
            )
            values["path_specs"] = [values.get("path_spec")]

        bucket_name: str = ""
        for path_spec in values.get("path_specs", []):
            if path_spec.is_s3:
                platform = "s3"
            else:
                if values.get("use_s3_object_tags") or values.get("use_s3_bucket_tags"):
                    raise ValueError(
                        "cannot grab s3 tags for platform != s3. Remove the flag or use s3."
                    )

                platform = "file"

            if values.get("platform", "") != "":
                if values["platform"] != platform:
                    raise ValueError("all path_spec should belong to the same platform")
            else:
                values["platform"] = platform
                logger.debug(f'Setting config "platform": {values.get("platform")}')

            if platform == "s3":
                if bucket_name == "":
                    bucket_name = get_bucket_name(path_spec.include)
                else:
                    if bucket_name != get_bucket_name(path_spec.include):
                        raise ValueError(
                            "all path_spec should reference the same s3 bucket"
                        )

        return values

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling: Optional[DataLakeProfilerConfig] = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling._allow_deny_patterns = values["profile_patterns"]
        return values
