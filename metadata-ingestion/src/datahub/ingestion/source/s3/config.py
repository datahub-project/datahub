import logging
from typing import Any, Dict, List, Optional

import pydantic
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvBasedSourceConfigBase,
    PlatformSourceConfigBase,
)
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.path_spec import PathSpec
from datahub.ingestion.source.aws.s3_util import get_bucket_name
from datahub.ingestion.source.s3.profiling import DataLakeProfilerConfig

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class DataLakeSourceConfig(PlatformSourceConfigBase, EnvBasedSourceConfigBase):
    path_specs: List[PathSpec] = Field(
        description="List of PathSpec. See [below](#path-spec) the details about PathSpec"
    )
    platform: str = Field(
        # The platform field already exists, but we want to override the type/default/docs.
        default="",
        description="The platform that this source connects to (either 's3' or 'file'). "
        "If not specified, the platform will be inferred from the path_specs.",
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

    _rename_path_spec_to_plural = pydantic_renamed_field(
        "path_spec", "path_specs", lambda path_spec: [path_spec]
    )

    @pydantic.validator("path_specs", always=True)
    def check_path_specs_and_infer_platform(
        cls, path_specs: List[PathSpec], values: Dict
    ) -> List[PathSpec]:
        if len(path_specs) == 0:
            raise ValueError("path_specs must not be empty")

        # Check that all path specs have the same platform.
        guessed_platforms = set(
            "s3" if path_spec.is_s3 else "file" for path_spec in path_specs
        )
        if len(guessed_platforms) > 1:
            raise ValueError(
                f"Cannot have multiple platforms in path_specs: {guessed_platforms}"
            )
        guessed_platform = guessed_platforms.pop()

        # If platform is s3, check that they're all the same bucket.
        if guessed_platform == "s3":
            bucket_names = set(
                get_bucket_name(path_spec.include) for path_spec in path_specs
            )
            if len(bucket_names) > 1:
                raise ValueError(
                    f"All path_specs should reference the same s3 bucket. Got {bucket_names}"
                )

        # Ensure s3 configs aren't used for file sources.
        if guessed_platform != "s3" and (
            values.get("use_s3_object_tags") or values.get("use_s3_bucket_tags")
        ):
            raise ValueError(
                "Cannot grab s3 object/bucket tags when platform is not s3. Remove the flag or use s3."
            )

        # Infer platform if not specified.
        if values.get("platform") and values["platform"] != guessed_platform:
            raise ValueError(
                f"All path_specs belong to {guessed_platform} platform, but platform is set to {values['platform']}"
            )
        else:
            logger.debug(f'Setting config "platform": {guessed_platform}')
            values["platform"] = guessed_platform

        return path_specs

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling: Optional[DataLakeProfilerConfig] = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling._allow_deny_patterns = values["profile_patterns"]
        return values
