import logging
from typing import Dict, Optional, Union

import pydantic
from cached_property import cached_property
from pydantic import Field
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    ConfigModel,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class S3(ConfigModel):
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None, description="AWS configuration"
    )

    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = Field(
        False, description="Whether or not to create tags in datahub from the s3 bucket"
    )
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = Field(
        False,
        description="# Whether or not to create tags in datahub from the s3 object",
    )


class DeltaLakeSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    base_path: Optional[str] = Field(
        description="Path to table (s3 or local file system). If path is not a delta table path "
        "then all subfolders will be scanned to detect and ingest delta tables."
    )
    relative_path: Optional[str] = Field(
        default=None,
        description="If set, delta-tables will be searched at location "
        "'<base_path>/<relative_path>' and URNs will be created using "
        "relative_path only.",
    )
    platform: Literal["delta-lake"] = Field(
        default="delta-lake",
        description="The platform that this source connects to",
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

    path_spec: Optional[PathSpec] = Field(
        description="PathSpec Property. See [below](#path-spec) the details about PathSpec"
    )

    require_files: Optional[bool] = Field(
        default=True,
        description="Whether DeltaTable should track files. "
        "Consider setting this to `False` for large delta tables, "
        "resulting in significant memory reduction for ingestion process."
        "When set to `False`, number_of_files in delta table can not be reported.",
    )

    number_of_files_to_sample: int = Field(
        default=100,
        description="Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec.",
    )

    verify_ssl: Union[bool, str] = Field(
        default=True,
        description="Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use.",
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

    add_partition_columns_to_schema: bool = Field(
        default=False,
        description="Whether to add partition fields to the schema.",
    )

    sort_schema_fields: bool = Field(
        default=False,
        description="Whether to sort schema fields by fieldPath when inferring schemas.",
    )

    s3: Optional[S3] = Field()

    @cached_property
    def is_s3(self):
        if self.base_path:
            return is_s3_uri(self.base_path or "")
        elif self.path_spec:
            return is_s3_uri(self.path_spec.include or "")

    @cached_property
    def complete_path(self):
        complete_path = self.base_path
        if self.relative_path is not None and complete_path is not None:
            complete_path = (
                f"{complete_path.rstrip('/')}/{self.relative_path.lstrip('/')}"
            )

        return complete_path

    @pydantic.validator("version_history_lookback")
    def negative_version_history_implies_no_limit(cls, v):
        if v and v < 0:
            return None
        return v

    @pydantic.validator("path_spec", always=True)
    def check_path_specs_and_infer_platform(
        cls, path_spec: PathSpec, values: Dict
    ) -> Optional[PathSpec]:
        if not path_spec:
            return None

        # Check that all path specs have the same platform.
        guessed_platforms = {"s3" if path_spec.is_s3 else "file"}
        if len(guessed_platforms) > 1:
            raise ValueError(
                f"Cannot have multiple platforms in path_spec: {guessed_platforms}"
            )
        guessed_platform = guessed_platforms.pop()

        # Ensure s3 configs aren't used for file sources.
        if guessed_platform != "s3" and (
            values.get("use_s3_object_tags") or values.get("use_s3_bucket_tags")
        ):
            raise ValueError(
                "Cannot grab s3 object/bucket tags when platform is not s3. Remove the flag or use s3."
            )

        return path_spec
