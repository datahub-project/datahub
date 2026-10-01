import logging
from typing import Any, Dict, Optional, Union

from pydantic import ValidationInfo, field_validator, model_validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.data_lake_common.config import PathSpecsConfigMixin
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class DataLakeSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    PathSpecsConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    platform: str = Field(
        default="",
        description="The platform that this source connects to (either 's3' or 'file'). "
        "If not specified, the platform will be inferred from the path_specs.",
    )
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None, description="AWS configuration"
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = Field(
        None, description="Whether or not to create tags in datahub from the s3 bucket"
    )
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = Field(
        None,
        description="Whether or not to create tags in datahub from the s3 object",
    )
    use_s3_content_type: bool = Field(
        default=False,
        description=(
            "If enabled, use S3 Object metadata to determine content type over file extension, if set."
            " Warning: this requires a separate query to S3 for each object, which can be slow for large datasets."
        ),
    )

    # Whether to update the table schema when schema in files within the partitions are updated
    _update_schema_on_partition_file_updates_deprecation = pydantic_field_deprecated(
        "update_schema_on_partition_file_updates",
        message="update_schema_on_partition_file_updates is deprecated. This behaviour is the default now.",
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

    spark_config: Dict[str, Any] = Field(
        description='Spark configuration properties to set on the SparkSession. Put config property names into quotes. For example: \'"spark.executor.memory": "2g"\'',
        default={},
    )

    max_rows: int = Field(
        default=100,
        ge=1,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files. Also caps JSON and JSONL schema inference: at most this many records of a top-level array are read, and arrays inside a single JSON object are truncated to this many elements, so fields that only appear later are not reported.",
    )
    add_partition_columns_to_schema: bool = Field(
        default=False,
        description="Whether to add partition fields to the schema.",
    )
    verify_ssl: Union[bool, str] = Field(
        default=True,
        description="Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use.",
    )

    number_of_files_to_sample: int = Field(
        default=100,
        description="Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec.",
    )

    _rename_path_spec_to_plural = pydantic_renamed_field(
        "path_spec", "path_specs", lambda path_spec: [path_spec]
    )

    sort_schema_fields: bool = Field(
        default=False,
        description="Whether to sort schema fields by fieldPath when inferring schemas.",
    )

    generate_partition_aspects: bool = Field(
        default=True,
        description="Whether to generate partition aspects for partitioned tables. On older servers for backward compatibility, this should be set to False. This flag will be removed in future versions.",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @classmethod
    def probe_provider_class(cls) -> type:
        # lazy: s3_probe imports this module and the S3 listing stack
        from datahub.ingestion.source.s3.s3_probe import S3MetadataProbe

        return S3MetadataProbe

    @classmethod
    def probe_rule_filtered_kinds(cls) -> Dict[str, str]:
        """path_specs, not an AllowDenyPattern, decide every level this source emits."""
        return {
            str(DatasetContainerSubTypes.S3_BUCKET): "path_specs",
            str(DatasetContainerSubTypes.FOLDER): "path_specs",
            str(DatasetSubTypes.TABLE): "path_specs",
        }

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        """Judge one name with the PathSpec calls S3Source makes; see path_spec_verdict."""
        # lazy: path_spec_verdict imports s3.source, which imports this module
        from datahub.ingestion.source.data_lake_common.path_spec_verdict import (
            judge_bucket,
            judge_dataset,
            judge_folder,
        )

        if self.platform != "s3":
            raise ValueError(
                f"probe filter judges s3:// path_specs; this recipe reads local "
                f"paths (platform '{self.platform}')"
            )
        if self.aws_config is None:
            ctx.warn(
                "aws_config is not set, so ingesting this recipe fails before it "
                "lists anything; the verdict is what path_specs decide once it is set"
            )
        kind = str(ctx.kind)
        if kind == str(DatasetContainerSubTypes.S3_BUCKET):
            if "/" in ctx.name:
                raise ValueError(
                    f"'{ctx.name}' is not a bucket name; pass bare names, as "
                    f"`probe run buckets` lists them"
                )
            return judge_bucket(self.path_specs, ctx.name, ctx.warn)
        # s3a:// and s3n:// are refused too: create_s3_path always writes s3://,
        # so ingestion never names a dataset or folder with them.
        if not ctx.name.startswith("s3://"):
            raise ValueError(
                f"'{ctx.name}' is not an s3:// URI; {kind} names are full s3:// "
                f"URIs, as `probe run datasets` lists them"
            )
        if kind == str(DatasetContainerSubTypes.FOLDER):
            return judge_folder(
                self.path_specs,
                ctx.name,
                ctx.warn,
                bucket_kind=str(DatasetContainerSubTypes.S3_BUCKET),
            )
        # ignore_ext mirrors get_workunits_internal: is_s3_platform() (checked
        # above) and use_s3_content_type.
        return judge_dataset(
            self.path_specs, ctx.name, ctx.warn, ignore_ext=self.use_s3_content_type
        )

    @field_validator("path_specs", mode="before")
    @classmethod
    def check_path_specs(cls, path_specs: Any, info: ValidationInfo) -> Any:
        if len(path_specs) == 0:
            raise ValueError("path_specs must not be empty")

        # Basic validation - path specs consistency and S3 config validation is now handled in model_validator

        return path_specs

    @model_validator(mode="after")
    def ensure_profiling_pattern_is_passed_to_profiling(self) -> "DataLakeSourceConfig":
        profiling = self.profiling
        if profiling is not None and profiling.enabled:
            profiling._allow_deny_patterns = self.profile_patterns
        return self

    @model_validator(mode="after")
    def validate_platform_and_config_consistency(self) -> "DataLakeSourceConfig":
        """Infer platform from path_specs and validate config consistency."""
        # Track whether platform was explicitly provided
        platform_was_explicit = bool(self.platform)

        # Infer platform from path_specs if not explicitly set
        if not self.platform and self.path_specs:
            guessed_platforms = set()
            for path_spec in self.path_specs:
                if (
                    hasattr(path_spec, "include")
                    and path_spec.include
                    and path_spec.include.startswith("s3://")
                ):
                    guessed_platforms.add("s3")
                else:
                    guessed_platforms.add("file")

            # Ensure all path specs belong to the same platform
            if len(guessed_platforms) > 1:
                raise ValueError(
                    f"Cannot have multiple platforms in path_specs: {guessed_platforms}"
                )

            if guessed_platforms:
                guessed_platform = guessed_platforms.pop()
                logger.debug(f"Inferred platform: {guessed_platform}")
                self.platform = guessed_platform
            else:
                self.platform = "file"
        elif not self.platform:
            self.platform = "file"

        # Validate platform consistency only when platform was inferred (not explicitly set)
        # This allows sources like GCS to set platform="gcs" with s3:// URIs for correct container subtypes
        if not platform_was_explicit and self.platform and self.path_specs:
            expected_platforms = set()
            for path_spec in self.path_specs:
                if (
                    hasattr(path_spec, "include")
                    and path_spec.include
                    and path_spec.include.startswith("s3://")
                ):
                    expected_platforms.add("s3")
                else:
                    expected_platforms.add("file")

            if len(expected_platforms) == 1:
                expected_platform = expected_platforms.pop()
                if self.platform != expected_platform:
                    raise ValueError(
                        f"All path_specs belong to {expected_platform} platform, but platform was inferred as {self.platform}"
                    )

        # Validate S3-specific configurations
        if self.platform != "s3":
            if self.use_s3_bucket_tags:
                raise ValueError(
                    "Cannot grab s3 bucket tags when platform is not s3. Remove the flag or ingest from s3."
                )
            if self.use_s3_object_tags:
                raise ValueError(
                    "Cannot grab s3 object tags when platform is not s3. Remove the flag or ingest from s3."
                )
            if self.use_s3_content_type:
                raise ValueError(
                    "Cannot grab s3 object content type when platform is not s3. Remove the flag or ingest from s3."
                )

        return self


def s3_probe_refusal(config: DataLakeSourceConfig) -> Optional[str]:
    """Why the probe cannot list for this recipe, or None.

    Mirrors where S3Source itself would not reach S3: a local-path recipe is
    walked with os.walk (local_browser), which the probe must never do on the
    host it runs on, and an S3 recipe without aws_config raises
    "aws_config not set" in s3_browser.
    """
    if config.platform != "s3":
        return (
            f"this recipe's path_specs are on the '{config.platform}' platform "
            f"(local paths, not s3://); the probe lists and judges S3 only"
        )
    if config.aws_config is None:
        return (
            "aws_config is not set, and S3 ingestion refuses to browse S3 "
            "without it; add aws_config (an empty mapping uses the default AWS "
            "credential chain)"
        )
    return None
