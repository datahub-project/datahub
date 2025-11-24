import logging
from typing import Any, Dict, List, Optional, Union

from pydantic import ValidationInfo, field_validator, model_validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.abs.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.azure.abs_utils import is_abs_uri
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.data_lake_common.config import PathSpecsConfigMixin
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
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
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, PathSpecsConfigMixin
):
    platform: str = Field(
        default="",
        description="The platform that this source connects to (either 'abs' or 'file'). "
        "If not specified, the platform will be inferred from the path_specs.",
    )

    azure_config: Optional[AzureConnectionConfig] = Field(
        default=None, description="Azure configuration"
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
    # Whether to create Datahub Azure Container properties
    use_abs_container_properties: Optional[bool] = Field(
        None,
        description="Whether to create tags in datahub from the abs container properties",
    )
    # Whether to create Datahub Azure blob tags
    use_abs_blob_tags: Optional[bool] = Field(
        None,
        description="Whether to create tags in datahub from the abs blob tags",
    )
    # Whether to create Datahub Azure blob properties
    use_abs_blob_properties: Optional[bool] = Field(
        None,
        description="Whether to create tags in datahub from the abs blob properties",
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
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
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

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    # NOTE: We keep this validator in mode="before" so we can inspect raw dict
    # path_specs before PathSpec validation runs. That prevents existing configs
    # that rely on inferred fields (like allow_double_stars) from failing early.
    @field_validator("path_specs", mode="before")
    @classmethod
    def check_path_specs_not_empty(
        cls, path_specs: List[Union[Dict[str, Any], PathSpec]], info: ValidationInfo
    ) -> List[Union[Dict[str, Any], PathSpec]]:
        if len(path_specs) == 0:
            raise ValueError("path_specs must not be empty")
        return path_specs

    @staticmethod
    def _infer_platform_from_path_spec(
        path_spec: Optional[Union[Dict, PathSpec]],
    ) -> str:
        """
        Determine the platform represented by a PathSpec or its raw dict form.

        Validators running in `mode="before"` receive the raw dictionary input, so
        we need to gracefully handle both dicts and already-instantiated PathSpec
        objects.
        """
        if isinstance(path_spec, PathSpec):
            return "abs" if path_spec.is_abs else "file"

        include = None
        if isinstance(path_spec, dict):
            include = path_spec.get("include")

        if not include:
            raise ValueError("Each path_spec must specify a non-empty include path.")

        return "abs" if is_abs_uri(include) else "file"

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
                guessed_platforms.add(self._infer_platform_from_path_spec(path_spec))

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

        # Validate platform consistency when platform was explicitly set
        if platform_was_explicit and self.platform and self.path_specs:
            expected_platforms = set()
            for path_spec in self.path_specs:
                expected_platforms.add(self._infer_platform_from_path_spec(path_spec))

            if len(expected_platforms) == 1:
                expected_platform = expected_platforms.pop()
                if self.platform != expected_platform:
                    raise ValueError(
                        f"All path_specs belong to {expected_platform} platform, but platform is set to {self.platform}"
                    )

        # Validate ABS-specific configurations
        if self.platform != "abs":
            if self.use_abs_container_properties:
                raise ValueError(
                    "Cannot grab abs blob/container tags when platform is not abs. Remove the flag or use abs."
                )
            if self.use_abs_blob_tags:
                raise ValueError(
                    "Cannot grab abs blob/container tags when platform is not abs. Remove the flag or use abs."
                )
            if self.use_abs_blob_properties:
                raise ValueError(
                    "Cannot grab abs blob/container tags when platform is not abs. Remove the flag or use abs."
                )

        return self
