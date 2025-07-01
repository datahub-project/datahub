import logging
from typing import Any, Dict, List, Optional, Union

import pydantic
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.abs.datalake_profiler_config import DataLakeProfilerConfig
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

    @pydantic.validator("path_specs", always=True)
    def check_path_specs_and_infer_platform(
        cls, path_specs: List[PathSpec], values: Dict
    ) -> List[PathSpec]:
        if len(path_specs) == 0:
            raise ValueError("path_specs must not be empty")

        # Check that all path specs have the same platform.
        guessed_platforms = set(
            "abs" if path_spec.is_abs else "file" for path_spec in path_specs
        )
        if len(guessed_platforms) > 1:
            raise ValueError(
                f"Cannot have multiple platforms in path_specs: {guessed_platforms}"
            )
        guessed_platform = guessed_platforms.pop()

        # Ensure abs configs aren't used for file sources.
        if guessed_platform != "abs" and (
            values.get("use_abs_container_properties")
            or values.get("use_abs_blob_tags")
            or values.get("use_abs_blob_properties")
        ):
            raise ValueError(
                "Cannot grab abs blob/container tags when platform is not abs. Remove the flag or use abs."
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

    @pydantic.validator("platform", always=True)
    def platform_not_empty(cls, platform: Any, values: dict) -> str:
        inferred_platform = values.get("platform")  # we may have inferred it above
        platform = platform or inferred_platform
        if not platform:
            raise ValueError("platform must not be empty")
        return platform

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling: Optional[DataLakeProfilerConfig] = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling._allow_deny_patterns = values["profile_patterns"]
        return values
