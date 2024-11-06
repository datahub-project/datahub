import logging
from typing import Any, Dict, List, Optional

import pydantic
from cached_property import cached_property
from pydantic import Field, root_validator
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    ConfigModel,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.azure.abs_utils import is_abs_uri
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig

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


class Azure(ConfigModel):
    """Azure configuration for Delta Lake source"""

    azure_config: Optional[AzureConnectionConfig] = Field(
        default=None, description="Azure configuration"
    )
    use_abs_blob_tags: Optional[bool] = Field(
        False,
        description="Whether or not to create tags in datahub from Azure blob metadata",
    )


class DeltaLakeSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    base_paths: Optional[List[str]] = Field(
        default=None,
        description="List of paths to tables (s3, abfss, or local file system). If a path is not a delta table path "
        "then all subfolders will be scanned to detect and ingest delta tables.",
    )
    # Not included in documentation, but kept for backwards compatibility
    base_path: Optional[str] = Field(
        default=None,
        exclude=True,
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

    require_files: Optional[bool] = Field(
        default=True,
        description="Whether DeltaTable should track files. "
        "Consider setting this to `False` for large delta tables, "
        "resulting in significant memory reduction for ingestion process."
        "When set to `False`, number_of_files in delta table can not be reported.",
    )

    s3: Optional[S3] = Field(default=None)
    azure: Optional[Azure] = Field(default=None)

    @cached_property
    def is_s3(self) -> bool:
        first_path = next(iter(self.paths_to_scan), "")
        return bool(
            self.s3 is not None
            and self.s3.aws_config is not None
            and is_s3_uri(first_path)
        )

    @cached_property
    def is_azure(self) -> bool:
        first_path = next(iter(self.paths_to_scan), "")
        return bool(
            self.azure is not None
            and self.azure.azure_config is not None
            and is_abs_uri(first_path)
        )

    @cached_property
    def complete_paths(self) -> List[str]:
        paths: List[str] = []
        base_paths = self.paths_to_scan
        if self.relative_path is not None:
            paths.extend(
                f"{path.rstrip('/')}/{self.relative_path.lstrip('/')}"
                for path in base_paths
            )
        else:
            paths.extend(base_paths)
        return paths

    @property
    def paths_to_scan(self) -> List[str]:
        paths: List[str] = []
        if self.base_paths is not None:
            paths.extend(self.base_paths)
        if self.base_path is not None:
            paths.append(self.base_path)
        if not paths:
            raise ValueError("At least one path must be specified via base_paths")
        return paths

    @root_validator
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        base_path = values.get("base_path")
        base_paths = values.get("base_paths", []) or []

        # Ensure at least one path is provided
        if not base_path and not base_paths:
            raise ValueError("Either base_path or base_paths must be specified")

        # Combine paths for validation
        paths = []
        if base_paths:
            paths.extend(base_paths)
        if base_path is not None:
            paths.append(base_path)

        has_s3 = any(is_s3_uri(path) for path in paths)
        has_azure = any(is_abs_uri(path) for path in paths)

        # Validate S3 configuration
        s3_config = values.get("s3")
        if has_s3:
            if not s3_config:
                raise ValueError("S3 configuration required for S3 paths")
            if not getattr(s3_config, "aws_config", None):
                raise ValueError("AWS configuration required for S3 paths")

        # Validate Azure configuration
        azure_config = values.get("azure")
        if has_azure:
            if not azure_config:
                raise ValueError("Azure configuration required for Azure Blob Storage paths")
            if not getattr(azure_config, "azure_config", None):
                raise ValueError("Azure connection config required for Azure Blob Storage paths")

        # Validate that all paths are of compatible types
        if has_s3 and has_azure:
            raise ValueError("Cannot mix S3 and Azure paths in the same source")

        return values

    @pydantic.validator("base_path")
    def warn_base_path_deprecated(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            logger.warning(
                "The 'base_path' option is deprecated and will be removed in a future release. "
                "Please use 'base_paths' instead. "
                "Example: base_paths: ['{path}']".format(path=v)
            )
        return v

    @pydantic.validator("version_history_lookback")
    def negative_version_history_implies_no_limit(cls, v):
        if v and v < 0:
            return None
        return v
