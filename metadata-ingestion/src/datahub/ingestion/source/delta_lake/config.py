import logging
from typing import Optional, Dict, Any, List

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
    base_path: str = Field(
        default=None,
        exclude=True,
    )
    base_paths: Optional[List[str]] = Field(
        default=None,
        description="List of paths to tables (s3, abfss, or local file system). If a path is not a delta table path "
        "then all subfolders will be scanned to detect and ingest delta tables.",
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
        return bool(
            self.s3 is not None
            and self.s3.aws_config is not None
            and is_s3_uri(self.base_path or "")
        )

    @cached_property
    def is_azure(self) -> bool:
        return bool(
            self.azure is not None
            and self.azure.azure_config is not None
            and is_abs_uri(self.base_path or "")
        )

    @cached_property
    def complete_path(self):
        complete_path = self.base_path
        if self.relative_path is not None:
            complete_path = (
                f"{complete_path.rstrip('/')}/{self.relative_path.lstrip('/')}"
            )

        return complete_path

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

    @root_validator
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        base_path = values.get("base_path")
        base_paths = values.get("base_paths", [])

        # Ensure at least one path is provided
        if not base_path and not base_paths:
            raise ValueError("Either base_path or base_paths must be specified")

        # Combine paths for validation
        paths = []
        if base_paths:
            paths.extend(base_paths)
        if base_path:
            paths.append(base_path)

        has_s3 = any(is_s3_uri(path) for path in paths)
        has_azure = any(is_abs_uri(path) for path in paths)

        # Validate S3 configuration
        if has_s3 and (not values.get("s3") or not values.get("s3").aws_config):
            raise ValueError("AWS configuration required for S3 paths")

        # Validate Azure configuration
        if has_azure and (
            not values.get("azure") or not values.get("azure").azure_config
        ):
            raise ValueError(
                "Azure configuration required for Azure Blob Storage paths"
            )

        # Validate that all paths are of compatible types
        if has_s3 and has_azure:
            raise ValueError("Cannot mix S3 and Azure paths in the same source")

        return values

    @property
    def paths_to_scan(self) -> List[str]:
        """Returns list of paths to scan, combining base_path and base_paths."""

        paths = []
        if self.base_paths:
            paths.extend(self.base_paths)
        if self.base_path:
            paths.append(self.base_path)
        if not paths:
            raise ValueError("At least one path must be specified via base_paths")
        return paths
