from typing import Any, Dict, Optional

import parse
import pydantic
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.data_lake.profiling import DataLakeProfilerConfig


class DataLakeSourceConfig(EnvBasedSourceConfigBase):

    base_path: str = Field(
        description="Path of the base folder to crawl. Unless `schema_patterns` and `profile_patterns` are set, the connector will ingest all files in this folder."
    )
    platform: str = Field(
        default="",
        description="Autodetected.  Platform to use in namespace when constructing URNs. If left blank, local paths will correspond to `file` and S3 paths will correspond to `s3`.",
    )  # overwritten by validator below

    use_relative_path: bool = Field(
        default=False,
        description="Whether to use the relative path when constructing URNs. Has no effect when a `path_spec` is provided.",
    )
    ignore_dotfiles: bool = Field(
        default=True,
        description="Whether to ignore files that start with `.`. For instance, `.DS_Store`, `.bash_profile`, etc.",
    )

    aws_config: Optional[AwsSourceConfig] = Field(
        default=None, description="AWS details"
    )

    schema_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to filter for ingestion.",
    )
    profile_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to profile ",
    )

    path_spec: Optional[str] = Field(
        default=None,
        description="Format string for constructing table identifiers from the relative path. See the above setup section for details.",
    )

    profiling: DataLakeProfilerConfig = Field(
        default=DataLakeProfilerConfig(), description="Profiling configurations"
    )

    spark_driver_memory: str = Field(
        default="4g", description="Max amount of memory to grant Spark."
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling.allow_deny_patterns = values["profile_patterns"]
        return values

    @pydantic.validator("platform", always=True)
    def validate_platform(cls, value: str, values: Dict[str, Any]) -> Optional[str]:
        if value != "":
            return value

        if is_s3_uri(values["base_path"]):
            return "s3"
        return "file"

    @pydantic.validator("path_spec", always=True)
    def validate_path_spec(
        cls, value: Optional[str], values: Dict[str, Any]
    ) -> Optional[str]:
        if value is None:
            return None

        if not value.startswith("./"):
            # enforce this for semantics
            raise ValueError("Path_spec must start with './'")

        name_indices = sorted([x[0] for x in parse.findall("{{name[{:d}]}}", value)])

        if len(name_indices) == 0:
            raise ValueError("Path spec must contain at least one name identifier")

        if name_indices != list(range(max(name_indices) + 1)):
            raise ValueError(
                "Path spec must contain consecutive name identifiers, starting at 0"
            )

        return value
