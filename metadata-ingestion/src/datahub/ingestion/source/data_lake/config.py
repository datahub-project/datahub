from typing import Any, Dict, Optional

import parse
import pydantic

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.data_lake.profiling import DataLakeProfilerConfig


class DataLakeSourceConfig(ConfigModel):

    env: str = DEFAULT_ENV
    base_path: str
    platform: str = ""  # overwritten by validator below

    use_relative_path: bool = False
    ignore_dotfiles: bool = True

    aws_config: Optional[AwsSourceConfig] = None

    schema_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    profile_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()

    path_spec: Optional[str] = None

    profiling: DataLakeProfilerConfig = DataLakeProfilerConfig()

    spark_driver_memory: str = "4g"

    max_rows: int = 100

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
