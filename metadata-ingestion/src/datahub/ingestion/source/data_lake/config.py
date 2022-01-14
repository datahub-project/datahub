from typing import Any, Dict, Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    get_sys_time,
    make_data_platform_urn,
    make_dataset_urn,
)
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.data_lake.profiling import DataLakeProfilerConfig


class DataLakeSourceConfig(ConfigModel):

    env: str = DEFAULT_ENV
    platform: str
    base_path: str

    use_relative_path: bool = False

    aws_config: Optional[AwsSourceConfig] = None

    schema_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    profile_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()

    profiling: DataLakeProfilerConfig = DataLakeProfilerConfig()

    spark_driver_memory: str = "4g"

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling.allow_deny_patterns = values["profile_patterns"]
        return values
