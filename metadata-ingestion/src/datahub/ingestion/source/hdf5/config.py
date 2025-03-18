from typing import List, Optional

from pydantic.fields import Field
from pydantic.types import PositiveInt

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled


class HDF5SourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    path_list: List[str] = Field(
        description="List of paths to HDF5 files or directories to ingest."
    )

    path_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for file paths to filter in ingestion.",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset paths to filter in ingestion.",
    )

    max_schema_size: Optional[PositiveInt] = Field(
        default=100, description="Maximum number of fields to include in the schema."
    )

    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset paths to profile",
    )

    profiling: GEProfilingConfig = Field(
        default=GEProfilingConfig(),
        description="Configuration for profiling",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )
