from typing import List, Optional

from pydantic import Field, validator

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class WorkspaceConfig(StatefulIngestionConfigBase):
    """Configuration for workspace filtering"""

    workspaces: Optional[List[str]] = Field(
        default=None,
        description="List of workspace IDs to process. If not provided, all workspaces will be discovered via API.",
    )
    workspace_names: Optional[List[str]] = Field(
        default=None,
        description="List of workspace names to process. If provided along with workspace_ids, both will be included.",
    )


class AzureFabricSourceConfig(
    StatefulIngestionConfigBase, EnvConfigMixin, PlatformInstanceConfigMixin
):
    """Configuration class for Azure Fabric source"""

    platform: str = Field(default="msfabric", hidden_from_docs=True)
    azure_config: Optional[AzureConnectionConfig] = Field(
        default=None, description="Azure configuration"
    )
    workspace_config: WorkspaceConfig = Field(
        default_factory=WorkspaceConfig, description="Workspace filtering configuration"
    )
    enable_dataset_discovery: bool = Field(
        default=True, description="Enable discovery of datasets"
    )
    enable_lineage_discovery: bool = Field(
        default=True, description="Enable discovery of lineage between datasets"
    )
    enable_usage_stats: bool = Field(
        default=True, description="Enable collection of usage statistics"
    )
    enable_profiling: bool = Field(
        default=False, description="Enable dataset profiling"
    )
    enable_job_extraction: bool = Field(
        default=True, description="Enable extraction of job metadata"
    )
    enable_dataflow_extraction: bool = Field(
        default=True, description="Enable extraction of dataflow metadata"
    )
    profiling_sample_size: int = Field(
        default=1000, description="Sample size to use for profiling"
    )
    incremental_lineage: bool = Field(
        default=True, description="Enable incremental lineage discovery"
    )
    check_timeout_sec: int = Field(
        default=120, description="Timeout in seconds for status checks"
    )
    retry_count: int = Field(
        default=3, description="Number of retries for failed requests"
    )
    batch_size: int = Field(default=100, description="Batch size for processing items")

    @validator("azure_config")
    def validate_azure_config(
        cls, v: Optional[AzureConnectionConfig]
    ) -> AzureConnectionConfig:
        if v is None:
            raise ValueError("azure_config must be configured")
        return v

    @validator("profiling_sample_size")
    def validate_sample_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("profiling_sample_size must be positive")
        return v

    @validator("check_timeout_sec")
    def validate_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("check_timeout_sec must be positive")
        return v

    @validator("retry_count")
    def validate_retry_count(cls, v: int) -> int:
        if v < 0:
            raise ValueError("retry_count cannot be negative")
        return v

    @validator("batch_size")
    def validate_batch_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("batch_size must be positive")
        return v
