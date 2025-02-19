from typing import Optional

from pydantic import Field

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class AzureFabricSourceConfig(
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    platform: str = Field(default="azurefabric", hidden_from_docs=True)
    azure_config: Optional[AzureConnectionConfig] = Field(
        default=None, description="Azure configuration"
    )
    workspace_url: Optional[str] = Field(default="", description="Workspace URL")
    workspace_name: Optional[str] = Field(default="", description="Workspace name")
    enable_dataset_discovery: bool = True
    enable_lineage_discovery: bool = True
    enable_usage_stats: bool = True
    enable_profiling: bool = False
    enable_job_extraction: bool = True
    enable_dataflow_extraction: bool = True
    profiling_sample_size: int = 1000
    incremental_lineage: bool = True
    check_timeout_sec: int = 120
    retry_count: int = 3
    batch_size: int = 100

    @property
    def get_azure_config(self) -> AzureConnectionConfig:
        """
        Gets the Azure config, ensuring it exists

        Raises:
            ValueError if azure_config is not set
        """
        if self.azure_config is None:
            raise ValueError("azure_config must be configured")
        return self.azure_config
