"""Configuration classes for Fabric Data Factory connector."""

from typing import Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class FabricDataFactorySourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin
):
    """Configuration for Fabric Data Factory source.

    This connector extracts metadata from Microsoft Fabric Data Factory items:
    - Workspaces as Containers
    - Data Pipelines as DataFlows with Activities as DataJobs
    - Copy Jobs as DataFlows with dataset-level lineage
    - Dataflow Gen2 as DataFlows (metadata only)
    """

    # Azure Authentication
    credential: AzureCredentialConfig = Field(
        default_factory=AzureCredentialConfig,
        description=(
            "Azure authentication configuration. Supports service principal, "
            "managed identity, Azure CLI, or auto-detection (DefaultAzureCredential)."
        ),
    )

    # Filtering
    workspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter workspaces by name.",
    )

    pipeline_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter data pipelines by name.",
    )

    copyjob_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter copy jobs by name.",
    )

    dataflow_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Dataflow Gen2 items by name.",
    )

    # Feature flags
    extract_pipelines: bool = Field(
        default=True,
        description="Whether to extract Data Pipelines and their activities.",
    )

    extract_copyjobs: bool = Field(
        default=True,
        description="Whether to extract Copy Jobs with dataset lineage.",
    )

    extract_dataflows: bool = Field(
        default=True,
        description="Whether to extract Dataflow Gen2 items.",
    )

    # Pipeline runs
    extract_pipeline_runs: bool = Field(
        default=True,
        description="Whether to extract pipeline run history as DataProcessInstances.",
    )

    lookback_days: int = Field(
        default=7,
        description=(
            "Number of days of pipeline run history to ingest. "
            "Only runs with startTimeUtc within this window are fetched. "
            "Note: Fabric API returns at most 100 recently completed runs per pipeline."
        ),
        ge=1,
    )

    # API timeout
    api_timeout: int = Field(
        default=30,
        description="Timeout for REST API calls in seconds.",
        ge=1,
        le=300,
    )

    # Stateful Ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Configuration for stateful ingestion and stale entity removal. "
            "When enabled, tracks ingested entities and removes those that "
            "no longer exist in Fabric."
        ),
    )
