"""Configuration classes for Fabric Data Factory connector."""

from typing import Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class FabricDataFactorySourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
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
            "managed identity, Azure CLI, or auto-detection (DefaultAzureCredential). "
            "See AzureCredentialConfig for detailed options."
        ),
    )

    # Filtering
    workspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter workspaces by name. "
            "Example: allow=['prod-.*'], deny=['.*-test']"
        ),
    )

    pipeline_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter data pipelines by name. "
            "Applied to all workspaces matching workspace_pattern."
        ),
    )

    # Feature Flags
    extract_pipelines: bool = Field(
        default=True,
        description="Whether to extract Data Pipelines and their activities.",
    )

    include_lineage: bool = Field(
        default=True,
        description=(
            "Extract lineage from activity inputs/outputs. "
            "Maps Fabric connections to DataHub datasets based on connection type."
        ),
    )

    include_execution_history: bool = Field(
        default=True,
        description=(
            "Extract pipeline and activity execution history as DataProcessInstance. "
            "Includes run status, duration, and parameters. "
            "Enables lineage extraction from parameterized activities using actual runtime values."
        ),
    )

    execution_history_days: int = Field(
        default=7,
        description=(
            "Number of days of execution history to extract. "
            "Only used when include_execution_history is True. "
            "Higher values increase ingestion time. "
            "Note: Fabric API returns at most 100 recently completed runs per pipeline."
        ),
        ge=1,
        le=90,
    )

    # Platform Mapping
    platform_instance_map: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Map connection names to DataHub platform instances. "
            "Example: {'my-snowflake-connection': 'prod_snowflake'}. "
            "Used for accurate lineage resolution to existing datasets."
        ),
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
