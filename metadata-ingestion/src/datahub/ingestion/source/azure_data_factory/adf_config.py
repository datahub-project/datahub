"""Configuration classes for Azure Data Factory connector."""

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


class AzureDataFactoryConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for Azure Data Factory source.

    This connector extracts metadata from Azure Data Factory including:
    - Data Factories as Containers
    - Pipelines as DataFlows
    - Activities as DataJobs
    - Dataset lineage
    - Execution history (optional)
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

    # Azure Scope
    subscription_id: str = Field(
        description=(
            "Azure subscription ID containing the Data Factories to ingest. "
            "Find this in Azure Portal > Subscriptions."
        ),
    )

    resource_group: Optional[str] = Field(
        default=None,
        description=(
            "Azure resource group name to filter Data Factories. "
            "If not specified, all Data Factories in the subscription will be ingested."
        ),
    )

    # Filtering
    factory_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter Data Factories by name. "
            "Example: allow=['prod-.*'], deny=['.*-test']"
        ),
    )

    pipeline_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter pipelines by name. "
            "Applied to all factories matching factory_pattern."
        ),
    )

    # Feature Flags
    include_lineage: bool = Field(
        default=True,
        description=(
            "Extract lineage from activity inputs/outputs. "
            "Maps ADF datasets to DataHub datasets based on linked service type."
        ),
    )

    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Extract column-level lineage from Data Flow activities. "
            "Requires parsing Data Flow definitions."
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
            "Higher values increase ingestion time."
        ),
        ge=1,
        le=90,
    )

    # Platform Mapping
    platform_instance_map: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Map linked service names to DataHub platform instances. "
            "Example: {'my-snowflake-connection': 'prod_snowflake'}. "
            "Used for accurate lineage resolution to existing datasets."
        ),
    )

    # Stateful Ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Configuration for stateful ingestion and stale entity removal. "
            "When enabled, tracks ingested entities and removes those that "
            "no longer exist in Azure Data Factory."
        ),
    )
