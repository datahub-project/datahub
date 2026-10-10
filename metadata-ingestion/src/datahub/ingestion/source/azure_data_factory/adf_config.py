"""Configuration classes for Azure Data Factory connector."""

from typing import Annotated, Optional, Sequence, Set

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, Filters
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.agent.verdicts import ancestors_in
from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.common.subtypes import FlowContainerSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

# ADF pipelines are DataFlows with no subtype and activities are DataJobs whose
# subtype varies by activity type, so neither has a DataHub subtype `probe filter
# --kind` could name. Declared here so the probe's kind= and the Filters()
# declaration below cannot drift apart. FlowContainerSubTypes.MATILLION_PIPELINE
# also spells "Pipeline", but it names another connector's container.
ADF_PIPELINE_KIND = "Pipeline"
ADF_ACTIVITY_KIND = "Activity"


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
    factory_pattern: Annotated[
        AllowDenyPattern, Filters(FlowContainerSubTypes.ADF_DATA_FACTORY)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter Data Factories by name. "
            "Example: allow=['prod-.*'], deny=['.*-test']"
        ),
    )

    pipeline_pattern: Annotated[AllowDenyPattern, Filters(ADF_PIPELINE_KIND)] = Field(
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
            "Extract column-level lineage from Copy activities. "
            "Supports explicit column mappings (translator configuration) "
            "and auto-mapping inference from source dataset schema."
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

    @classmethod
    def probe_provider_class(cls) -> type:
        # Lazy: adf_probe imports adf_source, which imports this module.
        from datahub.ingestion.source.azure_data_factory.adf_probe import (
            AzureDataFactoryMetadataProbe,
        )

        return AzureDataFactoryMetadataProbe

    @classmethod
    def probe_unfiltered_kinds(cls) -> Set[str]:
        """Every top-level activity of a kept pipeline is ingested; ADF has no
        activity pattern. Declared so `probe filter --kind Activity` says
        "unfiltered" rather than "unresolved"."""
        return {ADF_ACTIVITY_KIND}

    def probe_ancestor_kinds(self, kind: str) -> Optional[Sequence[str]]:
        """Pipelines are fetched only for factories factory_pattern keeps, and
        activities only for pipelines pipeline_pattern keeps
        (AzureDataFactorySource.get_workunits_internal / _process_pipelines)."""
        return ancestors_in(
            (str(FlowContainerSubTypes.ADF_DATA_FACTORY), ADF_PIPELINE_KIND),
            kind,
            (ADF_ACTIVITY_KIND,),
        )
