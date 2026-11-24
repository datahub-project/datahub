"""Configuration classes for Azure Data Factory connector."""

from typing import Annotated, Optional, Sequence, Set

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
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


class DatabricksCatalogMapping(ConfigModel):
    """Unity Catalog identity for one Databricks linked service, since
    ADF's own API exposes neither a metastore nor a catalog name for
    Databricks datasets or linked services."""

    catalog: str = Field(description="Unity Catalog catalog name for this workspace.")
    metastore: Optional[str] = Field(
        default=None,
        description=(
            "Unity Catalog metastore name for this workspace. Only set "
            "this if the DataHub Unity Catalog source for this same "
            "workspace was ingested with include_metastore enabled "
            "(which folds the metastore name into the dataset name "
            "ahead of catalog.schema.table) - otherwise leave unset to "
            "emit catalog.schema.table. Setting it also uses the workspace "
            "ID from the linked service's workspace URL (e.g. "
            "'adb-1234567890123456') as the platform_instance, as that "
            "source does; override it with platform_instance_map."
        ),
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

    databricks_default_catalog: Optional[str] = Field(
        default=None,
        description=(
            "Default catalog to prepend when fully qualifying a Databricks "
            "table reference that only resolves to schema.table. ADF's "
            "Databricks datasets and linked services expose database/table "
            "but never a catalog, and there is no reliable way to infer it "
            "(a workspace may be on the legacy hive_metastore or a "
            "Unity Catalog catalog with any name), so nothing is assumed "
            "by default and such references are left as schema.table. Set "
            "this if you know your workspace's catalog and want fully "
            "qualified catalog.schema.table references instead. Superseded "
            "for a given linked service by a more specific entry in "
            "databricks_catalog_map, if present."
        ),
    )
    databricks_catalog_map: dict[str, DatabricksCatalogMapping] = Field(
        default_factory=dict,
        description=(
            "Map a Databricks linked service name to its Unity Catalog "
            "catalog (and optionally metastore) name, for tenants with "
            "multiple Databricks workspaces that don't all share the same "
            "catalog. Example: {'MyDatabricksLS': {'catalog': "
            "'prod_catalog', 'metastore': 'my_metastore'}}. Takes "
            "precedence over databricks_default_catalog for linked "
            "services present in this map."
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

    max_dynamic_lineage_pairs_per_activity: int = Field(
        default=50,
        description=(
            "When execution history shows an activity (e.g. a Copy activity "
            "inside a ForEach) reading several distinct tables and writing "
            "several distinct tables, each source/sink pair gets its own "
            "DataJob, up to this many per activity. Pairs are taken in URN "
            "order; lineage for pairs over the cap is not emitted and a "
            "warning is reported."
        ),
        ge=1,
        le=500,
    )

    # Platform Mapping
    platform_instance_map: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Map linked service names to DataHub platform instances. "
            "Example: {'my-snowflake-connection': 'prod_snowflake'}. "
            "Used for accurate lineage resolution to existing datasets. "
            "An empty value means no platform instance for that linked "
            "service."
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
