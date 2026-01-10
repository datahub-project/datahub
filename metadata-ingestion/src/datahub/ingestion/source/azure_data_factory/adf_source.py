"""Azure Data Factory ingestion source for DataHub.

This connector extracts metadata from Azure Data Factory including:
- Data Factories as Containers
- Pipelines as DataFlows
- Activities as DataJobs
- Dataset lineage (activity inputs/outputs)
- Pipeline execution history (optional)

Usage:
    source:
      type: azure_data_factory
      config:
        subscription_id: ${AZURE_SUBSCRIPTION_ID}
        credential:
          authentication_method: service_principal
          client_id: ${AZURE_CLIENT_ID}
          client_secret: ${AZURE_CLIENT_SECRET}
          tenant_id: ${AZURE_TENANT_ID}
"""

import logging
from typing import Iterable, Optional

from azure.mgmt.datafactory.models import (
    Activity,
    DataFlowResource,
    DatasetResource,
    Factory,
    LinkedServiceResource,
    PipelineResource,
    PipelineRun,
    TriggerResource,
)

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    AzureDataFactoryConfig,
)
from datahub.ingestion.source.azure_data_factory.adf_report import (
    AzureDataFactorySourceReport,
)
from datahub.ingestion.source.common.subtypes import (
    DataJobSubTypes,
    FlowContainerSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessTypeClass,
    DataTransformClass,
    DataTransformLogicClass,
    QueryLanguageClass,
    QueryStatementClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk._shared import DatasetUrnOrStr
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob

logger = logging.getLogger(__name__)

# Platform identifier for Azure Data Factory
PLATFORM = "azure-data-factory"

# Constants for pipeline run processing
MAX_RUN_MESSAGE_LENGTH = 500  # Truncate long error/status messages
MAX_RUN_PARAMETERS = 10  # Limit number of parameters to store
MAX_PARAMETER_VALUE_LENGTH = 100  # Truncate long parameter values

# Mapping of ADF linked service types to DataHub platforms.
# Platform identifiers must match those defined in:
# metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml
# Unsupported linked service types will trigger a structured warning.
LINKED_SERVICE_PLATFORM_MAP: dict[str, str] = {
    # Azure Storage - all Azure storage types map to "abs" (Azure Blob Storage)
    "AzureBlobStorage": "abs",
    "AzureBlobFS": "abs",  # Azure Data Lake Storage Gen2 (uses abfs:// protocol)
    "AzureDataLakeStore": "abs",  # Azure Data Lake Storage Gen1
    "AzureDataLakeStoreCosmosStructuredStream": "abs",
    "AzureFileStorage": "abs",
    # Azure Databases - Synapse uses mssql protocol
    "AzureSqlDatabase": "mssql",
    "AzureSqlDW": "mssql",  # Azure Synapse (formerly SQL DW)
    "AzureSynapseAnalytics": "mssql",  # Azure Synapse Analytics
    "AzureSqlMI": "mssql",
    "SqlServer": "mssql",
    "AzurePostgreSql": "postgres",
    "AzureMySql": "mysql",
    # Databricks
    "AzureDatabricks": "databricks",
    "AzureDatabricksDeltaLake": "databricks",
    # Cloud Platforms
    "AmazonS3": "s3",
    "AmazonS3Compatible": "s3",
    "GoogleCloudStorage": "gcs",
    "AmazonRedshift": "redshift",
    "GoogleBigQuery": "bigquery",
    "Snowflake": "snowflake",
    # Traditional Databases
    "PostgreSql": "postgres",
    "MySql": "mysql",
    "Oracle": "oracle",
    "OracleServiceCloud": "oracle",
    "Db2": "db2",
    "Teradata": "teradata",
    "Vertica": "vertica",
    # Data Warehouses
    "Hive": "hive",
    "Spark": "spark",
    "Hdfs": "hdfs",
    # SaaS Applications
    "Salesforce": "salesforce",
    "SalesforceServiceCloud": "salesforce",
    "SalesforceMarketingCloud": "salesforce",
}

# Mapping of ADF activity types to DataHub subtypes
ACTIVITY_SUBTYPE_MAP: dict[str, str] = {
    "Copy": DataJobSubTypes.ADF_COPY_ACTIVITY,
    "DataFlow": DataJobSubTypes.ADF_DATA_FLOW_ACTIVITY,
    "ExecutePipeline": DataJobSubTypes.ADF_EXECUTE_PIPELINE,
    "ExecuteDataFlow": DataJobSubTypes.ADF_DATA_FLOW_ACTIVITY,
    "Lookup": DataJobSubTypes.ADF_LOOKUP_ACTIVITY,
    "GetMetadata": DataJobSubTypes.ADF_GET_METADATA_ACTIVITY,
    "SqlServerStoredProcedure": DataJobSubTypes.ADF_STORED_PROCEDURE_ACTIVITY,
    "Script": DataJobSubTypes.ADF_SCRIPT_ACTIVITY,
    "WebActivity": DataJobSubTypes.ADF_WEB_ACTIVITY,
    "WebHook": DataJobSubTypes.ADF_WEBHOOK_ACTIVITY,
    "IfCondition": DataJobSubTypes.ADF_IF_CONDITION,
    "ForEach": DataJobSubTypes.ADF_FOREACH_LOOP,
    "Until": DataJobSubTypes.ADF_UNTIL_LOOP,
    "Wait": DataJobSubTypes.ADF_WAIT_ACTIVITY,
    "SetVariable": DataJobSubTypes.ADF_SET_VARIABLE,
    "AppendVariable": DataJobSubTypes.ADF_APPEND_VARIABLE,
    "Switch": DataJobSubTypes.ADF_SWITCH_ACTIVITY,
    "Filter": DataJobSubTypes.ADF_FILTER_ACTIVITY,
    "Validation": DataJobSubTypes.ADF_VALIDATION_ACTIVITY,
    "DatabricksNotebook": DataJobSubTypes.ADF_DATABRICKS_NOTEBOOK,
    "DatabricksSparkJar": DataJobSubTypes.ADF_DATABRICKS_SPARK_JAR,
    "DatabricksSparkPython": DataJobSubTypes.ADF_DATABRICKS_SPARK_PYTHON,
    "HDInsightHive": DataJobSubTypes.ADF_HDINSIGHT_HIVE,
    "HDInsightPig": DataJobSubTypes.ADF_HDINSIGHT_PIG,
    "HDInsightSpark": DataJobSubTypes.ADF_HDINSIGHT_SPARK,
    "HDInsightMapReduce": DataJobSubTypes.ADF_HDINSIGHT_MAPREDUCE,
    "HDInsightStreaming": DataJobSubTypes.ADF_HDINSIGHT_STREAMING,
    "AzureFunctionActivity": DataJobSubTypes.ADF_AZURE_FUNCTION_ACTIVITY,
    "AzureMLBatchExecution": DataJobSubTypes.ADF_AZURE_ML_BATCH,
    "AzureMLUpdateResource": DataJobSubTypes.ADF_AZURE_ML_UPDATE,
    "AzureMLExecutePipeline": DataJobSubTypes.ADF_AZURE_ML_PIPELINE,
    "Custom": DataJobSubTypes.ADF_CUSTOM_ACTIVITY,
    "Delete": DataJobSubTypes.ADF_DELETE_ACTIVITY,
    "SynapseNotebook": DataJobSubTypes.ADF_SYNAPSE_NOTEBOOK,
    "SparkJob": DataJobSubTypes.ADF_SPARK_JOB,
    "SynapseSparkJob": DataJobSubTypes.ADF_SYNAPSE_SPARK_JOB,
    "SqlPoolStoredProcedure": DataJobSubTypes.ADF_SQL_POOL_STORED_PROCEDURE,
    "Fail": DataJobSubTypes.ADF_FAIL_ACTIVITY,
}


class AzureDataFactoryContainerKey(ContainerKey):
    """Container key for Azure Data Factory resources."""

    resource_group: str
    factory_name: str


@platform_name("Azure Data Factory")
@config_class(AzureDataFactoryConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Extracts lineage from Copy and Data Flow activities",
    subtype_modifier=[
        SourceCapabilityModifier.ADF_COPY_ACTIVITY,
        SourceCapabilityModifier.ADF_DATA_FLOW_ACTIVITY,
    ],
)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.ADF_DATA_FACTORY,
    ],
)
class AzureDataFactorySource(StatefulIngestionSourceBase):
    """Extracts metadata and lineage from Azure Data Factory pipelines, activities, and datasets."""

    config: AzureDataFactoryConfig
    report: AzureDataFactorySourceReport
    platform: str = PLATFORM

    def __init__(self, config: AzureDataFactoryConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = AzureDataFactorySourceReport()

        # Initialize Azure client
        credential = config.credential.get_credential()
        self.client = AzureDataFactoryClient(
            credential=credential,
            subscription_id=config.subscription_id,
        )

        # Cache for datasets, linked services, data flows, pipelines, and triggers.
        # Structure: {factory_key: {resource_name: resource_object}}
        # - factory_key: "{resource_group}/{factory_name}" - uniquely identifies a factory
        # - resource_name: Name of the ADF resource (e.g., "MyDataset", "MyPipeline")
        # - resource_object: Parsed ADF resource model
        # These caches enable resolution of cross-references (e.g., dataset -> linked service)
        self._datasets_cache: dict[str, dict[str, DatasetResource]] = {}
        self._linked_services_cache: dict[str, dict[str, LinkedServiceResource]] = {}
        self._data_flows_cache: dict[str, dict[str, DataFlowResource]] = {}
        self._pipelines_cache: dict[str, dict[str, PipelineResource]] = {}
        self._triggers_cache: dict[str, list[TriggerResource]] = {}

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AzureDataFactorySource":
        config = AzureDataFactoryConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for all Azure Data Factory resources."""
        logger.info(
            f"Starting Azure Data Factory ingestion for subscription: {self.config.subscription_id}"
        )
        if self.config.resource_group:
            logger.info(f"Filtering to resource group: {self.config.resource_group}")

        # Fetch all Data Factories first
        try:
            factories: list[Factory] = list(
                self.client.get_factories(resource_group=self.config.resource_group)
            )
        except Exception as e:
            self.report.report_failure(
                title="Failed to List Data Factories",
                message="Unable to retrieve Data Factories from Azure. Check permissions and subscription ID.",
                context=f"subscription={self.config.subscription_id}",
                exc=e,
            )
            return

        # Process each factory independently
        for factory in factories:
            self.report.report_api_call()

            # SDK attributes can be None - skip factories with missing required fields
            factory_name = factory.name
            factory_id = factory.id
            if not factory_name or not factory_id:
                logger.warning(f"Skipping factory with missing name or id: {factory}")
                continue

            # Check if factory matches pattern
            if not self.config.factory_pattern.allowed(factory_name):
                self.report.report_factory_filtered(factory_name)
                continue

            try:
                self.report.report_factory_scanned()
                logger.info(f"Processing Data Factory: {factory_name}")

                # Extract resource group from factory ID
                # Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/...
                resource_group = self._extract_resource_group(factory_id)

                # Cache datasets and linked services for this factory
                self._cache_factory_resources(resource_group, factory_name)

                # Emit factory as container and get the Container object for browse paths
                container, container_workunits = self._emit_factory(
                    factory, resource_group
                )
                yield from container_workunits

                # Process pipelines, passing the Container for proper browse path hierarchy
                yield from self._process_pipelines(factory, resource_group, container)

                # Process execution history if enabled
                if self.config.include_execution_history:
                    yield from self._process_execution_history(factory, resource_group)

            except Exception as e:
                self.report.report_warning(
                    title="Failed to Process Data Factory",
                    message="Error processing Data Factory. Skipping to next.",
                    context=f"factory={factory_name}",
                    exc=e,
                )

    def _extract_resource_group(self, resource_id: str) -> str:
        """Extract resource group name from Azure resource ID."""
        # Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/...
        parts = resource_id.split("/")
        try:
            rg_index = parts.index("resourceGroups")
            return parts[rg_index + 1]
        except (ValueError, IndexError):
            logger.warning(f"Could not extract resource group from: {resource_id}")
            return "unknown"

    def _cache_factory_resources(self, resource_group: str, factory_name: str) -> None:
        """Cache datasets, linked services, triggers, and data flows for a factory.

        Exceptions propagate to the parent handler which handles them at the
        Data Factory level.
        """
        factory_key = f"{resource_group}/{factory_name}"

        # Cache datasets (needed for lineage resolution)
        if self.config.include_lineage:
            self._datasets_cache[factory_key] = {}
            for dataset in self.client.get_datasets(resource_group, factory_name):
                self.report.report_api_call()
                self.report.report_dataset_scanned()
                if dataset.name:  # Skip datasets with no name
                    self._datasets_cache[factory_key][dataset.name] = dataset

        # Cache linked services (needed for lineage resolution - maps datasets to platforms)
        if self.config.include_lineage:
            self._linked_services_cache[factory_key] = {}
            for ls in self.client.get_linked_services(resource_group, factory_name):
                self.report.report_api_call()
                self.report.report_linked_service_scanned()
                if ls.name:  # Skip linked services with no name
                    self._linked_services_cache[factory_key][ls.name] = ls

        # Cache triggers (for custom properties on pipelines)
        self._triggers_cache[factory_key] = []
        for trigger in self.client.get_triggers(resource_group, factory_name):
            self.report.report_api_call()
            self.report.report_trigger_scanned()
            self._triggers_cache[factory_key].append(trigger)

        # Cache data flows (for lineage extraction from Data Flow activities)
        if self.config.include_lineage:
            self._data_flows_cache[factory_key] = {}
            for data_flow in self.client.get_data_flows(resource_group, factory_name):
                self.report.report_api_call()
                self.report.report_data_flow_scanned()
                if data_flow.name:  # Skip data flows with no name
                    self._data_flows_cache[factory_key][data_flow.name] = data_flow

    def _emit_factory(
        self, factory: Factory, resource_group: str
    ) -> tuple[Container, Iterable[MetadataWorkUnit]]:
        """Emit a Data Factory as a Container.

        Returns:
            Tuple of (Container object, workunits). The Container object is needed
            by child entities (DataFlows) to properly set up browse paths.
        """
        factory_name = factory.name or "Unknown"
        factory_id = factory.id or ""

        container_key = AzureDataFactoryContainerKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            resource_group=resource_group,
            factory_name=factory_name,
            env=self.config.env,
        )

        # Build custom properties
        custom_props: dict[str, str] = {}
        if factory_id:
            custom_props["azure_resource_id"] = factory_id
        if factory.location:
            custom_props["location"] = factory.location
        if factory.tags:
            for key, value in factory.tags.items():
                custom_props[f"tag:{key}"] = value
        if factory.provisioning_state:
            custom_props["provisioning_state"] = factory.provisioning_state

        container = Container(
            container_key,
            display_name=factory_name,
            description=f"Azure Data Factory: {factory_name}",
            subtype=FlowContainerSubTypes.ADF_DATA_FACTORY,
            external_url=self._get_factory_url(factory, resource_group),
            extra_properties=custom_props,
            parent_container=None,  # Top-level container
        )

        return container, container.as_workunits()

    def _get_factory_url(self, factory: Factory, resource_group: str) -> str:
        """Generate Azure Portal URL for a Data Factory."""
        return (
            f"https://adf.azure.com/en/home"
            f"?factory=/subscriptions/{self.config.subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{factory.name}"
        )

    def _process_pipelines(
        self, factory: Factory, resource_group: str, container: Container
    ) -> Iterable[MetadataWorkUnit]:
        """Process all pipelines in a factory using two-pass approach.

        First pass: Fetch and cache all pipelines for the factory.
        Second pass: Process pipelines and emit entities with proper lineage.

        This two-pass approach enables ExecutePipeline activities to reference
        child pipelines that may not have been processed yet.

        Args:
            factory: The Data Factory
            resource_group: Azure resource group name
            container: The parent Container object (for browse path hierarchy)
        """
        factory_name = factory.name or "Unknown"
        factory_key = f"{resource_group}/{factory_name}"

        # First pass: Cache all pipelines for this factory
        self._pipelines_cache[factory_key] = {}
        try:
            for pipeline in self.client.get_pipelines(resource_group, factory_name):
                self.report.report_api_call()
                if pipeline.name:  # Skip pipelines with no name
                    self._pipelines_cache[factory_key][pipeline.name] = pipeline
        except Exception as e:
            self.report.report_warning(
                title="Failed to List Pipelines",
                message="Unable to retrieve pipelines from factory.",
                context=f"factory={factory_name}",
                exc=e,
            )
            return  # Can't process pipelines if we can't list them

        # Second pass: Process pipelines and emit entities
        for pipeline_name, pipeline in self._pipelines_cache[factory_key].items():
            # Check if pipeline matches pattern
            if not self.config.pipeline_pattern.allowed(pipeline_name):
                self.report.report_pipeline_filtered(pipeline_name)
                continue

            self.report.report_pipeline_scanned()
            logger.debug(f"Processing pipeline: {factory_name}/{pipeline_name}")

            # Emit pipeline as DataFlow, passing the Container for proper browse paths
            dataflow = self._create_dataflow(
                pipeline, factory, resource_group, container
            )
            yield from dataflow.as_workunits()

            # Emit activities as DataJobs
            for activity in pipeline.activities or []:
                self.report.report_activity_scanned()

                datajob = self._create_datajob(
                    activity,
                    pipeline,
                    factory,
                    resource_group,
                    dataflow,
                    factory_key,
                )
                yield from datajob.as_workunits()

                # Emit dataTransformLogic for Data Flow activities
                if activity.type == "ExecuteDataFlow":
                    yield from self._emit_data_flow_script(
                        activity, datajob, factory_key
                    )

                # Emit pipeline-to-pipeline lineage for ExecutePipeline activities
                if activity.type == "ExecutePipeline":
                    yield from self._emit_pipeline_lineage(
                        activity, datajob, factory, factory_key
                    )

    def _create_dataflow(
        self,
        pipeline: PipelineResource,
        factory: Factory,
        resource_group: str,
        container: Container,
    ) -> DataFlow:
        """Create a DataFlow entity for a pipeline.

        Args:
            pipeline: The ADF pipeline
            factory: The parent Data Factory
            resource_group: Azure resource group name
            container: The parent Container object (enables proper browse path hierarchy)
        """
        factory_name = factory.name or "Unknown"
        pipeline_name = pipeline.name or "Unknown"

        # Build flow name with factory prefix for uniqueness across factories
        flow_name = f"{factory_name}.{pipeline_name}"

        # Custom properties
        custom_props: dict[str, str] = {
            "factory_name": factory_name,
        }
        if pipeline.id:
            custom_props["azure_resource_id"] = pipeline.id

        # Extract properties (PipelineResource has them at root level)
        if pipeline.concurrency:
            custom_props["concurrency"] = str(pipeline.concurrency)
        if pipeline.folder:
            folder_name = pipeline.folder.name if pipeline.folder.name else ""
            if folder_name:
                custom_props["folder"] = folder_name
        if pipeline.annotations:
            # annotations is list[Any] per SDK - convert to strings for display
            custom_props["annotations"] = ", ".join(
                str(a) for a in pipeline.annotations
            )
        description: Optional[str] = pipeline.description

        # Add trigger info if available
        triggers = self._get_pipeline_triggers(
            resource_group, factory_name, pipeline_name
        )
        if triggers:
            custom_props["triggers"] = ", ".join(triggers)

        # Pass the Container object directly so the SDK can properly build
        # browse paths by inheriting from the parent container's path
        dataflow = DataFlow(
            platform=PLATFORM,
            name=flow_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=pipeline_name,
            description=description,
            external_url=self._get_pipeline_url(factory, resource_group, pipeline_name),
            custom_properties=custom_props,
            parent_container=container,
        )

        return dataflow

    def _get_pipeline_triggers(
        self, resource_group: str, factory_name: str, pipeline_name: str
    ) -> list[str]:
        """Get trigger names associated with a pipeline."""
        factory_key = f"{resource_group}/{factory_name}"
        triggers = self._triggers_cache.get(factory_key, [])

        result: list[str] = []
        for trigger in triggers:
            # Check if trigger references this pipeline
            # Not all trigger types have pipelines (e.g., TumblingWindowTrigger)
            pipelines = getattr(trigger.properties, "pipelines", None) or []
            for pipeline_ref in pipelines:
                ref = pipeline_ref.pipeline_reference
                ref_name = ref.reference_name if ref else ""
                if ref_name == pipeline_name and trigger.name:
                    result.append(trigger.name)
                    break

        return result

    def _get_pipeline_url(
        self, factory: Factory, resource_group: str, pipeline_name: str
    ) -> str:
        """Generate Azure Portal URL for a pipeline."""
        return (
            f"https://adf.azure.com/en/authoring/pipeline/{pipeline_name}"
            f"?factory=/subscriptions/{self.config.subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{factory.name}"
        )

    def _create_datajob(
        self,
        activity: Activity,
        pipeline: PipelineResource,
        factory: Factory,
        resource_group: str,
        dataflow: DataFlow,
        factory_key: str,
    ) -> DataJob:
        """Create a DataJob entity for an activity."""
        activity_type = activity.type or "Unknown"
        activity_name = activity.name or "Unknown"

        # Determine activity subtype
        subtype = ACTIVITY_SUBTYPE_MAP.get(activity_type, activity_type)

        # Custom properties
        custom_props: dict[str, str] = {
            "activity_type": activity_type,
        }
        if activity.description:
            custom_props["activity_description"] = activity.description

        # Extract lineage (inlets/outlets)
        inlets: Optional[list[DatasetUrnOrStr]] = None
        outlets: Optional[list[DatasetUrnOrStr]] = None

        if self.config.include_lineage:
            extracted_inlets = self._extract_activity_inputs(activity, factory_key)
            extracted_outlets = self._extract_activity_outputs(activity, factory_key)
            if extracted_inlets:
                inlets = extracted_inlets
            if extracted_outlets:
                outlets = extracted_outlets

        # Create DataJob with external URL to the parent pipeline
        # (ADF doesn't have direct activity URLs, so we link to the pipeline)
        pipeline_name = pipeline.name or "Unknown"
        datajob = DataJob(
            name=activity_name,
            flow=dataflow,
            display_name=activity_name,
            description=activity.description,
            external_url=self._get_pipeline_url(factory, resource_group, pipeline_name),
            custom_properties=custom_props,
            subtype=subtype,
            inlets=inlets,
            outlets=outlets,
        )

        return datajob

    def _extract_activity_inputs(
        self, activity: Activity, factory_key: str
    ) -> list[DatasetUrnOrStr]:
        """Extract input dataset URNs from an activity."""
        inputs: list[DatasetUrnOrStr] = []

        # Process explicit inputs (for Copy activities and others)
        # Note: Only some activity types (e.g., CopyActivity) have inputs/outputs
        for input_ref in getattr(activity, "inputs", None) or []:
            dataset_urn = self._resolve_dataset_urn(
                input_ref.reference_name, factory_key
            )
            if dataset_urn:
                inputs.append(str(dataset_urn))
                self.report.report_lineage_extracted("dataset")

        # Process Data Flow activities - extract sources as inputs
        if activity.type == "ExecuteDataFlow":
            data_flow_inputs = self._extract_data_flow_sources(activity, factory_key)
            inputs.extend(data_flow_inputs)

        # Process source in typeProperties (for Copy activities)
        # SDK CopyActivity has source attribute directly, not in type_properties dict
        source = getattr(activity, "source", None)
        if source:
            dataset_settings = getattr(source, "dataset_settings", None)
            if dataset_settings:
                # Inline dataset configuration
                pass  # Complex case, skip for now
            # Source might reference a dataset in storeSettings
            store_settings = getattr(source, "store_settings", None)
            if store_settings and getattr(store_settings, "linked_service_name", None):
                # Could resolve to a dataset if we have schema info
                pass

        return inputs

    def _extract_activity_outputs(
        self, activity: Activity, factory_key: str
    ) -> list[DatasetUrnOrStr]:
        """Extract output dataset URNs from an activity."""
        outputs: list[DatasetUrnOrStr] = []

        # Process explicit outputs (for Copy activities and others)
        # Note: Only some activity types (e.g., CopyActivity) have inputs/outputs
        for output_ref in getattr(activity, "outputs", None) or []:
            dataset_urn = self._resolve_dataset_urn(
                output_ref.reference_name, factory_key
            )
            if dataset_urn:
                outputs.append(str(dataset_urn))
                self.report.report_lineage_extracted("dataset")

        # Process Data Flow activities - extract sinks as outputs
        if activity.type == "ExecuteDataFlow":
            data_flow_outputs = self._extract_data_flow_sinks(activity, factory_key)
            outputs.extend(data_flow_outputs)

        # Process sink in typeProperties (for Copy activities)
        # SDK CopyActivity has sink attribute directly, not in type_properties dict
        sink = getattr(activity, "sink", None)
        if sink:
            dataset_settings = getattr(sink, "dataset_settings", None)
            if dataset_settings:
                # Inline dataset configuration
                pass  # Complex case, skip for now

        return outputs

    def _get_data_flow_name_from_activity(
        self, activity: Activity, factory_key: str
    ) -> Optional[str]:
        """Get the Data Flow name referenced by an ExecuteDataFlow activity.

        Due to a case-sensitivity bug in the Azure SDK where it expects
        'typeProperties.dataFlow' but the API returns 'typeProperties.dataflow',
        we try multiple approaches to find the Data Flow name.

        Args:
            activity: The ExecuteDataFlow activity
            factory_key: Factory key for cache lookup

        Returns:
            Data Flow name if found, None otherwise
        """
        # Approach 1: SDK ExecuteDataFlowActivity has data_flow attribute directly
        data_flow_ref = getattr(activity, "data_flow", None)
        if data_flow_ref:
            # DataFlowReference has reference_name attribute
            name = getattr(data_flow_ref, "reference_name", None)
            if name:
                return name

        # Approach 2: Try to match activity name to Data Flow name
        # Many users name their activity similarly to the Data Flow
        data_flows = self._data_flows_cache.get(factory_key, {})

        # Exact match
        if activity.name in data_flows:
            logger.debug(
                f"Found Data Flow by exact activity name match: {activity.name}"
            )
            return activity.name

        # Fuzzy match - try removing common suffixes/variations
        activity_name_normalized = activity.name.replace(" ", "").lower()
        for df_name in data_flows:
            df_name_normalized = df_name.replace(" ", "").lower()
            if activity_name_normalized == df_name_normalized:
                logger.debug(
                    f"Found Data Flow by fuzzy match: activity='{activity.name}' -> dataflow='{df_name}'"
                )
                return df_name

        return None

    def _emit_data_flow_script(
        self, activity: Activity, datajob: DataJob, factory_key: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit the Data Flow script as a dataTransformLogic aspect.

        For ExecuteDataFlow activities, this extracts the Data Flow DSL script
        and emits it as a transformation aspect, making it viewable in the UI.

        Args:
            activity: The ExecuteDataFlow activity
            datajob: The DataJob entity for this activity
            factory_key: Factory key for cache lookup

        Yields:
            MetadataWorkUnit for the dataTransformLogic aspect
        """
        # Get the Data Flow name
        data_flow_name = self._get_data_flow_name_from_activity(activity, factory_key)
        if not data_flow_name:
            return

        # Look up the Data Flow definition
        data_flows = self._data_flows_cache.get(factory_key, {})
        data_flow = data_flows.get(data_flow_name)
        if not data_flow or not data_flow.properties:
            return

        # Get the script from the Data Flow (join script_lines or use script)
        # Note: script_lines/script are on MappingDataFlow, not base DataFlow
        props = data_flow.properties
        script_lines = getattr(props, "script_lines", None)
        script = (
            "\n".join(script_lines) if script_lines else getattr(props, "script", None)
        )
        if not script:
            logger.debug(f"No script found for Data Flow: {data_flow_name}")
            return

        # Emit the dataTransformLogic aspect
        # Note: Using SQL as language because UNKNOWN is not yet broadly supported
        # in the UI. The Data Flow DSL is similar to SQL in structure.
        logger.debug(
            f"Emitting Data Flow script for activity '{activity.name}' "
            f"({len(script)} chars)"
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=str(datajob.urn),
            aspect=DataTransformLogicClass(
                transforms=[
                    DataTransformClass(
                        queryStatement=QueryStatementClass(
                            value=script,
                            language=QueryLanguageClass.SQL,
                        )
                    )
                ]
            ),
        ).as_workunit()

    def _extract_data_flow_endpoints(
        self, activity: Activity, factory_key: str, endpoint_type: str
    ) -> list[str]:
        """Extract source or sink dataset URNs from a Data Flow activity.

        Data Flow activities reference a Data Flow definition which contains
        sources (inputs) and sinks (outputs). This method extracts either based
        on the endpoint_type parameter.

        Args:
            activity: The ExecuteDataFlow activity
            factory_key: Factory key for cache lookup
            endpoint_type: "sources" or "sinks"

        Returns:
            List of dataset URNs for the specified endpoint type
        """
        urns: list[str] = []

        # Get the Data Flow name using our robust lookup
        data_flow_name = self._get_data_flow_name_from_activity(activity, factory_key)

        if not data_flow_name:
            logger.debug(
                f"Could not find Data Flow reference for activity: {activity.name}"
            )
            return urns

        # Look up the Data Flow definition
        data_flows = self._data_flows_cache.get(factory_key, {})
        data_flow = data_flows.get(data_flow_name)

        if not data_flow:
            logger.debug(f"Data Flow not found in cache: {data_flow_name}")
            return urns

        # Extract endpoints from the Data Flow
        if data_flow.properties:
            endpoints = getattr(data_flow.properties, endpoint_type, [])
            endpoint_label = endpoint_type[:-1]  # "sources" -> "source"
            for endpoint in endpoints:
                if endpoint.dataset:
                    dataset_urn = self._resolve_dataset_urn(
                        endpoint.dataset.reference_name, factory_key
                    )
                    if dataset_urn:
                        urns.append(str(dataset_urn))
                        self.report.report_lineage_extracted("dataflow")
                        logger.debug(
                            f"Extracted Data Flow {endpoint_label}: {endpoint.name} -> {dataset_urn}"
                        )

        return urns

    def _extract_data_flow_sources(
        self, activity: Activity, factory_key: str
    ) -> list[str]:
        """Extract source dataset URNs from a Data Flow activity."""
        return self._extract_data_flow_endpoints(activity, factory_key, "sources")

    def _extract_data_flow_sinks(
        self, activity: Activity, factory_key: str
    ) -> list[str]:
        """Extract sink dataset URNs from a Data Flow activity."""
        return self._extract_data_flow_endpoints(activity, factory_key, "sinks")

    def _emit_pipeline_lineage(
        self,
        activity: Activity,
        datajob: DataJob,
        factory: Factory,
        factory_key: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit pipeline-to-pipeline lineage for ExecutePipeline activities.

        When a pipeline calls another pipeline via ExecutePipeline activity,
        we create a DataJob-to-DataJob dependency from the calling activity
        to the first activity in the child pipeline. This creates visible
        lineage edges in the DataHub UI.

        Args:
            activity: The ExecutePipeline activity
            datajob: The DataJob entity for this activity
            factory: The parent Data Factory
            factory_key: Factory key for URN construction

        Yields:
            MetadataWorkUnit for the pipeline dependency
        """
        # SDK ExecutePipelineActivity has pipeline attribute directly
        pipeline_ref = getattr(activity, "pipeline", None)
        if not pipeline_ref:
            return

        # PipelineReference has reference_name attribute
        child_pipeline_name = getattr(pipeline_ref, "reference_name", None)
        if not child_pipeline_name:
            logger.debug(
                f"ExecutePipeline activity {activity.name} has no pipeline reference"
            )
            return

        # Build the child pipeline's DataFlow URN
        child_flow_id = f"{factory.name}.{child_pipeline_name}"
        child_flow_urn = DataFlowUrn.create_from_ids(
            orchestrator=PLATFORM,
            flow_id=child_flow_id,
            env=self.config.env,
        )

        # Look up child pipeline from cache to get its first activity
        pipelines = self._pipelines_cache.get(factory_key, {})
        child_pipeline = pipelines.get(child_pipeline_name)

        child_datajob_urn: Optional[DataJobUrn] = None
        first_activity_name: Optional[str] = None

        if child_pipeline:
            activities = child_pipeline.activities
            if activities and activities[0].name:
                first_activity_name = activities[0].name
                child_datajob_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(child_flow_urn),
                    job_id=first_activity_name,
                )
                logger.debug(
                    f"ExecutePipeline {activity.name} -> {child_pipeline_name}."
                    f"{first_activity_name} (URN: {child_datajob_urn})"
                )
        else:
            logger.debug(
                f"Child pipeline {child_pipeline_name} not found in cache or has no activities"
            )

        # Update custom properties to include the child pipeline reference
        current_props = datajob.custom_properties
        current_props["calls_pipeline"] = child_pipeline_name
        current_props["child_pipeline_urn"] = str(child_flow_urn)
        if first_activity_name:
            current_props["child_first_activity"] = first_activity_name
        datajob.set_custom_properties(current_props)

        self.report.report_lineage_extracted("pipeline")

        # Emit DataJobInputOutput on the CHILD's first activity, setting ExecutePipeline as upstream
        # This creates lineage: ExecutePipeline -> ChildFirstActivity
        # (The parent activity triggers the child, so parent is upstream of child)
        if child_datajob_urn:
            yield MetadataChangeProposalWrapper(
                entityUrn=str(child_datajob_urn),  # Child's first activity
                aspect=DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=[],
                    inputDatajobs=[
                        str(datajob.urn)
                    ],  # ExecutePipeline as input/upstream
                ),
            ).as_workunit()

    def _resolve_dataset_urn(
        self, dataset_name: str, factory_key: str
    ) -> Optional[DatasetUrn]:
        """Resolve an ADF dataset reference to a DataHub DatasetUrn."""
        # Get dataset from cache
        datasets = self._datasets_cache.get(factory_key, {})
        dataset = datasets.get(dataset_name)

        if not dataset:
            logger.debug(f"Dataset not found in cache: {dataset_name}")
            return None

        # Get linked service to determine platform
        linked_service_ref = dataset.properties.linked_service_name
        if not linked_service_ref or not linked_service_ref.reference_name:
            self.report.report_unmapped_platform(dataset_name, "unknown")
            return None

        ls_ref_name = linked_service_ref.reference_name
        linked_services = self._linked_services_cache.get(factory_key, {})
        linked_service = linked_services.get(ls_ref_name)

        if not linked_service:
            self.report.report_unmapped_platform(dataset_name, "unknown")
            return None

        # Map linked service type to DataHub platform
        ls_type = linked_service.properties.type if linked_service.properties else None
        if not ls_type:
            self.report.report_unmapped_platform(dataset_name, "unknown")
            return None

        platform = LINKED_SERVICE_PLATFORM_MAP.get(ls_type)

        if not platform:
            self.report.report_unmapped_platform(dataset_name, ls_type)
            return None

        # Build dataset name from type properties
        table_name = self._extract_table_name(dataset, linked_service)
        if not table_name:
            table_name = dataset_name  # Fallback to ADF dataset name

        # Check if there's a platform instance mapping
        platform_instance = self.config.platform_instance_map.get(ls_ref_name)

        return DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=table_name,
            env=self.config.env,
            platform_instance=platform_instance,
        )

    def _extract_table_name(
        self, dataset: DatasetResource, linked_service: LinkedServiceResource
    ) -> Optional[str]:
        """Extract table/file name from dataset properties.

        SDK dataset subclasses have type-specific properties as direct attributes
        (e.g., table_name, table, schema_type_properties_schema, file_name, etc.)
        """
        props = dataset.properties

        # SQL-like datasets - check for table_name or table attributes
        table_name = getattr(props, "table_name", None)
        if table_name:
            return str(table_name) if not isinstance(table_name, str) else table_name

        table = getattr(props, "table", None)
        if table:
            return str(table) if not isinstance(table, str) else table

        # Structured table reference (schema.table)
        schema = getattr(props, "schema_type_properties_schema", None)
        if schema and table:
            return f"{schema}.{table}"

        # File-based datasets
        file_name = getattr(props, "file_name", None)
        if file_name:
            folder_path = getattr(props, "folder_path", None)
            if folder_path and file_name:
                return f"{folder_path}/{file_name}"
            return str(file_name) if not isinstance(file_name, str) else file_name

        # Container/path based (e.g., DelimitedTextDataset with location)
        location = getattr(props, "location", None)
        if location:
            container = getattr(location, "container", None)
            folder = getattr(location, "folder_path", None)
            filename = getattr(location, "file_name", None)
            parts = [str(p) for p in [container, folder, filename] if p]
            if parts:
                return "/".join(parts)

        return None

    def _process_execution_history(
        self, factory: Factory, resource_group: str
    ) -> Iterable[MetadataWorkUnit]:
        """Process pipeline execution history for a Data Factory."""
        factory_name = factory.name or "Unknown"
        logger.info(
            f"Fetching execution history for Data Factory: {factory_name} "
            f"(last {self.config.execution_history_days} days)"
        )

        try:
            pipeline_runs: list[PipelineRun] = list(
                self.client.get_pipeline_runs(
                    resource_group,
                    factory_name,
                    days=self.config.execution_history_days,
                )
            )
        except Exception as e:
            self.report.report_warning(
                title="Failed to Fetch Execution History",
                message="Unable to retrieve pipeline runs.",
                context=f"factory={factory_name}",
                exc=e,
            )
            return

        for pipeline_run in pipeline_runs:
            self.report.report_api_call()
            self.report.report_pipeline_run_scanned()

            # Skip runs with missing required fields
            if not pipeline_run.pipeline_name or not pipeline_run.run_id:
                continue

            # Check if pipeline matches pattern
            if not self.config.pipeline_pattern.allowed(pipeline_run.pipeline_name):
                continue

            yield from self._emit_pipeline_run(pipeline_run, factory, resource_group)

    def _emit_pipeline_run(
        self,
        pipeline_run: PipelineRun,
        factory: Factory,
        resource_group: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a pipeline run as DataProcessInstance."""
        factory_name = factory.name or "Unknown"
        pipeline_name = pipeline_run.pipeline_name or "Unknown"
        run_id = pipeline_run.run_id or "Unknown"
        status = pipeline_run.status or "Unknown"

        # Build DataFlow URN for the template - include factory name for uniqueness
        flow_name = f"{factory_name}.{pipeline_name}"
        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator=PLATFORM,
            flow_id=flow_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Map ADF status to InstanceRunResult
        result = self._map_run_status(status)

        # Build custom properties
        properties: dict[str, str] = {
            "run_id": run_id,
            "status": status,
        }
        if pipeline_run.message:
            properties["message"] = pipeline_run.message[:MAX_RUN_MESSAGE_LENGTH]
        if pipeline_run.invoked_by:
            invoker_name = pipeline_run.invoked_by.get("name", "")
            invoker_type = pipeline_run.invoked_by.get("invokedByType", "")
            if invoker_name:
                properties["invoked_by"] = invoker_name
            if invoker_type:
                properties["invoked_by_type"] = invoker_type
        if pipeline_run.parameters:
            for key, value in list(pipeline_run.parameters.items())[
                :MAX_RUN_PARAMETERS
            ]:
                properties[f"param:{key}"] = str(value)[:MAX_PARAMETER_VALUE_LENGTH]

        # Create DataProcessInstance
        dpi = DataProcessInstance(
            id=run_id,
            orchestrator=PLATFORM,
            cluster=self.config.env,
            type=DataProcessTypeClass.BATCH_SCHEDULED,
            template_urn=flow_urn,
            properties=properties,
            url=self._get_pipeline_run_url(factory, resource_group, run_id),
            data_platform_instance=self.config.platform_instance,
        )

        # Emit the instance
        for mcp in dpi.generate_mcp(
            created_ts_millis=(
                int(pipeline_run.run_start.timestamp() * 1000)
                if pipeline_run.run_start
                else None
            ),
            materialize_iolets=False,
        ):
            yield mcp.as_workunit()

        # Emit start event
        if pipeline_run.run_start:
            start_ts = int(pipeline_run.run_start.timestamp() * 1000)
            for mcp in dpi.start_event_mcp(start_ts):
                yield mcp.as_workunit()

        # Emit end event if run is complete
        if pipeline_run.run_end and result:
            end_ts = int(pipeline_run.run_end.timestamp() * 1000)
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=end_ts,
                result=result,
                result_type=pipeline_run.status,
            ):
                yield mcp.as_workunit()

        # Emit activity runs for this pipeline run
        yield from self._emit_activity_runs(pipeline_run, factory, resource_group)

    def _map_run_status(self, status: str) -> Optional[InstanceRunResult]:
        """Map ADF run status to DataHub InstanceRunResult."""
        status_map = {
            "Succeeded": InstanceRunResult.SUCCESS,
            "Failed": InstanceRunResult.FAILURE,
            "Cancelled": InstanceRunResult.SKIPPED,
            "Cancelling": None,  # Still running
            "InProgress": None,  # Still running
            "Queued": None,  # Not started
        }
        return status_map.get(status)

    def _get_pipeline_run_url(
        self, factory: Factory, resource_group: str, run_id: str
    ) -> str:
        """Generate Azure Portal URL for a pipeline run."""
        factory_name = factory.name or "Unknown"
        return (
            f"https://adf.azure.com/en/monitoring/pipelineruns/{run_id}"
            f"?factory=/subscriptions/{self.config.subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{factory_name}"
        )

    def _emit_activity_runs(
        self,
        pipeline_run: PipelineRun,
        factory: Factory,
        resource_group: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit activity runs as DataProcessInstance for each DataJob."""
        factory_name = factory.name or "Unknown"
        pipeline_run_id = pipeline_run.run_id or "Unknown"

        try:
            for activity_run in self.client.get_activity_runs(
                resource_group,
                factory_name,
                pipeline_run_id,
            ):
                self.report.report_api_call()
                self.report.report_activity_run_scanned()

                # Skip activity runs with missing required fields
                activity_name = activity_run.activity_name
                activity_run_id = activity_run.activity_run_id
                if not activity_name or not activity_run_id:
                    continue

                activity_pipeline = activity_run.pipeline_name or "Unknown"
                activity_status = activity_run.status or "Unknown"

                # Build DataJob URN for the template
                flow_name = f"{factory_name}.{activity_pipeline}"
                flow_urn = DataFlowUrn.create_from_ids(
                    orchestrator=PLATFORM,
                    flow_id=flow_name,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                job_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(flow_urn),
                    job_id=activity_name,
                )

                # Map ADF status to InstanceRunResult
                result = self._map_run_status(activity_status)

                # Build custom properties
                properties: dict[str, str] = {
                    "activity_run_id": activity_run_id,
                    "status": activity_status,
                }
                if activity_run.activity_type:
                    properties["activity_type"] = activity_run.activity_type
                if activity_run.pipeline_run_id:
                    properties["pipeline_run_id"] = activity_run.pipeline_run_id
                if activity_run.duration_in_ms is not None:
                    properties["duration_ms"] = str(activity_run.duration_in_ms)
                if activity_run.error:
                    error_msg = str(activity_run.error.get("message", ""))
                    if error_msg:
                        properties["error"] = error_msg[:MAX_RUN_MESSAGE_LENGTH]

                # Create DataProcessInstance linked to DataJob
                dpi = DataProcessInstance(
                    id=activity_run_id,
                    orchestrator=PLATFORM,
                    cluster=self.config.env,
                    type=DataProcessTypeClass.BATCH_SCHEDULED,
                    template_urn=job_urn,
                    properties=properties,
                    url=self._get_pipeline_run_url(
                        factory, resource_group, pipeline_run_id
                    ),
                    data_platform_instance=self.config.platform_instance,
                )

                # Emit the instance
                for mcp in dpi.generate_mcp(
                    created_ts_millis=(
                        int(activity_run.activity_run_start.timestamp() * 1000)
                        if activity_run.activity_run_start
                        else None
                    ),
                    materialize_iolets=False,
                ):
                    yield mcp.as_workunit()

                # Emit start event
                if activity_run.activity_run_start:
                    start_ts = int(activity_run.activity_run_start.timestamp() * 1000)
                    for mcp in dpi.start_event_mcp(start_ts):
                        yield mcp.as_workunit()

                # Emit end event if run is complete
                if activity_run.activity_run_end and result:
                    end_ts = int(activity_run.activity_run_end.timestamp() * 1000)
                    for mcp in dpi.end_event_mcp(
                        end_timestamp_millis=end_ts,
                        result=result,
                        result_type=activity_run.status,
                    ):
                        yield mcp.as_workunit()

        except Exception as e:
            logger.warning(
                f"Failed to fetch activity runs for pipeline run {pipeline_run.run_id}: {e}"
            )

    def get_report(self) -> AzureDataFactorySourceReport:
        return self.report

    def close(self) -> None:
        """Clean up resources."""
        self.client.close()
        super().close()
