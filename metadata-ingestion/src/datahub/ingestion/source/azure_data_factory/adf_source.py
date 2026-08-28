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
import re
from collections import deque
from typing import Any, Iterable, Optional

import sqlglot
import sqlglot.expressions as sqlglot_exp
from azure.mgmt.datafactory.models import (
    Activity,
    ActivityRun,
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure.constants import ADF_LINKED_SERVICE_PLATFORM_MAP
from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from datahub.ingestion.source.azure_data_factory.adf_column_lineage import (
    ColumnLineageExtractor,
    CopyActivityColumnLineageExtractor,
    DatasetSchemaInfo,
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
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessTypeClass,
    DataTransformClass,
    DataTransformLogicClass,
    FineGrainedLineageClass,
    QueryLanguageClass,
    QueryStatementClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk._shared import DatasetUrnOrStr
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sql_parsing.sql_parsing_common import get_dialect_str
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

logger = logging.getLogger(__name__)

# Platform identifier for Azure Data Factory
PLATFORM = "azure-data-factory"

# Constants for pipeline run processing
MAX_RUN_MESSAGE_LENGTH = 500  # Truncate long error/status messages
MAX_RUN_PARAMETERS = 10  # Limit number of parameters to store
MAX_PARAMETER_VALUE_LENGTH = 100  # Truncate long parameter values

# Matches ADF dynamic content that reads a dataset's own parameter, e.g.
# "@dataset().table_name" -> group(1) = "table_name".
DYNAMIC_CONTENT_DATASET_PARAM_PATTERN = re.compile(r"^@dataset\(\)\.(\w+)$")
# Matches ADF dynamic content that reads a pipeline parameter, e.g.
# "@pipeline().parameters.SourceTable" -> group(1) = "SourceTable". Only
# resolvable using a specific pipeline run's actual parameter values
# (execution history), not from static definitions alone.
PIPELINE_PARAMETER_REFERENCE_PATTERN = re.compile(r"^@pipeline\(\)\.parameters\.(\w+)$")
# Matches ADF dynamic content that reads a factory-level *global*
# parameter, e.g. "@pipeline().globalParameters.databricks_workspace_url"
# -> group(1) = "databricks_workspace_url". Unlike pipeline parameters,
# these are tenant-wide literal constants - always resolvable without any
# run history, from the factory's global parameters definition alone.
GLOBAL_PARAMETER_REFERENCE_PATTERN = re.compile(
    r"^@pipeline\(\)\.globalParameters\.(\w+)$"
)
# Matches the ADO.NET connection-string convention for naming the target
# database, e.g. "...;Initial Catalog=MyDb;..." or "...;Database=MyDb;...".
CONNECTION_STRING_DATABASE_PATTERN = re.compile(
    r"(?:Initial Catalog|Database)\s*=\s*([^;]+)", re.IGNORECASE
)
# ADF's "inline expression" templating syntax (e.g. "@{linkedService().X}",
# "@dataset()...", "@pipeline()...") that Azure does not always evaluate
# before recording a resolved query/DDL statement on an ActivityRun -
# observed on some sink preCopyScript values even though ActivityRun's own
# source.query is always fully resolved. Never treat a match as a real
# identifier - it's the same class of bug as the original garbage-URN issue.
UNRESOLVED_ADF_EXPRESSION_PATTERN = re.compile(
    r"@\{|@dataset\(\)|@pipeline\(\)|@activity\(|@item\(\)|@linkedService\(\)"
)
# Matches a "parameterized" linked service referencing one of its own
# declared parameters inline, e.g. "@{linkedService().dbNameParam}" ->
# group(1) = "dbNameParam". The parameter's own declared default value
# (resolved separately) is the real value, not this template text.
LINKED_SERVICE_SELF_REFERENCE_PATTERN = re.compile(r"^@\{linkedService\(\)\.(\w+)\}$")
# Oracle's linked service exposes no database/catalog field at all - its
# identity (a TNS service name or SID) is baked into a free-form "server"
# connect string, conventionally built (via a concat() expression) from a
# linked service parameter with "service"/"sid" in its name. Best-effort
# only: if a tenant doesn't follow this naming convention, this simply
# finds nothing and the reference is left unresolved rather than wrong.
ORACLE_SERVICE_PARAM_HINT_PATTERN = re.compile(
    r"linkedService\(\)\.(\w*(?:service|sid)\w*)", re.IGNORECASE
)

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
@support_status(SupportStatus.BETA)
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
    SourceCapability.LINEAGE_FINE,
    "Extracts column-level lineage from Copy activities",
    subtype_modifier=[
        SourceCapabilityModifier.ADF_COPY_ACTIVITY,
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
        # Factory-level global parameters (tenant-wide literal constants,
        # e.g. shared server names/workspace URLs referenced via
        # "@pipeline().globalParameters.X") - unlike pipeline parameters,
        # these are always resolvable without any run history.
        self._global_parameters_cache: dict[str, dict[str, str]] = {}

        # Dataset URNs resolved only via execution-history pipeline-run
        # parameters (e.g. an activity dataset parameter set to
        # "@pipeline().parameters.X"), aggregated across processed runs for
        # later union into the DataJob's static lineage. Keyed by
        # (factory_key, pipeline_name, activity_name) -> (input urns, output urns).
        self._dynamic_lineage_cache: dict[
            tuple[str, str, str], tuple[set[str], set[str]]
        ] = {}

        # Column-level lineage extractors - extensible for different activity types
        self._column_lineage_extractors: list[ColumnLineageExtractor] = [
            CopyActivityColumnLineageExtractor(),
        ]

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AzureDataFactorySource":
        config = AzureDataFactoryConfig.model_validate(config_dict)
        return cls(config, ctx)

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
            self.report.failure(
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
                self.report.warning(
                    title="Failed to Process Data Factory",
                    message="Error processing Data Factory. Skipping to next.",
                    context=f"factory={factory_name}",
                    exc=e,
                    log=False,
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

        # Cache global parameters (tenant-wide literal constants referenced
        # via "@pipeline().globalParameters.X" - needed for lineage resolution)
        if self.config.include_lineage:
            self._global_parameters_cache[factory_key] = {}
            for gp in self.client.get_global_parameters(resource_group, factory_name):
                self.report.report_api_call()
                for name, spec in (gp.properties or {}).items():
                    value = getattr(spec, "value", None)
                    if isinstance(value, str):
                        self._global_parameters_cache[factory_key][name] = value

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
            self.report.warning(
                title="Failed to List Pipelines",
                message="Unable to retrieve pipelines from factory.",
                context=f"factory={factory_name}",
                exc=e,
                log=False,
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

            # Emit activities as DataJobs, using BFS to recurse into container
            # activities (ForEach, IfCondition, Until, Switch) so nested activities
            # like Copy also get DataJobs with proper lineage.
            activities_to_process: deque[Activity] = deque(pipeline.activities or [])
            visited_activity_ids: set[int] = set()
            while activities_to_process:
                activity = activities_to_process.popleft()
                if id(activity) in visited_activity_ids:
                    # Defends against a malformed/self-referencing pipeline
                    # definition looping forever; a well-formed pipeline's
                    # activities form a tree, so this never triggers in
                    # practice.
                    continue
                visited_activity_ids.add(id(activity))
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

                # Recurse into container activities to process nested children
                activities_to_process.extend(self._get_nested_activities(activity))

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
        fine_grained_lineages: Optional[list[FineGrainedLineageClass]] = None

        if self.config.include_lineage:
            extracted_inlets = self._extract_activity_inputs(activity, factory_key)
            extracted_outlets = self._extract_activity_outputs(activity, factory_key)
            if extracted_inlets:
                inlets = extracted_inlets
            if extracted_outlets:
                outlets = extracted_outlets

            # Extract column-level lineage if enabled
            if self.config.include_column_lineage and inlets and outlets:
                fine_grained_lineages = self._extract_column_lineage(
                    activity=activity,
                    activity_type=activity_type,
                    inlets=inlets,
                    outlets=outlets,
                    factory_key=factory_key,
                )

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
            fine_grained_lineages=fine_grained_lineages,
        )

        return datajob

    def _get_nested_activities(self, activity: Activity) -> list[Activity]:
        """Extract nested child activities from container activities.

        Handles ForEach/Until (activity.activities),
        IfCondition (activity.if_true_activities + activity.if_false_activities),
        and Switch (activity.cases[].activities + activity.default_activities).

        Returns a flat list of child activities.
        """
        nested: list[Activity] = []

        activity_type = activity.type or ""

        if activity_type in ("ForEach", "Until"):
            nested.extend(getattr(activity, "activities", None) or [])
        elif activity_type == "IfCondition":
            nested.extend(getattr(activity, "if_true_activities", None) or [])
            nested.extend(getattr(activity, "if_false_activities", None) or [])
        elif activity_type == "Switch":
            for case in getattr(activity, "cases", None) or []:
                nested.extend(getattr(case, "activities", None) or [])
            nested.extend(getattr(activity, "default_activities", None) or [])

        return nested

    def _extract_activity_inputs(
        self, activity: Activity, factory_key: str
    ) -> list[DatasetUrnOrStr]:
        """Extract input dataset URNs from an activity."""
        inputs: list[DatasetUrnOrStr] = []

        # Process explicit inputs (for Copy activities and others)
        # Note: Only some activity types (e.g., CopyActivity) have inputs/outputs
        for input_ref in getattr(activity, "inputs", None) or []:
            dataset_urn = self._resolve_dataset_urn(
                input_ref.reference_name,
                factory_key,
                activity_dataset_parameters=getattr(input_ref, "parameters", None),
            )
            if dataset_urn:
                inputs.append(str(dataset_urn))
                self.report.report_lineage_extracted("dataset")

        # Process Data Flow activities - extract sources as inputs
        if activity.type == "ExecuteDataFlow":
            data_flow_inputs = self._extract_data_flow_sources(activity, factory_key)
            inputs.extend(data_flow_inputs)

        # Process Lookup activities - the dataset reference is the input
        if activity.type == "Lookup":
            dataset_ref = getattr(activity, "dataset", None)
            if dataset_ref:
                ref_name = getattr(dataset_ref, "reference_name", None)
                if ref_name:
                    dataset_urn = self._resolve_dataset_urn(
                        ref_name,
                        factory_key,
                        activity_dataset_parameters=getattr(
                            dataset_ref, "parameters", None
                        ),
                    )
                    if dataset_urn:
                        inputs.append(str(dataset_urn))
                        self.report.report_lineage_extracted("dataset")

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
                output_ref.reference_name,
                factory_key,
                activity_dataset_parameters=getattr(output_ref, "parameters", None),
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

    def _extract_column_lineage(
        self,
        activity: Activity,
        activity_type: str,
        inlets: list[DatasetUrnOrStr],
        outlets: list[DatasetUrnOrStr],
        factory_key: str,
    ) -> Optional[list[FineGrainedLineageClass]]:
        """Extract column-level lineage from an activity.

        Uses registered column lineage extractors to parse activity-specific
        column mapping configurations. Each extractor is responsible for
        selecting which inlets/outlets to use based on the activity semantics.

        Args:
            activity: The ADF activity object
            activity_type: The activity type (e.g., "Copy")
            inlets: List of input dataset URNs
            outlets: List of output dataset URNs
            factory_key: Factory key for cache lookups

        Returns:
            List of FineGrainedLineageClass objects, or None if no mappings found
        """
        if not inlets or not outlets:
            # No inlets or outlets provided for activity
            logger.debug(f"No inlets or outlets provided for activity: {activity.name}")
            self.report.report_column_lineage_unsupported(activity_type)
            return None

        # Find an extractor that supports this activity type
        extractor = self._get_extractor_for_activity_type(activity_type)

        if extractor is None:
            # No extractor supports this activity type - this is expected for most activities
            logger.debug(
                f"No column lineage extractor for activity type: {activity_type}"
            )
            self.report.report_column_lineage_unsupported(activity_type)
            return None

        # Create schema resolver bound to this factory
        def schema_resolver(dataset_urn: str) -> Optional[DatasetSchemaInfo]:
            return self._get_source_dataset_schema(dataset_urn, factory_key)

        # Extract column lineage - extractor decides which inlets/outlets to use
        fine_grained_lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=inlets,
            outlets=outlets,
            schema_resolver=schema_resolver,
        )

        if not fine_grained_lineages:
            return None

        for _ in fine_grained_lineages:
            self.report.report_column_lineage_extracted()

        return fine_grained_lineages

    def _get_extractor_for_activity_type(
        self, activity_type: str
    ) -> Optional[ColumnLineageExtractor]:
        """Find a column lineage extractor that supports the given activity type.

        Args:
            activity_type: The ADF activity type (e.g., "Copy", "ExecuteDataFlow")

        Returns:
            A ColumnLineageExtractor instance if one supports this type, None otherwise
        """
        for extractor in self._column_lineage_extractors:
            if extractor.supports_activity(activity_type):
                return extractor
        return None

    def _get_source_dataset_schema(
        self, source_urn: str, factory_key: str
    ) -> Optional[DatasetSchemaInfo]:
        """Get schema information for a source dataset.

        Looks up the dataset in the cache and extracts column names from
        schema_definition or structure properties.

        Args:
            source_urn: URN of the source dataset
            factory_key: Factory key for cache lookups

        Returns:
            DatasetSchemaInfo with column names, or None if schema not available
        """
        # Extract dataset name from URN
        # URN format: urn:li:dataset:(platform,name,env)
        try:
            # Verify URN is parseable before searching cache
            DatasetUrn.from_string(source_urn)
            # For ADF datasets, the name in cache is the ADF dataset name, not the table name
            # We need to search for a dataset that resolves to this URN
        except Exception:
            logger.debug(f"Could not parse dataset URN: {source_urn}")
            return None

        # Search datasets cache for matching dataset
        datasets = self._datasets_cache.get(factory_key, {})
        for dataset_name, dataset in datasets.items():
            # Check if this dataset resolves to the source URN
            resolved_urn = self._resolve_dataset_urn(dataset_name, factory_key)
            if resolved_urn and str(resolved_urn) == source_urn:
                # Found the dataset, extract schema
                return self._extract_dataset_schema(dataset)

        return None

    def _extract_dataset_schema(
        self, dataset: DatasetResource
    ) -> Optional[DatasetSchemaInfo]:
        """Extract schema information from a dataset resource.

        Tries schema_definition first, then falls back to structure field.
        """
        props = dataset.properties

        # Try schema_definition first (newer format), then structure (legacy)
        schema_def = getattr(props, "schema", None)
        columns = self._extract_column_names_from_field_list(schema_def)

        if not columns:
            structure = getattr(props, "structure", None)
            columns = self._extract_column_names_from_field_list(structure)

        if columns:
            return DatasetSchemaInfo(columns=columns)

        return None

    def _extract_column_names_from_field_list(
        self, field_list: Optional[list[Any]]
    ) -> list[str]:
        """Extract column names from a list of field definitions.

        Handles both dict-style fields ({"name": "col"}) and SDK objects
        with a name attribute.

        Args:
            field_list: List of field definitions, or None

        Returns:
            List of column names extracted from the field list
        """
        if not field_list or not isinstance(field_list, list):
            return []

        columns: list[str] = []
        for field in field_list:
            if isinstance(field, dict):
                name = field.get("name")
                if name:
                    columns.append(str(name))
            elif hasattr(field, "name") and field.name:
                columns.append(str(field.name))

        return columns

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

    def _resolve_dataset_platform_context(
        self,
        dataset_name: str,
        factory_key: str,
        activity_dataset_parameters: Optional[dict[str, Any]] = None,
        report_unmapped_platform: bool = False,
    ) -> Optional[
        tuple[DatasetResource, LinkedServiceResource, str, str, Optional[str]]
    ]:
        """Shared lookup chain used by both the static (_resolve_dataset_urn)
        and query-based (_resolve_dataset_ref_context) lineage resolution
        paths: dataset -> linked service -> DataHub platform -> platform
        instance (auto-derived for Databricks when not already mapped).
        Returns (dataset, linked_service, ls_ref_name, platform,
        platform_instance), or None if any step can't be resolved.

        report_unmapped_platform controls whether an unresolvable platform
        mapping is reported via self.report.report_unmapped_platform -
        only _resolve_dataset_urn does this; _resolve_dataset_ref_context
        is a best-effort Layer 2 helper that fails silently by design.
        """
        dataset = self._datasets_cache.get(factory_key, {}).get(dataset_name)
        if not dataset:
            logger.debug(f"Dataset not found in cache: {dataset_name}")
            return None

        linked_service_ref = dataset.properties.linked_service_name
        if not linked_service_ref or not linked_service_ref.reference_name:
            if report_unmapped_platform:
                self.report.report_unmapped_platform(dataset_name, "unknown")
            return None

        ls_ref_name = linked_service_ref.reference_name
        linked_service = self._linked_services_cache.get(factory_key, {}).get(
            ls_ref_name
        )
        if not linked_service or not linked_service.properties:
            if report_unmapped_platform:
                self.report.report_unmapped_platform(dataset_name, "unknown")
            return None

        ls_type = linked_service.properties.type
        if not ls_type:
            if report_unmapped_platform:
                self.report.report_unmapped_platform(dataset_name, "unknown")
            return None

        platform = ADF_LINKED_SERVICE_PLATFORM_MAP.get(ls_type)
        if not platform:
            if report_unmapped_platform:
                self.report.report_unmapped_platform(dataset_name, ls_type)
            return None

        platform_instance = self.config.platform_instance_map.get(ls_ref_name)
        if not platform_instance and platform == "databricks":
            platform_instance = self._derive_databricks_platform_instance(
                linked_service,
                factory_key,
                dataset=dataset,
                activity_dataset_parameters=activity_dataset_parameters,
            )

        return dataset, linked_service, ls_ref_name, platform, platform_instance

    def _resolve_dataset_urn(
        self,
        dataset_name: str,
        factory_key: str,
        activity_dataset_parameters: Optional[dict[str, Any]] = None,
    ) -> Optional[DatasetUrn]:
        """Resolve an ADF dataset reference to a DataHub DatasetUrn."""
        context = self._resolve_dataset_platform_context(
            dataset_name,
            factory_key,
            activity_dataset_parameters,
            report_unmapped_platform=True,
        )
        if not context:
            return None
        dataset, linked_service, _ls_ref_name, platform, platform_instance = context

        # Build dataset name from type properties
        table_name = self._extract_table_name(
            dataset, linked_service, dataset_name, activity_dataset_parameters
        )
        if not table_name:
            table_name = dataset_name  # Fallback to ADF dataset name

        return DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=table_name,
            env=self.config.env,
            platform_instance=platform_instance,
        )

    def _resolve_default_database(
        self,
        linked_service: LinkedServiceResource,
        platform: str,
        factory_key: str,
        ls_ref_name: Optional[str] = None,
        dataset: Optional[DatasetResource] = None,
        activity_dataset_parameters: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        """Best-effort resolution of a default database/catalog to fully
        qualify a 2-part schema.table reference, since ADF's own metadata
        rarely exposes it directly:
        - For connection-string-based linked services (SQL Server,
          Synapse, ODBC, etc.), parse "Initial Catalog=" / "Database="
          from the connection string - the standard ADO.NET convention.
        - MySQL and PostgreSQL instead expose a standalone "database"
          typeProperty rather than embedding it in a connection string.
        - Oracle exposes no database/catalog field at all - its identity
          (a TNS service name or SID) is baked into a free-form "server"
          connect string, so it's only resolvable when the tenant
          parameterizes "server" using a "service"/"sid"-named parameter.
        - Whichever shape yields a candidate value, it commonly turns out
          to be a "parameterized" reference to one of the linked
          service's own declared parameters (e.g.
          "@{linkedService().dbNameParam}") rather than a literal - see
          _resolve_database_param_via_dataset_chain for how that's
          resolved to a real value, in the same activity-override >
          dataset-default > linked-service-default precedence already
          used for table/schema names.
        - For Databricks, ADF datasets and linked services only ever
          model database(=schema)/table and workspace connection details,
          with no catalog or metastore anywhere in the API - resolve
          those from the operator-supplied databricks_catalog_map (keyed
          by linked service name), falling back to the simpler
          databricks_default_catalog if the linked service isn't in that
          map; otherwise leave the reference as schema.table rather than
          guessing a catalog that may not match reality. When a metastore
          is also configured, it's folded into this same return value as
          "metastore.catalog" - DataHub's own dataset name join treats
          this as one opaque segment either way, so this alone produces
          the metastore.catalog.schema.table shape DataHub's Unity
          Catalog source uses when ingested with include_metastore.
        """

        def resolve_param(param_name: str) -> Optional[str]:
            if dataset is not None:
                return self._resolve_database_param_via_dataset_chain(
                    dataset,
                    linked_service,
                    param_name,
                    activity_dataset_parameters,
                    factory_key,
                )
            return self._resolve_linked_service_parameter_default(
                linked_service, param_name, factory_key
            )

        def resolve_candidate(candidate: str) -> Optional[str]:
            self_ref = LINKED_SERVICE_SELF_REFERENCE_PATTERN.match(candidate)
            if self_ref:
                return resolve_param(self_ref.group(1))
            if not UNRESOLVED_ADF_EXPRESSION_PATTERN.search(candidate):
                return candidate
            return None

        connection_string = getattr(
            linked_service.properties, "connection_string", None
        )
        if isinstance(connection_string, str):
            match = CONNECTION_STRING_DATABASE_PATTERN.search(connection_string)
            if match:
                resolved = resolve_candidate(match.group(1).strip())
                if resolved:
                    return resolved

        database = getattr(linked_service.properties, "database", None)
        if isinstance(database, str) and database:
            resolved = resolve_candidate(database)
            if resolved:
                return resolved

        if platform == "oracle":
            server = getattr(linked_service.properties, "server", None)
            if isinstance(server, str):
                hint = ORACLE_SERVICE_PARAM_HINT_PATTERN.search(server)
                if hint:
                    resolved = resolve_param(hint.group(1))
                    if resolved:
                        return resolved

        if platform == "databricks":
            catalog_mapping = (
                self.config.databricks_catalog_map.get(ls_ref_name)
                if ls_ref_name
                else None
            )
            if catalog_mapping:
                if catalog_mapping.metastore:
                    return f"{catalog_mapping.metastore}.{catalog_mapping.catalog}"
                return catalog_mapping.catalog
            return self.config.databricks_default_catalog

        return None

    def _resolve_literal_or_global_parameter(
        self, value: Any, factory_key: str
    ) -> Optional[str]:
        """Resolve a value that is either already a literal, or a
        reference to a factory-level global parameter (e.g.
        "@pipeline().globalParameters.databricks_workspace_url") - unlike
        pipeline parameters, global parameters are tenant-wide literal
        constants and are always resolvable without any run history."""
        unwrapped = self._unwrap_expression_value(value)
        if not isinstance(unwrapped, str) or not unwrapped:
            return None
        global_param_match = GLOBAL_PARAMETER_REFERENCE_PATTERN.match(unwrapped.strip())
        if global_param_match:
            return self._global_parameters_cache.get(factory_key, {}).get(
                global_param_match.group(1)
            )
        if not UNRESOLVED_ADF_EXPRESSION_PATTERN.search(unwrapped):
            return unwrapped
        return None

    def _resolve_database_param_via_dataset_chain(
        self,
        dataset: DatasetResource,
        linked_service: LinkedServiceResource,
        ls_param_name: str,
        activity_dataset_parameters: Optional[dict[str, Any]],
        factory_key: str,
    ) -> Optional[str]:
        """Resolve a linked service parameter (e.g. the one backing its
        connection string's database/catalog reference) by walking the
        same precedence chain already used for table/schema names:
        1. A value supplied by the calling activity's
           DatasetReference.parameters - the most specific, per-usage
           value. This is commonly a literal, but can also be a
           reference to a factory-level global parameter (e.g.
           "@pipeline().globalParameters.X"), which is just as
           resolvable since global parameters are tenant-wide constants.
        2. The dataset's own declared parameter default.
        3. Only if the dataset doesn't override this linked service
           parameter at all: the linked service's own declared default
           (the previous, less precise behavior) - since once a dataset
           does override it, the linked service's separate default may
           not even apply at connection time.
        """
        ls_ref_params = (
            getattr(dataset.properties.linked_service_name, "parameters", None) or {}
        )
        forward_value = self._unwrap_expression_value(ls_ref_params.get(ls_param_name))

        if forward_value is None:
            return self._resolve_linked_service_parameter_default(
                linked_service, ls_param_name, factory_key
            )
        if not isinstance(forward_value, str):
            return None

        dataset_param_match = DYNAMIC_CONTENT_DATASET_PARAM_PATTERN.match(
            forward_value.strip()
        )
        if not dataset_param_match:
            # The dataset forwards a fixed, hardcoded value rather than
            # one of its own parameters.
            if UNRESOLVED_ADF_EXPRESSION_PATTERN.search(forward_value):
                return None
            return forward_value

        dataset_param_name = dataset_param_match.group(1)

        if activity_dataset_parameters:
            resolved_override = self._resolve_literal_or_global_parameter(
                activity_dataset_parameters.get(dataset_param_name), factory_key
            )
            if resolved_override:
                return resolved_override

        dataset_params = getattr(dataset.properties, "parameters", None) or {}
        param_spec = dataset_params.get(dataset_param_name)
        default_value = None
        if isinstance(param_spec, dict):
            default_value = param_spec.get(
                "defaultValue", param_spec.get("default_value")
            )
        elif param_spec is not None:
            default_value = getattr(param_spec, "default_value", None)
        resolved_default = self._resolve_literal_or_global_parameter(
            default_value, factory_key
        )
        if resolved_default:
            return resolved_default

        # The dataset overrides this linked service parameter but we
        # couldn't resolve what it forwards - don't fall back to the
        # linked service's own default, since that may not even apply
        # once the dataset explicitly overrides it.
        return None

    def _resolve_linked_service_parameter_default(
        self, linked_service: LinkedServiceResource, param_name: str, factory_key: str
    ) -> Optional[str]:
        """Resolve a linked service's own declared parameter default
        value (e.g. its database/catalog parameter), mirroring how a
        dataset's declared parameter defaults are resolved."""
        params = getattr(linked_service.properties, "parameters", None) or {}
        param_spec = params.get(param_name)
        if param_spec is None:
            return None
        if isinstance(param_spec, dict):
            default_value = param_spec.get(
                "defaultValue", param_spec.get("default_value")
            )
        else:
            default_value = getattr(param_spec, "default_value", None)
        return self._resolve_literal_or_global_parameter(default_value, factory_key)

    def _derive_databricks_platform_instance(
        self,
        linked_service: LinkedServiceResource,
        factory_key: str,
        dataset: Optional[DatasetResource] = None,
        activity_dataset_parameters: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        """Auto-derive a Databricks platform_instance from the linked
        service's workspace URL, mirroring exactly how DataHub's own
        Unity Catalog source derives its platform_instance when
        workspace_name isn't explicitly configured: the first
        dot-separated label of the workspace URL's host (e.g.
        "https://adb-1234567890123456.4.azuredatabricks.net/" ->
        "adb-1234567890123456"). The domain is very often a
        "parameterized" self-reference (e.g. "@{linkedService().dbx_domain}")
        rather than a literal - resolved through the same
        activity-override > dataset-default > linked-service-default
        chain used elsewhere, plus (uniquely relevant here) an
        activity-level reference to a factory-level global parameter
        (e.g. "@pipeline().globalParameters.databricks_workspace_url"),
        since that's how this value is commonly threaded through in
        practice."""
        domain = getattr(linked_service.properties, "domain", None)
        resolved_domain = self._resolve_literal_or_global_parameter(domain, factory_key)
        if resolved_domain is None and isinstance(domain, str):
            self_ref = LINKED_SERVICE_SELF_REFERENCE_PATTERN.match(domain.strip())
            if self_ref:
                param_name = self_ref.group(1)
                if dataset is not None:
                    resolved_domain = self._resolve_database_param_via_dataset_chain(
                        dataset,
                        linked_service,
                        param_name,
                        activity_dataset_parameters,
                        factory_key,
                    )
                else:
                    resolved_domain = self._resolve_linked_service_parameter_default(
                        linked_service, param_name, factory_key
                    )

        if not resolved_domain or "//" not in resolved_domain:
            return None
        host = resolved_domain.split("//", 1)[1].split("/", 1)[0]
        workspace_instance = host.split(".")[0]
        return workspace_instance or None

    @staticmethod
    def _unwrap_expression_value(value: Any) -> Any:
        """ADF dynamic-content values can appear either as a plain string
        (e.g. "@item().TableName") or as an Expression-wrapped dict
        ({"value": "@item().TableName", "type": "Expression"}), depending
        on how they were authored - ADF's "Add dynamic content" UI wraps
        even literal values this way. Normalize either shape to the
        underlying value."""
        if isinstance(value, dict) and value.get("type") == "Expression":
            return value.get("value")
        return value

    def _resolve_dynamic_content_value(
        self,
        expr: str,
        dataset_props: Any,
        dataset_name: str,
        activity_dataset_parameters: Optional[dict[str, Any]],
    ) -> Optional[str]:
        """Resolve an ADF dynamic-content expression of the form
        "@dataset().<paramName>" using (in order): a literal override
        supplied by the calling activity's DatasetReference.parameters, or
        the dataset's own declared parameter default. Returns None (and
        warns, unless deferring to execution-history resolution) if the
        expression can't be resolved statically."""
        match = DYNAMIC_CONTENT_DATASET_PARAM_PATTERN.match(expr.strip())
        if not match:
            self.report.report_unresolved_dynamic_property(dataset_name, expr)
            return None
        param_name = match.group(1)

        activity_params = activity_dataset_parameters or {}
        has_override = param_name in activity_params
        override = (
            self._unwrap_expression_value(activity_params.get(param_name))
            if has_override
            else None
        )
        if has_override:
            if isinstance(override, str) and not override.startswith("@"):
                # Even a value that looks literal can carry embedded,
                # still-unevaluated ADF templating (the same quirk seen on
                # preCopyScript/pipeline parameters) - never trust it blindly.
                if UNRESOLVED_ADF_EXPRESSION_PATTERN.search(override):
                    self.report.report_unresolved_dynamic_property(dataset_name, expr)
                    return None
                return override
            if isinstance(override, str) and PIPELINE_PARAMETER_REFERENCE_PATTERN.match(
                override.strip()
            ):
                # Only resolvable using a specific pipeline run's actual
                # parameter values - see _harvest_dynamic_dataset_lineage.
                if not self.config.include_execution_history:
                    self.report.report_unresolved_dynamic_property(dataset_name, expr)
                return None
            # The activity supplies an override, but it's some other dynamic
            # expression (e.g. "@item().TableName" from a ForEach loop) that
            # we have no way to resolve - not even from execution history,
            # since Azure's ForEach ActivityRun only reports an item *count*,
            # never the per-iteration resolved values. Falling through to the
            # dataset's own static default here would be actively misleading
            # (it would silently substitute a placeholder default for what is
            # explicitly a per-iteration dynamic value), so warn and stop.
            self.report.report_unresolved_dynamic_property(dataset_name, expr)
            return None

        # No override at all - the activity relies on the dataset's own
        # declared parameter default.
        dataset_params = getattr(dataset_props, "parameters", None) or {}
        param_spec = dataset_params.get(param_name)
        default_value = None
        if isinstance(param_spec, dict):
            default_value = param_spec.get(
                "defaultValue", param_spec.get("default_value")
            )
        elif param_spec is not None:
            default_value = getattr(param_spec, "default_value", None)
        if isinstance(default_value, str) and not default_value.startswith("@"):
            if UNRESOLVED_ADF_EXPRESSION_PATTERN.search(default_value):
                self.report.report_unresolved_dynamic_property(dataset_name, expr)
                return None
            return default_value

        self.report.report_unresolved_dynamic_property(dataset_name, expr)
        return None

    def _extract_table_name(
        self,
        dataset: DatasetResource,
        linked_service: LinkedServiceResource,
        dataset_name: str,
        activity_dataset_parameters: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        """Extract table/file name from dataset properties.

        SDK dataset subclasses have type-specific properties as direct attributes
        (e.g., table_name, table, schema_type_properties_schema, file_name, etc.).
        A property may instead be ADF dynamic content, e.g.
        {"value": "@dataset().table_name", "type": "Expression"} - resolved
        against the parameter value supplied by the calling activity's
        DatasetReference, falling back to the dataset's own declared parameter
        default.
        """
        props = dataset.properties

        def resolve_value(raw: Any) -> Optional[str]:
            """Resolve a typeProperties value that may be a literal or ADF
            dynamic content. Never blindly stringifies an Expression dict."""
            if raw is None:
                return None
            if isinstance(raw, str):
                return raw
            if not isinstance(raw, dict) or raw.get("type") != "Expression":
                return str(raw)

            expr = raw.get("value")
            if not isinstance(expr, str):
                return None
            return self._resolve_dynamic_content_value(
                expr, props, dataset_name, activity_dataset_parameters
            )

        # SQL-like datasets - check for table_name or table attributes
        table_name = resolve_value(getattr(props, "table_name", None))
        if table_name:
            return table_name

        table = resolve_value(getattr(props, "table", None))
        # Structured table reference (schema.table) - computed independently
        # of `table` so schema+table can combine even when table alone is set.
        schema = resolve_value(getattr(props, "schema_type_properties_schema", None))
        if schema and table:
            return f"{schema}.{table}"
        if table:
            return table
        if schema:
            return schema

        # File-based datasets
        file_name = resolve_value(getattr(props, "file_name", None))
        if file_name:
            folder_path = resolve_value(getattr(props, "folder_path", None))
            if folder_path:
                return f"{folder_path}/{file_name}"
            return file_name

        # Container/path based (e.g., DelimitedTextDataset with location)
        location = getattr(props, "location", None)
        if location:
            container = resolve_value(getattr(location, "container", None))
            folder = resolve_value(getattr(location, "folder_path", None))
            filename = resolve_value(getattr(location, "file_name", None))
            parts = [p for p in [container, folder, filename] if p]
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
            self.report.warning(
                title="Failed to Fetch Execution History",
                message="Unable to retrieve pipeline runs.",
                context=f"factory={factory_name}",
                exc=e,
                log=False,
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

        yield from self._emit_dynamic_lineage_augmentation(factory, resource_group)

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
            invoked_by = pipeline_run.invoked_by
            if isinstance(invoked_by, dict):
                invoker_name = invoked_by.get("name", "")
                invoker_type = invoked_by.get("invokedByType", "")
            else:
                # SDK's PipelineRunInvokedBy is a typed model, not a dict.
                invoker_name = getattr(invoked_by, "name", "") or ""
                invoker_type = getattr(invoked_by, "invoked_by_type", "") or ""
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

    def _resolve_dynamic_dataset_refs(
        self,
        dataset_refs: list[Any],
        factory_key: str,
        pipeline_run_parameters: dict[str, str],
    ) -> set[str]:
        """Resolve DatasetReference.parameters values driven by
        "@pipeline().parameters.X" against a specific run's actual resolved
        pipeline parameter values."""
        resolved: set[str] = set()
        for ref in dataset_refs:
            static_params = getattr(ref, "parameters", None)
            reference_name = getattr(ref, "reference_name", None)
            if not static_params or not reference_name:
                continue
            effective_params = self._substitute_pipeline_run_parameters(
                static_params, pipeline_run_parameters
            )
            if not effective_params:
                continue
            urn = self._resolve_dataset_urn(
                reference_name,
                factory_key,
                activity_dataset_parameters=effective_params,
            )
            if urn:
                resolved.add(str(urn))
        return resolved

    def _substitute_pipeline_run_parameters(
        self,
        static_params: dict[str, Any],
        pipeline_run_parameters: dict[str, str],
    ) -> Optional[dict[str, Any]]:
        """Replace "@pipeline().parameters.X" values in a DatasetReference's
        static parameters dict with this run's actual resolved value for X.
        Returns None if nothing was substituted."""
        substituted = False
        effective: dict[str, Any] = dict(static_params)
        for key, raw_value in static_params.items():
            value = self._unwrap_expression_value(raw_value)
            if not isinstance(value, str):
                continue
            match = PIPELINE_PARAMETER_REFERENCE_PATTERN.match(value.strip())
            if not match:
                continue
            pipeline_param_name = match.group(1)
            if pipeline_param_name not in pipeline_run_parameters:
                continue
            resolved_value = pipeline_run_parameters[pipeline_param_name]
            # Azure sometimes records a pipeline parameter's value as
            # literal, still-unevaluated ADF templating text (the same
            # quirk observed on preCopyScript) rather than a resolved
            # string - e.g. a parameter whose own default forwards to
            # "@{linkedService().someField}" for the connector to
            # evaluate at connection time. Never trust it as a real value.
            if UNRESOLVED_ADF_EXPRESSION_PATTERN.search(resolved_value):
                self.report.report_unresolved_dynamic_property(
                    pipeline_param_name, resolved_value
                )
                continue
            effective[key] = resolved_value
            substituted = True
        return effective if substituted else None

    def _find_activity_by_name(
        self, pipeline: PipelineResource, activity_name: str
    ) -> Optional[Activity]:
        """BFS over a pipeline's activities (including nested container
        children) to find the activity with the given name."""
        activities_to_process: deque[Activity] = deque(pipeline.activities or [])
        visited_activity_ids: set[int] = set()
        while activities_to_process:
            activity = activities_to_process.popleft()
            if id(activity) in visited_activity_ids:
                continue
            visited_activity_ids.add(id(activity))
            if activity.name == activity_name:
                return activity
            activities_to_process.extend(self._get_nested_activities(activity))
        return None

    def _emit_dynamic_lineage_augmentation(
        self, factory: Factory, resource_group: str
    ) -> Iterable[MetadataWorkUnit]:
        """Union dynamically-resolved dataset URNs (from execution-history
        pipeline run parameters) into each affected DataJob's static
        dataJobInputOutput aspect, so per-run-parameterized lineage shows up
        in the main lineage graph."""
        factory_name = factory.name or "Unknown"
        factory_key = f"{resource_group}/{factory_name}"

        for cache_key, (
            dynamic_inputs,
            dynamic_outputs,
        ) in self._dynamic_lineage_cache.items():
            cache_factory_key, pipeline_name, activity_name = cache_key
            if cache_factory_key != factory_key:
                continue

            pipeline = self._pipelines_cache.get(factory_key, {}).get(pipeline_name)
            if not pipeline:
                continue
            activity = self._find_activity_by_name(pipeline, activity_name)
            if activity is None:
                continue

            static_inputs = {
                str(u) for u in self._extract_activity_inputs(activity, factory_key)
            }
            static_outputs = {
                str(u) for u in self._extract_activity_outputs(activity, factory_key)
            }

            # Dynamically-resolved values are strictly more accurate than
            # the static/placeholder fallback (e.g. the generic ADF dataset
            # name used when an "@item()"/pipeline-parameter expression
            # can't be resolved statically) - prefer them outright rather
            # than unioning, so the placeholder doesn't linger alongside
            # the real table once it's known.
            combined_inputs = dynamic_inputs or static_inputs
            combined_outputs = dynamic_outputs or static_outputs
            if combined_inputs == static_inputs and combined_outputs == static_outputs:
                continue

            flow_name = f"{factory_name}.{pipeline_name}"
            flow_urn = DataFlowUrn.create_from_ids(
                orchestrator=PLATFORM,
                flow_id=flow_name,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
            job_urn = DataJobUrn.create_from_ids(
                data_flow_urn=str(flow_urn), job_id=activity_name
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=str(job_urn),
                aspect=DataJobInputOutputClass(
                    inputDatasets=sorted(combined_inputs),
                    outputDatasets=sorted(combined_outputs),
                ),
            ).as_workunit()

    def _resolve_activity_run_lineage(
        self,
        activity: Activity,
        factory_key: str,
        pipeline_run: PipelineRun,
        activity_run: ActivityRun,
    ) -> tuple[list[DatasetUrn], list[DatasetUrn]]:
        """Resolve the best-available inlets/outlets for one activity run:
        static lineage, plus anything additionally resolvable from this
        specific run's actual pipeline parameters
        (e.g. "@pipeline().parameters.X"), plus - for Copy activities with
        a custom SQL query source - the real source table(s) parsed from
        the resolved query recorded on this specific run. That query is
        the only Azure API field that exposes a per-iteration resolved
        value for ForEach-looped Copy activities (e.g. "@item().X") - the
        ForEach activity's own run only ever reports an item count."""
        input_urns = {
            str(u) for u in self._extract_activity_inputs(activity, factory_key)
        }
        output_urns = {
            str(u) for u in self._extract_activity_outputs(activity, factory_key)
        }

        if pipeline_run.parameters:
            # Prefer resolved-from-parameters values over the static
            # fallback outright (rather than unioning) so a generic
            # placeholder name doesn't linger alongside the real table.
            resolved_inputs = self._resolve_dynamic_dataset_refs(
                getattr(activity, "inputs", None) or [],
                factory_key,
                pipeline_run.parameters,
            )
            resolved_outputs = self._resolve_dynamic_dataset_refs(
                getattr(activity, "outputs", None) or [],
                factory_key,
                pipeline_run.parameters,
            )
            if resolved_inputs:
                input_urns = resolved_inputs
            if resolved_outputs:
                output_urns = resolved_outputs

        query_input_urns = self._extract_query_source_urns(
            activity, activity_run, factory_key
        )
        if query_input_urns:
            # A per-run observed query is even more authoritative than
            # pipeline-parameter substitution, since it reflects what this
            # specific run actually read rather than a resolved reference.
            input_urns = query_input_urns

        query_output_urns = self._extract_query_sink_urns(
            activity, activity_run, factory_key
        )
        if query_output_urns:
            output_urns = query_output_urns

        inlets = [DatasetUrn.from_string(u) for u in input_urns]
        outlets = [DatasetUrn.from_string(u) for u in output_urns]
        return inlets, outlets

    def _resolve_dataset_ref_context(
        self,
        reference_name: str,
        factory_key: str,
        activity_dataset_parameters: Optional[dict[str, Any]] = None,
    ) -> Optional[tuple[str, Optional[str], Optional[str]]]:
        """Resolve (platform, platform_instance, default_db) for a dataset
        reference, independent of whether the table name itself is
        statically resolvable. Needed to pick the right SQL dialect and
        fully qualify a table name parsed from a raw query/DDL string.
        activity_dataset_parameters, when given, is the calling
        activity's own DatasetReference.parameters for this reference -
        the most specific source for resolving a parameterized database
        name (see _resolve_database_param_via_dataset_chain)."""
        context = self._resolve_dataset_platform_context(
            reference_name, factory_key, activity_dataset_parameters
        )
        if not context:
            return None
        dataset, linked_service, ls_ref_name, platform, platform_instance = context
        default_db = self._resolve_default_database(
            linked_service,
            platform,
            factory_key,
            ls_ref_name=ls_ref_name,
            dataset=dataset,
            activity_dataset_parameters=activity_dataset_parameters,
        )
        return platform, platform_instance, default_db

    def _extract_query_source_urns(
        self,
        activity: Activity,
        activity_run: ActivityRun,
        factory_key: str,
    ) -> set[str]:
        """For Copy activities with a custom SQL query source, parse the
        resolved query recorded on this specific ActivityRun to recover
        the real source table(s)."""
        if activity.type != "Copy":
            return set()

        activity_input = activity_run.input
        if not isinstance(activity_input, dict):
            return set()
        source = activity_input.get("source")
        if not isinstance(source, dict):
            return set()
        query = source.get("query")
        if not isinstance(query, str) or not query.strip():
            return set()
        if UNRESOLVED_ADF_EXPRESSION_PATTERN.search(query):
            self.report.warning(
                title="Unresolved ADF Expression in Activity Run Query",
                message="The resolved query recorded on an activity run still contains unevaluated ADF templating syntax; skipping to avoid emitting a garbage table reference.",
                context=f"activity={activity.name}",
                log=False,
            )
            return set()

        input_ref = next(iter(getattr(activity, "inputs", None) or []), None)
        reference_name = (
            getattr(input_ref, "reference_name", None) if input_ref else None
        )
        if not reference_name:
            return set()
        context = self._resolve_dataset_ref_context(
            reference_name,
            factory_key,
            activity_dataset_parameters=getattr(input_ref, "parameters", None),
        )
        if not context:
            return set()
        platform, platform_instance, default_db = context

        try:
            result = create_lineage_sql_parsed_result(
                query=query,
                default_db=default_db,
                default_schema=None,
                platform=platform,
                platform_instance=platform_instance,
                env=self.config.env,
            )
        except Exception as e:
            self.report.warning(
                title="Failed to Parse Activity Run Query",
                message="Could not parse the resolved SQL query recorded on an activity run for lineage.",
                context=f"activity={activity.name}",
                exc=e,
                log=False,
            )
            return set()

        if result.debug_info.error or not result.in_tables:
            return set()
        return {str(u) for u in result.in_tables}

    def _extract_query_sink_urns(
        self,
        activity: Activity,
        activity_run: ActivityRun,
        factory_key: str,
    ) -> set[str]:
        """Mirror of _extract_query_source_urns for the destination side:
        parses a sink-side DDL statement (e.g. a Copy activity's
        preCopyScript, typically "TRUNCATE TABLE schema.table") recorded
        on this specific ActivityRun to recover the real destination
        table - the only field that exposes a per-iteration resolved sink
        identity for otherwise-unresolvable ("@item()"-driven) outputs."""
        if activity.type != "Copy":
            return set()

        activity_input = activity_run.input
        if not isinstance(activity_input, dict):
            return set()
        sink = activity_input.get("sink")
        if not isinstance(sink, dict):
            return set()
        statement = sink.get("preCopyScript")
        if not isinstance(statement, str) or not statement.strip():
            return set()
        if UNRESOLVED_ADF_EXPRESSION_PATTERN.search(statement):
            self.report.warning(
                title="Unresolved ADF Expression in Activity Run DDL Statement",
                message="A sink DDL statement (e.g. preCopyScript) recorded on an activity run still contains unevaluated ADF templating syntax; skipping to avoid emitting a garbage table reference.",
                context=f"activity={activity.name}",
                log=False,
            )
            return set()

        output_ref = next(iter(getattr(activity, "outputs", None) or []), None)
        reference_name = (
            getattr(output_ref, "reference_name", None) if output_ref else None
        )
        if not reference_name:
            return set()
        context = self._resolve_dataset_ref_context(
            reference_name,
            factory_key,
            activity_dataset_parameters=getattr(output_ref, "parameters", None),
        )
        if not context:
            return set()
        platform, platform_instance, default_db = context

        table_name = self._extract_ddl_target_table(statement, platform, default_db)
        if not table_name:
            return set()

        urn = DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=table_name,
            env=self.config.env,
            platform_instance=platform_instance,
        )
        return {str(urn)}

    def _extract_ddl_target_table(
        self, statement: str, platform: str, default_db: Optional[str]
    ) -> Optional[str]:
        """Best-effort extraction of the single table referenced by an
        administrative DDL/DML statement (e.g. "TRUNCATE TABLE
        schema.table") that sqlglot's lineage machinery doesn't model -
        it resolves SELECT/INSERT/MERGE data flow, not standalone
        statements like this."""
        try:
            dialect = get_dialect_str(platform)
            parsed = sqlglot.parse_one(statement, dialect=dialect)
        except Exception as e:
            self.report.warning(
                title="Failed to Parse Activity Run DDL Statement",
                message="Could not parse a sink DDL statement (e.g. preCopyScript) recorded on an activity run for lineage.",
                context=f"platform={platform}",
                exc=e,
                log=False,
            )
            return None

        table = parsed.find(sqlglot_exp.Table)
        if table is None:
            return None

        parts = [p for p in (table.catalog, table.db, table.name) if p]
        if not parts:
            return None
        if len(parts) < 3 and default_db:
            parts = [default_db, *parts]
        return ".".join(parts)

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

                inlets: list[DatasetUrn] = []
                outlets: list[DatasetUrn] = []
                if self.config.include_lineage:
                    factory_key = f"{resource_group}/{factory_name}"
                    pipeline = self._pipelines_cache.get(factory_key, {}).get(
                        activity_pipeline
                    )
                    activity_obj = (
                        self._find_activity_by_name(pipeline, activity_name)
                        if pipeline
                        else None
                    )
                    if activity_obj is not None:
                        inlets, outlets = self._resolve_activity_run_lineage(
                            activity_obj, factory_key, pipeline_run, activity_run
                        )
                        if inlets or outlets:
                            cache_key = (factory_key, activity_pipeline, activity_name)
                            cached_inputs, cached_outputs = (
                                self._dynamic_lineage_cache.setdefault(
                                    cache_key, (set(), set())
                                )
                            )
                            cached_inputs.update(str(u) for u in inlets)
                            cached_outputs.update(str(u) for u in outlets)

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
                    inlets=inlets,
                    outlets=outlets,
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
