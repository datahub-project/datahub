"""Hightouch Reverse ETL Source for DataHub

This connector extracts metadata from Hightouch, including:
- Sources (databases/warehouses)
- Models (SQL queries)
- Syncs (data pipelines)
- Destinations (target systems)
- Sync Runs (execution history)
"""

import logging
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceReport,
    StructuredLogCategory,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hightouch.config import (
    Constant,
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.data_classes import (
    HightouchDestination,
    HightouchModel,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient

if TYPE_CHECKING:
    from datahub.ingestion.source.hightouch import data_classes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

CORPUSER_DATAHUB = "urn:li:corpuser:datahub"
HIGHTOUCH_PLATFORM = "hightouch"

# Mapping of Hightouch source types to DataHub platform names
# Reference: https://hightouch.com/docs/getting-started/concepts/#sources
KNOWN_SOURCE_PLATFORM_MAPPING = {
    # Data Warehouses
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "synapse": "mssql",
    "athena": "athena",
    # Databases
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    "sql_server": "mssql",
    "azure_sql": "mssql",
    "oracle": "oracle",
    "mongodb": "mongodb",
    "dynamodb": "dynamodb",
    # BI & Analytics Tools
    "looker": "looker",
    "tableau": "tableau",
    "metabase": "metabase",
    "mode": "mode",
    "sigma": "sigma",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "azure-blob-storage",
    "azure_blob_storage": "azure-blob-storage",
    "azure_storage": "azure-blob-storage",
    "adls": "adls",
    "azure_data_lake": "adls",
    "azure_data_lake_storage": "adls",
    # SaaS & Other
    "salesforce": "salesforce",
    "google_sheets": "google-sheets",
    "airtable": "airtable",
    "google_analytics": "google-analytics",
    "hubspot": "hubspot",
}

# Mapping of Hightouch destination types to DataHub platform names
# Reference: https://hightouch.com/docs/destinations/overview/
KNOWN_DESTINATION_PLATFORM_MAPPING = {
    # Data Warehouses & Databases
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "azure-blob-storage",
    "azure_blob_storage": "azure-blob-storage",
    "azure_storage": "azure-blob-storage",
    "adls": "adls",
    "azure_data_lake": "adls",
    "azure_data_lake_storage": "adls",
    # CRM & Sales
    "salesforce": "salesforce",
    "hubspot": "hubspot",
    "zendesk": "zendesk",
    "pipedrive": "pipedrive",
    "outreach": "outreach",
    "salesloft": "salesloft",
    # Marketing Automation
    "braze": "braze",
    "iterable": "iterable",
    "customer_io": "customerio",
    "marketo": "marketo",
    "klaviyo": "klaviyo",
    "mailchimp": "mailchimp",
    "activecampaign": "activecampaign",
    "eloqua": "eloqua",
    "sendgrid": "sendgrid",
    # Analytics & Product
    "segment": "segment",
    "mixpanel": "mixpanel",
    "amplitude": "amplitude",
    "google_analytics": "google-analytics",
    "heap": "heap",
    "pendo": "pendo",
    "intercom": "intercom",
    # Advertising
    "facebook_ads": "facebook",
    "google_ads": "google-ads",
    "linkedin_ads": "linkedin",
    "snapchat_ads": "snapchat",
    "tiktok_ads": "tiktok",
    "pinterest_ads": "pinterest",
    "twitter_ads": "twitter",
    # Support & Customer Success (additional)
    "freshdesk": "freshdesk",
    "kustomer": "kustomer",
    # Collaboration & Productivity
    "google_sheets": "google-sheets",
    "airtable": "airtable",
    "slack": "slack",
    # Payment & Finance
    "stripe": "stripe",
    "chargebee": "chargebee",
    # Other
    "webhook": "http",
    "http": "http",
}


@platform_name("Hightouch")
@config_class(HightouchSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, showing data flow from source -> model -> sync -> destination",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default via configuration `include_column_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class HightouchSource(StatefulIngestionSourceBase):
    """
    This plugin extracts Hightouch reverse ETL metadata including sources,
    models, syncs, destinations, and sync run history.
    """

    config: HightouchSourceConfig
    report: HightouchSourceReport
    platform: str = "hightouch"

    def __init__(self, config: HightouchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = HightouchSourceReport()
        self.api_client = HightouchAPIClient(self.config.api_config)

        self._sources_cache: Dict[str, "data_classes.HightouchSource"] = {}
        self._models_cache: Dict[str, HightouchModel] = {}
        self._destinations_cache: Dict[str, HightouchDestination] = {}
        self._users_cache: Dict[str, HightouchUser] = {}

    def _get_source(self, source_id: str) -> Optional["data_classes.HightouchSource"]:
        if source_id not in self._sources_cache:
            self.report.report_api_call()
            source = self.api_client.get_source_by_id(source_id)
            if source:
                self._sources_cache[source_id] = source
        return self._sources_cache.get(source_id)

    def _get_model(self, model_id: str) -> Optional[HightouchModel]:
        if model_id not in self._models_cache:
            self.report.report_api_call()
            model = self.api_client.get_model_by_id(model_id)
            if model:
                self._models_cache[model_id] = model
        return self._models_cache.get(model_id)

    def _get_destination(self, destination_id: str) -> Optional[HightouchDestination]:
        if destination_id not in self._destinations_cache:
            self.report.report_api_call()
            destination = self.api_client.get_destination_by_id(destination_id)
            if destination:
                self._destinations_cache[destination_id] = destination
        return self._destinations_cache.get(destination_id)

    def _get_user(self, user_id: str) -> Optional[HightouchUser]:
        if user_id not in self._users_cache:
            self.report.report_api_call()
            user = self.api_client.get_user_by_id(user_id)
            if user:
                self._users_cache[user_id] = user
        return self._users_cache.get(user_id)

    def _get_platform_for_source(
        self, source: "data_classes.HightouchSource"
    ) -> PlatformDetail:
        """Get platform details for a source with fallback logic"""
        source_details = self.config.sources_to_platform_instance.get(
            source.id, PlatformDetail()
        )

        if source_details.platform is None:
            if source.type.lower() in KNOWN_SOURCE_PLATFORM_MAPPING:
                source_details.platform = KNOWN_SOURCE_PLATFORM_MAPPING[
                    source.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown source platform type",
                    message=f"Source type '{source.type}' is not in the known platform mapping. "
                    f"Using source type as platform name. Consider adding a mapping in "
                    f"sources_to_platform_instance config for source_id '{source.id}'.",
                    context=f"source_name: {source.name} (source_id: {source.id}, type: {source.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                source_details.platform = source.type.lower()

        if source_details.env is None:
            source_details.env = self.config.env

        return source_details

    def _get_platform_for_destination(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        """Get platform details for a destination with fallback logic"""
        destination_details = self.config.destinations_to_platform_instance.get(
            destination.id, PlatformDetail()
        )

        if destination_details.platform is None:
            if destination.type.lower() in KNOWN_DESTINATION_PLATFORM_MAPPING:
                destination_details.platform = KNOWN_DESTINATION_PLATFORM_MAPPING[
                    destination.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown destination platform type",
                    message=f"Destination type '{destination.type}' is not in the known platform mapping. "
                    f"Using destination type as platform name. Consider adding a mapping in "
                    f"destinations_to_platform_instance config for destination_id '{destination.id}'.",
                    context=f"destination_name: {destination.name} (destination_id: {destination.id}, type: {destination.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                destination_details.platform = destination.type.lower()

        if destination_details.env is None:
            destination_details.env = self.config.env

        return destination_details

    def _generate_model_dataset(
        self, model: HightouchModel, source: Optional["data_classes.HightouchSource"]
    ) -> Dataset:
        custom_properties = {
            "model_id": model.id,
            "query_type": model.query_type,
            "is_schema": str(model.is_schema),
        }

        if model.primary_key:
            custom_properties["primary_key"] = model.primary_key

        if source:
            custom_properties["source_id"] = source.id
            custom_properties["source_name"] = source.name
            custom_properties["source_type"] = source.type

        if model.tags:
            for key, value in model.tags.items():
                custom_properties[f"tag_{key}"] = value

        # TODO: SQL parsing for raw_sql models to extract upstream table dependencies

        dataset = Dataset(
            name=model.slug,
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=model.name,
            description=model.description,
            created=model.created_at,
            last_modified=model.updated_at,
            custom_properties=custom_properties,
        )

        dataset.set_custom_properties(custom_properties)

        return dataset

    def _generate_dataflow_from_sync(self, sync: HightouchSync) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=sync.id,
            env=self.config.env,
            display_name=sync.slug,
            platform_instance=self.config.platform_instance,
        )

    def _generate_datajob_from_sync(self, sync: HightouchSync) -> DataJob:  # noqa: C901
        """Generate a DataJob entity from a Hightouch sync"""
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=sync.id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Extract ownership if available
        # Note: Hightouch API doesn't expose created_by on syncs directly
        # This would need to be added when the API supports it
        owners = None
        # if hasattr(sync, 'created_by_user_id') and sync.created_by_user_id:
        #     user = self._get_user(sync.created_by_user_id)
        #     if user and user.email:
        #         owners = [CorpUserUrn(user.email)]

        datajob = DataJob(
            name=sync.id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=sync.slug,
            owners=owners,
        )

        model = self._get_model(sync.model_id)
        destination = self._get_destination(sync.destination_id)

        inlets: List[Union[str, DatasetUrn]] = []
        if model:
            if self.config.emit_models_as_datasets:
                model_dataset_urn = DatasetUrn.create_from_ids(
                    platform_id=HIGHTOUCH_PLATFORM,
                    table_name=model.slug,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                inlets.append(model_dataset_urn)
            else:
                source = self._get_source(model.source_id)
                if source:
                    source_details = self._get_platform_for_source(source)
                    if source_details.platform:
                        model_table_name = model.slug
                        if source_details.database:
                            model_table_name = (
                                f"{source_details.database.lower()}.{model_table_name}"
                            )

                        input_dataset_urn = DatasetUrn.create_from_ids(
                            platform_id=source_details.platform,
                            table_name=model_table_name,
                            env=source_details.env,
                            platform_instance=source_details.platform_instance,
                        )
                        inlets.append(input_dataset_urn)
                else:
                    self.report.warning(
                        title="Failed to get source for model",
                        message="Could not retrieve source information for lineage creation.",
                        context=f"sync_slug: {sync.slug} (model_id: {model.source_id})",
                    )

        datajob.set_inlets(inlets)

        fine_grained_lineages = []
        if self.config.include_column_lineage and model and destination:
            field_mappings = self.api_client.extract_field_mappings(sync)

            if field_mappings:
                inlet_urn = inlets[0] if inlets else None

                dest_details = self._get_platform_for_destination(destination)
                outlet_urn = None
                if dest_details.platform:
                    dest_table = (
                        sync.configuration.get("destinationTable")
                        if sync.configuration
                        else None
                    )
                    if not dest_table:
                        dest_table = sync.slug

                    dest_table_name = dest_table
                    if dest_details.database:
                        dest_table_name = (
                            f"{dest_details.database.lower()}.{dest_table_name}"
                        )

                    outlet_urn = DatasetUrn.create_from_ids(
                        platform_id=dest_details.platform,
                        table_name=dest_table_name,
                        env=dest_details.env,
                        platform_instance=dest_details.platform_instance,
                    )

                for mapping in field_mappings:
                    if inlet_urn and outlet_urn:
                        fine_grained_lineages.append(
                            FineGrainedLineage(
                                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                                upstreams=[
                                    builder.make_schema_field_urn(
                                        str(inlet_urn), mapping.source_field
                                    )
                                ],
                                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                                downstreams=[
                                    builder.make_schema_field_urn(
                                        str(outlet_urn), mapping.destination_field
                                    )
                                ],
                            )
                        )

                if fine_grained_lineages:
                    datajob.set_fine_grained_lineages(fine_grained_lineages)

                if outlet_urn:
                    datajob.set_outlets([outlet_urn])
        elif destination:
            dest_details = self._get_platform_for_destination(destination)
            if dest_details.platform:
                dest_table = (
                    sync.configuration.get("destinationTable")
                    if sync.configuration
                    else None
                )
                if not dest_table:
                    dest_table = sync.slug

                dest_table_name = dest_table
                if dest_details.database:
                    dest_table_name = (
                        f"{dest_details.database.lower()}.{dest_table_name}"
                    )

                output_dataset_urn = DatasetUrn.create_from_ids(
                    platform_id=dest_details.platform,
                    table_name=dest_table_name,
                    env=dest_details.env,
                    platform_instance=dest_details.platform_instance,
                )
                datajob.set_outlets([output_dataset_urn])
        custom_properties: Dict[str, str] = {
            "sync_id": sync.id,
            "model_id": sync.model_id,
            "destination_id": sync.destination_id,
            "disabled": str(sync.disabled),
        }

        if sync.schedule:
            custom_properties["schedule"] = str(sync.schedule)

        if model:
            custom_properties["model_name"] = model.name
            custom_properties["model_slug"] = model.slug
            custom_properties["query_type"] = model.query_type
            if model.description:
                custom_properties["model_description"] = model.description

        if destination:
            custom_properties["destination_name"] = destination.name
            custom_properties["destination_type"] = destination.type

        datajob.set_custom_properties(custom_properties)

        return datajob

    def _generate_dpi_from_sync_run(
        self, sync_run: HightouchSyncRun, datajob: DataJob
    ) -> DataProcessInstance:
        datajob_v1 = DataJobV1(
            id=datajob.name,
            flow_urn=datajob.flow_urn,
            platform_instance=self.config.platform_instance,
            name=datajob.name,
            inlets=datajob.inlets,
            outlets=datajob.outlets,
        )

        dpi = DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=sync_run.id,
            clone_inlets=True,
            clone_outlets=True,
        )

        custom_props = {
            "sync_run_id": sync_run.id,
            "sync_id": sync_run.sync_id,
            "status": sync_run.status,
        }
        if sync_run.planned_rows:
            custom_props["planned_rows_added"] = str(
                sync_run.planned_rows.get("added", 0)
            )
            custom_props["planned_rows_changed"] = str(
                sync_run.planned_rows.get("changed", 0)
            )
            custom_props["planned_rows_removed"] = str(
                sync_run.planned_rows.get("removed", 0)
            )
            custom_props["planned_rows_total"] = str(
                sum(sync_run.planned_rows.values())
            )

        if sync_run.successful_rows:
            custom_props["successful_rows_added"] = str(
                sync_run.successful_rows.get("added", 0)
            )
            custom_props["successful_rows_changed"] = str(
                sync_run.successful_rows.get("changed", 0)
            )
            custom_props["successful_rows_removed"] = str(
                sync_run.successful_rows.get("removed", 0)
            )
            custom_props["successful_rows_total"] = str(
                sum(sync_run.successful_rows.values())
            )

        if sync_run.failed_rows:
            failed_total = sum(sync_run.failed_rows.values())
            custom_props["failed_rows_total"] = str(failed_total)
            custom_props["failed_rows_added"] = str(
                sync_run.failed_rows.get("added", 0)
            )
            custom_props["failed_rows_changed"] = str(
                sync_run.failed_rows.get("changed", 0)
            )
            custom_props["failed_rows_removed"] = str(
                sync_run.failed_rows.get("removed", 0)
            )

        if sync_run.query_size:
            custom_props["query_size_bytes"] = str(sync_run.query_size)

        if sync_run.completion_ratio is not None:
            custom_props["completion_ratio"] = f"{sync_run.completion_ratio * 100:.1f}%"
        if sync_run.error:
            custom_props["error_message"] = sync_run.error.get(
                "message", "Unknown error"
            )
            if "code" in sync_run.error:
                custom_props["error_code"] = sync_run.error["code"]

        dpi.properties.update(custom_props)

        return dpi

    def _get_dpi_workunits(
        self, sync_run: HightouchSyncRun, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_map = {
            "success": InstanceRunResult.SUCCESS,
            "failed": InstanceRunResult.FAILURE,
            "cancelled": InstanceRunResult.SKIPPED,
            "interrupted": InstanceRunResult.SKIPPED,
            "warning": InstanceRunResult.SUCCESS,
        }

        status = status_map.get(sync_run.status.lower(), InstanceRunResult.SUCCESS)

        start_timestamp_millis = int(sync_run.started_at.timestamp() * 1000)

        for mcp in dpi.generate_mcp(
            created_ts_millis=start_timestamp_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()

        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()

        end_timestamp_millis = (
            int(sync_run.finished_at.timestamp() * 1000)
            if sync_run.finished_at
            else start_timestamp_millis
        )

        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=status,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    def _get_sync_workunits(
        self, sync: HightouchSync
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_syncs_scanned()

        if self.config.emit_models_as_datasets:
            model = self._get_model(sync.model_id)
            if model and self.config.model_patterns.allowed(model.name):
                source = self._get_source(model.source_id)
                model_dataset = self._generate_model_dataset(model, source)
                self.report.report_models_emitted()
                yield model_dataset

        dataflow = self._generate_dataflow_from_sync(sync)
        yield dataflow

        datajob = self._generate_datajob_from_sync(sync)
        yield datajob

        if self.config.include_sync_runs:
            self.report.report_api_call()
            sync_runs = self.api_client.get_sync_runs(
                sync.id, limit=self.config.max_sync_runs_per_sync
            )

            for sync_run in sync_runs:
                self.report.report_sync_runs_scanned()
                dpi = self._generate_dpi_from_sync_run(sync_run, datajob)
                yield from self._get_dpi_workunits(sync_run, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _get_model_workunits(
        self, model: HightouchModel
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_models_scanned()

        source = self._get_source(model.source_id)
        model_dataset = self._generate_model_dataset(model, source)
        self.report.report_models_emitted()
        yield model_dataset

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        logger.info("Starting Hightouch metadata extraction")

        emitted_model_ids = set()

        self.report.report_api_call()
        syncs = self.api_client.get_syncs()
        logger.info(f"Found {len(syncs)} syncs")

        filtered_syncs = [
            sync for sync in syncs if self.config.sync_patterns.allowed(sync.slug)
        ]

        logger.info(f"Processing {len(filtered_syncs)} syncs after filtering")

        for sync in filtered_syncs:
            try:
                if self.config.emit_models_as_datasets:
                    emitted_model_ids.add(sync.model_id)

                yield from self._get_sync_workunits(sync)
            except Exception as e:
                self.report.warning(
                    title="Failed to process sync",
                    message=f"An error occurred while processing sync: {str(e)}",
                    context=f"sync_slug: {sync.slug} (sync_id: {sync.id})",
                    exc=e,
                )

        if self.config.emit_models_as_datasets:
            logger.info("Fetching standalone models")
            self.report.report_api_call()
            all_models = self.api_client.get_models()
            logger.info(f"Found {len(all_models)} models total")

            standalone_models = [
                model
                for model in all_models
                if model.id not in emitted_model_ids
                and self.config.model_patterns.allowed(model.name)
            ]

            logger.info(
                f"Processing {len(standalone_models)} standalone models after filtering"
            )

            for model in standalone_models:
                try:
                    yield from self._get_model_workunits(model)
                except Exception as e:
                    self.report.warning(
                        title="Failed to process model",
                        message=f"An error occurred while processing model: {str(e)}",
                        context=f"model_name: {model.name} (model_id: {model.id})",
                        exc=e,
                    )

    def get_report(self) -> SourceReport:
        return self.report
