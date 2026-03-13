import logging
from typing import Callable, Dict, Iterable, List, Union

from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hightouch.config import (
    Constant,
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_container import (
    HightouchContainerHandler,
)
from datahub.ingestion.source.hightouch.hightouch_lineage import (
    HightouchLineageHandler,
)
from datahub.ingestion.source.hightouch.hightouch_model import HightouchModelHandler
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSync,
    HightouchSyncRun,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import SchemaFieldClass
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)


class HightouchSyncHandler:
    def __init__(
        self,
        config: "HightouchSourceConfig",
        report: "HightouchSourceReport",
        api_client: "HightouchAPIClient",
        urn_builder: "HightouchUrnBuilder",
        lineage_handler: "HightouchLineageHandler",
        container_handler: "HightouchContainerHandler",
        model_handler: "HightouchModelHandler",
        model_schema_fields_cache: Dict[str, List[SchemaFieldClass]],
        get_model: Callable,
        get_source: Callable,
        get_destination: Callable,
    ):
        self.config = config
        self.report = report
        self.api_client = api_client
        self.urn_builder = urn_builder
        self.lineage_handler = lineage_handler
        self.container_handler = container_handler
        self.model_handler = model_handler
        self.model_schema_fields_cache = model_schema_fields_cache
        self.get_model = get_model
        self.get_source = get_source
        self.get_destination = get_destination

    def generate_dataflow_from_sync(self, sync: HightouchSync) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=sync.id,
            env=self.config.env,
            display_name=sync.slug,
            platform_instance=self.config.platform_instance,
        )

    def get_inlet_urn_for_model(
        self, model: HightouchModel, sync: HightouchSync
    ) -> Union[str, DatasetUrn, None]:
        source = self.get_source(model.source_id)
        if not source:
            self.report.warning(
                title="Failed to get source for model",
                message="Could not retrieve source information for lineage creation.",
                context=f"sync_slug: {sync.slug} (model_id: {model.source_id})",
            )
            return None

        if not self.config.emit_models_as_datasets:
            if model.query_type == "table" and model.name:
                table_name = model.name
                if source.configuration:
                    database = source.configuration.get("database", "")
                    schema = source.configuration.get("schema", "")
                    source_details = self.urn_builder._get_cached_source_details(source)
                    if source_details.include_schema_in_urn and schema:
                        table_name = f"{database}.{schema}.{table_name}"
                    elif database and "." not in table_name:
                        table_name = f"{database}.{table_name}"

                return self.urn_builder.make_upstream_table_urn(table_name, source)
            else:
                logger.warning(
                    f"Sync {sync.slug}: emit_models_as_datasets=False but model {model.slug} is not a table type"
                )
                return None

        return self.urn_builder.make_model_urn(model, source)

    def get_outlet_urn_for_sync(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Union[str, DatasetUrn, None]:
        dest_table = None

        if sync.configuration:
            sync_type = sync.configuration.get("type")

            if sync_type == "event":
                event_name_value = sync.configuration.get("eventName")
                if isinstance(event_name_value, dict):
                    dest_table = event_name_value.get("from")
                elif isinstance(event_name_value, str):
                    dest_table = event_name_value
            else:
                for key in [
                    "object",
                    "tableName",
                    "table",
                    "destinationTable",
                    "objectName",
                ]:
                    value = sync.configuration.get(key)
                    if value:
                        if isinstance(value, str):
                            dest_table = value
                        elif isinstance(value, dict):
                            dest_table = value.get("from") or value.get("name")

                        if dest_table:
                            logger.debug(
                                f"Found destination table '{dest_table}' using key '{key}'"
                            )
                            break

        if not dest_table:
            dest_table = f"{sync.slug}_destination"
            logger.warning(
                f"Could not determine destination table for sync {sync.slug}, using fallback: {dest_table}"
            )

        return self.urn_builder.make_destination_urn(dest_table, destination)

    def generate_datajob_from_sync(self, sync: HightouchSync) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=sync.id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        datajob = DataJob(
            name=sync.id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=sync.slug,
        )

        model = self.get_model(sync.model_id)
        destination = self.get_destination(sync.destination_id)

        inlets: List[Union[str, DatasetUrn]] = []
        if model:
            inlet_urn = self.get_inlet_urn_for_model(model, sync)
            if inlet_urn:
                inlets.append(inlet_urn)

        datajob.set_inlets(inlets)

        outlet_urn = None
        if destination:
            outlet_urn = self.get_outlet_urn_for_sync(sync, destination)

        if outlet_urn:
            datajob.set_outlets([outlet_urn])

        if model and outlet_urn and inlets:
            model_schema_fields = self.model_schema_fields_cache.get(model.id)
            fine_grained_lineages = self.lineage_handler.generate_column_lineage(
                sync, model, inlets[0], outlet_urn, model_schema_fields
            )
            if fine_grained_lineages:
                datajob.set_fine_grained_lineages(fine_grained_lineages)

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

        if sync.tags:
            tag_list = [
                f"{key}:{value}" for key, value in sync.tags.items() if key and value
            ]
            if tag_list:
                custom_properties["hightouch_tags"] = ", ".join(tag_list)
                self.report.tags_emitted += len(tag_list)

        datajob.set_custom_properties(custom_properties)

        return datajob

    def generate_dpi_from_sync_run(
        self, sync_run: HightouchSyncRun, datajob: DataJob, sync_id: str
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
            "sync_id": sync_id,
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
            if isinstance(sync_run.error, str):
                custom_props["error_message"] = sync_run.error
            else:
                custom_props["error_message"] = sync_run.error.get(
                    "message", "Unknown error"
                )
                if "code" in sync_run.error:
                    custom_props["error_code"] = sync_run.error["code"]

        dpi.properties.update(custom_props)

        return dpi

    def get_dpi_workunits(
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

    def get_sync_workunits(
        self, sync: HightouchSync
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_syncs_scanned()

        model = None
        if self.config.emit_models_as_datasets:
            model = self.get_model(sync.model_id)
            if model and self.config.model_patterns.allowed(model.name):
                source = self.get_source(model.source_id)
                # Delegate to model handler for model workunits
                result = self.model_handler.generate_model_dataset(
                    model, source, referenced_columns=sync.referenced_columns
                )
                self.report.report_models_emitted()
                yield result.dataset
                yield from self.model_handler.emit_model_aspects(
                    model, result.dataset, source, result.schema_fields
                )

                yield from self.container_handler.add_model_to_container(
                    result.dataset.urn
                )

                if source:
                    self.lineage_handler.register_model_lineage(
                        model,
                        str(result.dataset.urn),
                        source,
                        self.model_handler.get_platform_for_source,
                        self.model_handler.get_aggregator_for_platform,
                    )

        destination = self.get_destination(sync.destination_id)
        outlet_urn = None
        if destination:
            outlet_urn = self.get_outlet_urn_for_sync(sync, destination)
            if outlet_urn:
                self.report.report_destinations_emitted()

        dataflow = self.generate_dataflow_from_sync(sync)
        yield dataflow

        yield from self.container_handler.add_dataflow_to_container(str(dataflow.urn))

        datajob = self.generate_datajob_from_sync(sync)
        yield datajob

        yield from self.container_handler.add_datajob_browse_path(
            datajob_urn=str(datajob.urn),
            dataflow_urn=str(dataflow.urn),
            pipeline_name=sync.slug,
        )

        if outlet_urn and datajob.inlets:
            self.lineage_handler.accumulate_destination_lineage(
                str(outlet_urn),
                datajob.inlets,
                datajob.fine_grained_lineages,
            )

        if self.config.include_sync_runs:
            self.report.report_api_call()
            sync_runs = self.api_client.get_sync_runs(
                sync.id, limit=self.config.max_sync_runs_per_sync
            )

            for sync_run in sync_runs:
                self.report.report_sync_runs_scanned()
                dpi = self.generate_dpi_from_sync_run(sync_run, datajob, sync.id)
                yield from self.get_dpi_workunits(sync_run, dpi)
