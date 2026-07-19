import logging
from typing import Dict, Iterable, List, Union

from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import (
    DESTINATION_FALLBACK_SUFFIX,
    HIGHTOUCH_PLATFORM,
    QUERY_TYPE_TABLE,
    SYNC_CONFIG_DEST_TABLE_KEYS,
    SYNC_CONFIG_KEY_EVENT_NAME,
    SYNC_CONFIG_KEY_TYPE,
    SYNC_CONFIG_TYPE_EVENT,
    SYNC_CONFIG_VALUE_KEY_FROM,
    SYNC_CONFIG_VALUE_KEY_NAME,
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
from datahub.ingestion.source.hightouch.protocols import (
    GetDestination,
    GetModel,
    GetSource,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import SchemaFieldClass
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

# Maps a Hightouch sync-run status to a DataHub run result. Unknown statuses are
# intentionally absent so they can be mapped to a neutral result instead of a
# misleading SUCCESS.
_SYNC_RUN_STATUS_MAP = {
    "success": InstanceRunResult.SUCCESS,
    "failed": InstanceRunResult.FAILURE,
    "cancelled": InstanceRunResult.SKIPPED,
    "interrupted": InstanceRunResult.SKIPPED,
    "warning": InstanceRunResult.SUCCESS,
}


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
        get_model: GetModel,
        get_source: GetSource,
        get_destination: GetDestination,
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
            platform=HIGHTOUCH_PLATFORM,
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
            if model.query_type == QUERY_TYPE_TABLE and model.name:
                table_name = self.urn_builder.qualified_table_name(model, source)
                return self.urn_builder.make_upstream_table_urn(table_name, source)
            else:
                self.report.report_lineage_resolution_failure(
                    f"sync_slug: {sync.slug} (model_slug: {model.slug})"
                )
                self.report.warning(
                    title="No upstream lineage for non-table model",
                    message="emit_models_as_datasets=False but the model is not a "
                    "table-type model, so no upstream table URN can be resolved; "
                    "lineage for this sync will be missing.",
                    context=f"sync_slug: {sync.slug} (query_type: {model.query_type})",
                )
                return None

        return self.urn_builder.make_model_urn(model, source)

    def get_outlet_urn_for_sync(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Union[str, DatasetUrn, None]:
        dest_table = None

        if sync.configuration:
            sync_type = sync.configuration.get(SYNC_CONFIG_KEY_TYPE)

            if sync_type == SYNC_CONFIG_TYPE_EVENT:
                event_name_value = sync.configuration.get(SYNC_CONFIG_KEY_EVENT_NAME)
                if isinstance(event_name_value, dict):
                    dest_table = event_name_value.get(SYNC_CONFIG_VALUE_KEY_FROM)
                elif isinstance(event_name_value, str):
                    dest_table = event_name_value
            else:
                for key in SYNC_CONFIG_DEST_TABLE_KEYS:
                    value = sync.configuration.get(key)
                    if value:
                        if isinstance(value, str):
                            dest_table = value
                        elif isinstance(value, dict):
                            dest_table = value.get(
                                SYNC_CONFIG_VALUE_KEY_FROM
                            ) or value.get(SYNC_CONFIG_VALUE_KEY_NAME)

                        if dest_table:
                            break

        if not dest_table:
            dest_table = f"{sync.slug}{DESTINATION_FALLBACK_SUFFIX}"
            logger.warning(
                f"Could not determine destination table for sync {sync.slug}, using fallback: {dest_table}"
            )

        return self.urn_builder.make_destination_urn(dest_table, destination)

    def generate_datajob_from_sync(self, sync: HightouchSync) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=HIGHTOUCH_PLATFORM,
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
        for prefix, rows in (
            ("planned_rows", sync_run.planned_rows),
            ("successful_rows", sync_run.successful_rows),
            ("failed_rows", sync_run.failed_rows),
        ):
            if not rows:
                continue
            custom_props[f"{prefix}_added"] = str(rows.get("added", 0))
            custom_props[f"{prefix}_changed"] = str(rows.get("changed", 0))
            custom_props[f"{prefix}_removed"] = str(rows.get("removed", 0))
            custom_props[f"{prefix}_total"] = str(sum(rows.values()))

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
        status = _SYNC_RUN_STATUS_MAP.get(sync_run.status.lower())
        if status is None:
            # Never coerce an unrecognized status to SUCCESS — that would silently
            # record a failed/aborted run as a success. Fall back to a neutral result.
            self.report.report_unknown_sync_run_status(sync_run.status)
            self.report.warning(
                title="Unknown sync run status",
                message="Encountered a sync-run status that is not recognized; "
                "recording it as SKIPPED rather than assuming success.",
                context=f"sync_run_id: {sync_run.id}, status: {sync_run.status}",
            )
            status = InstanceRunResult.SKIPPED

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
            result_type=HIGHTOUCH_PLATFORM,
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
                yield from self.model_handler.emit_model(
                    model, source, referenced_columns=sync.referenced_columns
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
