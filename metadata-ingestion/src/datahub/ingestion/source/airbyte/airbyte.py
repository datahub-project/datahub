import logging
from typing import Dict, Iterable, List, Optional

from datahub.api.entities.datajob import DataFlow, DataJob
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.airbyte.config import (
    AirbyteSourceConfig,
    AirbyteSourceReport,
    Constant,
    PlatformDetail,
    SupportedDataPlatform,
)
from datahub.ingestion.source.airbyte.rest_api_wrapper.airbyte_api import AirbyteAPI
from datahub.ingestion.source.airbyte.rest_api_wrapper.data_classes import (
    Connection,
    Connector,
    Job,
    Workspace,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Airbyte")
@config_class(AirbyteSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class AirbyteSource(StatefulIngestionSourceBase):
    """
    This plugin extracts airbyte workspace, connections, sources, destinations and jobs.
    This plugin is in beta and has only been tested on PostgreSQL.
    """

    config: AirbyteSourceConfig
    report: AirbyteSourceReport
    platform: str = "airbyte"

    def __init__(self, config: AirbyteSourceConfig, ctx: PipelineContext):
        super(AirbyteSource, self).__init__(config, ctx)
        self.config = config
        self.report = AirbyteSourceReport()
        try:
            self.airbyte_client = AirbyteAPI(self.config)
        except Exception as e:
            logger.warning(e)
            # Exit pipeline as we are not able to connect to Airbyte API Service.
            # This exit will avoid raising unwanted stacktrace on console
            exit(1)

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    def generate_dataflow_from_workspace(self, workspace: Workspace) -> DataFlow:
        dataflow = DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=workspace.workspace_id,
            env=self.config.env,
            name=workspace.name,
            platform_instance=self.config.platform_instance,
        )
        return dataflow

    def generate_dataset_urn_from_connector(
        self, connector: Connector
    ) -> Optional[DatasetUrn]:
        supported_data_platform: dict = {
            item.value.airbyte_data_platform_name: item.value.datahub_data_platform_name
            for item in SupportedDataPlatform
        }
        if connector.type not in supported_data_platform:
            logger.debug(
                f"Airbyte source/destination type: {connector.type} is not supported to mapped with Datahub dataset entity."
            )
            return None

        # Get platform details for connector
        connector_platform_detail: PlatformDetail = PlatformDetail()
        if connector.server in self.config.server_to_platform_instance.keys():
            connector_platform_detail = self.config.server_to_platform_instance[
                connector.server
            ]

        return DatasetUrn.create_from_ids(
            platform_id=supported_data_platform[connector.type],
            table_name=connector.name,
            env=connector_platform_detail.env,
            platform_instance=connector_platform_detail.platform_instance,
        )

    def generate_datajob_from_connection(
        self, connection: Connection, workspace: Workspace
    ) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=workspace.workspace_id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        datajob = DataJob(
            id=connection.connection_id,
            flow_urn=dataflow_urn,
            name=connection.name,
        )

        job_property_bag: Dict[str, str] = {}
        allowed_connection_keys = [
            Constant.STATUS,
            Constant.NAMESPACE_DEFINITION,
            Constant.NAMESPACE_FORMAT,
            Constant.PREFIX,
        ]
        for key in allowed_connection_keys:
            if hasattr(connection, key) and getattr(connection, key) is not None:
                job_property_bag[key] = repr(getattr(connection, key))
        datajob.properties = job_property_bag

        # Map source and destination connector with dataset entity
        source_dataset_urn = self.generate_dataset_urn_from_connector(connection.source)
        if source_dataset_urn is not None:
            datajob.inlets.extend([source_dataset_urn])
        destination_dataset_urn = self.generate_dataset_urn_from_connector(
            connection.destination
        )
        if destination_dataset_urn is not None:
            datajob.outlets.extend([destination_dataset_urn])

        return datajob

    def generate_dpi_from_job(self, job: Job, datajob: DataJob) -> DataProcessInstance:
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=job.job_id,
            clone_inlets=True,
            clone_outlets=True,
        )

        dpi_property_bag: Dict[str, str] = {}
        allowed_task_run_keys = [
            Constant.JOB_TYPE,
            Constant.LAST_UPDATED_AT,
            Constant.BYTES_SYNCED,
            Constant.ROWS_SYNCED,
        ]
        for key in allowed_task_run_keys:
            if hasattr(job, key) and getattr(job, key) is not None:
                dpi_property_bag[key] = str(getattr(job, key))
        dpi.properties.update(dpi_property_bag)

        return dpi

    def get_dpi_workunit(
        self, job: Job, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_result_map: Dict[str, InstanceRunResult] = {
            Constant.SUCCEEDED: InstanceRunResult.SUCCESS,
            Constant.FAILED: InstanceRunResult.FAILURE,
            Constant.CANCELLED: InstanceRunResult.SKIPPED,
        }
        if job.status not in status_result_map:
            logger.debug(
                f"Status should be either succeeded, failed or cancelled and it was "
                f"{job.status}"
            )
            return []
        result = status_result_map[job.status]
        start_timestamp_millis = job.start_time * 1000
        for mcp in dpi.generate_mcp(created_ts_millis=start_timestamp_millis):
            yield mcp.as_workunit()
        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()
        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=job.end_time * 1000,
            result=result,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = AirbyteSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workspace_workunit(
        self, workspace: Workspace
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_workspaces_scanned()
        # Map Airbyte's workspace entity with Datahub's dataflow entity
        dataflow = self.generate_dataflow_from_workspace(workspace)
        for mcp in dataflow.generate_mcp():
            # return workunit to Datahub Ingestion framework
            yield mcp.as_workunit()
        # Map Airbyte's connection entity with Datahub's datajob entity
        for connection in workspace.connections:
            self.report.report_connections_scanned()
            datajob = self.generate_datajob_from_connection(connection, workspace)
            for mcp in datajob.generate_mcp():
                # return workunit to Datahub Ingestion framework
                yield mcp.as_workunit()

            # Map Airbyte's job entity with Datahub's data process entity
            for job in connection.jobs:
                self.report.report_jobs_scanned()
                dpi = self.generate_dpi_from_job(job, datajob)
                yield from self.get_dpi_workunit(job, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Airbyte plugin execution is started")
        # Fetch Airbyte workspace
        workspaces = self.airbyte_client.get_workspaces()
        for workspace in workspaces:
            logger.info(f"Processing workspace id: {workspace.workspace_id}")
            yield from self.get_workspace_workunit(workspace)

    def get_report(self) -> SourceReport:
        return self.report
