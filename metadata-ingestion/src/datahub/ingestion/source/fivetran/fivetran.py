import logging
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.fivetran.config import (
    KNOWN_DATA_PLATFORM_MAPPING,
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.data_classes import Connector, Job
from datahub.ingestion.source.fivetran.fivetran_log_api import FivetranLogAPI
from datahub.ingestion.source.fivetran.fivetran_query import (
    MAX_JOBS_PER_CONNECTOR,
    MAX_TABLE_LINEAGE_PER_CONNECTOR,
)
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
from datahub.metadata.schema_classes import StatusClass
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Fivetran")
@config_class(FivetranSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
)
class FivetranSource(StatefulIngestionSourceBase):
    """
    This plugin extracts fivetran users, connectors, destinations and sync history.
    This plugin is in beta and has only been tested on Snowflake connector.
    """

    config: FivetranSourceConfig
    report: FivetranSourceReport
    platform: str = "fivetran"

    def __init__(self, config: FivetranSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FivetranSourceReport()

        self.audit_log = FivetranLogAPI(self.config.fivetran_log_config)

    def _extend_lineage(self, connector: Connector, datajob: DataJob) -> Dict[str, str]:
        input_dataset_urn_list: List[DatasetUrn] = []
        output_dataset_urn_list: List[DatasetUrn] = []
        fine_grained_lineage: List[FineGrainedLineage] = []

        # TODO: Once Fivetran exposes the database via the API, we shouldn't ask for it via config.

        # Get platform details for connector source
        source_details = self.config.sources_to_platform_instance.get(
            connector.connector_id, PlatformDetail()
        )
        if source_details.platform is None:
            if connector.connector_type in KNOWN_DATA_PLATFORM_MAPPING:
                source_details.platform = KNOWN_DATA_PLATFORM_MAPPING[
                    connector.connector_type
                ]
            else:
                self.report.info(
                    title="Guessing source platform for lineage",
                    message="We encountered a connector type that we don't fully support yet. "
                    "We will attempt to guess the platform based on the connector type.",
                    context=f"{connector.connector_name} (connector_id: {connector.connector_id}, connector_type: {connector.connector_type})",
                )
                source_details.platform = connector.connector_type

        # Get platform details for destination
        destination_details = self.config.destination_to_platform_instance.get(
            connector.destination_id, PlatformDetail()
        )
        if destination_details.platform is None:
            destination_details.platform = (
                self.config.fivetran_log_config.destination_platform
            )
        if destination_details.database is None:
            destination_details.database = self.audit_log.fivetran_log_database

        if len(connector.lineage) >= MAX_TABLE_LINEAGE_PER_CONNECTOR:
            self.report.warning(
                title="Table lineage truncated",
                message=f"The connector had more than {MAX_TABLE_LINEAGE_PER_CONNECTOR} table lineage entries. "
                f"Only the most recent {MAX_TABLE_LINEAGE_PER_CONNECTOR} entries were ingested.",
                context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
            )

        for lineage in connector.lineage:
            input_dataset_urn = DatasetUrn.create_from_ids(
                platform_id=source_details.platform,
                table_name=(
                    f"{source_details.database.lower()}.{lineage.source_table}"
                    if source_details.database
                    else lineage.source_table
                ),
                env=source_details.env,
                platform_instance=source_details.platform_instance,
            )
            input_dataset_urn_list.append(input_dataset_urn)

            output_dataset_urn = DatasetUrn.create_from_ids(
                platform_id=destination_details.platform,
                table_name=f"{destination_details.database.lower()}.{lineage.destination_table}",
                env=destination_details.env,
                platform_instance=destination_details.platform_instance,
            )
            output_dataset_urn_list.append(output_dataset_urn)

            if self.config.include_column_lineage:
                for column_lineage in lineage.column_lineage:
                    fine_grained_lineage.append(
                        FineGrainedLineage(
                            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                            upstreams=(
                                [
                                    builder.make_schema_field_urn(
                                        str(input_dataset_urn),
                                        column_lineage.source_column,
                                    )
                                ]
                                if input_dataset_urn
                                else []
                            ),
                            downstreamType=FineGrainedLineageDownstreamType.FIELD,
                            downstreams=(
                                [
                                    builder.make_schema_field_urn(
                                        str(output_dataset_urn),
                                        column_lineage.destination_column,
                                    )
                                ]
                                if output_dataset_urn
                                else []
                            ),
                        )
                    )

        datajob.inlets.extend(input_dataset_urn_list)
        datajob.outlets.extend(output_dataset_urn_list)
        datajob.fine_grained_lineages.extend(fine_grained_lineage)

        return dict(
            **{
                f"source.{k}": str(v)
                for k, v in source_details.dict().items()
                if v is not None
            },
            **{
                f"destination.{k}": str(v)
                for k, v in destination_details.dict().items()
                if v is not None
            },
        )

    def _generate_dataflow_from_connector(self, connector: Connector) -> DataFlow:
        return DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=connector.connector_id,
            env=self.config.env,
            name=connector.connector_name,
            platform_instance=self.config.platform_instance,
        )

    def _generate_datajob_from_connector(self, connector: Connector) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=connector.connector_id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        owner_email = self.audit_log.get_user_email(connector.user_id)
        datajob = DataJob(
            id=connector.connector_id,
            flow_urn=dataflow_urn,
            name=connector.connector_name,
            owners={owner_email} if owner_email else set(),
        )

        # Map connector source and destination table with dataset entity
        # Also extend the fine grained lineage of column if include_column_lineage is True
        lineage_properties = self._extend_lineage(connector=connector, datajob=datajob)
        # TODO: Add fine grained lineages of dataset after FineGrainedLineageDownstreamType.DATASET enabled

        connector_properties: Dict[str, str] = {
            "connector_id": connector.connector_id,
            "connector_type": connector.connector_type,
            "paused": str(connector.paused),
            "sync_frequency": str(connector.sync_frequency),
            "destination_id": connector.destination_id,
        }
        datajob.properties = {
            **connector_properties,
            **lineage_properties,
        }

        return datajob

    def _generate_dpi_from_job(self, job: Job, datajob: DataJob) -> DataProcessInstance:
        return DataProcessInstance.from_datajob(
            datajob=datajob,
            id=job.job_id,
            clone_inlets=True,
            clone_outlets=True,
        )

    def _get_dpi_workunits(
        self, job: Job, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_result_map: Dict[str, InstanceRunResult] = {
            Constant.SUCCESSFUL: InstanceRunResult.SUCCESS,
            Constant.FAILURE_WITH_TASK: InstanceRunResult.FAILURE,
            Constant.CANCELED: InstanceRunResult.SKIPPED,
        }
        if job.status not in status_result_map:
            logger.debug(
                f"Status should be either SUCCESSFUL, FAILURE_WITH_TASK or CANCELED and it was "
                f"{job.status}"
            )
            return
        result = status_result_map[job.status]
        start_timestamp_millis = job.start_time * 1000
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_timestamp_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()
        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=job.end_time * 1000,
            result=result,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    def _get_connector_workunits(
        self, connector: Connector
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_connectors_scanned()
        # Create dataflow entity with same name as connector name
        dataflow = self._generate_dataflow_from_connector(connector)
        for mcp in dataflow.generate_mcp():
            yield mcp.as_workunit()

        # Map Fivetran's connector entity with Datahub's datajob entity
        datajob = self._generate_datajob_from_connector(connector)
        for mcp in datajob.generate_mcp(materialize_iolets=False):
            yield mcp.as_workunit()

        # Materialize the upstream referenced datasets.
        # We assume that the downstreams are materialized by other ingestion sources.
        for iolet in datajob.inlets:
            # We don't want these to be tracked by stateful ingestion.
            yield MetadataChangeProposalWrapper(
                entityUrn=str(iolet),
                aspect=StatusClass(removed=False),
            ).as_workunit(is_primary_source=False)

        # Map Fivetran's job/sync history entity with Datahub's data process entity
        if len(connector.jobs) >= MAX_JOBS_PER_CONNECTOR:
            self.report.warning(
                title="Not all sync history was captured",
                message=f"The connector had more than {MAX_JOBS_PER_CONNECTOR} sync runs in the past {self.config.history_sync_lookback_period} days. "
                f"Only the most recent {MAX_JOBS_PER_CONNECTOR} syncs were ingested.",
                context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
            )
        for job in connector.jobs:
            dpi = self._generate_dpi_from_job(job, datajob)
            yield from self._get_dpi_workunits(job, dpi)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = FivetranSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Fivetran plugin execution is started")
        connectors = self.audit_log.get_allowed_connectors_list(
            self.config.connector_patterns,
            self.config.destination_patterns,
            self.report,
            self.config.history_sync_lookback_period,
        )
        for connector in connectors:
            logger.info(f"Processing connector id: {connector.connector_id}")
            yield from self._get_connector_workunits(connector)

    def get_report(self) -> SourceReport:
        return self.report
