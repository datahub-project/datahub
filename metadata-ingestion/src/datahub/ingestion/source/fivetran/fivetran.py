import logging
from typing import Dict, Iterable, List, Optional, Union
from urllib.parse import urlparse

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
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
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
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import FivetranConnectionDetails
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

# Logger instance
logger = logging.getLogger(__name__)
CORPUSER_DATAHUB = "urn:li:corpuser:datahub"


@platform_name("Fivetran")
@config_class(FivetranSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
)
class FivetranSource(StatefulIngestionSourceBase):
    """
    This plugin extracts fivetran users, connectors, destinations and sync history.
    """

    config: FivetranSourceConfig
    report: FivetranSourceReport
    platform: str = "fivetran"

    def __init__(self, config: FivetranSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FivetranSourceReport()
        self.audit_log = FivetranLogAPI(self.config.fivetran_log_config)
        self.api_client: Optional[FivetranAPIClient] = None
        self._connection_details_cache: Dict[str, FivetranConnectionDetails] = {}

        if self.config.api_config:
            self.api_client = FivetranAPIClient(self.config.api_config)

    def _extend_lineage(self, connector: Connector, datajob: DataJob) -> Dict[str, str]:
        input_dataset_urn_list: List[Union[str, DatasetUrn]] = []
        output_dataset_urn_list: List[Union[str, DatasetUrn]] = []
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
                    "We will attempt to guess the platform based on the connector type. "
                    "Note that we use connector_id as the key not connector_name which you may see in the UI of Fivetran. ",
                    context=f"connector_name: {connector.connector_name} (connector_id: {connector.connector_id}, connector_type: {connector.connector_type})",
                    log_category=StructuredLogCategory.LINEAGE,
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
            source_table = (
                lineage.source_table
                if source_details.include_schema_in_urn
                else lineage.source_table.split(".", 1)[1]
            )
            input_dataset_urn: Optional[DatasetUrn] = None
            # Special Handling for Google Sheets Connectors
            if connector.connector_type == Constant.GOOGLE_SHEETS_CONNECTOR_TYPE:
                logger.debug(
                    f"Processing Google Sheets connector for lineage extraction: "
                    f"connector_name={connector.connector_name}, connector_id={connector.connector_id}"
                )
                # Get Google Sheet dataset details from Fivetran API
                # This is cached in the api_client
                gsheets_conn_details: Optional[FivetranConnectionDetails] = (
                    self._get_connection_details_by_id(connector.connector_id)
                )

                named_range = (
                    self._get_gsheet_named_range_dataset_id(gsheets_conn_details)
                    if gsheets_conn_details
                    else None
                )
                if named_range:
                    input_dataset_urn = DatasetUrn.create_from_ids(
                        platform_id=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
                        table_name=named_range,
                        env=source_details.env,
                    )
                else:
                    self.report.warning(
                        title="Failed to extract lineage for Google Sheets Connector",
                        message="Unable to extract lineage for Google Sheets Connector. "
                        "This may occur if: (1) connector details could not be fetched from Fivetran API, or "
                        "(2) the sheet URL format is invalid or unsupported.",
                        context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
                    )
            else:
                input_dataset_urn = DatasetUrn.create_from_ids(
                    platform_id=source_details.platform,
                    table_name=(
                        f"{source_details.database.lower()}.{source_table}"
                        if source_details.database
                        else source_table
                    ),
                    env=source_details.env,
                    platform_instance=source_details.platform_instance,
                )

            if input_dataset_urn:
                input_dataset_urn_list.append(input_dataset_urn)

            destination_table = (
                lineage.destination_table
                if destination_details.include_schema_in_urn
                else lineage.destination_table.split(".", 1)[1]
            )
            output_dataset_urn = DatasetUrn.create_from_ids(
                platform_id=destination_details.platform,
                table_name=f"{destination_details.database.lower()}.{destination_table}",
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

        datajob.set_inlets(input_dataset_urn_list)
        datajob.set_outlets(output_dataset_urn_list)
        datajob.set_fine_grained_lineages(fine_grained_lineage)

        return dict(
            **{
                f"source.{k}": str(v)
                for k, v in source_details.model_dump().items()
                if v is not None and not isinstance(v, bool)
            },
            **{
                f"destination.{k}": str(v)
                for k, v in destination_details.model_dump().items()
                if v is not None and not isinstance(v, bool)
            },
        )

    def _generate_dataflow_from_connector(self, connector: Connector) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=connector.connector_id,
            env=self.config.env,
            display_name=connector.connector_name,
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
            name=connector.connector_id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=connector.connector_name,
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
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

        datajob.set_custom_properties({**connector_properties, **lineage_properties})

        return datajob

    def _generate_dpi_from_job(self, job: Job, datajob: DataJob) -> DataProcessInstance:
        # hack: convert to old instance for DataProcessInstance.from_datajob compatibility
        datajob_v1 = DataJobV1(
            id=datajob.name,
            flow_urn=datajob.flow_urn,
            platform_instance=self.config.platform_instance,
            name=datajob.name,
            inlets=datajob.inlets,
            outlets=datajob.outlets,
            fine_grained_lineages=datajob.fine_grained_lineages,
        )
        return DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=job.job_id,
            clone_inlets=True,
            clone_outlets=True,
        )

    def _get_connection_details_by_id(
        self, connection_id: str
    ) -> Optional[FivetranConnectionDetails]:
        if self.api_client is None:
            self.report.warning(
                title="Fivetran API client is not initialized",
                message="Google Sheets Connector details cannot be extracted, as Fivetran API client is not initialized.",
                context=f"connector_id: {connection_id}",
            )
            return None

        if connection_id in self._connection_details_cache:
            return self._connection_details_cache[connection_id]

        self.report.report_fivetran_rest_api_call_count()
        try:
            conn_details = self.api_client.get_connection_details_by_id(connection_id)
            self._connection_details_cache[connection_id] = conn_details
            return conn_details
        except Exception as e:
            logger.error(
                f"Failed to get connection details using rest-api for connector_id: {connection_id}. Error: {str(e)}",
                exc_info=True,
            )
            self.report.failure(
                title="Failed to get connection details for Google Sheets Connector",
                message="Exception occurred while getting connection details from Fivetran API",
                context=f"connector_id: {connection_id}",
                exc=e,
            )
            return None

    def _get_gsheet_sheet_id_from_url(
        self, gsheets_conn_details: FivetranConnectionDetails
    ) -> Optional[str]:
        """
        Extract the Google Sheets ID from the sheet_id field.
        Handles both cases:
        1. Full URL: "https://docs.google.com/spreadsheets/d/<spreadsheetId>/edit..."
        2. Just the ID: "<spreadsheetId>"
        """
        sheet_id = gsheets_conn_details.config.sheet_id

        # If it's already just an ID (no URL structure), return it as-is
        if not sheet_id.startswith(("http://", "https://")):
            return sheet_id

        # Otherwise, extract from URL
        # Example: https://docs.google.com/spreadsheets/d/<spreadsheetId>/edit?gid=0#gid=0
        try:
            parsed = urlparse(sheet_id)
            parts = parsed.path.split("/")
            # Path format: /spreadsheets/d/<spreadsheetId>/edit
            # parts[0] = '', parts[1] = 'spreadsheets', parts[2] = 'd', parts[3] = '<spreadsheetId>'
            if len(parts) > 3 and parts[2] == "d":
                return parts[3]
            logger.warning(
                f"Unexpected URL format for sheet_id: {sheet_id}. Expected format: "
                f"https://docs.google.com/spreadsheets/d/<spreadsheetId>/..."
            )
        except Exception as e:
            logger.warning(
                f"Failed to extract sheet_id from URL: {sheet_id}, Error: {e}"
            )

        return None

    def _get_gsheet_named_range_dataset_id(
        self, gsheets_conn_details: FivetranConnectionDetails
    ) -> Optional[str]:
        sheet_id = self._get_gsheet_sheet_id_from_url(gsheets_conn_details)
        if sheet_id is None:
            return None
        named_range_id = (
            f"{sheet_id}.{gsheets_conn_details.config.named_range}"
            if sheet_id
            else gsheets_conn_details.config.named_range
        )
        logger.debug(
            f"Using gsheet_named_range_dataset_id: {named_range_id} for connector: {gsheets_conn_details.id}"
        )
        return named_range_id

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
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_connectors_scanned()

        """
        -------------------------------------------------------
        Special Handling for Google Sheets Connectors
        -------------------------------------------------------
        Google Sheets source is not supported by Datahub yet.   
        As a workaround, we are emitting a dataset entity for the Google Sheet
        and adding it to the lineage. This workaround needs to be removed once 
        Datahub supports Google Sheets source natively.
        -------------------------------------------------------
        """
        if connector.connector_type == Constant.GOOGLE_SHEETS_CONNECTOR_TYPE:
            logger.debug(
                f"Processing Google Sheets connector for workunit generation: "
                f"connector_name={connector.connector_name}, connector_id={connector.connector_id}"
            )
            # Get Google Sheet dataset details from Fivetran API
            gsheets_conn_details: Optional[FivetranConnectionDetails] = (
                self._get_connection_details_by_id(connector.connector_id)
            )

            sheet_id = (
                self._get_gsheet_sheet_id_from_url(gsheets_conn_details)
                if gsheets_conn_details
                else None
            )
            named_range = (
                self._get_gsheet_named_range_dataset_id(gsheets_conn_details)
                if gsheets_conn_details
                else None
            )

            if gsheets_conn_details and sheet_id and named_range:
                gsheets_dataset = Dataset(
                    name=sheet_id,
                    platform=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
                    env=self.config.env,
                    display_name=sheet_id,
                    external_url=gsheets_conn_details.config.sheet_id,
                    created=gsheets_conn_details.created_at,
                    last_modified=gsheets_conn_details.succeeded_at,
                    subtype=DatasetSubTypes.GOOGLE_SHEETS,
                    custom_properties={
                        "ingested_by": "fivetran source",
                        "connector_id": gsheets_conn_details.id,
                    },
                )
                gsheets_named_range_dataset = Dataset(
                    name=named_range,
                    platform=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
                    env=self.config.env,
                    display_name=gsheets_conn_details.config.named_range,
                    external_url=gsheets_conn_details.config.sheet_id,
                    created=gsheets_conn_details.created_at,
                    last_modified=gsheets_conn_details.succeeded_at,
                    subtype=DatasetSubTypes.GOOGLE_SHEETS_NAMED_RANGE,
                    custom_properties={
                        "ingested_by": "fivetran source",
                        "connector_id": gsheets_conn_details.id,
                    },
                    upstreams=UpstreamLineage(
                        upstreams=[
                            UpstreamClass(
                                dataset=str(gsheets_dataset.urn),
                                type=DatasetLineageTypeClass.VIEW,
                                auditStamp=AuditStamp(
                                    time=int(
                                        gsheets_conn_details.created_at.timestamp()
                                        * 1000
                                    ),
                                    actor=CORPUSER_DATAHUB,
                                ),
                            )
                        ],
                        fineGrainedLineages=None,
                    ),
                )

                yield gsheets_dataset
                yield gsheets_named_range_dataset

            else:
                self.report.warning(
                    title="Failed to generate entities for Google Sheets",
                    message="Failed to generate Google Sheets dataset entities. "
                    "This may occur if: (1) connector details could not be fetched from Fivetran API, or "
                    "(2) the sheet URL format is invalid or unsupported.",
                    context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
                )

        # Create dataflow entity with same name as connector name
        dataflow = self._generate_dataflow_from_connector(connector)
        yield dataflow

        # Map Fivetran's connector entity with Datahub's datajob entity
        datajob = self._generate_datajob_from_connector(connector)
        yield datajob

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

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
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
