import logging
from typing import Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fivetran.config import (
    KNOWN_DATA_PLATFORM_MAPPING,
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.data_classes import Connector, Job
from datahub.ingestion.source.fivetran.fivetran_access import (
    create_fivetran_access,
)
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
    Supports both enterprise and standard versions.
    """

    config: FivetranSourceConfig
    report: FivetranSourceReport
    platform: str = "fivetran"

    def __init__(self, config: FivetranSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FivetranSourceReport()

        # Create the appropriate access implementation using the factory
        self.fivetran_access = create_fivetran_access(config)

        # For backward compatibility with existing tests
        self.audit_log = self.fivetran_access

    def _extend_lineage(self, connector: Connector, datajob: DataJob) -> Dict[str, str]:
        """Build lineage between source and destination datasets."""
        # Initialize empty lists for dataset URNs and fine-grained lineage
        input_dataset_urn_list: List[DatasetUrn] = []
        output_dataset_urn_list: List[DatasetUrn] = []
        fine_grained_lineage: List[FineGrainedLineage] = []

        # Obtain source and destination platform details
        source_details = self._get_source_details(connector)
        destination_details = self._get_destination_details(connector)

        # Handle lineage truncation if needed
        if len(connector.lineage) >= MAX_TABLE_LINEAGE_PER_CONNECTOR:
            self._report_lineage_truncation(connector)

        # Process each table lineage entry
        for lineage in connector.lineage:
            # Create source and destination URNs
            source_urn = self._create_dataset_urn(
                lineage.source_table,
                source_details,
                is_source=True,
            )

            dest_urn = self._create_dataset_urn(
                lineage.destination_table,
                destination_details,
                is_source=False,
            )

            # Add URNs to lists (avoiding duplicates)
            if source_urn and source_urn not in input_dataset_urn_list:
                input_dataset_urn_list.append(source_urn)

            if dest_urn and dest_urn not in output_dataset_urn_list:
                output_dataset_urn_list.append(dest_urn)

            # Create column lineage if enabled
            if self.config.include_column_lineage and source_urn and dest_urn:
                self._create_column_lineage(
                    lineage=lineage,
                    source_urn=source_urn,
                    dest_urn=dest_urn,
                    fine_grained_lineage=fine_grained_lineage,
                )

        # Add URNs and lineage to the datajob
        datajob.inlets.extend(input_dataset_urn_list)
        datajob.outlets.extend(output_dataset_urn_list)
        datajob.fine_grained_lineages.extend(fine_grained_lineage)

        # Build properties from details and connector properties
        return self._build_lineage_properties(
            connector=connector,
            source_details=source_details,
            destination_details=destination_details,
        )

    def _get_source_details(self, connector: Connector) -> PlatformDetail:
        """Get source platform details for a connector."""
        source_details = self.config.sources_to_platform_instance.get(
            connector.connector_id, PlatformDetail()
        )

        # Map connector type to known platform if needed
        if source_details.platform is None:
            connector_type = connector.connector_type.lower()
            if connector_type in KNOWN_DATA_PLATFORM_MAPPING:
                source_details.platform = KNOWN_DATA_PLATFORM_MAPPING[connector_type]
            else:
                source_details.platform = connector_type

        # Set default database if not present
        if source_details.database is None:
            source_details.database = ""

        return source_details

    def _get_destination_details(self, connector: Connector) -> PlatformDetail:
        """Get destination platform details for a connector."""
        destination_details = self.config.destination_to_platform_instance.get(
            connector.destination_id, PlatformDetail()
        )

        # Set platform if not present
        if destination_details.platform is None:
            # For enterprise version, get from config
            if (
                hasattr(self.config, "fivetran_log_config")
                and self.config.fivetran_log_config is not None
            ):
                destination_details.platform = (
                    self.config.fivetran_log_config.destination_platform
                )
            # For standard version, check additional properties
            elif "destination_platform" in connector.additional_properties:
                destination_details.platform = connector.additional_properties.get(
                    "destination_platform"
                )
            else:
                destination_details.platform = "snowflake"  # Default

        # Set database if not present
        if destination_details.database is None:
            if "destination_database" in connector.additional_properties:
                destination_details.database = connector.additional_properties.get(
                    "destination_database"
                )
            else:
                destination_details.database = (
                    self.fivetran_access.fivetran_log_database or ""
                )

        return destination_details

    def _create_dataset_urn(
        self, table_name: str, details: PlatformDetail, is_source: bool
    ) -> Optional[DatasetUrn]:
        """Create a dataset URN for a table."""
        if not table_name:
            return None

        # Handle schema inclusion based on configuration
        if not details.include_schema_in_urn and "." in table_name:
            table_name = table_name.split(".", 1)[1]

        # Include database in the table name if available
        full_table_name = (
            f"{details.database.lower()}.{table_name}"
            if details.database
            else table_name
        )

        try:
            return DatasetUrn.create_from_ids(
                platform_id=details.platform,
                table_name=full_table_name,
                env=details.env,
                platform_instance=details.platform_instance,
            )
        except Exception as e:
            logger.warning(
                f"Failed to create {'source' if is_source else 'destination'} URN: {e}"
            )
            return None

    def _report_lineage_truncation(self, connector: Connector) -> None:
        """Report warning about truncated lineage."""
        self.report.warning(
            title="Table lineage truncated",
            message=f"The connector had more than {MAX_TABLE_LINEAGE_PER_CONNECTOR} table lineage entries. "
            f"Only the most recent {MAX_TABLE_LINEAGE_PER_CONNECTOR} entries were ingested.",
            context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
        )

    def _create_column_lineage(
        self,
        lineage,
        source_urn: DatasetUrn,
        dest_urn: DatasetUrn,
        fine_grained_lineage: List[FineGrainedLineage],
    ) -> None:
        """Create column-level lineage between source and destination tables."""
        for column_lineage in lineage.column_lineage:
            fine_grained_lineage.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        builder.make_schema_field_urn(
                            str(source_urn),
                            column_lineage.source_column,
                        )
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(
                            str(dest_urn),
                            column_lineage.destination_column,
                        )
                    ],
                )
            )

    def _build_lineage_properties(
        self,
        connector: Connector,
        source_details: PlatformDetail,
        destination_details: PlatformDetail,
    ) -> Dict[str, str]:
        """Build properties dictionary from source and destination details."""
        lineage_properties = {}

        # Add source details
        for k, v in source_details.dict().items():
            if v is not None and not isinstance(v, bool):
                lineage_properties[f"source.{k}"] = str(v)

        # Add destination details
        for k, v in destination_details.dict().items():
            if v is not None and not isinstance(v, bool):
                lineage_properties[f"destination.{k}"] = str(v)

        # Add connector-specific properties
        for k, v in connector.additional_properties.items():
            if k not in ["destination_platform", "destination_database"]:
                lineage_properties[f"connector.{k}"] = str(v)

        return lineage_properties

    def _detect_source_platform(self, connector: Connector) -> str:
        """Detect source platform for a connector."""
        connector_type = connector.connector_type.lower()

        # Common platforms
        common_platforms = {
            "salesforce": "salesforce",
            "postgres": "postgres",
            "postgresql": "postgres",
            "bigquery": "bigquery",
            "google_bigquery": "bigquery",
            "mysql": "mysql",
            "snowflake": "snowflake",
            "redshift": "redshift",
            "mssql": "mssql",
            "sql_server": "mssql",
            "oracle": "oracle",
            "kafka": "kafka",
            "mongodb": "mongodb",
            "s3": "s3",
            "azure_blob_storage": "azure_blob_storage",
            "gcs": "gcs",
            "google_cloud_storage": "gcs",
        }

        for match, platform in common_platforms.items():
            if match in connector_type:
                return platform

        # No match found, use connector type as platform
        self.report.info(
            title="Guessing source platform for lineage",
            message="We encountered a connector type that we don't fully support yet. "
            "We will attempt to guess the platform based on the connector type.",
            context=f"{connector.connector_name} (connector_id: {connector.connector_id}, connector_type: {connector.connector_type})",
        )
        return connector.connector_type

    def _add_lineage_to_datajob(
        self,
        datajob: DataJob,
        input_urns: List[DatasetUrn],
        output_urns: List[DatasetUrn],
        fine_grained_lineage: List[FineGrainedLineage],
    ) -> None:
        """Add lineage information to a DataJob."""
        # Only add unique URNs to avoid duplicates
        datajob.inlets.extend(list(set(input_urns)))
        datajob.outlets.extend(list(set(output_urns)))
        datajob.fine_grained_lineages.extend(fine_grained_lineage)

    def _create_lineage_entries(
        self,
        connector: Connector,
        source_details: PlatformDetail,
        destination_details: PlatformDetail,
        input_urns: List[DatasetUrn],
        output_urns: List[DatasetUrn],
        fine_grained_lineage: List[FineGrainedLineage],
    ) -> None:
        """Create lineage entries between source and destination tables."""
        processed_tables: Set[str] = set()

        for lineage in connector.lineage:
            # Skip duplicates
            lineage_key = f"{lineage.source_table}:{lineage.destination_table}"
            if lineage_key in processed_tables:
                continue
            processed_tables.add(lineage_key)

            # Create source and destination URNs
            source_urn = self._create_source_urn(lineage, source_details)
            dest_urn = self._create_destination_urn(lineage, destination_details)

            # Add URNs to lists
            input_urns.append(source_urn)
            output_urns.append(dest_urn)

            # Create column lineage if enabled
            if self.config.include_column_lineage:
                self._create_column_lineage(
                    lineage=lineage,
                    source_urn=source_urn,
                    dest_urn=dest_urn,
                    fine_grained_lineage=fine_grained_lineage,
                )

    def _create_source_urn(self, lineage, source_details: PlatformDetail) -> DatasetUrn:
        """Create a dataset URN for a source table."""
        source_table = (
            lineage.source_table
            if source_details.include_schema_in_urn
            else lineage.source_table.split(".", 1)[1]
        )

        # Safe access to database.lower() with None check
        source_table_name = (
            f"{source_details.database.lower()}.{source_table}"
            if source_details.database
            else source_table
        )

        return DatasetUrn.create_from_ids(
            platform_id=source_details.platform,
            table_name=source_table_name,
            env=source_details.env,
            platform_instance=source_details.platform_instance,
        )

    def _detect_destination_platform(self, connector: Connector) -> str:
        """Detect destination platform for a connector."""
        # If using enterprise version, get destination platform from config
        if (
            hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config is not None
        ):
            return self.config.fivetran_log_config.destination_platform

        # For standard version, use the detected platform if available
        if "destination_platform" in connector.additional_properties:
            return connector.additional_properties.get("destination_platform")

        # Default to snowflake if detection failed
        return "snowflake"

    def _create_destination_urn(
        self, lineage, destination_details: PlatformDetail
    ) -> DatasetUrn:
        """Create a dataset URN for a destination table."""
        destination_table = (
            lineage.destination_table
            if destination_details.include_schema_in_urn
            else lineage.destination_table.split(".", 1)[1]
        )

        # Safe access to database.lower() with None check
        destination_table_name = (
            f"{destination_details.database.lower()}.{destination_table}"
            if destination_details.database
            else destination_table
        )

        return DatasetUrn.create_from_ids(
            platform_id=destination_details.platform,
            table_name=destination_table_name,
            env=destination_details.env,
            platform_instance=destination_details.platform_instance,
        )

    def _generate_dataflow_from_connector(self, connector: Connector) -> DataFlow:
        """Generate a DataFlow entity from a connector."""
        # Extract connector-specific metadata to enrich the dataflow
        description = f"Fivetran connector for {connector.connector_type}"
        properties = {}

        # Add connector properties to DataFlow
        for k, v in connector.additional_properties.items():
            properties[k] = str(v)

        # Add connector core properties
        properties["connector_type"] = connector.connector_type
        properties["sync_frequency"] = str(connector.sync_frequency)
        properties["paused"] = str(connector.paused)
        properties["destination_id"] = connector.destination_id

        return DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=connector.connector_id,
            env=self.config.env,
            name=connector.connector_name,
            description=description,
            properties=properties,
            platform_instance=self.config.platform_instance,
        )

    def _generate_datajob_from_connector(self, connector: Connector) -> DataJob:
        """Generate a DataJob entity from a connector."""
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=connector.connector_id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Get owner information
        owner_email = self.fivetran_access.get_user_email(connector.user_id)

        # Create job description based on connector properties
        description = f"Fivetran data pipeline from {connector.connector_type}"
        if "destination_platform" in connector.additional_properties:
            destination = connector.additional_properties.get("destination_platform")
            description += f" to {destination}"

        # Create the DataJob with basic information
        datajob = DataJob(
            id=connector.connector_id,
            flow_urn=dataflow_urn,
            name=connector.connector_name,
            description=description,
            owners={owner_email} if owner_email else set(),
        )

        # Map connector source and destination table with dataset entity
        # Also extend the fine grained lineage of column if include_column_lineage is True
        lineage_properties = self._extend_lineage(connector=connector, datajob=datajob)

        # Extract connector properties for the DataJob
        connector_properties: Dict[str, str] = {
            "connector_id": connector.connector_id,
            "connector_type": connector.connector_type,
            "paused": str(connector.paused),
            "sync_frequency": str(connector.sync_frequency),
            "destination_id": connector.destination_id,
        }

        # Combine all properties
        datajob.properties = {
            **connector_properties,
            **lineage_properties,
        }

        return datajob

    def _generate_dpi_from_job(self, job: Job, datajob: DataJob) -> DataProcessInstance:
        """Generate a DataProcessInstance entity from a job."""
        return DataProcessInstance.from_datajob(
            datajob=datajob,
            id=job.job_id,
            clone_inlets=True,
            clone_outlets=True,
        )

    def _get_dpi_workunits(
        self, job: Job, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a DataProcessInstance."""
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
        """Generate workunits for a connector."""
        self.report.report_connectors_scanned()

        # Create dataflow entity with detailed properties from connector
        dataflow = self._generate_dataflow_from_connector(connector)
        for mcp in dataflow.generate_mcp():
            yield mcp.as_workunit()

        # Map Fivetran's connector entity with Datahub's datajob entity
        datajob = self._generate_datajob_from_connector(connector)
        for mcp in datajob.generate_mcp(materialize_iolets=False):
            yield mcp.as_workunit()

        # Map Fivetran's job/sync history entity with Datahub's data process entity
        if len(connector.jobs) >= MAX_JOBS_PER_CONNECTOR:
            self.report.warning(
                title="Not all sync history was captured",
                message=f"The connector had more than {MAX_JOBS_PER_CONNECTOR} sync runs in the past {self.config.history_sync_lookback_period} days. "
                f"Only the most recent {MAX_JOBS_PER_CONNECTOR} syncs were ingested.",
                context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
            )

        # Sort jobs by end_time to ensure most recent jobs are included
        sorted_jobs = sorted(connector.jobs, key=lambda j: j.end_time, reverse=True)
        for job in sorted_jobs[:MAX_JOBS_PER_CONNECTOR]:
            dpi = self._generate_dpi_from_job(job, datajob)
            yield from self._get_dpi_workunits(job, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Get the workunit processors for this source."""
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
        connectors = self.fivetran_access.get_allowed_connectors_list(
            self.config.connector_patterns,
            self.config.destination_patterns,
            self.report,
            self.config.history_sync_lookback_period,
        )
        for connector in connectors:
            logger.info(f"Processing connector id: {connector.connector_id}")
            yield from self._get_connector_workunits(connector)

    def get_report(self) -> SourceReport:
        """Get the report for this source."""
        return self.report
