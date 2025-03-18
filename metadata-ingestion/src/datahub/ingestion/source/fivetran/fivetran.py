import logging
from typing import Dict, Iterable, List, Optional

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
from datahub.ingestion.source.fivetran.data_classes import Connector, Job, TableLineage
from datahub.ingestion.source.fivetran.fivetran_access import (
    create_fivetran_access,
)
from datahub.ingestion.source.fivetran.fivetran_constants import DataJobMode
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

    def _get_source_details(self, connector: Connector) -> PlatformDetail:
        """Get source platform details for a connector."""
        # Look up source details in the configuration mapping
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

        logger.debug(
            f"Source details for connector {connector.connector_id}: "
            f"platform={source_details.platform}, "
            f"platform_instance={source_details.platform_instance}, "
            f"database={source_details.database}"
        )

        return source_details

    def _get_destination_details(self, connector: Connector) -> PlatformDetail:
        """Get destination platform details for a connector."""
        # Look up destination details in the configuration mapping
        destination_details = self.config.destination_to_platform_instance.get(
            connector.destination_id, PlatformDetail()
        )

        # Set platform if not present
        if destination_details.platform is None:
            # First check if there's a destination platform in additional properties
            if "destination_platform" in connector.additional_properties:
                destination_details.platform = connector.additional_properties.get(
                    "destination_platform"
                )
            # Then try to get from fivetran_log_config
            elif (
                hasattr(self.config, "fivetran_log_config")
                and self.config.fivetran_log_config is not None
            ):
                destination_details.platform = (
                    self.config.fivetran_log_config.destination_platform
                )
            else:
                # Default based on the configuration
                destination_details.platform = (
                    "bigquery"
                    if (
                        hasattr(self.config, "fivetran_log_config")
                        and self.config.fivetran_log_config
                        and hasattr(
                            self.config.fivetran_log_config, "destination_platform"
                        )
                        and self.config.fivetran_log_config.destination_platform
                        == "bigquery"
                    )
                    else "snowflake"
                )

        # Set database if not present
        if destination_details.database is None:
            # First check if there's a destination database in additional properties
            if "destination_database" in connector.additional_properties:
                destination_details.database = connector.additional_properties.get(
                    "destination_database"
                )
            # For BigQuery, use the dataset from the config
            elif (
                destination_details.platform == "bigquery"
                and hasattr(self.config, "fivetran_log_config")
                and self.config.fivetran_log_config
                and hasattr(
                    self.config.fivetran_log_config, "bigquery_destination_config"
                )
                and self.config.fivetran_log_config.bigquery_destination_config
            ):
                destination_details.database = (
                    self.config.fivetran_log_config.bigquery_destination_config.dataset
                )
            # For Snowflake, use the database from the config
            elif (
                destination_details.platform == "snowflake"
                and hasattr(self.config, "fivetran_log_config")
                and self.config.fivetran_log_config
                and hasattr(
                    self.config.fivetran_log_config, "snowflake_destination_config"
                )
                and self.config.fivetran_log_config.snowflake_destination_config
            ):
                destination_details.database = self.config.fivetran_log_config.snowflake_destination_config.database
            else:
                # Fallback to fivetran_log_database
                destination_details.database = (
                    self.fivetran_access.fivetran_log_database or ""
                )

        logger.debug(
            f"Destination details for connector {connector.connector_id}: "
            f"platform={destination_details.platform}, "
            f"platform_instance={destination_details.platform_instance}, "
            f"database={destination_details.database}"
        )

        return destination_details

    def _extend_lineage(
        self,
        connector: Connector,
        datajob: DataJob,
        source_details: Optional[PlatformDetail] = None,
        destination_details: Optional[PlatformDetail] = None,
    ) -> Dict[str, str]:
        """Build lineage between source and destination datasets."""
        # Initialize empty lists for dataset URNs and fine-grained lineage
        input_dataset_urn_list: List[DatasetUrn] = []
        output_dataset_urn_list: List[DatasetUrn] = []
        fine_grained_lineage: List[FineGrainedLineage] = []

        # Obtain source and destination platform details if not provided
        if source_details is None:
            source_details = self._get_source_details(connector)
        if destination_details is None:
            destination_details = self._get_destination_details(connector)

        # Ensure platform is set to avoid URN creation issues
        if not source_details.platform:
            source_details.platform = self._detect_source_platform(connector)

        if not destination_details.platform:
            destination_details.platform = "snowflake"  # Default to snowflake

        # Log the lineage information for debugging
        logger.info(
            f"Processing lineage for connector {connector.connector_id}: "
            f"source_platform={source_details.platform}, "
            f"destination_platform={destination_details.platform}, "
            f"{len(connector.lineage)} table lineage entries"
        )

        # Handle lineage truncation if needed
        if len(connector.lineage) >= MAX_TABLE_LINEAGE_PER_CONNECTOR:
            self._report_lineage_truncation(connector)

        # Process each table lineage entry
        for lineage in connector.lineage:
            try:
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

                # Skip if either URN creation failed
                if not source_urn or not dest_urn:
                    logger.warning(
                        f"Skipping lineage for {lineage.source_table} -> {lineage.destination_table}: "
                        f"Failed to create URNs"
                    )
                    continue

                # Add URNs to lists (avoiding duplicates)
                if str(source_urn) not in [str(u) for u in input_dataset_urn_list]:
                    input_dataset_urn_list.append(source_urn)

                if str(dest_urn) not in [str(u) for u in output_dataset_urn_list]:
                    output_dataset_urn_list.append(dest_urn)

                # Create column lineage if enabled
                if self.config.include_column_lineage:
                    self._create_column_lineage(
                        lineage=lineage,
                        source_urn=source_urn,
                        dest_urn=dest_urn,
                        fine_grained_lineage=fine_grained_lineage,
                    )

                logger.debug(f"Created lineage from {source_urn} to {dest_urn}")
            except Exception as e:
                logger.warning(
                    f"Error creating lineage for table {lineage.source_table} -> {lineage.destination_table}: {e}"
                )

        # Log the lineage that was created for debugging
        logger.info(
            f"Created lineage with {len(input_dataset_urn_list)} input URNs and {len(output_dataset_urn_list)} output URNs"
        )

        # Add URNs and lineage to the datajob
        datajob.inlets.extend(input_dataset_urn_list)
        datajob.outlets.extend(output_dataset_urn_list)
        datajob.fine_grained_lineages.extend(fine_grained_lineage)

        # Build properties from details and connector properties
        lineage_properties = self._build_lineage_properties(
            connector=connector,
            source_details=source_details,
            destination_details=destination_details,
        )

        return lineage_properties

    def _create_dataset_urn(
        self, table_name: str, details: PlatformDetail, is_source: bool
    ) -> Optional[DatasetUrn]:
        """Create a dataset URN for a table."""
        if not table_name:
            return None

        try:
            # Handle schema inclusion based on configuration
            if not details.include_schema_in_urn and "." in table_name:
                table_name = table_name.split(".", 1)[1]

            # Ensure we have a platform
            platform = details.platform
            if not platform:
                platform = "snowflake" if not is_source else "external"
                logger.info(
                    f"Using default platform {platform} for {'source' if is_source else 'destination'} table {table_name}"
                )

            # Include database in the table name if available and ensure it's lowercase
            database = details.database.lower() if details.database else ""
            full_table_name = f"{database}.{table_name}" if database else table_name

            # Ensure environment is set
            env = details.env or "PROD"

            # Log the URN creation details for debugging
            logger.debug(
                f"Creating {'source' if is_source else 'destination'} URN with: "
                f"platform={platform}, table_name={full_table_name}, env={env}, "
                f"platform_instance={details.platform_instance}"
            )

            return DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=full_table_name,
                env=env,
                platform_instance=details.platform_instance,
            )
        except Exception as e:
            logger.warning(
                f"Failed to create {'source' if is_source else 'destination'} URN for {table_name}: {e}"
            )
            return None

    def _create_column_lineage(
        self,
        lineage: TableLineage,
        source_urn: Optional[DatasetUrn],
        dest_urn: Optional[DatasetUrn],
        fine_grained_lineage: List[FineGrainedLineage],
    ) -> None:
        """Create column-level lineage between source and destination tables."""
        if not source_urn or not dest_urn:
            return

        # Log details for debugging
        logger.info(f"Creating column lineage from {source_urn} to {dest_urn}")

        # If there are explicit column mappings, use them directly
        if lineage.column_lineage:
            # Log the number of column mappings we're processing
            logger.info(f"Processing {len(lineage.column_lineage)} column mappings")

            for column_lineage in lineage.column_lineage:
                if (
                    not column_lineage.source_column
                    or not column_lineage.destination_column
                    or column_lineage.destination_column.startswith("_fivetran")
                ):
                    continue

                # Log the column mapping
                logger.debug(
                    f"Column mapping: {column_lineage.source_column} -> {column_lineage.destination_column}"
                )

                try:
                    # Use column names directly as they should already be in the correct form
                    source_column = column_lineage.source_column
                    destination_column = column_lineage.destination_column

                    # For BigQuery specifically, ensure the field names are correctly formed
                    dest_platform = str(dest_urn).split(",")[0].split(":")[-1]
                    if dest_platform.lower() == "bigquery":
                        # BigQuery fields are case-sensitive, ensure proper case
                        destination_column = destination_column.lower()
                        # Make sure we don't have any special characters in field path
                        destination_column = destination_column.replace(".", "_")

                    # Create field URNs for source and destination
                    source_field_urn = builder.make_schema_field_urn(
                        str(source_urn),
                        source_column,
                    )

                    dest_field_urn = builder.make_schema_field_urn(
                        str(dest_urn),
                        destination_column,
                    )

                    # Add to fine-grained lineage
                    fine_grained_lineage.append(
                        FineGrainedLineage(
                            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                            upstreams=[source_field_urn],
                            downstreamType=FineGrainedLineageDownstreamType.FIELD,
                            downstreams=[dest_field_urn],
                        )
                    )

                    logger.debug(
                        f"Added field lineage: {source_field_urn} -> {dest_field_urn}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to create column lineage for {column_lineage.source_column} -> {column_lineage.destination_column}: {e}"
                    )

            # Log the total number of lineage entries created
            if fine_grained_lineage:
                logger.info(
                    f"Created {len(fine_grained_lineage)} field lineage entries for {source_urn} -> {dest_urn}"
                )
            else:
                logger.warning(
                    f"No valid column lineage mappings found for {source_urn} -> {dest_urn}"
                )
        else:
            # No column mappings provided - log a warning
            logger.warning(
                f"No column lineage data available for {lineage.source_table} -> {lineage.destination_table}. "
                f"This may indicate an issue with schema retrieval from the Fivetran API."
            )

            # Add a placeholder entry to indicate table-level lineage only
            fine_grained_lineage.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[str(source_urn)],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[str(dest_urn)],
                )
            )

    def _create_field_lineage_mcp(
        self,
        source_urn: DatasetUrn,
        dest_urn: DatasetUrn,
        lineage_field_map: Dict[str, List[str]],
    ) -> Optional[MetadataWorkUnit]:
        """
        Create field-level lineage between datasets using MetadataChangeProposal.

        Args:
            source_urn: Source dataset URN
            dest_urn: Destination dataset URN
            lineage_field_map: Map of destination field URNs to lists of source field URNs
        """
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
                DatasetLineageType,
                FineGrainedLineage,
                FineGrainedLineageDownstreamType,
                FineGrainedLineageUpstreamType,
                Upstream,
                UpstreamLineage,
            )

            # Create the upstream relationship
            upstream = Upstream(
                dataset=str(source_urn), type=DatasetLineageType.TRANSFORMED
            )

            # Create fine-grained lineages for each field mapping
            fine_grained_lineages = []

            for dest_field, source_fields in lineage_field_map.items():
                fine_grained_lineages.append(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=source_fields,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[dest_field],
                    )
                )

            # Create the lineage aspect
            upstream_lineage = UpstreamLineage(
                upstreams=[upstream], fineGrainedLineages=fine_grained_lineages
            )

            # Create and emit the MCP
            lineage_mcp = MetadataChangeProposalWrapper(
                entityUrn=str(dest_urn),
                aspect=upstream_lineage,
            )

            # Now create a workunit from this MCP
            wu = MetadataWorkUnit(id=f"{dest_urn}-field-lineage", mcp=lineage_mcp)

            # Return the workunit - it will be collected and emitted by the main process
            return wu

        except Exception as e:
            logger.error(f"Error creating field-level lineage MCP: {e}", exc_info=True)
            return None

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
            "azure_blob_storage": "abs",
            "gcs": "gcs",
            "google_cloud_storage": "gcs",
            "confluent_cloud": "kafka",
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
        return connector_type

    def _generate_dataflow_from_connector(self, connector: Connector) -> DataFlow:
        """Generate a DataFlow entity from a connector."""
        # Extract connector-specific metadata to enrich the dataflow
        connector_name = (
            connector.connector_name or f"Fivetran-{connector.connector_id}"
        )
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

        # Get destination platform with special handling for streaming sources
        # The connector type should not dictate the destination platform
        destination_details = self._get_destination_details(connector)
        destination: str

        # Special handling for streaming connectors
        if (
            connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            # For streaming sources, always use the destination from config
            destination = self.config.fivetran_log_config.destination_platform
            logger.info(
                f"Special handling for {connector.connector_type}: Using destination platform {destination} from config"
            )
        else:
            # For non-streaming sources, use the details from _get_destination_details
            destination = destination_details.platform or "snowflake"

        description += f" to {destination}"

        # Add destination platform to properties for transparency
        properties["destination_platform"] = destination

        return DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=connector.connector_id,
            env=self.config.env or "PROD",
            name=connector_name,
            description=description,
            properties=properties,
            platform_instance=self.config.platform_instance,
        )

    def _generate_datajob_for_table(
        self,
        connector: Connector,
        lineage: TableLineage,
        dataflow_urn: DataFlowUrn,
        source_details: PlatformDetail,
        destination_details: PlatformDetail,
    ) -> Optional[DataJob]:
        """Generate a DataJob entity for a specific table lineage."""
        source_table = lineage.source_table
        destination_table = lineage.destination_table

        # Create a unique ID for this table's job by combining connector and table names
        datajob_id = f"{connector.connector_id}.{source_table.replace('.', '_')}_to_{destination_table.replace('.', '_')}"
        # Truncate if too long
        if len(datajob_id) > 100:
            datajob_id = (
                f"{connector.connector_id}.{hash(source_table + destination_table)}"
            )

        # Create job name and description
        job_name = f"Fivetran: {source_table} → {destination_table}"
        job_description = (
            f"Fivetran data pipeline from {source_table} to {destination_table}"
        )

        # Get owner information
        owner_email = self.fivetran_access.get_user_email(connector.user_id)
        owner_set = {owner_email} if owner_email else set()

        # Create the DataJob instance
        datajob = DataJob(
            id=datajob_id,
            flow_urn=dataflow_urn,
            name=job_name,
            description=job_description,
            owners=owner_set,
        )

        # Build lineage for this specific table using the common function
        self._build_table_lineage(
            connector=connector,
            lineage=lineage,
            datajob=datajob,
            source_details=source_details,
            destination_details=destination_details,
        )

        # Add connector properties to the job
        connector_properties: Dict[str, str] = {
            "connector_id": connector.connector_id,
            "connector_name": connector.connector_name
            or f"Fivetran-{connector.connector_id}",
            "connector_type": connector.connector_type,
            "paused": str(connector.paused),
            "sync_frequency": str(connector.sync_frequency),
            "destination_id": connector.destination_id,
            "source_table": source_table,
            "destination_table": destination_table,
        }

        # Add platform details
        lineage_properties = self._build_lineage_properties(
            connector=connector,
            source_details=source_details,
            destination_details=destination_details,
        )

        # Combine all properties
        datajob.properties = {
            **connector_properties,
            **lineage_properties,
        }

        return datajob

    def _build_table_lineage(
        self,
        connector: Connector,
        lineage: TableLineage,
        datajob: DataJob,
        source_details: Optional[PlatformDetail] = None,
        destination_details: Optional[PlatformDetail] = None,
    ) -> None:
        """Build lineage between source and destination tables and add to datajob."""
        # Get platform details if not provided
        if source_details is None:
            source_details = self._get_source_details(connector)
            if not source_details.platform:
                source_details.platform = self._detect_source_platform(connector)

        if destination_details is None:
            destination_details = self._get_destination_details(connector)
            if not destination_details.platform:
                default_dest = "snowflake"
                if (
                    hasattr(self.config, "fivetran_log_config")
                    and self.config.fivetran_log_config
                ):
                    default_dest = self.config.fivetran_log_config.destination_platform
                destination_details.platform = default_dest

        # Extract source and destination information from the lineage object
        source_table = lineage.source_table
        destination_table = lineage.destination_table

        # Log detailed debug information for troubleshooting
        logger.debug(
            f"Building table lineage from {source_table} to {destination_table}"
        )
        logger.debug(
            f"Source details: platform={source_details.platform}, database={source_details.database}"
        )
        logger.debug(
            f"Destination details: platform={destination_details.platform}, database={destination_details.database}"
        )

        try:
            # Create source and destination URNs
            source_urn = self._create_dataset_urn(
                source_table,
                source_details,
                is_source=True,
            )

            dest_urn = self._create_dataset_urn(
                destination_table,
                destination_details,
                is_source=False,
            )

            # Skip if either URN creation failed
            if not source_urn or not dest_urn:
                logger.warning(
                    f"Skipping lineage for {source_table} -> {destination_table}: "
                    f"Failed to create URNs"
                )
                return

            # Add URNs to datajob (avoiding duplicates)
            if str(source_urn) not in [str(u) for u in datajob.inlets]:
                datajob.inlets.append(source_urn)
                # Log for debugging
                logger.debug(f"Added source URN: {source_urn}")

            if str(dest_urn) not in [str(u) for u in datajob.outlets]:
                datajob.outlets.append(dest_urn)
                # Log for debugging
                logger.debug(f"Added destination URN: {dest_urn}")

            # Create column lineage if enabled
            if self.config.include_column_lineage:
                fine_grained_lineage: List[FineGrainedLineage] = []
                self._create_column_lineage(
                    lineage=lineage,
                    source_urn=source_urn,
                    dest_urn=dest_urn,
                    fine_grained_lineage=fine_grained_lineage,
                )
                datajob.fine_grained_lineages.extend(fine_grained_lineage)
                # Log for debugging
                logger.debug(
                    f"Added {len(fine_grained_lineage)} column lineage entries"
                )

            logger.debug(f"Completed lineage from {source_urn} to {dest_urn}")
        except Exception as e:
            logger.warning(
                f"Error creating lineage for table {source_table} -> {destination_table}: {e}"
            )

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

    def _get_per_table_datajob_workunits(
        self, connector: Connector, dataflow: DataFlow
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a connector using per-table mode (one datajob per table)."""
        # Get source and destination platform details
        source_details = self._get_source_details(connector)
        source_details.platform = self._detect_source_platform(connector)

        destination_details = self._get_destination_details(connector)

        # Get dataflow URN for creating datajobs
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=connector.connector_id,
            env=self.config.env or "PROD",
            platform_instance=self.config.platform_instance,
        )

        # Create job instances for each table lineage
        processed_tables = set()
        table_job_map = {}  # Map to track table specific jobs

        for lineage in connector.lineage:
            # Create a unique key to avoid duplicates
            table_key = f"{lineage.source_table}:{lineage.destination_table}"
            if table_key in processed_tables:
                continue
            processed_tables.add(table_key)

            # Generate a datajob for this table lineage
            datajob = self._generate_datajob_for_table(
                connector=connector,
                lineage=lineage,
                dataflow_urn=dataflow_urn,
                source_details=source_details,
                destination_details=destination_details,
            )

            if datajob:
                # Store the datajob in our mapping
                table_job_map[table_key] = datajob

                # Emit the datajob
                for mcp in datajob.generate_mcp(materialize_iolets=False):
                    yield mcp.as_workunit()

        # Now process job history for each table
        sorted_jobs = sorted(connector.jobs, key=lambda j: j.end_time, reverse=True)[
            :5
        ]  # Limit to 5 recent jobs

        # For each job in connector's history, create DPIs for each table
        for job in sorted_jobs:
            for table_key, datajob in table_job_map.items():
                # Create a unique job ID for this table-specific job run
                table_job_id = f"{job.job_id}_{hash(table_key)}"

                # Create a DPI specific to this table
                table_dpi = DataProcessInstance.from_datajob(
                    datajob=datajob,
                    id=table_job_id,
                    clone_inlets=True,
                    clone_outlets=True,
                )

                # Generate DPI workunits
                yield from self._get_dpi_workunits(job, table_dpi)

    def _generate_datajob_from_connector(self, connector: Connector) -> DataJob:
        """Generate a DataJob entity from a connector."""
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=connector.connector_id,
            env=self.config.env or "PROD",
            platform_instance=self.config.platform_instance,
        )

        # Extract useful connector information
        connector_name = (
            connector.connector_name or f"Fivetran-{connector.connector_id}"
        )

        # Get source platform from connector type
        source_platform = self._detect_source_platform(connector)

        # Get destination platform - with special handling for streaming sources
        if (
            connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            # For streaming sources, always use the destination from config
            destination_platform = self.config.fivetran_log_config.destination_platform
            logger.info(
                f"Special handling for {connector.connector_type}: Using destination platform {destination_platform} from config"
            )
        else:
            # Normal path for non-streaming sources
            default_destination = (
                "bigquery"
                if (
                    hasattr(self.config, "fivetran_log_config")
                    and self.config.fivetran_log_config
                    and self.config.fivetran_log_config.destination_platform
                    == "bigquery"
                )
                else "snowflake"
            )
            destination_platform = connector.additional_properties.get(
                "destination_platform", default_destination
            )

        # Create job description
        description = f"Fivetran data pipeline from {connector.connector_type} to {destination_platform}"

        # Get owner information
        owner_email = self.fivetran_access.get_user_email(connector.user_id)
        owner_set = {owner_email} if owner_email else set()

        # Create the DataJob with enhanced information
        datajob = DataJob(
            id=connector.connector_id,
            flow_urn=dataflow_urn,
            name=connector_name,
            description=description,
            owners=owner_set,
        )

        # Map connector source and destination table with dataset entity
        # Also extend the fine grained lineage of column if include_column_lineage is True
        source_details = self._get_source_details(connector)
        source_details.platform = source_platform

        destination_details = self._get_destination_details(connector)
        # Override the platform for streaming sources
        if (
            connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]
            and hasattr(self.config, "fivetran_log_config")
            and self.config.fivetran_log_config
        ):
            destination_details.platform = (
                self.config.fivetran_log_config.destination_platform
            )
        else:
            destination_details.platform = destination_platform

        lineage_properties = self._extend_lineage(
            connector=connector,
            datajob=datajob,
            source_details=source_details,
            destination_details=destination_details,
        )

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

    def _get_consolidated_datajob_workunits(
        self, connector: Connector, dataflow: DataFlow
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a connector using consolidated mode (one datajob per connector)."""
        # Get source and destination details
        source_details = self._get_source_details(connector)
        source_details.platform = self._detect_source_platform(connector)

        destination_details = self._get_destination_details(connector)

        owner_email = self.fivetran_access.get_user_email(connector.user_id)
        owner_set = {owner_email} if owner_email else set()

        datajob = DataJob(
            id=connector.connector_id,
            flow_urn=DataFlowUrn.create_from_ids(
                orchestrator=Constant.ORCHESTRATOR,
                flow_id=connector.connector_id,
                env=self.config.env or "PROD",
                platform_instance=self.config.platform_instance,
            ),
            name=connector.connector_name or f"Fivetran-{connector.connector_id}",
            description=f"Fivetran data pipeline from {connector.connector_type} to {destination_details.platform}",
            owners=owner_set,
        )

        # Process each table lineage through the common function
        for lineage in connector.lineage:
            self._build_table_lineage(
                connector=connector,
                lineage=lineage,
                datajob=datajob,
                source_details=source_details,
                destination_details=destination_details,
            )

        # Add connector properties
        connector_properties: Dict[str, str] = {
            "connector_id": connector.connector_id,
            "connector_type": connector.connector_type,
            "paused": str(connector.paused),
            "sync_frequency": str(connector.sync_frequency),
            "destination_id": connector.destination_id,
        }

        # Add platform details
        lineage_properties = self._build_lineage_properties(
            connector=connector,
            source_details=source_details,
            destination_details=destination_details,
        )

        # Combine all properties
        datajob.properties = {
            **connector_properties,
            **lineage_properties,
        }

        # Emit the datajob
        for mcp in datajob.generate_mcp(materialize_iolets=False):
            yield mcp.as_workunit()

        # Process job history
        if len(connector.jobs) >= MAX_JOBS_PER_CONNECTOR:
            self._report_lineage_truncation(connector)
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

    def _get_connector_workunits(
        self, connector: Connector
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a connector."""
        self.report.report_connectors_scanned()

        # Create dataflow entity with detailed properties from connector
        dataflow = self._generate_dataflow_from_connector(connector)
        for mcp in dataflow.generate_mcp():
            yield mcp.as_workunit()

        # Store field lineage workunits to emit after dataset workunits
        field_lineage_workunits = []

        # Check if we should create one datajob per table or one per connector
        if self.config.datajob_mode == DataJobMode.PER_TABLE:
            # Create one datajob per table
            for wu in self._get_per_table_datajob_workunits(connector, dataflow):
                # If this is a field lineage workunit, store it for later
                if wu.id.endswith("-field-lineage"):
                    field_lineage_workunits.append(wu)
                else:
                    yield wu
        else:
            # Default: consolidated mode - one datajob per connector
            for wu in self._get_consolidated_datajob_workunits(connector, dataflow):
                # If this is a field lineage workunit, store it for later
                if wu.id.endswith("-field-lineage"):
                    field_lineage_workunits.append(wu)
                else:
                    yield wu

        # Now emit the field lineage workunits after all dataset workunits
        for wu in field_lineage_workunits:
            yield wu

    def _report_lineage_truncation(self, connector: Connector) -> None:
        """Report warning about truncated lineage."""
        self.report.warning(
            title="Table lineage truncated",
            message=f"The connector had more than {MAX_TABLE_LINEAGE_PER_CONNECTOR} table lineage entries. "
            f"Only the most recent {MAX_TABLE_LINEAGE_PER_CONNECTOR} entries were ingested.",
            context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
        )

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
