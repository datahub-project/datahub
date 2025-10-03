import functools
import logging
from typing import Dict, Iterable, List, Optional, Union

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter import mce_builder as builder
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
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
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.fivetran_access import (
    create_fivetran_access,
)
from datahub.ingestion.source.fivetran.fivetran_constants import (
    MAX_JOBS_PER_CONNECTOR,
    DataJobMode,
    get_platform_from_fivetran_service,
)
from datahub.ingestion.source.fivetran.models import Connector, Job, TableLineage
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetLineageTypeClass
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

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

        # Alias for consistency with existing interface
        self.audit_log = self.fivetran_access

    def _get_source_details(self, connector: Connector) -> PlatformDetail:
        """Get source platform details for a connector."""
        # Look up source details in the configuration mapping
        source_details = self.config.sources_to_platform_instance.get(
            connector.connector_id, PlatformDetail()
        )

        # Map connector type to known platform using service information from API
        if source_details.platform is None:
            # Use the improved service-based mapping instead of hardcoded connector aliases
            source_details.platform = get_platform_from_fivetran_service(
                connector.connector_type
            )

        # Auto-detect source database if not present in config
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
                platform = connector.additional_properties.get("destination_platform")
                destination_details.platform = (
                    str(platform) if platform is not None else None
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
                database = connector.additional_properties.get("destination_database")
                destination_details.database = (
                    str(database) if database is not None else None
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

    def _build_source_details(
        self, connector: Connector, lineage: TableLineage
    ) -> PlatformDetail:
        """Build source details using metadata from TableLineage and connector."""
        # Start with existing logic as fallback
        source_details = self._get_source_details(connector)

        # Enhance with TableLineage metadata if available
        if lineage.source_platform:
            source_details.platform = lineage.source_platform
            logger.debug(
                f"Using source platform from lineage metadata: {lineage.source_platform}"
            )
        elif not source_details.platform:
            source_details.platform = self._detect_source_platform(connector)

        if lineage.source_database:
            source_details.database = lineage.source_database
            logger.debug(
                f"Using source database from lineage metadata: {lineage.source_database}"
            )

        if lineage.source_env:
            source_details.env = lineage.source_env
            logger.debug(
                f"Using source env from lineage metadata: {lineage.source_env}"
            )

        # Use connector metadata if available
        if lineage.connector_type_id and not source_details.platform:
            detected_platform = self._detect_platform_from_connector_type(
                lineage.connector_type_id
            )
            if detected_platform:
                source_details.platform = detected_platform
                logger.debug(
                    f"Detected source platform from connector type: {detected_platform}"
                )

        return source_details

    def _build_destination_details(
        self, connector: Connector, lineage: TableLineage
    ) -> PlatformDetail:
        """Build destination details using metadata from TableLineage and connector."""
        # Start with existing logic as fallback
        destination_details = self._get_destination_details(connector)

        # Enhance with TableLineage metadata if available
        if lineage.destination_platform:
            destination_details.platform = lineage.destination_platform
            logger.debug(
                f"Using destination platform from lineage metadata: {lineage.destination_platform}"
            )
        elif not destination_details.platform:
            # Use fallback logic
            default_dest = "snowflake"
            if (
                hasattr(self.config, "fivetran_log_config")
                and self.config.fivetran_log_config
            ):
                default_dest = self.config.fivetran_log_config.destination_platform
            destination_details.platform = default_dest

        if lineage.destination_database:
            destination_details.database = lineage.destination_database
            logger.debug(
                f"Using destination database from lineage metadata: {lineage.destination_database}"
            )

        if lineage.destination_env:
            destination_details.env = lineage.destination_env
            logger.debug(
                f"Using destination env from lineage metadata: {lineage.destination_env}"
            )

        return destination_details

    def _detect_platform_from_connector_type(
        self, connector_type_id: str
    ) -> Optional[str]:
        """Detect source platform based on Fivetran connector type ID using existing mapping."""
        if not connector_type_id:
            return None

        # Use the existing platform detection function
        detected_platform = get_platform_from_fivetran_service(connector_type_id)

        # Don't return 'unknown' or the raw service name if it's not a known DataHub platform
        if (
            detected_platform
            and detected_platform != "unknown"
            and detected_platform != connector_type_id.lower()
        ):
            logger.debug(
                f"Detected platform '{detected_platform}' from connector type '{connector_type_id}'"
            )
            return detected_platform

        logger.debug(
            f"Could not detect platform for connector type: {connector_type_id}"
        )
        return None

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
        fine_grained_lineage: List[FineGrainedLineageClass] = []

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
        max_lineage_limit = self.config.max_table_lineage_per_connector
        if max_lineage_limit != -1 and len(connector.lineage) >= max_lineage_limit:
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

        # Add URNs and lineage to the datajob using SDK API
        if input_dataset_urn_list:
            datajob.set_inlets([str(urn) for urn in input_dataset_urn_list])
        if output_dataset_urn_list:
            datajob.set_outlets([str(urn) for urn in output_dataset_urn_list])
        if fine_grained_lineage:
            datajob.set_fine_grained_lineages(fine_grained_lineage)

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
        """Create a dataset URN for a table with enhanced handling for BigQuery datasets."""
        if not table_name:
            logger.warning("Cannot create dataset URN: empty table name provided")
            return None

        try:
            # Handle schema inclusion based on configuration
            if not details.include_schema_in_urn and "." in table_name:
                logger.debug(
                    f"Removing schema from table name due to include_schema_in_urn=False: {table_name}"
                )
                table_name = table_name.split(".", 1)[1]

            # Ensure we have a platform
            platform = details.platform
            if not platform:
                platform = "snowflake" if not is_source else "external"
                logger.info(
                    f"Using default platform {platform} for {'source' if is_source else 'destination'} table {table_name}"
                )

            # Include database in the table name if available
            database = details.database.lower() if details.database else ""

            # If include_schema_in_urn=False, table_name won't have the schema part
            if "." in table_name:
                schema, table = table_name.split(".", 1)
                table_name = f"{schema.lower()}.{table.lower()}"
            else:
                table_name = table_name.lower()

            full_table_name = f"{database}.{table_name}" if database else table_name
            logger.debug(f"Dataset URN table name: {full_table_name}")

            # Ensure environment is set
            env = details.env or "PROD"

            # Log the URN creation details for debugging
            logger.debug(
                f"Creating {'source' if is_source else 'destination'} URN with: "
                f"platform={platform}, table_name={full_table_name}, env={env}, "
                f"platform_instance={details.platform_instance}"
            )

            urn_str = make_dataset_urn_with_platform_instance(
                platform=platform,
                name=full_table_name,
                platform_instance=details.platform_instance,
                env=env,
            )

            urn = DatasetUrn.from_string(urn_str)
            logger.debug(f"Created URN: {urn}")
            return urn
        except Exception as e:
            logger.warning(
                f"Failed to create {'source' if is_source else 'destination'} URN for {table_name}: {e}",
                exc_info=True,
            )
            return None

    def _create_column_lineage(
        self,
        lineage: TableLineage,
        source_urn: Optional[DatasetUrn],
        dest_urn: Optional[DatasetUrn],
        fine_grained_lineage: List[FineGrainedLineageClass],
    ) -> None:
        """Create column-level lineage between source and destination tables with better diagnostics."""
        if not source_urn or not dest_urn:
            logger.warning(
                "Cannot create column lineage: Missing source or destination URN"
            )
            return

        logger.info(f"Creating column lineage from {source_urn} to {dest_urn}")

        # Extract destination platform from the URN
        dest_platform = str(dest_urn).split(",")[0].split(":")[-1]
        is_bigquery = dest_platform.lower() == "bigquery"

        if not lineage.column_lineage:
            logger.warning(
                f"No column lineage data available for {lineage.source_table} -> {lineage.destination_table}"
            )
            return

        logger.info(f"Processing {len(lineage.column_lineage)} column mappings")

        # Filter out invalid column mappings
        valid_lineage = []
        for column_lineage in lineage.column_lineage:
            if (
                not column_lineage.source_column
                or not column_lineage.destination_column
            ):
                logger.debug(
                    "Skipping invalid column mapping: missing source or destination column"
                )
                continue

            if column_lineage.destination_column.startswith("_fivetran"):
                logger.debug(
                    f"Skipping Fivetran system column: {column_lineage.destination_column}"
                )
                continue

            valid_lineage.append(column_lineage)

        if not valid_lineage:
            logger.warning("No valid column mappings found after filtering")
            return

        # Process valid column mappings
        for column_lineage in valid_lineage:
            try:
                # Create field URNs
                source_field_urn = builder.make_schema_field_urn(
                    str(source_urn),
                    column_lineage.source_column,
                )

                # For BigQuery, ensure proper case and format
                dest_column = column_lineage.destination_column
                if is_bigquery:
                    # Ensure it's lowercase for BigQuery
                    dest_column = dest_column.lower()

                dest_field_urn = builder.make_schema_field_urn(
                    str(dest_urn),
                    dest_column,
                )

                # Add to fine-grained lineage
                fine_grained_lineage.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[source_field_urn],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
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
        """Detect source platform for a connector using service information from API."""
        # Use the improved service-based mapping
        platform = get_platform_from_fivetran_service(connector.connector_type)

        # Log if we're using the service name directly (no mapping found)
        if platform == connector.connector_type.lower():
            self.report.info(
                title="Using service name as platform",
                message="No explicit platform mapping found for this connector service. "
                "Using the Fivetran service name as the DataHub platform.",
                context=f"{connector.connector_name} (connector_id: {connector.connector_id}, service: {connector.connector_type})",
            )

        return platform

    def _generate_dataflow_from_connector(self, connector: Connector) -> DataFlow:
        """Generate a DataFlow entity from a connector."""
        # Extract connector-specific metadata to enrich the dataflow
        connector_name = connector.connector_name or connector.connector_id
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
            platform=Constant.ORCHESTRATOR,
            name=connector.connector_id,
            env=self.config.env or "PROD",
            display_name=connector_name,
            description=description,
            custom_properties=properties,
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
        job_name = f"{source_table} → {destination_table}"
        job_description = f"Data pipeline from {source_table} to {destination_table}"

        # Get owner information
        owner_email = (
            self.fivetran_access.get_user_email(connector.user_id)
            if connector.user_id
            else None
        )

        # Create the DataJob instance
        datajob = DataJob(
            name=datajob_id,
            flow_urn=dataflow_urn,
            display_name=job_name,
            description=job_description,
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
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
            "connector_name": connector.connector_name or connector.connector_id,
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
        datajob.set_custom_properties(
            {
                **connector_properties,
                **lineage_properties,
            }
        )

        return datajob

    def _validate_lineage_data(
        self,
        connector: Connector,
        lineage: TableLineage,
        source_details: PlatformDetail,
        destination_details: PlatformDetail,
    ) -> bool:
        """Validate lineage data before creating DataHub entities."""
        validation_errors = []

        # Validate basic table information
        if not lineage.source_table or not lineage.source_table.strip():
            validation_errors.append("Missing or empty source table name")

        if not lineage.destination_table or not lineage.destination_table.strip():
            validation_errors.append("Missing or empty destination table name")

        # Validate platform information
        if not source_details.platform:
            validation_errors.append("Missing source platform information")

        if not destination_details.platform:
            validation_errors.append("Missing destination platform information")

        # Validate connector information
        if not connector.connector_id:
            validation_errors.append("Missing connector ID")

        # Validate column lineage if present
        if lineage.column_lineage:
            for i, col_lineage in enumerate(lineage.column_lineage):
                if (
                    not col_lineage.source_column
                    or not col_lineage.source_column.strip()
                ):
                    validation_errors.append(
                        f"Column lineage {i}: Missing source column name"
                    )

                if (
                    not col_lineage.destination_column
                    or not col_lineage.destination_column.strip()
                ):
                    validation_errors.append(
                        f"Column lineage {i}: Missing destination column name"
                    )

                # Skip Fivetran system columns
                if col_lineage.destination_column.startswith("_fivetran"):
                    continue

        # Log validation results
        if validation_errors:
            logger.warning(
                f"Lineage validation failed for connector {connector.connector_id}, "
                f"table {lineage.source_table} -> {lineage.destination_table}. "
                f"Errors: {'; '.join(validation_errors)}"
            )
            return False

        logger.debug(
            f"Lineage validation passed for {lineage.source_table} -> {lineage.destination_table}"
        )
        return True

    def _build_table_lineage(
        self,
        connector: Connector,
        lineage: TableLineage,
        datajob: DataJob,
        source_details: Optional[PlatformDetail] = None,
        destination_details: Optional[PlatformDetail] = None,
    ) -> None:
        """Build lineage between source and destination tables and add to datajob."""
        # Use metadata from TableLineage object if available
        if source_details is None:
            source_details = self._build_source_details(connector, lineage)

        if destination_details is None:
            destination_details = self._build_destination_details(connector, lineage)

        # Validate lineage data before processing
        if not self._validate_lineage_data(
            connector, lineage, source_details, destination_details
        ):
            logger.warning(
                f"Skipping invalid lineage for {lineage.source_table} -> {lineage.destination_table}"
            )
            return

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
            # Create source and destination URNs with detailed error handling
            source_urn = None
            dest_urn = None

            try:
                source_urn = self._create_dataset_urn(
                    source_table,
                    source_details,
                    is_source=True,
                )
            except Exception as e:
                logger.error(
                    f"Failed to create source URN for {source_table}: {e}",
                    exc_info=True,
                )

            try:
                dest_urn = self._create_dataset_urn(
                    destination_table,
                    destination_details,
                    is_source=False,
                )
            except Exception as e:
                logger.error(
                    f"Failed to create destination URN for {destination_table}: {e}",
                    exc_info=True,
                )

            # Skip if either URN creation failed
            if not source_urn or not dest_urn:
                logger.warning(
                    f"Skipping lineage for {source_table} -> {destination_table}: "
                    f"Failed to create URNs (source_urn: {source_urn is not None}, "
                    f"dest_urn: {dest_urn is not None})"
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
                fine_grained_lineage: List[FineGrainedLineageClass] = []
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

    def _create_synthetic_datajob_from_connector(
        self, connector: Connector, dataflow_urn: Optional[DataFlowUrn] = None
    ) -> DataJob:
        """Generate a synthetic DataJob entity for connectors with lineage but no job history."""
        if dataflow_urn is None:
            dataflow_urn = DataFlowUrn.create_from_ids(
                orchestrator=Constant.ORCHESTRATOR,
                flow_id=connector.connector_id,
                env=self.config.env or "PROD",
                platform_instance=self.config.platform_instance,
            )

        # Extract useful connector information
        connector_name = connector.connector_name or connector.connector_id

        # Get source platform from connector type
        source_platform = self._detect_source_platform(connector)

        # Get destination platform in a more platform-agnostic way
        destination_platform = self._get_destination_platform(connector)

        # Create job description
        description = (
            f"Data pipeline from {connector.connector_type} to {destination_platform}"
        )

        # Get owner information
        owner_email = (
            self.fivetran_access.get_user_email(connector.user_id)
            if connector.user_id
            else None
        )

        # Create the DataJob with enhanced information
        datajob = DataJob(
            name=connector.connector_id,
            flow_urn=dataflow_urn,
            display_name=connector_name,
            description=description,
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
        )

        # Map connector source and destination table with dataset entity
        # Also extend the fine grained lineage of column if include_column_lineage is True
        source_details = self._get_source_details(connector)
        source_details.platform = source_platform

        destination_details = self._get_destination_details(connector)

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
            "synthetic": "true",
            "lineage_only": "true",
        }

        # Combine all properties
        datajob.set_custom_properties(
            {
                **connector_properties,
                **lineage_properties,
            }
        )

        return datajob

    def _get_destination_platform(self, connector: Connector) -> str:
        """
        Determine the destination platform in a platform-agnostic way.

        Order of precedence:
        1. Check destination_to_platform_instance config for this destination
        2. Check connector's additional_properties (from API detection)
        3. Check destination_details.platform from _get_destination_details
        4. Use a safe default based on common standards
        """
        # First check for explicit mapping in config
        if (
            hasattr(self.config, "destination_to_platform_instance")
            and connector.destination_id in self.config.destination_to_platform_instance
        ):
            platform_details = self.config.destination_to_platform_instance[
                connector.destination_id
            ]
            if platform_details.platform:
                logger.info(
                    f"Using destination platform '{platform_details.platform}' from config for {connector.destination_id}"
                )
                return platform_details.platform

        # Next check additional properties from API
        if "destination_platform" in connector.additional_properties:
            platform = connector.additional_properties["destination_platform"]
            if platform is not None:
                platform_str = str(platform)
            logger.info(
                f"Using destination platform '{platform_str}' from connector properties for {connector.connector_id}"
            )
            return platform_str

        # Use _get_destination_details which has its own logic for detecting platforms
        destination_details = self._get_destination_details(connector)
        if destination_details.platform:
            platform = destination_details.platform
            logger.info(
                f"Using destination platform '{platform}' from destination details for {connector.connector_id}"
            )
            return platform

        # If we still don't have a platform, use a safe default without assumptions
        # First check if it's a streaming source
        if connector.connector_type.lower() in ["confluent_cloud", "kafka", "pubsub"]:
            logger.info(
                f"Detected streaming connector type {connector.connector_type}, using 'kafka' as destination platform"
            )
            return "kafka"

        # Final fallback - use a generic platform name
        logger.info(
            f"No specific destination platform detected for {connector.connector_id}, using 'database' as generic platform"
        )
        return "database"

    def _generate_dpi_from_job(self, job: Job, datajob: DataJob) -> DataProcessInstance:
        """Generate a DataProcessInstance entity from a job."""
        return DataProcessInstance(
            id=job.job_id,
            orchestrator=datajob.flow_urn.orchestrator,
            cluster=datajob.flow_urn.cluster,
            template_urn=datajob.urn,
            data_platform_instance=self.config.platform_instance,
            inlets=list(datajob.inlets),
            outlets=list(datajob.outlets),
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
            created_ts_millis=start_timestamp_millis, materialize_iolets=True
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
        dataflow_urn = dataflow.urn

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
                for workunit in datajob.as_workunits():
                    yield workunit

        # Now process job history for each table
        sorted_jobs = sorted(connector.jobs, key=lambda j: j.end_time, reverse=True)[
            :MAX_JOBS_PER_CONNECTOR
        ]

        # For each job in connector's history, create DPIs for each table
        # Note: In per-table mode, each job represents a connector-level sync that affects all tables
        # We create separate DPIs for each table to show table-level execution status
        # This is intentional - each Fivetran job sync affects all tables in the connector
        for job in sorted_jobs:
            for table_key, datajob in table_job_map.items():
                # Create a unique DPI ID that combines job and table info
                # Use a more readable format for the ID
                source_table, dest_table = table_key.split(":", 1)
                table_job_id = f"{job.job_id}_{source_table.replace('.', '_')}_to_{dest_table.replace('.', '_')}"

                # Create a DPI specific to this table for this job execution
                table_dpi = DataProcessInstance(
                    id=table_job_id,
                    orchestrator=datajob.flow_urn.orchestrator,
                    cluster=datajob.flow_urn.cluster,
                    template_urn=datajob.urn,
                    data_platform_instance=self.config.platform_instance,
                    inlets=list(datajob.inlets),
                    outlets=list(datajob.outlets),
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
        connector_name = connector.connector_name or connector.connector_id

        # Get source platform from connector type
        source_platform = self._detect_source_platform(connector)

        # Get destination platform - with special handling for streaming sources
        destination_platform: str
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
            # For API-based approach, we can support any destination platform
            # Get from connector properties first, then fall back to detecting from destination
            platform = connector.additional_properties.get("destination_platform")
            if platform is not None:
                destination_platform = str(platform)
            else:
                # Try to detect from the destination details
                destination_details = self._get_destination_details(connector)
                destination_platform = destination_details.platform or "unknown"

        # Create job description
        description = (
            f"Data pipeline from {connector.connector_type} to {destination_platform}"
        )

        # Get owner information
        owner_email = (
            self.fivetran_access.get_user_email(connector.user_id)
            if connector.user_id
            else None
        )

        # Create the DataJob with enhanced information
        datajob = DataJob(
            name=connector.connector_id,
            flow_urn=dataflow_urn,
            display_name=connector_name,
            description=description,
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
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
        datajob.set_custom_properties(
            {
                **connector_properties,
                **lineage_properties,
            }
        )

        return datajob

    def _get_consolidated_datajob_workunits(
        self, connector: Connector, dataflow: DataFlow
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a connector using consolidated mode (one datajob per connector)."""
        # Get source and destination details
        source_details = self._get_source_details(connector)
        source_details.platform = self._detect_source_platform(connector)

        destination_details = self._get_destination_details(connector)

        owner_email = (
            self.fivetran_access.get_user_email(connector.user_id)
            if connector.user_id
            else None
        )

        datajob = DataJob(
            name=connector.connector_id,
            flow_urn=dataflow.urn,
            display_name=connector.connector_name or connector.connector_id,
            description=f"Data pipeline from {connector.connector_type} to {destination_details.platform}",
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
        )

        # Add lineage to the datajob (inlets, outlets, fine-grained lineage)
        lineage_properties = self._extend_lineage(
            connector=connector,
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

        # Note: lineage_properties already obtained from _extend_lineage above

        # Combine all properties
        datajob.set_custom_properties(
            {
                **connector_properties,
                **lineage_properties,
            }
        )

        # Emit the datajob
        for workunit in datajob.as_workunits():
            yield workunit

        # Process job history
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

    def _get_connector_workunits(
        self, connector: Connector
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for a connector, ensuring lineage works even without job history."""
        self.report.report_connectors_scanned()

        # Create dataflow entity with detailed properties from connector
        dataflow = self._generate_dataflow_from_connector(connector)
        for workunit in dataflow.as_workunits():
            yield workunit

        # Store field lineage workunits to emit after dataset workunits
        field_lineage_workunits = []

        # Check if connector has any lineage at all
        if not connector.lineage:
            logger.warning(
                f"No lineage data available for connector '{connector.connector_name}' ({connector.connector_id}). "
                f"This connector will not contribute any lineage information to DataHub. "
                f"If lineage is expected, check: 1) Connector has enabled tables, 2) Tables are syncing data, "
                f"3) API permissions include lineage access, 4) Connector is properly configured."
            )
            # Still create the dataflow, but without lineage
            return

        # Special handling for connectors with lineage but no job history
        if not connector.jobs and connector.lineage:
            logger.info(
                f"Connector {connector.connector_name} (ID: {connector.connector_id}) "
                f"has {len(connector.lineage)} lineage entries but no job history. "
                f"Creating synthetic jobs for lineage."
            )

            # Check if we should create one datajob per table or one per connector
            if self.config.datajob_mode == DataJobMode.PER_TABLE:
                # Create one datajob per table
                # Get source and destination details
                source_details = self._get_source_details(connector)
                source_details.platform = self._detect_source_platform(connector)

                destination_details = self._get_destination_details(connector)

                dataflow_urn = dataflow.urn

                # Keep track of tables processed to avoid duplicates
                processed_tables = set()

                # Process each table lineage entry
                for lineage in connector.lineage:
                    # Create a unique key to avoid duplicates
                    table_key = f"{lineage.source_table}:{lineage.destination_table}"
                    if table_key in processed_tables:
                        continue
                    processed_tables.add(table_key)

                    # Generate a synthetic datajob for this table lineage
                    datajob = self._create_synthetic_datajob_for_table(
                        connector=connector,
                        lineage=lineage,
                        dataflow_urn=dataflow_urn,
                        source_details=source_details,
                        destination_details=destination_details,
                    )

                    if datajob:
                        # Emit the datajob
                        for workunit in datajob.as_workunits():
                            if workunit.id.endswith("-field-lineage"):
                                field_lineage_workunits.append(workunit)
                            else:
                                yield workunit
            else:
                # Default: consolidated mode - one datajob per connector
                # Create a single synthetic datajob with all lineage
                synthetic_datajob = self._create_synthetic_datajob_from_connector(
                    connector, dataflow.urn
                )

                # Emit the datajob
                for workunit in synthetic_datajob.as_workunits():
                    if workunit.id.endswith("-field-lineage"):
                        field_lineage_workunits.append(workunit)
                    else:
                        yield workunit
        else:
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

        # Generate upstreamLineage aspects for destination datasets
        # This is crucial for DataHub to display lineage relationships
        if connector.lineage:
            yield from self._create_upstream_lineage_workunits(connector)

        # Now emit the field lineage workunits after all dataset workunits
        for wu in field_lineage_workunits:
            yield wu

    def _create_synthetic_datajob_for_table(
        self,
        connector: Connector,
        lineage: TableLineage,
        dataflow_urn: DataFlowUrn,
        source_details: PlatformDetail,
        destination_details: PlatformDetail,
    ) -> Optional[DataJob]:
        """Generate a synthetic DataJob entity for a specific table lineage when no job history exists."""
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
        job_name = f"{source_table} → {destination_table}"
        job_description = f"Data pipeline from {source_table} to {destination_table}"

        # Get owner information
        owner_email = (
            self.fivetran_access.get_user_email(connector.user_id)
            if connector.user_id
            else None
        )

        # Create the DataJob instance
        datajob = DataJob(
            name=datajob_id,
            flow_urn=dataflow_urn,
            display_name=job_name,
            description=job_description,
            owners=[CorpUserUrn(owner_email)] if owner_email else None,
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
            "connector_name": connector.connector_name or connector.connector_id,
            "connector_type": connector.connector_type,
            "paused": str(connector.paused),
            "sync_frequency": str(connector.sync_frequency),
            "destination_id": connector.destination_id,
            "source_table": source_table,
            "destination_table": destination_table,
            "synthetic": "true",
            "lineage_only": "true",
        }

        # Add platform details
        lineage_properties = self._build_lineage_properties(
            connector=connector,
            source_details=source_details,
            destination_details=destination_details,
        )

        # Combine all properties
        datajob.set_custom_properties(
            {
                **connector_properties,
                **lineage_properties,
            }
        )

        return datajob

    def _report_lineage_truncation(self, connector: Connector) -> None:
        """Report warning about truncated lineage."""
        max_lineage_limit = self.config.max_table_lineage_per_connector
        self.report.warning(
            title="Table lineage truncated",
            message=f"The connector had more than {max_lineage_limit} table lineage entries. "
            f"Only the most recent {max_lineage_limit} entries were ingested. "
            f"You can increase the limit by setting 'max_table_lineage_per_connector' in your config.",
            context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Get the workunit processors for this source."""
        return [
            *super().get_workunit_processors(),
            functools.partial(
                auto_incremental_lineage, self.config.incremental_lineage
            ),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Fivetran plugin execution is started")

        # Track statistics for final summary
        ingestion_stats: Dict[str, Union[int, List[str]]] = {
            "connectors_processed": 0,
            "total_datasets": 0,
            "total_datajobs": 0,
            "total_lineage_aspects": 0,
            "sample_inputs": [],
            "sample_outputs": [],
            "sample_datajobs": [],
            "connectors_with_lineage": 0,
            "connectors_without_lineage": 0,
        }

        # Process connectors progressively, yielding work units as we go
        logger.info("Processing connectors with progressive workunit emission")
        for connector in self.fivetran_access.get_allowed_connectors_stream(
            self.config.connector_patterns,
            self.config.destination_patterns,
            self.report,
            self.config.history_sync_lookback_period,
        ):
            logger.info(f"Processing connector id: {connector.connector_id}")
            connectors_processed = ingestion_stats["connectors_processed"]
            assert isinstance(connectors_processed, int)
            ingestion_stats["connectors_processed"] = connectors_processed + 1

            # Track lineage availability
            if connector.lineage:
                connectors_with_lineage = ingestion_stats["connectors_with_lineage"]
                assert isinstance(connectors_with_lineage, int)
                ingestion_stats["connectors_with_lineage"] = connectors_with_lineage + 1
            else:
                connectors_without_lineage = ingestion_stats[
                    "connectors_without_lineage"
                ]
                assert isinstance(connectors_without_lineage, int)
                ingestion_stats["connectors_without_lineage"] = (
                    connectors_without_lineage + 1
                )

            # Process connector and track statistics
            for workunit in self._get_connector_workunits(connector):
                self._update_ingestion_stats(workunit, ingestion_stats)
                yield workunit

        # Log comprehensive summary at the end
        self._log_ingestion_summary(ingestion_stats)

    def _update_ingestion_stats(self, workunit: MetadataWorkUnit, stats: Dict) -> None:
        """Update ingestion statistics based on the workunit."""
        if not workunit.metadata:
            return

        entity_type = getattr(workunit.metadata, "entityType", "unknown")
        aspect_name = getattr(workunit.metadata, "aspectName", "unknown")
        urn = str(workunit.get_urn())

        # Count entity types
        if entity_type == "dataset":
            stats["total_datasets"] += 1

            # Collect sample inputs and outputs based on platform
            if "snowflake" in urn or "bigquery" in urn or "redshift" in urn:
                # This is likely a destination/output
                if len(stats["sample_outputs"]) < 5:
                    stats["sample_outputs"].append(urn)
            else:
                # This is likely a source/input
                if len(stats["sample_inputs"]) < 5:
                    stats["sample_inputs"].append(urn)

        elif entity_type == "dataJob":
            stats["total_datajobs"] += 1
            if len(stats["sample_datajobs"]) < 5:
                stats["sample_datajobs"].append(urn)

        # Count lineage aspects
        if aspect_name == "upstreamLineage":
            stats["total_lineage_aspects"] += 1

    def _log_ingestion_summary(self, stats: Dict) -> None:
        """Log comprehensive summary of what was ingested."""
        logger.info("=" * 80)
        logger.info("FIVETRAN INGESTION SUMMARY")
        logger.info("=" * 80)

        # Overall statistics
        logger.info("OVERALL STATISTICS:")
        logger.info(f"  • Connectors Processed: {stats['connectors_processed']}")
        logger.info(f"  • Connectors with Lineage: {stats['connectors_with_lineage']}")
        logger.info(
            f"  • Connectors without Lineage: {stats['connectors_without_lineage']}"
        )
        logger.info(f"  • Total Datasets: {stats['total_datasets']}")
        logger.info(f"  • Total DataJobs: {stats['total_datajobs']}")
        logger.info(f"  • Total Lineage Aspects: {stats['total_lineage_aspects']}")

        # Sample inputs (sources)
        if stats["sample_inputs"]:
            logger.info("\nSAMPLE INPUT URNS (Sources):")
            for i, urn in enumerate(stats["sample_inputs"], 1):
                logger.info(f"  {i}. {urn}")
        else:
            logger.warning(
                "  WARNING: No input URNs found - this may indicate missing source lineage"
            )

        # Sample outputs (destinations)
        if stats["sample_outputs"]:
            logger.info("\nSAMPLE OUTPUT URNS (Destinations):")
            for i, urn in enumerate(stats["sample_outputs"], 1):
                logger.info(f"  {i}. {urn}")
        else:
            logger.warning(
                "  WARNING: No output URNs found - this may indicate missing destination lineage"
            )

        # Sample datajobs
        if stats["sample_datajobs"]:
            logger.info("\nSAMPLE DATAJOB URNS (Fivetran Syncs):")
            for i, urn in enumerate(stats["sample_datajobs"], 1):
                logger.info(f"  {i}. {urn}")
        else:
            logger.warning(
                "  WARNING: No DataJob URNs found - this may indicate missing job history"
            )

        # Lineage validation
        if stats["total_lineage_aspects"] == 0:
            logger.warning("\nLINEAGE VALIDATION:")
            logger.warning("  WARNING: NO LINEAGE ASPECTS FOUND!")
            logger.warning(
                "  This means no lineage relationships will be created in DataHub."
            )
            logger.warning("  Check:")
            logger.warning("    1. Connector configurations in Fivetran")
            logger.warning("    2. API permissions for lineage access")
            logger.warning("    3. Table sync status and enabled tables")
            logger.warning("    4. Fivetran connector types support lineage")
        else:
            logger.info("\nLINEAGE VALIDATION:")
            logger.info(
                f"  Successfully found {stats['total_lineage_aspects']} lineage relationships"
            )

        # Configuration info
        logger.info("\nCONFIGURATION:")
        logger.info(f"  • Incremental Lineage: {self.config.incremental_lineage}")
        logger.info(f"  • Include Column Lineage: {self.config.include_column_lineage}")
        logger.info(
            f"  • History Lookback Days: {self.config.history_sync_lookback_period}"
        )
        logger.info(
            f"  • Max Table Lineage per Connector: {self.config.max_table_lineage_per_connector}"
        )

        logger.info("=" * 80)

    def _create_upstream_lineage_workunits(
        self, connector: Connector
    ) -> Iterable[MetadataWorkUnit]:
        """Create upstreamLineage aspects for destination datasets."""
        # Get platform details
        source_details = self._get_source_details(connector)
        destination_details = self._get_destination_details(connector)

        # Group lineage by destination table to avoid duplicate upstreamLineage aspects
        dest_to_sources: Dict[str, List[TableLineage]] = {}

        for lineage in connector.lineage:
            dest_table = lineage.destination_table
            if dest_table not in dest_to_sources:
                dest_to_sources[dest_table] = []
            dest_to_sources[dest_table].append(lineage)

        logger.info(
            f"Creating upstreamLineage aspects for {len(dest_to_sources)} destination tables"
        )

        # Create upstreamLineage aspect for each destination table
        for dest_table, source_lineages in dest_to_sources.items():
            try:
                # Create destination URN
                dest_urn = self._create_dataset_urn(
                    dest_table,
                    destination_details,
                    is_source=False,
                )

                if not dest_urn:
                    logger.warning(f"Failed to create destination URN for {dest_table}")
                    continue

                # Create upstream entries
                upstreams = []
                fine_grained_lineages = []

                for lineage in source_lineages:
                    # Create source URN
                    source_urn = self._create_dataset_urn(
                        lineage.source_table,
                        source_details,
                        is_source=True,
                    )

                    if not source_urn:
                        logger.warning(
                            f"Failed to create source URN for {lineage.source_table}"
                        )
                        continue

                    # Create upstream entry
                    upstream = UpstreamClass(
                        dataset=str(source_urn),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    upstreams.append(upstream)

                    # Add column-level lineage if available
                    if self.config.include_column_lineage and lineage.column_lineage:
                        for col_lineage in lineage.column_lineage:
                            if (
                                col_lineage.source_column
                                and col_lineage.destination_column
                            ):
                                fine_grained_lineage = FineGrainedLineageClass(
                                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                    upstreams=[
                                        str(
                                            builder.make_schema_field_urn(
                                                str(source_urn),
                                                col_lineage.source_column,
                                            )
                                        )
                                    ],
                                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                    downstreams=[
                                        str(
                                            builder.make_schema_field_urn(
                                                str(dest_urn),
                                                col_lineage.destination_column,
                                            )
                                        )
                                    ],
                                )
                                fine_grained_lineages.append(fine_grained_lineage)

                if not upstreams:
                    logger.warning(
                        f"No valid upstreams found for destination {dest_table}"
                    )
                    continue

                # Create upstreamLineage aspect
                upstream_lineage = UpstreamLineageClass(
                    upstreams=upstreams,
                    fineGrainedLineages=fine_grained_lineages
                    if fine_grained_lineages
                    else None,
                )

                # Create workunit
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=str(dest_urn),
                    aspect=upstream_lineage,
                )
                workunit = mcp.as_workunit()

                logger.debug(
                    f"Created upstreamLineage aspect for {dest_urn} with {len(upstreams)} upstreams"
                )
                yield workunit

            except Exception as e:
                logger.warning(
                    f"Failed to create upstreamLineage for {dest_table}: {e}"
                )
                import traceback

                logger.debug(f"Full traceback: {traceback.format_exc()}")
                continue

    def get_report(self) -> SourceReport:
        """Get the report for this source."""
        return self.report
