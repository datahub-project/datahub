import logging
from typing import List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import FivetranSourceReport
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran_access import FivetranAccessInterface
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_query import (
    MAX_JOBS_PER_CONNECTOR,
    MAX_TABLE_LINEAGE_PER_CONNECTOR,
)

logger = logging.getLogger(__name__)


class FivetranStandardAPI(FivetranAccessInterface):
    """
    API adapter for the standard version of Fivetran using the REST API
    instead of querying log tables directly.
    """

    def __init__(self, api_client: FivetranAPIClient) -> None:
        """Initialize with a FivetranAPIClient instance."""
        self.api_client = api_client
        self._fivetran_log_database = None  # Not used in standard version

    @property
    def fivetran_log_database(self) -> Optional[str]:
        """Standard version doesn't have a log database."""
        return self._fivetran_log_database

    def get_user_email(self, user_id: str) -> Optional[str]:
        """Get a user's email from their user ID."""
        if not user_id:
            return None

        try:
            user_data = self.api_client.get_user(user_id)
            return user_data.get("email")
        except Exception as e:
            logger.warning(f"Failed to get user email for user ID {user_id}: {e}")
            return None

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        report: FivetranSourceReport,
        syncs_interval: int,
    ) -> List[Connector]:
        """
        Get a list of connectors filtered by the provided patterns.
        This is the standard version replacement for querying log tables.
        """
        connectors: List[Connector] = []

        with report.metadata_extraction_perf.connectors_metadata_extraction_sec:
            logger.info("Fetching connector list from Fivetran API")
            connector_list = self.api_client.list_connectors()

            for api_connector in connector_list:
                connector_id = api_connector.get("id", "")
                if not connector_id:
                    logger.warning(
                        f"Skipping connector with missing id: {api_connector}"
                    )
                    continue

                connector_name = api_connector.get("name", "")
                if not connector_name:
                    connector_name = f"connector-{connector_id}"

                destination_id = api_connector.get("group", {}).get("id", "")

                if not connector_patterns.allowed(connector_name):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
                    )
                    continue

                if not destination_patterns.allowed(destination_id):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id})"
                    )
                    continue

                # Get sync history for this connector
                sync_history = self.api_client.list_connector_sync_history(
                    connector_id=connector_id, days=syncs_interval
                )

                # Convert from API format to our internal model
                connector = self.api_client.extract_connector_metadata(
                    api_connector=api_connector, sync_history=sync_history
                )

                report.report_connectors_scanned()
                connectors.append(connector)

        if not connectors:
            logger.info("No allowed connectors found")
            return []

        logger.info(f"Found {len(connectors)} allowed connectors")

        with report.metadata_extraction_perf.connectors_lineage_extraction_sec:
            logger.info("Fetching connector lineage from Fivetran API")
            self._fill_connectors_lineage(connectors)

        # Jobs are already filled when we create the connector object
        # Just check if we need to truncate the list
        for connector in connectors:
            if len(connector.jobs) >= MAX_JOBS_PER_CONNECTOR:
                report.warning(
                    title="Job history truncated",
                    message=f"The connector had more than {MAX_JOBS_PER_CONNECTOR} sync runs in the past {syncs_interval} days. "
                    f"Only the most recent {MAX_JOBS_PER_CONNECTOR} syncs were ingested.",
                    context=f"{connector.connector_name} (connector_id: {connector.connector_id})",
                )
                connector.jobs = sorted(
                    connector.jobs, key=lambda j: j.end_time, reverse=True
                )[:MAX_JOBS_PER_CONNECTOR]

        return connectors

    def _fill_connectors_lineage(self, connectors: List[Connector]) -> None:
        """
        Fill in lineage information for connectors by calling the API.
        """
        for connector in connectors:
            try:
                logger.info(
                    f"Extracting lineage for connector {connector.connector_id}"
                )

                # Get destination platform from connector properties
                destination_platform = connector.additional_properties.get(
                    "destination_platform", "snowflake"
                )

                # Try to get schema information with detailed logging
                schemas = self.api_client.list_connector_schemas(connector.connector_id)
                logger.info(
                    f"Retrieved {len(schemas)} schemas for connector {connector.connector_id}"
                )

                lineage_list = []

                # Process each schema
                for schema in schemas:
                    try:
                        schema_name = schema.get("name", "")
                        if not schema_name:
                            logger.warning(f"Skipping schema with no name: {schema}")
                            continue

                        tables = schema.get("tables", [])
                        if not isinstance(tables, list):
                            logger.warning(
                                f"Schema {schema_name} has non-list tables: {tables}"
                            )
                            continue

                        # Log the number of tables found
                        logger.info(
                            f"Processing {len(tables)} tables in schema {schema_name}"
                        )

                        # Process each table in the schema
                        for table in tables:
                            try:
                                if not isinstance(table, dict):
                                    continue

                                table_name = table.get("name", "")
                                enabled = table.get("enabled", False)

                                if not enabled or not table_name:
                                    continue

                                # Create source and destination table identifiers
                                source_table = f"{schema_name}.{table_name}"

                                # Adjust case based on destination platform
                                dest_schema = (
                                    schema_name.upper()
                                    if destination_platform != "bigquery"
                                    else schema_name
                                )
                                dest_table = (
                                    table_name.upper()
                                    if destination_platform != "bigquery"
                                    else table_name
                                )
                                destination_table = f"{dest_schema}.{dest_table}"

                                # Process columns for lineage
                                column_lineage = []
                                columns = table.get("columns", [])

                                if isinstance(columns, list):
                                    for column in columns:
                                        try:
                                            if not isinstance(column, dict):
                                                continue

                                            col_name = column.get("name", "")
                                            if not col_name:
                                                continue

                                            # Destination column name follows same case convention as table
                                            dest_col_name = (
                                                col_name.upper()
                                                if destination_platform != "bigquery"
                                                else col_name
                                            )

                                            column_lineage.append(
                                                ColumnLineage(
                                                    source_column=col_name,
                                                    destination_column=dest_col_name,
                                                )
                                            )
                                        except Exception as col_e:
                                            logger.warning(
                                                f"Error processing column in table {table_name}: {col_e}"
                                            )

                                # Add this table's lineage
                                lineage_list.append(
                                    TableLineage(
                                        source_table=source_table,
                                        destination_table=destination_table,
                                        column_lineage=column_lineage,
                                    )
                                )

                                logger.debug(
                                    f"Added lineage: {source_table} -> {destination_table} with {len(column_lineage)} columns"
                                )
                            except Exception as table_e:
                                logger.warning(
                                    f"Error processing table {table.get('name', 'unknown')}: {table_e}"
                                )
                    except Exception as schema_e:
                        logger.warning(
                            f"Error processing schema {schema.get('name', 'unknown')}: {schema_e}"
                        )

                # Truncate if necessary
                if len(lineage_list) > MAX_TABLE_LINEAGE_PER_CONNECTOR:
                    logger.warning(
                        f"Connector {connector.connector_name} has {len(lineage_list)} tables, "
                        f"truncating to {MAX_TABLE_LINEAGE_PER_CONNECTOR}"
                    )
                    lineage_list = lineage_list[:MAX_TABLE_LINEAGE_PER_CONNECTOR]

                connector.lineage = lineage_list

                logger.info(
                    f"Successfully extracted {len(lineage_list)} table lineages for connector {connector.connector_id}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to extract lineage for connector {connector.connector_name}: {e}",
                    exc_info=True,
                )
                connector.lineage = []
