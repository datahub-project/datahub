import functools
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranLogConfig,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.fivetran_access import FivetranAccessInterface
from datahub.ingestion.source.fivetran.fivetran_api_client import FivetranAPIClient
from datahub.ingestion.source.fivetran.fivetran_constants import (
    DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery
from datahub.ingestion.source.fivetran.models import (
    ColumnLineage,
    Connector,
    Job,
    TableLineage,
)

logger: logging.Logger = logging.getLogger(__name__)


class FivetranLogAPI(FivetranAccessInterface):
    def __init__(
        self,
        fivetran_log_config: FivetranLogConfig,
        config: Optional[FivetranSourceConfig] = None,
    ) -> None:
        self.fivetran_log_config = fivetran_log_config
        self.config = config
        (
            self.engine,
            self.fivetran_log_query,
            self._fivetran_log_database,
        ) = self._initialize_fivetran_variables()

    @property
    def fivetran_log_database(self) -> Optional[str]:
        return self._fivetran_log_database

    def test_connection(self) -> bool:
        """
        Test database connectivity by executing a simple query.
        Raises an exception if the connection fails.
        """
        try:
            # Execute a simple query that should work on any database
            test_query = "SELECT 1 as test"
            if self.fivetran_log_config.destination_platform != "snowflake":
                test_query = sqlglot.parse_one(test_query, dialect="snowflake").sql(
                    dialect=self.fivetran_log_config.destination_platform, pretty=True
                )

            self.engine.execute(test_query)
            logger.info("Successfully tested database connection")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection test failed: {e}")
            raise ConfigurationError(f"Failed to connect to the database: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during connection test: {e}")
            raise ConfigurationError(
                f"Unexpected error during connection test: {e}"
            ) from e

    def _initialize_fivetran_variables(
        self,
    ) -> Tuple[Any, FivetranLogQuery, str]:
        fivetran_log_query = FivetranLogQuery()
        destination_platform = self.fivetran_log_config.destination_platform
        # For every destination, create sqlalchemy engine,
        # set db_clause to generate select queries and set fivetran_log_database class variable
        if destination_platform == "snowflake":
            snowflake_destination_config = (
                self.fivetran_log_config.snowflake_destination_config
            )
            if snowflake_destination_config is not None:
                engine = create_engine(
                    snowflake_destination_config.get_sql_alchemy_url(),
                    **snowflake_destination_config.get_options(),
                )
                engine.execute(
                    fivetran_log_query.use_database(
                        snowflake_destination_config.database,
                    )
                )
                fivetran_log_query.set_schema(
                    snowflake_destination_config.log_schema,
                )
                fivetran_log_database = snowflake_destination_config.database
        elif destination_platform == "bigquery":
            bigquery_destination_config = (
                self.fivetran_log_config.bigquery_destination_config
            )
            if bigquery_destination_config is not None:
                engine = create_engine(
                    bigquery_destination_config.get_sql_alchemy_url(),
                )
                fivetran_log_query.set_schema(bigquery_destination_config.dataset)

                # The "database" should be the BigQuery project name.
                result = engine.execute("SELECT @@project_id").fetchone()
                if result is None:
                    raise ValueError("Failed to retrieve BigQuery project ID")
                fivetran_log_database = result[0]
        else:
            raise ConfigurationError(
                f"Destination platform '{destination_platform}' is not yet supported."
            )
        return (
            engine,
            fivetran_log_query,
            fivetran_log_database,
        )

    def _query(self, query: str) -> List[Dict]:
        # Automatically transpile snowflake query syntax to the target dialect.
        if self.fivetran_log_config.destination_platform != "snowflake":
            query = sqlglot.parse_one(query, dialect="snowflake").sql(
                dialect=self.fivetran_log_config.destination_platform, pretty=True
            )
        logger.info(f"Executing query: {query}")
        resp = self.engine.execute(query)
        return [row for row in resp]

    def _get_column_lineage_metadata(
        self, connector_ids: List[str]
    ) -> Dict[Tuple[str, str], List]:
        """
        Returns dict of column lineage metadata with key as (<SOURCE_TABLE_ID>, <DESTINATION_TABLE_ID>)
        """
        all_column_lineage = defaultdict(list)
        column_lineage_result = self._query(
            self.fivetran_log_query.get_column_lineage_query(
                connector_ids=connector_ids
            )
        )
        for column_lineage in column_lineage_result:
            key = (
                column_lineage[Constant.SOURCE_TABLE_ID],
                column_lineage[Constant.DESTINATION_TABLE_ID],
            )
            all_column_lineage[key].append(column_lineage)
        return dict(all_column_lineage)

    def _get_table_lineage_metadata(self, connector_ids: List[str]) -> Dict[str, List]:
        """
        Returns dict of table lineage metadata with key as 'CONNECTOR_ID'
        """
        connectors_table_lineage_metadata = defaultdict(list)

        table_lineage_result = self._query(
            self.fivetran_log_query.get_table_lineage_query(
                connector_ids=connector_ids,
                max_table_lineage_per_connector=self.config.max_table_lineage_per_connector
                if self.config
                else DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR,
            )
        )
        for table_lineage in table_lineage_result:
            connectors_table_lineage_metadata[
                table_lineage[Constant.CONNECTOR_ID]
            ].append(table_lineage)
        return dict(connectors_table_lineage_metadata)

    def _validate_log_table_lineage(self, table_lineage: Dict) -> bool:
        """Validate lineage data from log tables."""
        required_fields = [
            Constant.SOURCE_SCHEMA_NAME,
            Constant.SOURCE_TABLE_NAME,
            Constant.DESTINATION_SCHEMA_NAME,
            Constant.DESTINATION_TABLE_NAME,
        ]

        for field in required_fields:
            if not table_lineage.get(field):
                logger.debug(f"Missing required field '{field}' in table lineage data")
                return False

        return True

    def _extract_connector_lineage(
        self,
        table_lineage_result: Optional[List],
        column_lineage_metadata: Dict[Tuple[str, str], List],
    ) -> List[TableLineage]:
        table_lineage_list: List[TableLineage] = []
        if table_lineage_result is None:
            logger.debug("No table lineage result provided")
            return table_lineage_list

        valid_lineage_count = 0
        invalid_lineage_count = 0

        for table_lineage in table_lineage_result:
            # Validate lineage data
            if not self._validate_log_table_lineage(table_lineage):
                invalid_lineage_count += 1
                logger.debug(f"Skipping invalid table lineage: {table_lineage}")
                continue

            valid_lineage_count += 1
            # Join the column lineage into the table lineage.
            column_lineage_result = column_lineage_metadata.get(
                (
                    table_lineage[Constant.SOURCE_TABLE_ID],
                    table_lineage[Constant.DESTINATION_TABLE_ID],
                )
            )
            column_lineage_list: List[ColumnLineage] = []
            if column_lineage_result:
                column_lineage_list = [
                    ColumnLineage(
                        source_column=column_lineage[Constant.SOURCE_COLUMN_NAME],
                        destination_column=column_lineage[
                            Constant.DESTINATION_COLUMN_NAME
                        ],
                        # Column metadata
                        source_column_type=column_lineage.get("source_column_type"),
                        destination_column_type=column_lineage.get(
                            "destination_column_type"
                        ),
                    )
                    for column_lineage in column_lineage_result
                ]

            table_lineage_list.append(
                TableLineage(
                    source_table=f"{table_lineage[Constant.SOURCE_SCHEMA_NAME]}.{table_lineage[Constant.SOURCE_TABLE_NAME]}",
                    destination_table=f"{table_lineage[Constant.DESTINATION_SCHEMA_NAME]}.{table_lineage[Constant.DESTINATION_TABLE_NAME]}",
                    column_lineage=column_lineage_list,
                    # Table metadata from log tables
                    source_schema=table_lineage.get("source_schema_name"),
                    destination_schema=table_lineage.get("destination_schema_name"),
                    source_database=table_lineage.get("source_database_name"),
                    destination_database=table_lineage.get("destination_database_name"),
                    source_platform=table_lineage.get("source_platform"),
                    destination_platform=table_lineage.get("destination_platform"),
                    source_env=table_lineage.get("source_env"),
                    destination_env=table_lineage.get("destination_env"),
                    connector_type_id=table_lineage.get("connector_type_id"),
                    connector_name=table_lineage.get("connector_name"),
                    destination_id=table_lineage.get("destination_id"),
                )
            )

        # Log validation results
        total_lineage = valid_lineage_count + invalid_lineage_count
        if total_lineage > 0:
            logger.info(
                f"Processed {total_lineage} table lineage entries: "
                f"{valid_lineage_count} valid, {invalid_lineage_count} invalid"
            )

        return table_lineage_list

    def _get_all_connector_sync_logs(
        self, syncs_interval: int, connector_ids: List[str]
    ) -> Dict[str, Dict[str, Dict[str, Tuple[float, Optional[str]]]]]:
        sync_logs: Dict[str, Dict[str, Dict[str, Tuple[float, Optional[str]]]]] = {}

        query = self.fivetran_log_query.get_sync_logs_query(
            syncs_interval=syncs_interval,
            connector_ids=connector_ids,
        )

        for row in self._query(query):
            connector_id = row[Constant.CONNECTOR_ID]
            sync_id = row[Constant.SYNC_ID]

            if connector_id not in sync_logs:
                sync_logs[connector_id] = {}

            sync_logs[connector_id][sync_id] = {
                "sync_start": (row["start_time"].timestamp(), None),
                "sync_end": (row["end_time"].timestamp(), row["end_message_data"]),
            }

        return sync_logs

    def _get_jobs_list(
        self, connector_sync_log: Optional[Dict[str, Dict]]
    ) -> List[Job]:
        jobs: List[Job] = []
        if connector_sync_log is None:
            return jobs
        for sync_id in connector_sync_log:
            if len(connector_sync_log[sync_id]) != 2:
                # If both sync-start and sync-end event log not present for this sync that means sync is still in progress
                continue

            message_data = connector_sync_log[sync_id]["sync_end"][1]
            if message_data is None:
                continue
            message_data = json.loads(message_data)
            if isinstance(message_data, str):
                # Sometimes message_data contains json string inside string
                # Ex: '"{\"status\":\"SUCCESSFUL\"}"'
                # Hence, need to do json loads twice.
                message_data = json.loads(message_data)

            jobs.append(
                Job(
                    job_id=sync_id,
                    start_time=round(connector_sync_log[sync_id]["sync_start"][0]),
                    end_time=round(connector_sync_log[sync_id]["sync_end"][0]),
                    status=message_data[Constant.STATUS],
                )
            )
        return jobs

    @functools.lru_cache()
    def _get_users(self) -> Dict[str, str]:
        users = self._query(self.fivetran_log_query.get_users_query())
        if not users:
            return {}
        return {user[Constant.USER_ID]: user[Constant.EMAIL] for user in users}

    def get_user_email(self, user_id: str) -> Optional[str]:
        if not user_id:
            return None
        return self._get_users().get(user_id)

    def _fill_connectors_lineage(self, connectors: List[Connector]) -> None:
        connector_ids = [connector.connector_id for connector in connectors]
        table_lineage_metadata = self._get_table_lineage_metadata(connector_ids)
        column_lineage_metadata = self._get_column_lineage_metadata(connector_ids)
        for connector in connectors:
            connector.lineage = self._extract_connector_lineage(
                table_lineage_result=table_lineage_metadata.get(connector.connector_id),
                column_lineage_metadata=column_lineage_metadata,
            )

    def _enrich_connector_with_api_metadata(self, connector: Connector) -> None:
        """
        Enhance Enterprise mode connectors with API metadata for consistency with Standard mode.
        This provides a hybrid approach: fast log table queries + rich API metadata.
        """
        try:
            # Only attempt API enrichment if we have API config available
            if (
                self.config is not None
                and hasattr(self.config, "api_config")
                and self.config.api_config is not None
            ):
                api_client = FivetranAPIClient(self.config.api_config)

                # Get enhanced connector metadata from API
                try:
                    api_connector_data = api_client.get_connector(
                        connector.connector_id
                    )

                    # Enhance connector name with display_name if available
                    display_name = api_connector_data.get("display_name")
                    if display_name and display_name != connector.connector_name:
                        logger.info(
                            f"Enriching connector {connector.connector_id}: "
                            f"'{connector.connector_name}' -> '{display_name}'"
                        )
                        connector.connector_name = display_name

                    # Add service information for better platform detection
                    service = api_connector_data.get("service", "")
                    if service:
                        connector.additional_properties["service"] = service
                        logger.debug(
                            f"Added service '{service}' for connector {connector.connector_id}"
                        )

                    # Add schedule information
                    schedule = api_connector_data.get("schedule", {})
                    if schedule:
                        sync_freq = schedule.get("sync_frequency")
                        if sync_freq and sync_freq != connector.sync_frequency:
                            connector.sync_frequency = sync_freq
                            logger.debug(
                                f"Updated sync frequency to {sync_freq} minutes for connector {connector.connector_id}"
                            )

                    # Enhance destination platform detection
                    try:
                        destination_platform = api_client.detect_destination_platform(
                            connector.destination_id
                        )
                        if destination_platform:
                            connector.additional_properties["destination_platform"] = (
                                destination_platform
                            )
                            logger.debug(
                                f"Detected destination platform '{destination_platform}' for connector {connector.connector_id}"
                            )
                    except Exception as e:
                        logger.debug(
                            f"Could not detect destination platform for {connector.connector_id}: {e}"
                        )

                    logger.info(
                        f"Successfully enriched connector {connector.connector_id} with API metadata"
                    )

                except Exception as e:
                    logger.debug(
                        f"Could not enrich connector {connector.connector_id} with API metadata: {e}"
                    )

        except Exception as e:
            logger.debug(f"API enrichment not available for Enterprise mode: {e}")
            # This is expected if no API config is provided - Enterprise mode works fine without it

    def _validate_connector_accessibility(
        self, connector: Connector, report: FivetranSourceReport
    ) -> bool:
        """
        Validate connector accessibility with consistent error handling across modes.
        Returns True if connector should be processed, False if it should be skipped.
        """
        try:
            # If we have API access, validate like Standard mode
            if (
                self.config is not None
                and hasattr(self.config, "api_config")
                and self.config.api_config is not None
            ):
                api_client = FivetranAPIClient(self.config.api_config)
                validation_result = api_client.validate_connector_accessibility(
                    connector.connector_id
                )

                if not validation_result["is_accessible"]:
                    error_msg = (
                        f"Skipping connector {connector.connector_name} ({connector.connector_id}): "
                        f"{validation_result['error_message']}"
                    )
                    logger.warning(error_msg)
                    report.report_connectors_dropped(error_msg)
                    return False

                logger.info(
                    f"Connector {connector.connector_name} is accessible and ready for processing"
                )
                return True
            else:
                # Without API access, assume connector is valid (Enterprise mode default)
                logger.debug(
                    f"No API validation available for connector {connector.connector_id}, proceeding"
                )
                return True

        except Exception as e:
            logger.warning(
                f"Error validating connector {connector.connector_id}: {e}. Proceeding anyway."
            )
            return True  # Default to processing in Enterprise mode

    def _fill_connectors_jobs(
        self, connectors: List[Connector], syncs_interval: int
    ) -> None:
        connector_ids = [connector.connector_id for connector in connectors]
        sync_logs = self._get_all_connector_sync_logs(
            syncs_interval, connector_ids=connector_ids
        )

        for connector in connectors:
            connector.jobs = self._get_jobs_list(sync_logs.get(connector.connector_id))

            # Enhanced job history extraction with API fallback (like Standard mode)
            self._enhance_job_history_with_api_fallback(connector, syncs_interval)

            # Normalize job statuses for consistency with Standard mode
            self._normalize_job_statuses(connector)

    def _enhance_job_history_with_api_fallback(
        self, connector: Connector, syncs_interval: int
    ) -> None:
        """
        Enhance Enterprise mode job history with API fallback for maximum completeness.
        This provides the same fallback strategy as Standard mode.
        """
        # If we already have enough jobs from log tables, we're good
        if len(connector.jobs) >= 10:  # Reasonable threshold
            logger.debug(
                f"Connector {connector.connector_id} already has {len(connector.jobs)} jobs from log tables"
            )
            return

        try:
            # Only attempt API fallback if we have API config available
            if (
                self.config is not None
                and hasattr(self.config, "api_config")
                and self.config.api_config is not None
            ):
                api_client = FivetranAPIClient(self.config.api_config)

                logger.info(
                    f"Connector {connector.connector_id} has only {len(connector.jobs)} jobs from log tables. "
                    f"Attempting API fallback for more complete history."
                )

                try:
                    # Try extended lookback like Standard mode does
                    extended_days = max(syncs_interval, 30)  # At least 30 days
                    api_sync_history = api_client.list_connector_sync_history(
                        connector_id=connector.connector_id, days=extended_days
                    )

                    if api_sync_history:
                        # Extract jobs from API sync history
                        api_jobs = api_client._extract_jobs_from_sync_history(
                            api_sync_history
                        )

                        if api_jobs:
                            # Merge API jobs with existing log table jobs (avoid duplicates)
                            existing_job_ids = {job.job_id for job in connector.jobs}
                            new_jobs = [
                                job
                                for job in api_jobs
                                if job.job_id not in existing_job_ids
                            ]

                            if new_jobs:
                                connector.jobs.extend(new_jobs)
                                logger.info(
                                    f"Enhanced connector {connector.connector_id} with {len(new_jobs)} additional jobs from API. "
                                    f"Total jobs: {len(connector.jobs)}"
                                )
                            else:
                                logger.debug(
                                    f"No new jobs found via API for connector {connector.connector_id}"
                                )

                except Exception as e:
                    logger.debug(
                        f"API fallback failed for connector {connector.connector_id}: {e}"
                    )

        except Exception as e:
            logger.debug(
                f"Job history enhancement not available for Enterprise mode: {e}"
            )
            # This is expected if no API config is provided

    def _normalize_job_statuses(self, connector: Connector) -> None:
        """
        Normalize job statuses for consistency between Enterprise and Standard modes.
        Uses the same mapping as Standard API mode.
        """
        status_mapping = {
            # Standard API -> Enterprise log format
            "COMPLETED": "SUCCESSFUL",
            "SUCCEEDED": "SUCCESSFUL",
            "SUCCESS": "SUCCESSFUL",
            "SUCCESSFUL": "SUCCESSFUL",  # Already correct
            "FAILED": "FAILURE_WITH_TASK",
            "FAILURE": "FAILURE_WITH_TASK",
            "ERROR": "FAILURE_WITH_TASK",
            "FAILURE_WITH_TASK": "FAILURE_WITH_TASK",  # Already correct
            "CANCELLED": "CANCELED",
            "CANCELED": "CANCELED",  # Already correct
            "RESCHEDULED": "RESCHEDULED",  # Already correct
        }

        for job in connector.jobs:
            original_status = job.status
            if original_status in status_mapping:
                job.status = status_mapping[original_status]
                if original_status != job.status:
                    logger.debug(
                        f"Normalized job {job.job_id} status: {original_status} -> {job.status}"
                    )

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        report: FivetranSourceReport,
        syncs_interval: int,
    ) -> List[Connector]:
        connectors: List[Connector] = []
        with report.metadata_extraction_perf.connectors_metadata_extraction_sec:
            logger.info("Fetching connector list")
            connector_list = self._query(self.fivetran_log_query.get_connectors_query())
            for connector in connector_list:
                connector_id = connector[Constant.CONNECTOR_ID]
                connector_name = connector[Constant.CONNECTOR_NAME]
                destination_id = connector[Constant.DESTINATION_ID]

                # Check if this connector ID is explicitly specified in sources_to_platform_instance
                # If it is, we should include it regardless of connector_patterns
                explicitly_included = False
                if (
                    self.config
                    and hasattr(self.config, "sources_to_platform_instance")
                    and connector_id in self.config.sources_to_platform_instance
                ):
                    explicitly_included = True
                    logger.info(
                        f"Connector {connector_name} (ID: {connector_id}) explicitly included via sources_to_platform_instance"
                    )

                # Apply connector pattern filter only if not explicitly included
                # Check both connector_name and connector_id for maximum flexibility
                if not explicitly_included and not (
                    connector_patterns.allowed(connector_name)
                    or connector_patterns.allowed(connector_id)
                ):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
                    )
                    continue

                # Apply destination filter - check both ID and name for maximum flexibility
                destination_allowed = destination_patterns.allowed(destination_id)
                destination_name = None

                # If API config is available, try to get destination name for enhanced filtering
                if (
                    not destination_allowed
                    and self.config is not None
                    and self.config.api_config is not None
                ):
                    try:
                        from datahub.ingestion.source.fivetran.fivetran_api_client import (
                            FivetranAPIClient,
                        )

                        api_client = FivetranAPIClient(self.config.api_config)
                        dest_details = api_client.get_destination_details(
                            destination_id
                        )
                        destination_name = dest_details.get("name", "")

                        if destination_name:
                            destination_allowed = destination_patterns.allowed(
                                destination_name
                            )
                            logger.debug(
                                f"Enterprise mode: checking destination name '{destination_name}' for filtering"
                            )
                    except Exception as e:
                        logger.debug(
                            f"Could not fetch destination name for filtering: {e}"
                        )

                if not destination_allowed:
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id}, destination_name: {destination_name})"
                    )
                    continue

                # Create base connector from log data
                base_connector = Connector(
                    connector_id=connector_id,
                    connector_name=connector_name,
                    connector_type=connector[Constant.CONNECTOR_TYPE_ID],
                    paused=connector[Constant.PAUSED],
                    sync_frequency=connector[Constant.SYNC_FREQUENCY],
                    destination_id=destination_id,
                    user_id=connector[Constant.CONNECTING_USER_ID],
                    lineage=[],  # filled later
                    jobs=[],  # filled later
                )

                # Enhance with API metadata if available (for hybrid enrichment)
                self._enrich_connector_with_api_metadata(base_connector)

                # Validate connector accessibility if API is available
                if self._validate_connector_accessibility(base_connector, report):
                    connectors.append(base_connector)

        if not connectors:
            # Some of our queries don't work well when there's no connectors, since
            # we push down connector id filters.
            logger.info("No allowed connectors found")
            return []
        logger.info(f"Found {len(connectors)} allowed connectors")

        with report.metadata_extraction_perf.connectors_lineage_extraction_sec:
            logger.info("Fetching connector lineage")
            self._fill_connectors_lineage(connectors)
        with report.metadata_extraction_perf.connectors_jobs_extraction_sec:
            logger.info("Fetching connector job run history")
            self._fill_connectors_jobs(connectors, syncs_interval)
        return connectors
