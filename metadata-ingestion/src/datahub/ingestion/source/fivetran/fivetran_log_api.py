import functools
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlalchemy import create_engine

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranLogConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    Job,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery

logger: logging.Logger = logging.getLogger(__name__)


class FivetranLogAPI:
    def __init__(self, fivetran_log_config: FivetranLogConfig) -> None:
        self.fivetran_log_config = fivetran_log_config
        (
            self.engine,
            self.fivetran_log_query,
            self.fivetran_log_database,
        ) = self._initialize_fivetran_variables()

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
                fivetran_log_database = engine.execute(
                    "SELECT @@project_id"
                ).fetchone()[0]
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
            self.fivetran_log_query.get_table_lineage_query(connector_ids=connector_ids)
        )
        for table_lineage in table_lineage_result:
            connectors_table_lineage_metadata[
                table_lineage[Constant.CONNECTOR_ID]
            ].append(table_lineage)
        return dict(connectors_table_lineage_metadata)

    def _extract_connector_lineage(
        self,
        table_lineage_result: Optional[List],
        column_lineage_metadata: Dict[Tuple[str, str], List],
    ) -> List[TableLineage]:
        table_lineage_list: List[TableLineage] = []
        if table_lineage_result is None:
            return table_lineage_list
        for table_lineage in table_lineage_result:
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
                    )
                    for column_lineage in column_lineage_result
                ]

            table_lineage_list.append(
                TableLineage(
                    source_table=f"{table_lineage[Constant.SOURCE_SCHEMA_NAME]}.{table_lineage[Constant.SOURCE_TABLE_NAME]}",
                    destination_table=f"{table_lineage[Constant.DESTINATION_SCHEMA_NAME]}.{table_lineage[Constant.DESTINATION_TABLE_NAME]}",
                    column_lineage=column_lineage_list,
                )
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

    def _fill_connectors_jobs(
        self, connectors: List[Connector], syncs_interval: int
    ) -> None:
        connector_ids = [connector.connector_id for connector in connectors]
        sync_logs = self._get_all_connector_sync_logs(
            syncs_interval, connector_ids=connector_ids
        )
        for connector in connectors:
            connector.jobs = self._get_jobs_list(sync_logs.get(connector.connector_id))

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
                if not connector_patterns.allowed(connector_name):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
                    )
                    continue
                if not destination_patterns.allowed(
                    destination_id := connector[Constant.DESTINATION_ID]
                ):
                    report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id})"
                    )
                    continue
                connectors.append(
                    Connector(
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
                )

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
