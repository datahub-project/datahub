import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine

from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class Connector:
    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    user_name: str
    source_tables: List[str]
    destination_tables: List[str]
    jobs: List["Job"]


@dataclass
class Job:
    job_id: str
    start_time: int
    end_time: int
    status: str


class FivetranLogDataDictionary:
    def __init__(
        self, config: FivetranSourceConfig, report: FivetranSourceReport
    ) -> None:
        self.logger = logger
        self.config = config
        self.report = report
        self.engine = self._get_log_destination_engine()

    def _get_log_destination_engine(self) -> Any:
        destination_platform = self.config.fivetran_log_config.destination_platform
        engine = None
        if destination_platform == "snowflake":
            snowflake_destination_config = (
                self.config.fivetran_log_config.snowflake_destination_config
            )
            if snowflake_destination_config is not None:
                engine = create_engine(
                    snowflake_destination_config.get_sql_alchemy_url(),
                    **snowflake_destination_config.get_options(),
                )
                engine.execute(
                    FivetranLogQuery.use_schema(
                        snowflake_destination_config.database,
                        snowflake_destination_config.log_schema,
                    )
                )
        return engine

    def _query(self, query: str) -> List[Dict]:
        logger.debug("Query : {}".format(query))
        resp = self.engine.execute(query)
        return [row for row in resp]

    def _get_table_lineage(self, connector_id: str) -> Tuple[List[str], List[str]]:
        table_lineage = self._query(
            FivetranLogQuery.get_table_lineage_query(connector_id=connector_id)
        )
        source_tables: List[str] = []
        destination_tables: List[str] = []
        for each in table_lineage:
            source_tables.append(
                f"{each[Constant.SOURCE_SCHEMA_NAME]}.{each[Constant.SOURCE_TABLE_NAME]}"
            )
            destination_tables.append(
                f"{each[Constant.DESTINATION_SCHEMA_NAME]}.{each[Constant.DESTINATION_TABLE_NAME]}"
            )
        return source_tables, destination_tables

    def _get_jobs_list(self, connector_id: str) -> List[Job]:
        jobs: List[Job] = []
        sync_start_logs = {
            row[Constant.SYNC_ID]: row
            for row in self._query(
                FivetranLogQuery.get_sync_start_logs_query(connector_id=connector_id)
            )
        }
        sync_end_logs = {
            row[Constant.SYNC_ID]: row
            for row in self._query(
                FivetranLogQuery.get_sync_end_logs_query(connector_id=connector_id)
            )
        }
        for sync_id in sync_start_logs.keys():
            if sync_end_logs.get(sync_id) is None:
                # If no sync-end event log for this sync id that means sync is still in progress
                continue

            message_data = json.loads(sync_end_logs[sync_id][Constant.MESSAGE_DATA])
            if type(message_data) is str:
                # Sometimes message_data contains json string inside string
                # Ex: '"{\"status\":\"SUCCESSFUL\"}"'
                # Hence, need to do json loads twice.
                message_data = json.loads(message_data)

            jobs.append(
                Job(
                    job_id=sync_id,
                    start_time=round(
                        sync_start_logs[sync_id][Constant.TIME_STAMP].timestamp()
                    ),
                    end_time=round(
                        sync_end_logs[sync_id][Constant.TIME_STAMP].timestamp()
                    ),
                    status=message_data[Constant.STATUS],
                )
            )
        return jobs

    def _get_user_name(self, user_id: str) -> str:
        user_details = self._query(FivetranLogQuery.get_user_query(user_id=user_id))[0]
        return (
            f"{user_details[Constant.GIVEN_NAME]} {user_details[Constant.FAMILY_NAME]}"
        )

    def get_connectors_list(self) -> List[Connector]:
        connectors: List[Connector] = []
        connector_list = self._query(FivetranLogQuery.get_connectors_query())
        for connector in connector_list:
            if not self.config.connector_patterns.allowed(
                connector[Constant.CONNECTOR_NAME]
            ):
                self.report.report_connectors_dropped(connector[Constant.CONNECTOR_ID])
                continue

            source_tables, destination_tables = self._get_table_lineage(
                connector[Constant.CONNECTOR_ID]
            )

            connectors.append(
                Connector(
                    connector_id=connector[Constant.CONNECTOR_ID],
                    connector_name=connector[Constant.CONNECTOR_NAME],
                    connector_type=connector[Constant.CONNECTOR_TYPE_ID],
                    paused=connector[Constant.PAUSED],
                    sync_frequency=connector[Constant.SYNC_FREQUENCY],
                    destination_id=connector[Constant.DESTINATION_ID],
                    user_name=self._get_user_name(
                        connector[Constant.CONNECTING_USER_ID]
                    ),
                    source_tables=source_tables,
                    destination_tables=destination_tables,
                    jobs=self._get_jobs_list(connector[Constant.CONNECTOR_ID]),
                )
            )
        return connectors
