import json
import logging
from dataclasses import dataclass
from typing import List, Tuple

from datahub.ingestion.source.fivetran.config import Constant, FivetranSourceConfig
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery
from datahub.ingestion.source.fivetran.log_destination import (
    LogDestination,
    SnowflakeDestination,
)

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class Connector:
    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    source_tables: List[str]
    destination_tables: List[str]
    job: "Job"
    user: "User"


@dataclass
class Job:
    job_id: str
    start_time: int
    end_time: int
    status: str


@dataclass
class User:
    user_id: str
    given_name: str
    family_name: str
    email: str
    email_disabled: bool
    verified: bool
    created_at: int


class FivetranLogDataDictionary:
    def __init__(self, config: FivetranSourceConfig) -> None:
        self.logger = logger
        self.config = config
        self.log_destination: LogDestination = self._get_fivetran_log_destination()

    def _get_fivetran_log_destination(self) -> LogDestination:
        destination_platform = self.config.fivetran_log_config.destination_platform
        if destination_platform == "snowflake":
            return SnowflakeDestination(
                self.config.fivetran_log_config.snowflake_destination_config
            )
        else:
            raise ValueError(
                f"Destination platform '{destination_platform}' is not yet supported."
            )

    def _get_table_lineage(self, connector_id: str) -> Tuple[List[str], List[str]]:
        table_lineage = self.log_destination.query(
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
            for row in self.log_destination.query(
                FivetranLogQuery.get_sync_start_logs_query(connector_id=connector_id)
            )
        }
        sync_end_logs = {
            row[Constant.SYNC_ID]: row
            for row in self.log_destination.query(
                FivetranLogQuery.get_sync_end_logs_query(connector_id=connector_id)
            )
        }
        for sync_id in sync_start_logs.keys():
            if sync_end_logs.get(sync_id) is None:
                continue
            message_data = json.loads(sync_end_logs[sync_id][Constant.MESSAGE_DATA])
            if type(message_data) is str:
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

    def _get_user_obj(self, user_id: str) -> User:
        user_details = self.log_destination.query(
            FivetranLogQuery.get_user_query(user_id=user_id)
        )[0]
        return User(
            user_id=user_details[Constant.USER_ID],
            given_name=user_details[Constant.GIVEN_NAME],
            family_name=user_details[Constant.FAMILY_NAME],
            email=user_details[Constant.EMAIL],
            email_disabled=user_details[Constant.EMAIL_DISABLED],
            verified=user_details[Constant.VERIFIED],
            created_at=round(user_details[Constant.CREATED_AT].timestamp()),
        )

    def get_connectors_list(self) -> List[Connector]:
        self.log_destination.query(
            FivetranLogQuery.use_schema(
                self.log_destination.get_database(), self.log_destination.get_schema()
            )
        )

        connectors: List[Connector] = []
        connector_list = self.log_destination.query(
            FivetranLogQuery.get_connectors_query()
        )
        for connector in connector_list:
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
                    source_tables=source_tables,
                    destination_tables=destination_tables,
                    job=self._get_jobs_list(connector[Constant.CONNECTOR_ID]),
                    user=self._get_user_obj(connector[Constant.CONNECTING_USER_ID]),
                )
            )
        return connectors
