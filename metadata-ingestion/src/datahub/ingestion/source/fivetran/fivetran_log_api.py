import json
import logging
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine

from datahub.ingestion.source.fivetran.config import Constant, FivetranLogConfig
from datahub.ingestion.source.fivetran.data_classes import Connector, Job
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery

logger: logging.Logger = logging.getLogger(__name__)


class FivetranLogAPI:
    def __init__(self, fivetran_log_config: FivetranLogConfig) -> None:
        self.fivetran_log_config = fivetran_log_config
        self.engine = self._get_log_destination_engine()

    def _get_log_destination_engine(self) -> Any:
        destination_platform = self.fivetran_log_config.destination_platform
        engine = None
        if destination_platform == "snowflake":
            snowflake_destination_config = self.fivetran_log_config.destination_config
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

    def _get_table_lineage(self, connector_id: str) -> List[Tuple[str, str]]:
        table_lineage_resut = self._query(
            FivetranLogQuery.get_table_lineage_query(connector_id=connector_id)
        )
        table_lineage: List[Tuple[str, str]] = []
        for each in table_lineage_resut:
            table_lineage.append(
                (
                    f"{each[Constant.SOURCE_SCHEMA_NAME]}.{each[Constant.SOURCE_TABLE_NAME]}",
                    f"{each[Constant.DESTINATION_SCHEMA_NAME]}.{each[Constant.DESTINATION_TABLE_NAME]}",
                )
            )
        return table_lineage

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
                    table_lineage=self._get_table_lineage(
                        connector[Constant.CONNECTOR_ID]
                    ),
                    jobs=self._get_jobs_list(connector[Constant.CONNECTOR_ID]),
                )
            )
        return connectors
