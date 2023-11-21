import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine

from datahub.ingestion.source.fivetran.config import Constant, FivetranLogConfig
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
        self.fivetran_log_database: Optional[str] = None
        self.fivetran_log_config = fivetran_log_config
        self.engine = self._get_log_destination_engine()

    def _get_log_destination_engine(self) -> Any:
        destination_platform = self.fivetran_log_config.destination_platform
        engine = None
        # For every destination, create sqlalchemy engine,
        # select the database and schema and set fivetran_log_database class variable
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
                self.fivetran_log_database = snowflake_destination_config.database
        return engine

    def _query(self, query: str) -> List[Dict]:
        logger.debug("Query : {}".format(query))
        resp = self.engine.execute(query)
        return [row for row in resp]

    def _get_table_lineage(self, connector_id: str) -> List[TableLineage]:
        table_lineage_result = self._query(
            FivetranLogQuery.get_table_lineage_query(connector_id=connector_id)
        )
        table_lineage_list: List[TableLineage] = []
        for table_lineage in table_lineage_result:
            column_lineage_result = self._query(
                FivetranLogQuery.get_column_lineage_query(
                    source_table_id=table_lineage[Constant.SOURCE_TABLE_ID],
                    destination_table_id=table_lineage[Constant.DESTINATION_TABLE_ID],
                )
            )
            column_lineage_list: List[ColumnLineage] = [
                ColumnLineage(
                    source_column=column_lineage[Constant.SOURCE_COLUMN_NAME],
                    destination_column=column_lineage[Constant.DESTINATION_COLUMN_NAME],
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
            if isinstance(message_data, str):
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
