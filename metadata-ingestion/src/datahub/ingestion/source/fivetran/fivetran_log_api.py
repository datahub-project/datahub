import json
import logging
from typing import Any, Dict, List, Optional, Tuple

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
                fivetran_log_query.set_db(
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
                fivetran_log_query.set_db(bigquery_destination_config.dataset)
                fivetran_log_database = bigquery_destination_config.dataset
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
        logger.debug("Query : {}".format(query))
        resp = self.engine.execute(query)
        return [row for row in resp]

    def _get_table_lineage(self, connector_id: str) -> List[TableLineage]:
        table_lineage_result = self._query(
            self.fivetran_log_query.get_table_lineage_query(connector_id=connector_id)
        )
        table_lineage_list: List[TableLineage] = []
        for table_lineage in table_lineage_result:
            column_lineage_result = self._query(
                self.fivetran_log_query.get_column_lineage_query(
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
                self.fivetran_log_query.get_sync_start_logs_query(
                    connector_id=connector_id
                )
            )
        }
        sync_end_logs = {
            row[Constant.SYNC_ID]: row
            for row in self._query(
                self.fivetran_log_query.get_sync_end_logs_query(
                    connector_id=connector_id
                )
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

    def _get_user_name(self, user_id: Optional[str]) -> Optional[str]:
        if not user_id:
            return None
        user_details = self._query(
            self.fivetran_log_query.get_user_query(user_id=user_id)
        )

        if not user_details:
            return None

        return f"{user_details[0][Constant.GIVEN_NAME]} {user_details[0][Constant.FAMILY_NAME]}"

    def get_allowed_connectors_list(
        self, connector_patterns: AllowDenyPattern, report: FivetranSourceReport
    ) -> List[Connector]:
        connectors: List[Connector] = []
        connector_list = self._query(self.fivetran_log_query.get_connectors_query())
        for connector in connector_list:
            if not connector_patterns.allowed(connector[Constant.CONNECTOR_NAME]):
                report.report_connectors_dropped(connector[Constant.CONNECTOR_NAME])
                continue
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
