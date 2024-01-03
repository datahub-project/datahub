class FivetranLogQuery:
    def __init__(self) -> None:
        # Select query db clause
        self.db_clause: str = ""

    def set_db(self, db_name: str) -> None:
        self.db_clause = f"{db_name}."

    def use_database(self, db_name: str) -> str:
        return f"use database {db_name}"

    def get_connectors_query(self) -> str:
        return f"""
        SELECT connector_id,
        connecting_user_id,
        connector_type_id,
        connector_name,
        paused,
        sync_frequency,
        destination_id
        FROM {self.db_clause}connector
        WHERE _fivetran_deleted = FALSE"""

    def get_user_query(self, user_id: str) -> str:
        return f"""
        SELECT id as user_id,
        given_name,
        family_name
        FROM {self.db_clause}user
        WHERE id = '{user_id}'"""

    def get_sync_start_logs_query(self, connector_id: str) -> str:
        return f"""
        SELECT time_stamp,
        sync_id
        FROM {self.db_clause}log
        WHERE message_event = 'sync_start'
        and connector_id = '{connector_id}' order by time_stamp"""

    def get_sync_end_logs_query(self, connector_id: str) -> str:
        return f"""
        SELECT time_stamp,
        sync_id,
        message_data
        FROM {self.db_clause}log
        WHERE message_event = 'sync_end'
        and connector_id = '{connector_id}' order by time_stamp"""

    def get_table_lineage_query(self, connector_id: str) -> str:
        return f"""
        SELECT stm.id as source_table_id,
        stm.name as source_table_name,
        ssm.name as source_schema_name,
        dtm.id as destination_table_id,
        dtm.name as destination_table_name,
        dsm.name as destination_schema_name
        FROM {self.db_clause}table_lineage as tl
        JOIN {self.db_clause}source_table_metadata as stm on tl.source_table_id = stm.id
        JOIN {self.db_clause}destination_table_metadata as dtm on tl.destination_table_id = dtm.id
        JOIN {self.db_clause}source_schema_metadata as ssm on stm.schema_id = ssm.id
        JOIN {self.db_clause}destination_schema_metadata as dsm on dtm.schema_id = dsm.id
        WHERE stm.connector_id = '{connector_id}'"""

    def get_column_lineage_query(
        self, source_table_id: str, destination_table_id: str
    ) -> str:
        return f"""
        SELECT scm.name as source_column_name,
        dcm.name as destination_column_name
        FROM {self.db_clause}column_lineage as cl
        JOIN {self.db_clause}source_column_metadata as scm on
        (cl.source_column_id = scm.id and scm.table_id = {source_table_id})
        JOIN {self.db_clause}destination_column_metadata as dcm on
        (cl.destination_column_id = dcm.id and dcm.table_id = {destination_table_id})"""
