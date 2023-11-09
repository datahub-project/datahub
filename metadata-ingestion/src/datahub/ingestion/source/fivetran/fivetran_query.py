class FivetranLogQuery:
    @staticmethod
    def use_schema(db_name: str, schema_name: str) -> str:
        return f'use schema "{db_name}"."{schema_name}"'

    @staticmethod
    def get_connectors_query() -> str:
        return """
        SELECT connector_id as "CONNECTOR_ID",
        connecting_user_id as "CONNECTING_USER_ID",
        connector_type_id as "CONNECTOR_TYPE_ID",
        connector_name as "CONNECTOR_NAME",
        paused as "PAUSED",
        sync_frequency as "SYNC_FREQUENCY",
        destination_id as "DESTINATION_ID"
        FROM CONNECTOR
        WHERE _fivetran_deleted = FALSE"""

    @staticmethod
    def get_user_query(user_id: str) -> str:
        return f"""
        SELECT id as "USER_ID",
        given_name as "GIVEN_NAME",
        family_name as "FAMILY_NAME"
        FROM USER
        WHERE id = '{user_id}'"""

    @staticmethod
    def get_sync_start_logs_query(
        connector_id: str,
    ) -> str:
        return f"""
        SELECT time_stamp as "TIME_STAMP",
        sync_id as "SYNC_ID"
        FROM LOG
        WHERE message_event = 'sync_start'
        and connector_id = '{connector_id}' order by time_stamp"""

    @staticmethod
    def get_sync_end_logs_query(connector_id: str) -> str:
        return f"""
        SELECT time_stamp as "TIME_STAMP",
        sync_id as "SYNC_ID",
        message_data as "MESSAGE_DATA"
        FROM LOG
        WHERE message_event = 'sync_end'
        and connector_id = '{connector_id}' order by time_stamp"""

    @staticmethod
    def get_table_lineage_query(connector_id: str) -> str:
        return f"""
        SELECT stm.id as "SOURCE_TABLE_ID",
        stm.name as "SOURCE_TABLE_NAME",
        ssm.name as "SOURCE_SCHEMA_NAME",
        dtm.id as "DESTINATION_TABLE_ID",
        dtm.name as "DESTINATION_TABLE_NAME",
        dsm.name as "DESTINATION_SCHEMA_NAME"
        FROM table_lineage as tl
        JOIN source_table_metadata as stm on tl.source_table_id = stm.id
        JOIN destination_table_metadata as dtm on tl.destination_table_id = dtm.id
        JOIN source_schema_metadata as ssm on stm.schema_id = ssm.id
        JOIN destination_schema_metadata as dsm on dtm.schema_id = dsm.id
        WHERE stm.connector_id = '{connector_id}'"""

    @staticmethod
    def get_column_lineage_query(
        source_table_id: str, destination_table_id: str
    ) -> str:
        return f"""
        SELECT scm.name as "SOURCE_COLUMN_NAME",
        dcm.name as "DESTINATION_COLUMN_NAME"
        FROM column_lineage as cl
        JOIN source_column_metadata as scm on
        (cl.source_column_id = scm.id and scm.table_id = {source_table_id})
        JOIN destination_column_metadata as dcm on
        (cl.destination_column_id = dcm.id and dcm.table_id = {destination_table_id})"""
