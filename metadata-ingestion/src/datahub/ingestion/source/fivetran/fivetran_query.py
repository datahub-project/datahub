from typing import List

# Safeguards to prevent fetching massive amounts of data.
MAX_TABLE_LINEAGE_PER_CONNECTOR = 120
MAX_COLUMN_LINEAGE_PER_CONNECTOR = 1000
MAX_JOBS_PER_CONNECTOR = 500


class FivetranLogQuery:
    # Note: All queries are written in Snowflake SQL.
    # They will be transpiled to the target database's SQL dialect at runtime.

    def __init__(self) -> None:
        # Select query db clause
        self.db_clause: str = ""

    def set_db(self, db_name: str) -> None:
        self.db_clause = f"{db_name}."

    def use_database(self, db_name: str) -> str:
        return f"use database {db_name}"

    def get_connectors_query(self) -> str:
        return f"""\
SELECT
  connector_id,
  connecting_user_id,
  connector_type_id,
  connector_name,
  paused,
  sync_frequency,
  destination_id
FROM {self.db_clause}connector
WHERE
  _fivetran_deleted = FALSE
QUALIFY ROW_NUMBER() OVER (PARTITION BY connector_id ORDER BY _fivetran_synced DESC) = 1
"""

    def get_users_query(self) -> str:
        return f"""\
SELECT id as user_id,
given_name,
family_name,
email
FROM {self.db_clause}user
"""

    def get_sync_logs_query(
        self,
        syncs_interval: int,
        connector_ids: List[str],
    ) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        return f"""\
WITH ranked_syncs AS (
    SELECT
        connector_id,
        sync_id,
        MAX(CASE WHEN message_event = 'sync_start' THEN time_stamp END) as start_time,
        MAX(CASE WHEN message_event = 'sync_end' THEN time_stamp END) as end_time,
        MAX(CASE WHEN message_event = 'sync_end' THEN message_data END) as end_message_data,
        ROW_NUMBER() OVER (PARTITION BY connector_id ORDER BY MAX(time_stamp) DESC) as rn
    FROM {self.db_clause}log
    WHERE message_event in ('sync_start', 'sync_end')
    AND time_stamp > CURRENT_TIMESTAMP - INTERVAL '{syncs_interval} days'
    AND connector_id IN ({formatted_connector_ids})
    GROUP BY connector_id, sync_id
)
SELECT
    connector_id,
    sync_id,
    start_time,
    end_time,
    end_message_data
FROM ranked_syncs
WHERE rn <= {MAX_JOBS_PER_CONNECTOR}
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
ORDER BY connector_id, end_time DESC
"""

    def get_table_lineage_query(self, connector_ids: List[str]) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        return f"""\
SELECT
    *
FROM (
    SELECT
        stm.connector_id as connector_id,
        stm.id as source_table_id,
        stm.name as source_table_name,
        ssm.name as source_schema_name,
        dtm.id as destination_table_id,
        dtm.name as destination_table_name,
        dsm.name as destination_schema_name,
        tl.created_at as created_at,
        ROW_NUMBER() OVER (PARTITION BY stm.connector_id, stm.id, dtm.id ORDER BY tl.created_at DESC) as table_combo_rn
    FROM {self.db_clause}table_lineage as tl
    JOIN {self.db_clause}source_table_metadata as stm on tl.source_table_id = stm.id
    JOIN {self.db_clause}destination_table_metadata as dtm on tl.destination_table_id = dtm.id
    JOIN {self.db_clause}source_schema_metadata as ssm on stm.schema_id = ssm.id
    JOIN {self.db_clause}destination_schema_metadata as dsm on dtm.schema_id = dsm.id
    WHERE stm.connector_id IN ({formatted_connector_ids})
)
-- Ensure that we only get back one entry per source and destination pair.
WHERE table_combo_rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY connector_id ORDER BY created_at DESC) <= {MAX_TABLE_LINEAGE_PER_CONNECTOR}
ORDER BY connector_id, created_at DESC
"""

    def get_column_lineage_query(self, connector_ids: List[str]) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        return f"""\
SELECT
    source_table_id,
    destination_table_id,
    source_column_name,
    destination_column_name
FROM (
    SELECT
        stm.connector_id as connector_id,
        scm.table_id as source_table_id,
        dcm.table_id as destination_table_id,
        scm.name as source_column_name,
        dcm.name as destination_column_name,
        cl.created_at as created_at,
        ROW_NUMBER() OVER (PARTITION BY stm.connector_id, cl.source_column_id, cl.destination_column_id ORDER BY cl.created_at DESC) as column_combo_rn
    FROM {self.db_clause}column_lineage as cl
    JOIN {self.db_clause}source_column_metadata as scm
      ON cl.source_column_id = scm.id
    JOIN {self.db_clause}destination_column_metadata as dcm
      ON cl.destination_column_id = dcm.id
    -- Only joining source_table_metadata to get the connector_id.
    JOIN {self.db_clause}source_table_metadata as stm
      ON scm.table_id = stm.id
    WHERE stm.connector_id IN ({formatted_connector_ids})
)
-- Ensure that we only get back one entry per (connector, source column, destination column) pair.
WHERE column_combo_rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY connector_id ORDER BY created_at DESC) <= {MAX_COLUMN_LINEAGE_PER_CONNECTOR}
ORDER BY connector_id, created_at DESC
"""
