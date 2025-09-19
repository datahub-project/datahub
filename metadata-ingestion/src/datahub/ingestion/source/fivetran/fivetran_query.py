from typing import List

from datahub.ingestion.source.fivetran.fivetran_constants import (
    MAX_COLUMN_LINEAGE_PER_CONNECTOR,
    MAX_JOBS_PER_CONNECTOR,
)


class FivetranLogQuery:
    # Note: All queries are written in Snowflake SQL.
    # They will be transpiled to the target database's SQL dialect at runtime.

    def __init__(self) -> None:
        # Select query db clause
        self.schema_clause: str = ""

    def use_database(self, db_name: str) -> str:
        return f"use database {db_name}"

    def set_schema(self, schema_name: str) -> None:
        """
        Using Snowflake quoted identifiers convention

        Add double quotes around an identifier
        Use two quotes to use the double quote character inside a quoted identifier
        """
        schema_name = schema_name.replace('"', '""')
        self.schema_clause = f'"{schema_name}".'

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
FROM {self.schema_clause}connector
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
FROM {self.schema_clause}user
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
    FROM {self.schema_clause}log
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

    def get_table_lineage_query(
        self, connector_ids: List[str], max_table_lineage_per_connector: int
    ) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        # Build the QUALIFY clause - if limit is -1, skip the limit entirely
        if max_table_lineage_per_connector == -1:
            qualify_clause = ""
        else:
            qualify_clause = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY connector_id ORDER BY created_at DESC) <= {max_table_lineage_per_connector}"

        return f"""\
SELECT
    *
FROM (
    SELECT
        stm.connector_id as connector_id,
        stm.id as source_table_id,
        stm.name as source_table_name,
        ssm.name as source_schema_name,
        ssm.database_name as source_database_name,
        dtm.id as destination_table_id,
        dtm.name as destination_table_name,
        dsm.name as destination_schema_name,
        dsm.database_name as destination_database_name,
        -- Connector metadata for enhanced platform detection
        c.connector_type_id as connector_type_id,
        c.connector_name as connector_name,
        c.destination_id as destination_id,
        -- Additional metadata that might be available
        ssm.platform as source_platform,
        ssm.env as source_env,
        dsm.platform as destination_platform,
        dsm.env as destination_env,
        tl.created_at as created_at,
        ROW_NUMBER() OVER (PARTITION BY stm.connector_id, stm.id, dtm.id ORDER BY tl.created_at DESC) as table_combo_rn
    FROM {self.schema_clause}table_lineage as tl
    JOIN {self.schema_clause}source_table_metadata as stm on tl.source_table_id = stm.id
    JOIN {self.schema_clause}destination_table_metadata as dtm on tl.destination_table_id = dtm.id
    JOIN {self.schema_clause}source_schema_metadata as ssm on stm.schema_id = ssm.id
    JOIN {self.schema_clause}destination_schema_metadata as dsm on dtm.schema_id = dsm.id
    -- Join connector table for additional metadata
    JOIN {self.schema_clause}connector as c on stm.connector_id = c.connector_id
    WHERE stm.connector_id IN ({formatted_connector_ids})
    AND c._fivetran_deleted = FALSE
)
-- Ensure that we only get back one entry per source and destination pair.
WHERE table_combo_rn = 1
{qualify_clause}
ORDER BY connector_id, created_at DESC
"""

    def get_column_lineage_query(self, connector_ids: List[str]) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        return f"""\
SELECT
    connector_id,
    source_table_id,
    destination_table_id,
    source_column_name,
    destination_column_name,
    source_table_name,
    destination_table_name,
    source_schema_name,
    destination_schema_name,
    source_database_name,
    destination_database_name,
    connector_type_id,
    connector_name,
    destination_id,
    source_platform,
    source_env,
    destination_platform,
    destination_env,
    source_column_type,
    destination_column_type
FROM (
    SELECT
        stm.connector_id as connector_id,
        scm.table_id as source_table_id,
        dcm.table_id as destination_table_id,
        scm.name as source_column_name,
        dcm.name as destination_column_name,
        -- Table and schema information
        stm.name as source_table_name,
        dtm.name as destination_table_name,
        ssm.name as source_schema_name,
        dsm.name as destination_schema_name,
        ssm.database_name as source_database_name,
        dsm.database_name as destination_database_name,
        -- Connector metadata
        c.connector_type_id as connector_type_id,
        c.connector_name as connector_name,
        c.destination_id as destination_id,
        -- Platform and environment information
        ssm.platform as source_platform,
        ssm.env as source_env,
        dsm.platform as destination_platform,
        dsm.env as destination_env,
        -- Column type information for better lineage
        scm.type as source_column_type,
        dcm.type as destination_column_type,
        cl.created_at as created_at,
        ROW_NUMBER() OVER (PARTITION BY stm.connector_id, cl.source_column_id, cl.destination_column_id ORDER BY cl.created_at DESC) as column_combo_rn
    FROM {self.schema_clause}column_lineage as cl
    JOIN {self.schema_clause}source_column_metadata as scm
      ON cl.source_column_id = scm.id
    JOIN {self.schema_clause}destination_column_metadata as dcm
      ON cl.destination_column_id = dcm.id
    JOIN {self.schema_clause}source_table_metadata as stm
      ON scm.table_id = stm.id
    JOIN {self.schema_clause}destination_table_metadata as dtm
      ON dcm.table_id = dtm.id
    -- Join schema metadata for enhanced platform detection
    JOIN {self.schema_clause}source_schema_metadata as ssm
      ON stm.schema_id = ssm.id
    JOIN {self.schema_clause}destination_schema_metadata as dsm
      ON dtm.schema_id = dsm.id
    -- Join connector for additional metadata
    JOIN {self.schema_clause}connector as c
      ON stm.connector_id = c.connector_id
    WHERE stm.connector_id IN ({formatted_connector_ids})
    AND c._fivetran_deleted = FALSE
)
-- Ensure that we only get back one entry per (connector, source column, destination column) pair.
WHERE column_combo_rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY connector_id ORDER BY created_at DESC) <= {MAX_COLUMN_LINEAGE_PER_CONNECTOR}
ORDER BY connector_id, created_at DESC
"""
