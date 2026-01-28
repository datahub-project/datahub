import re
from typing import List

# Safeguards to prevent fetching massive amounts of data.
MAX_TABLE_LINEAGE_PER_CONNECTOR = 120
MAX_COLUMN_LINEAGE_PER_CONNECTOR = 1000
MAX_JOBS_PER_CONNECTOR = 500


"""
------------------------------------------------------------------------------------------------------------
Fivetran Platform Connector Handling
------------------------------------------------------------------------------------------------------------
Current Query Change Log: August 2025 (See: https://fivetran.com/docs/changelog/2025/august-2025)

All queries have to be updated as per Fivetran Platform Connector release if any. We expect customers
and fivetran to keep platform connector configured for DataHub with auto sync enabled to get latest changes.

References:
- Fivetran Release Notes: https://fivetran.com/docs/changelog (Look for "Fivetran Platform Connector")
- Latest Platform Connector Schema: https://fivetran.com/docs/logs/fivetran-platform?erdModal=open
"""


class FivetranLogQuery:
    # Note: All queries are written in Snowflake SQL.
    # They will be transpiled to the target database's SQL dialect at runtime.

    def __init__(self) -> None:
        # Select query db clause
        self.schema_clause: str = ""

    @staticmethod
    def _is_valid_unquoted_identifier(identifier: str) -> bool:
        """
        Check if an identifier can be used unquoted in Snowflake.

        Snowflake unquoted identifiers must:
        - Start with a letter (A-Z) or underscore (_)
        - Contain only letters, numbers, and underscores
        - Be uppercase (Snowflake auto-converts unquoted identifiers to uppercase)

        Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax#unquoted-identifiers
        """
        if not identifier:
            return False

        # Check if it's already quoted (starts and ends with double quotes)
        if identifier.startswith('"') and identifier.endswith('"'):
            return False

        # Check if it starts with letter or underscore
        if not (identifier[0].isalpha() or identifier[0] == "_"):
            return False

        # Check if it contains only alphanumeric characters and underscores
        if not re.match(r"^[A-Za-z0-9_]+$", identifier):
            return False

        # For Snowflake, unquoted identifiers are case-insensitive and auto-converted to uppercase
        # This means we have recieved an unquoted identifier, and we can convert it to quoted identifier with uppercase
        return True

    def use_database(self, db_name: str) -> str:
        """
        Using Snowflake quoted identifiers convention
        Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers

        Add double quotes around an identifier
        """
        db_name = db_name.replace(
            '"', '""'
        )  # Replace double quotes with two double quotes to use the double quote character inside a quoted identifier
        return f'use database "{db_name}"'

    def set_schema(self, schema_name: str) -> None:
        """
        Using Snowflake quoted identifiers convention
        Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers

        Add double quotes around an identifier
        Use two quotes to use the double quote character inside a quoted identifier
        """
        schema_name = schema_name.replace(
            '"', '""'
        )  # Replace double quotes with two double quotes to use the double quote character inside a quoted identifier
        self.schema_clause = f'"{schema_name}".'

    def get_connectors_query(self) -> str:
        return f"""\
SELECT
  connection_id,
  connecting_user_id,
  connector_type_id,
  connection_name,
  paused,
  sync_frequency,
  destination_id
FROM {self.schema_clause}connection
WHERE
  _fivetran_deleted = FALSE
QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY _fivetran_synced DESC) = 1
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
        connection_id,
        sync_id,
        MAX(CASE WHEN message_event = 'sync_start' THEN time_stamp END) as start_time,
        MAX(CASE WHEN message_event = 'sync_end' THEN time_stamp END) as end_time,
        MAX(CASE WHEN message_event = 'sync_end' THEN message_data END) as end_message_data,
        ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY MAX(time_stamp) DESC) as rn
    FROM {self.schema_clause}log
    WHERE message_event in ('sync_start', 'sync_end')
    AND time_stamp > CURRENT_TIMESTAMP - INTERVAL '{syncs_interval} days'
    AND connection_id IN ({formatted_connector_ids})
    GROUP BY connection_id, sync_id
)
SELECT
    connection_id,
    sync_id,
    start_time,
    end_time,
    end_message_data
FROM ranked_syncs
WHERE rn <= {MAX_JOBS_PER_CONNECTOR}
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
ORDER BY connection_id, end_time DESC
"""

    def get_table_lineage_query(self, connector_ids: List[str]) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        return f"""\
SELECT
    *
FROM (
    SELECT
        stm.connection_id as connection_id,
        stm.id as source_table_id,
        stm.name as source_table_name,
        ssm.name as source_schema_name,
        dtm.id as destination_table_id,
        dtm.name as destination_table_name,
        dsm.name as destination_schema_name,
        tl.created_at as created_at,
        ROW_NUMBER() OVER (PARTITION BY stm.connection_id, stm.id, dtm.id ORDER BY tl.created_at DESC) as table_combo_rn
    FROM {self.schema_clause}table_lineage as tl
    JOIN {self.schema_clause}source_table as stm on tl.source_table_id = stm.id -- stm: source_table_metadata
    JOIN {self.schema_clause}destination_table as dtm on tl.destination_table_id = dtm.id -- dtm: destination_table_metadata
    JOIN {self.schema_clause}source_schema as ssm on stm.schema_id = ssm.id -- ssm: source_schema_metadata
    JOIN {self.schema_clause}destination_schema as dsm on dtm.schema_id = dsm.id -- dsm: destination_schema_metadata
    WHERE stm.connection_id IN ({formatted_connector_ids})
)
-- Ensure that we only get back one entry per source and destination pair.
WHERE table_combo_rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY created_at DESC) <= {MAX_TABLE_LINEAGE_PER_CONNECTOR}
ORDER BY connection_id, created_at DESC
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
        stm.connection_id as connection_id,
        scm.table_id as source_table_id,
        dcm.table_id as destination_table_id,
        scm.name as source_column_name,
        dcm.name as destination_column_name,
        cl.created_at as created_at,
        ROW_NUMBER() OVER (PARTITION BY stm.connection_id, cl.source_column_id, cl.destination_column_id ORDER BY cl.created_at DESC) as column_combo_rn
    FROM {self.schema_clause}column_lineage as cl
    JOIN {self.schema_clause}source_column as scm -- scm: source_column_metadata
      ON cl.source_column_id = scm.id
    JOIN {self.schema_clause}destination_column as dcm -- dcm: destination_column_metadata
      ON cl.destination_column_id = dcm.id
    -- Only joining source_table to get the connection_id.
    JOIN {self.schema_clause}source_table as stm -- stm: source_table_metadata
      ON scm.table_id = stm.id
    WHERE stm.connection_id IN ({formatted_connector_ids})
)
-- Ensure that we only get back one entry per (connector, source column, destination column) pair.
WHERE column_combo_rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY created_at DESC) <= {MAX_COLUMN_LINEAGE_PER_CONNECTOR}
ORDER BY connection_id, created_at DESC
"""
