from typing import Any, Dict, List, Optional

from datahub.ingestion.source.fivetran.fivetran_constants import (
    MAX_JOBS_PER_CONNECTOR,
)

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
        # Table name compatibility for schema changes
        self._table_names: Dict[str, str] = {}
        self._table_names_initialized = False

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

    def get_table_lineage_query(
        self,
        connector_ids: List[str],
        max_lineage: Optional[int] = None,
    ) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        # Build base query
        base_query = f"""\
SELECT
    connection_id,
    source_table_id,
    source_table_name,
    source_schema_name,
    destination_table_id,
    destination_table_name,
    destination_schema_name
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
WHERE table_combo_rn = 1"""

        # Add QUALIFY clause only if max_lineage is specified (not None)
        if max_lineage is not None:
            base_query += f"""
QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY created_at DESC) <= {max_lineage}"""

        base_query += """
ORDER BY connection_id, created_at DESC
"""

        return base_query

    def get_column_lineage_query(
        self, connector_ids: List[str], max_column_lineage: Optional[int] = None
    ) -> str:
        # Format connector_ids as a comma-separated string of quoted IDs
        formatted_connector_ids = ", ".join(f"'{id}'" for id in connector_ids)

        # Build the base query
        base_query = f"""\
SELECT
    connection_id,
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
WHERE column_combo_rn = 1"""

        # Add QUALIFY clause only if max_column_lineage is specified (not None)
        if max_column_lineage is not None:
            base_query += f"""
QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY created_at DESC) <= {max_column_lineage}"""

        base_query += """
ORDER BY connection_id, created_at DESC
"""

        return base_query

    def initialize_table_names(self, engine: Any) -> None:
        """
        Initialize table name mappings for backward compatibility.
        Detects whether to use new table names (without _metadata suffix) or old ones.
        """
        if self._table_names_initialized:
            return

        # Define the mapping from logical name to (new_name, old_name)
        table_mappings = {
            "source_table": ("source_table", "source_table_metadata"),
            "destination_table": ("destination_table", "destination_table_metadata"),
            "source_schema": ("source_schema", "source_schema_metadata"),
            "destination_schema": ("destination_schema", "destination_schema_metadata"),
            "source_column": ("source_column", "source_column_metadata"),
            "destination_column": ("destination_column", "destination_column_metadata"),
            "source_foreign_key": ("source_foreign_key", "source_foreign_key_metadata"),
        }

        # Check which table names exist
        for logical_name, (new_name, old_name) in table_mappings.items():
            # Try new name first
            new_table_name = f"{self.schema_clause}{new_name}"
            old_table_name = f"{self.schema_clause}{old_name}"

            try:
                # Check if new table exists
                check_query = f"SELECT 1 FROM {new_table_name} LIMIT 1"
                engine.execute(check_query)
                self._table_names[logical_name] = new_name
                continue
            except Exception:
                pass

            try:
                # Fall back to old table name
                check_query = f"SELECT 1 FROM {old_table_name} LIMIT 1"
                engine.execute(check_query)
                self._table_names[logical_name] = old_name
                continue
            except Exception:
                # Default to new name if neither exists (will fail later with clearer error)
                self._table_names[logical_name] = new_name

        self._table_names_initialized = True

    def get_table_name(self, logical_name: str) -> str:
        """Get the actual table name for a logical table name."""
        if not self._table_names_initialized:
            raise RuntimeError(
                "Table names not initialized. Call initialize_table_names() first."
            )
        return self._table_names.get(logical_name, logical_name)
