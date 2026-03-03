"""
Patch for SqliteHook to provide OpenLineage database info.

SqliteHook doesn't implement get_openlineage_database_info(), which causes
SQL lineage extraction to fail in Airflow 3.x. This patch adds the missing
implementation so that SQLite operators can properly extract lineage.
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from airflow.models.connection import Connection
    from airflow.providers.openlineage.sqlparser import DatabaseInfo

logger = logging.getLogger(__name__)


def patch_sqlite_hook() -> None:
    """
    Patch SqliteHook to provide OpenLineage database info.

    This is necessary because SqliteHook doesn't override get_openlineage_database_info(),
    causing it to return None and preventing SQL lineage extraction.
    """
    try:
        from airflow.providers.openlineage.sqlparser import DatabaseInfo
        from airflow.providers.sqlite.hooks.sqlite import SqliteHook

        # Check if already patched
        if hasattr(SqliteHook, "_datahub_openlineage_patched"):
            logger.debug("SqliteHook already patched for OpenLineage")
            return

        def get_openlineage_database_info(
            self: Any, connection: "Connection"
        ) -> Optional["DatabaseInfo"]:
            """
            Return database info for SQLite connections.

            For SQLite, the database name is derived from the connection's host field,
            which contains the path to the SQLite database file.
            """
            # Get database path from connection
            db_path = connection.host
            if not db_path:
                # Try to get from connection extra or schema
                logger.debug("SQLite connection has no host (database path)")
                return None

            # Extract database name from file path
            # For SQLite, we use the filename without extension as the database name
            db_name = os.path.splitext(os.path.basename(db_path))[0]

            logger.debug(
                f"SQLite OpenLineage database info: path={db_path}, name={db_name}"
            )

            # Use connection type as scheme (e.g., "sqlite")
            scheme = connection.conn_type or "sqlite"

            # Create DatabaseInfo with SQLite-specific settings
            return DatabaseInfo(
                scheme=scheme,
                authority=None,  # SQLite doesn't have authority (host:port)
                database=db_name,
                # SQLite doesn't have information_schema, so these won't be used
                information_schema_columns=[],
                information_schema_table_name="",
                use_flat_cross_db_query=False,
                is_information_schema_cross_db=False,
                is_uppercase_names=False,
                normalize_name_method=lambda x: x.lower(),  # SQLite is case-insensitive
            )

        # Apply the patch (mypy doesn't like dynamic method assignment, but it's necessary for patching)
        SqliteHook.get_openlineage_database_info = get_openlineage_database_info  # type: ignore[method-assign,attr-defined]
        SqliteHook._datahub_openlineage_patched = True  # type: ignore[attr-defined]

        logger.debug(
            "Patched SqliteHook.get_openlineage_database_info to provide database info for lineage extraction"
        )

    except ImportError as e:
        logger.debug(
            f"Could not patch SqliteHook for OpenLineage (likely Airflow < 3.0 or provider not installed): {e}"
        )
