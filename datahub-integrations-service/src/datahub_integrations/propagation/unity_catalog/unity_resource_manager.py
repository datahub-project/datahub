"""
Unity Catalog Tag Management Script
This script provides functions to add, remove, and update tags on Unity Catalog tables and columns.
"""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from databricks.sql import connect

# Configure logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UnityResourceManager:
    """Manages Unity Catalog tags for tables and columns using SQL commands."""

    def __init__(
        self,
        connection_string: Optional[str] = None,
        conn: Optional[Any] = None,
        connection_params: Optional[Dict] = None,
    ):
        """
        Initialize the Unity Resource Manager.

        Args:
            connection_string: Databricks connection string
            conn: Pre-configured Databricks SQL connection (not recommended for long-running)
            connection_params: Dict with connection parameters for creating fresh connections
        """
        self.conn = None
        self.connection_string = connection_string
        self.connection_params = connection_params

        if conn:
            logger.warning(
                "Using pre-configured connection. This may not work for long-running operations."
            )
            self.conn = conn
        elif connection_string:
            self.connection_string = connection_string
            self._parse_connection_string()
        elif connection_params:
            self.connection_params = connection_params
        else:
            raise ValueError(
                "Either connection_string, conn, or connection_params must be provided"
            )

    def _parse_connection_string(self) -> None:
        """Parse connection string to extract parameters for fresh connections."""
        if not self.connection_string:
            return

        parsed = urlparse(self.connection_string)
        query_params = parse_qs(parsed.query)

        self.connection_params = {
            "server_hostname": parsed.hostname,
            "http_path": query_params.get("http_path", [""])[0],
            "access_token": parsed.password,
        }

    def _test_connection(self, conn: Any) -> bool:
        """Test if a connection is still alive."""
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            logger.warning(f"Connection test failed: {str(e)}")
            return False

    def _execute_sql(self, query: str) -> List[Dict[str, str]]:
        """Execute SQL query using databricks-sql connector for better performance"""
        try:
            logger.info(f"Executing SQL query: {query}")
            with (
                connect(**self.connection_params) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(query)
                logger.info("SQL query executed successfully")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = [dict(zip(columns, row, strict=False)) for row in rows]
                return result

        except Exception as e:
            logger.warning(f"Failed to execute SQL query: {e}", exc_info=True)
            raise e

    def _execute_multiple_sql(self, sql_commands: List[str]) -> bool:
        """Execute multiple SQL commands in sequence with connection management."""
        try:
            with (
                connect(**self.connection_params) as connection,
                connection.cursor() as cursor,
            ):
                for sql_cmd in sql_commands:
                    cursor.execute(sql_cmd)

            logger.info(f"Successfully executed {len(sql_commands)} SQL commands")
            return True

        except Exception as e:
            logger.error(f"Failed to execute SQL commands: {str(e)}")
            return False

    def close_persistent_connection(self) -> None:
        """Manually close the persistent connection if it exists."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Closed persistent connection")
            except Exception as e:
                logger.warning(f"Error closing persistent connection: {str(e)}")
            finally:
                self.conn = None

    def add_table_tags(
        self, catalog: str, schema: str, table: str, tags: Dict[str, str]
    ) -> bool:
        """
        Add tags to a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            tags: Dictionary of tag key-value pairs

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

            # Build SET TAGS SQL command
            tag_pairs = [f"'{key}' = '{value}'" for key, value in tags.items()]
            tag_string = ", ".join(tag_pairs)

            sql_command = f"ALTER TABLE {full_table_name} SET TAGS ({tag_string})"

            self._execute_sql(sql_command)
            logger.info(f"Successfully added tags to table {full_table_name}: {tags}")

            return True

        except Exception as e:
            logger.error(
                f"Failed to add tags to table {catalog}.{schema}.{table}: {str(e)}"
            )
            return False

    def remove_table_tags(
        self, catalog: str, schema: str, table: str, tag_keys: List[str]
    ) -> bool:
        """
        Remove specific tags from a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            tag_keys: List of tag keys to remove

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

            # Build UNSET TAGS SQL command
            quoted_keys = [f"'{key}'" for key in tag_keys]
            keys_string = ", ".join(quoted_keys)

            sql_command = f"ALTER TABLE {full_table_name} UNSET TAGS ({keys_string})"

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully removed tags from table {full_table_name}: {tag_keys}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to remove tags from table {catalog}.{schema}.{table}: {str(e)}"
            )
            return False

    def update_table_tags(
        self,
        catalog: str,
        schema: str,
        table: str,
        tags: Dict[str, str],
        replace_all: bool = False,
    ) -> bool:
        """
        Update tags on a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            tags: Dictionary of tag key-value pairs
            replace_all: If True, unset all existing tags first. If False, merge with existing.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

            sql_commands = []

            if replace_all:
                # First, get existing tags to unset them
                existing_tags = self.get_table_tags(catalog, schema, table)
                if existing_tags:
                    existing_keys = list(existing_tags.keys())
                    if existing_keys:
                        quoted_keys = [f"'{key}'" for key in existing_keys]
                        keys_string = ", ".join(quoted_keys)
                        unset_command = (
                            f"ALTER TABLE {full_table_name} UNSET TAGS ({keys_string})"
                        )
                        sql_commands.append(unset_command)

            # Set new tags
            if tags:
                tag_pairs = [f"'{key}' = '{value}'" for key, value in tags.items()]
                tag_string = ", ".join(tag_pairs)
                set_command = f"ALTER TABLE {full_table_name} SET TAGS ({tag_string})"
                sql_commands.append(set_command)

            if sql_commands:
                success = self._execute_multiple_sql(sql_commands)
                if success:
                    action = "replaced" if replace_all else "updated"
                    logger.info(
                        f"Successfully {action} tags on table {full_table_name}: {tags}"
                    )
                return success
            else:
                logger.info(f"No tag operations needed for table {full_table_name}")
                return True

        except Exception as e:
            logger.error(
                f"Failed to update tags on table {catalog}.{schema}.{table}: {str(e)}"
            )
            return False

    def add_column_tags(
        self, catalog: str, schema: str, table: str, column: str, tags: Dict[str, str]
    ) -> bool:
        """
        Add tags to a Unity Catalog column using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name
            tags: Dictionary of tag key-value pairs

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

            # Build SET TAGS SQL command for column
            tag_pairs = [f"'{key}' = '{value}'" for key, value in tags.items()]
            tag_string = ", ".join(tag_pairs)

            sql_command = f"ALTER TABLE {full_table_name} ALTER COLUMN `{column}` SET TAGS ({tag_string})"

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully added tags to column {full_table_name}.{column}: {tags}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to add tags to column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return False

    def remove_column_tags(
        self, catalog: str, schema: str, table: str, column: str, tag_keys: List[str]
    ) -> bool:
        """
        Remove specific tags from a Unity Catalog column using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name
            tag_keys: List of tag keys to remove

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

            # Build UNSET TAGS SQL command for column
            quoted_keys = [f"'{key}'" for key in tag_keys]
            keys_string = ", ".join(quoted_keys)

            sql_command = f"ALTER TABLE {full_table_name} ALTER COLUMN `{column}` UNSET TAGS ({keys_string})"

            success = self._execute_sql(sql_command)
            if success:
                logger.info(
                    f"Successfully removed tags from column {full_table_name}.{column}: {tag_keys}"
                )

            return True

        except Exception as e:
            logger.error(
                f"Failed to remove tags from column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return False

    def update_column_tags(
        self,
        catalog: str,
        schema: str,
        table: str,
        column: str,
        tags: Dict[str, str],
        replace_all: bool = False,
    ) -> bool:
        """
        Update tags on a Unity Catalog column using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name
            tags: Dictionary of tag key-value pairs
            replace_all: If True, unset all existing tags first. If False, merge with existing.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f'"{catalog}"."{schema}"."{table}"'

            sql_commands = []

            if replace_all:
                # First, get existing column tags to unset them
                existing_tags = self.get_column_tags(catalog, schema, table, column)
                if existing_tags:
                    existing_keys = list(existing_tags.keys())
                    if existing_keys:
                        quoted_keys = [f"'{key}'" for key in existing_keys]
                        keys_string = ", ".join(quoted_keys)
                        unset_command = f"ALTER TABLE {full_table_name} ALTER COLUMN `{column}` UNSET TAGS ({keys_string})"
                        sql_commands.append(unset_command)

            # Set new tags
            if tags:
                tag_pairs = [f"'{key}' = '{value}'" for key, value in tags.items()]
                tag_string = ", ".join(tag_pairs)
                set_command = f"ALTER TABLE {full_table_name} ALTER COLUMN `{column}` SET TAGS ({tag_string})"
                sql_commands.append(set_command)

            if sql_commands:
                success = self._execute_multiple_sql(sql_commands)
                if success:
                    action = "replaced" if replace_all else "updated"
                    logger.info(
                        f"Successfully {action} tags on column {full_table_name}.{column}: {tags}"
                    )
                return success
            else:
                logger.info(
                    f"No tag operations needed for column {full_table_name}.{column}"
                )
                return True

        except Exception as e:
            logger.error(
                f"Failed to update tags on column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return False

    def get_table_tags(
        self, catalog: str, schema: str, table: str
    ) -> Optional[Dict[str, str]]:
        """
        Get all tags for a Unity Catalog table using SQL query.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            Dict of tags or None if error
        """
        try:
            # Use information_schema.table_tags to get table tags
            sql_query = f"""SELECT catalog_name, schema_name, table_name, tag_name, tag_value
            FROM `{catalog}`.information_schema.table_tags 
            WHERE catalog_name = '{catalog}' 
            AND schema_name = '{schema}' 
            AND table_name = '{table}'"""

            results = self._execute_sql(sql_query)

            if results:
                # Parse the results to extract tags
                tags = {}
                for row in results:
                    tags[row["tag_name"]] = row["tag_value"]
                return tags

            return {}

        except Exception as e:
            logger.error(
                f"Failed to get tags for table {catalog}.{schema}.{table}: {str(e)}"
            )
            return None

    def get_column_tags(
        self, catalog: str, schema: str, table: str, column: str
    ) -> Optional[Dict[str, str]]:
        """
        Get all tags for a Unity Catalog column using SQL query.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name

        Returns:
            Dict of tags or None if error
        """
        try:
            sql_query = f"""SELECT tag_name, tag_value 
            FROM `{catalog}`.information_schema.column_tags 
            WHERE catalog_name = '{catalog}' 
            AND schema_name = '{schema}' 
            AND table_name = '{table}' 
            AND column_name = '{column}'"""

            results = self._execute_sql(sql_query)

            if results:
                # Parse the results to extract tags
                tags = {}
                for row in results:
                    tags[row["tag_name"]] = row["tag_value"]
                return tags

            return {}

        except Exception as e:
            logger.error(
                f"Failed to get tags for column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return None

    def list_all_table_tags(
        self, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> Optional[List[Dict]]:
        """
        List all tables and their tags in a catalog/schema.

        Args:
            catalog: Optional catalog name filter
            schema: Optional schema name filter

        Returns:
            List of dictionaries containing table info and tags
        """
        try:
            where_clauses = []
            if catalog:
                where_clauses.append(f"table_catalog = '{catalog}'")
            if schema:
                where_clauses.append(f"table_schema = '{schema}'")

            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

            sql_query = f"""
            SELECT table_catalog, table_schema, table_name, table_type, owner, comment
            FROM system.information_schema.tables 
            WHERE {where_clause}
            ORDER BY table_catalog, table_schema, table_name
            """

            results: List[Dict] = self._execute_sql(sql_query)

            if results:
                # For each table, get its tags
                enriched_results = []
                for table_info in results:
                    table_tags = self.get_table_tags(
                        table_info["table_catalog"],
                        table_info["table_schema"],
                        table_info["table_name"],
                    )
                    table_info["tags"] = table_tags or {}
                    enriched_results.append(table_info)

                return enriched_results

            return []

        except Exception as e:
            logger.error(f"Failed to list table tags: {str(e)}")
            return None

    def set_catalog_description(self, catalog: str, description: str) -> bool:
        """
        Set description for a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            description: Description text to set

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Escape single quotes for SQL
            safe_description = description.replace("'", "\\'")
            sql_command = f"COMMENT ON CATALOG {catalog} IS '{safe_description}'"

            self._execute_sql(sql_command)
            logger.info(f"Successfully set description for catalog {catalog}")
            return True

        except Exception as e:
            logger.error(f"Failed to set description for catalog {catalog}: {str(e)}")
            return False

    def remove_catalog_description(self, catalog: str) -> bool:
        """
        Remove description from a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            sql_command = f"COMMENT ON CATALOG {catalog} IS ''"

            self._execute_sql(sql_command)
            logger.info(f"Successfully removed description from catalog {catalog}")
            return True

        except Exception as e:
            logger.error(f"Failed to remove description from table {catalog}: {str(e)}")
            return False

    def get_catalog_description(self, catalog: str) -> Optional[str]:
        """
        Get description for a Unity Catalog catalog using SQL query.

        Args:
            catalog: Catalog name

        Returns:
            str: Description text or None if error
        """
        try:
            sql_query = f"""SELECT comment
            FROM `{catalog}`.information_schema.catalogs 
            WHERE table_catalog = '{catalog}'"""

            results = self._execute_sql(sql_query)

            if results and len(results) > 0:
                return results[0].get("comment")

            return None

        except Exception as e:
            logger.error(f"Failed to get description for catalog {catalog}: {str(e)}")
            return None

    def set_schema_description(
        self, catalog: str, schema: str, description: str
    ) -> bool:
        """
        Set description for a Unity Catalog schema using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            description: Description text to set

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_schema_name = f"`{catalog}`.`{schema}`"
            # Escape single quotes for SQL
            safe_description = description.replace("'", "\\'")
            sql_command = (
                f"COMMENT ON SCHEMA {full_schema_name} is '{safe_description}'"
            )

            self._execute_sql(sql_command)
            logger.info(f"Successfully set description for schema {full_schema_name}")
            return True
        except Exception as e:
            logger.error(
                f"Failed to set description for table {catalog}.{schema}: {str(e)}"
            )
            return False

    def remove_schema_description(self, catalog: str, schema: str) -> bool:
        """
        Remove description from a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_schema_name = f"`{catalog}`.`{schema}`"
            sql_command = f"COMMENT ON SCHEMA {full_schema_name} IS ''"

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully removed description from table {full_schema_name}"
            )

            return True

        except Exception as e:
            logger.error(
                f"Failed to remove description from schema {catalog}.{schema}: {str(e)}"
            )
            return False

    def get_schema_description(
        self, catalog: str, schema: str, table: str
    ) -> Optional[str]:
        """
        Get description for a Unity Catalog table using SQL query.

        Args:
            catalog: Catalog name
            schema: Schema name

        Returns:
            str: Description text or None if error
        """
        try:
            sql_query = f"""SELECT comment
            FROM `{catalog}`.information_schema.schemata 
            WHERE catalog_name = '{catalog}' 
            AND schema_name = '{schema}'"""

            results = self._execute_sql(sql_query)

            if results and len(results) > 0:
                return results[0].get("comment")

            return None

        except Exception as e:
            logger.error(
                f"Failed to get description for schema {catalog}.{schema}: {str(e)}"
            )
            return None

    def set_table_description(
        self, catalog: str, schema: str, table: str, description: str
    ) -> bool:
        """
        Set description for a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            description: Description text to set

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
            # Escape single quotes for SQL
            safe_description = description.replace("'", "\\'")
            sql_command = f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{safe_description}')"

            self._execute_sql(sql_command)
            logger.info(f"Successfully set description for table {full_table_name}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to set description for table {catalog}.{schema}.{table}: {str(e)}"
            )
            return False

    def remove_table_description(self, catalog: str, schema: str, table: str) -> bool:
        """
        Remove description from a Unity Catalog table using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
            sql_command = (
                f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '')"
            )

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully removed description from table {full_table_name}"
            )

            return True

        except Exception as e:
            logger.error(
                f"Failed to remove description from table {catalog}.{schema}.{table}: {str(e)}"
            )
            return False

    def get_table_description(
        self, catalog: str, schema: str, table: str
    ) -> Optional[str]:
        """
        Get description for a Unity Catalog table using SQL query.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            str: Description text or None if error
        """
        try:
            sql_query = f"""SELECT comment
            FROM `{catalog}`.information_schema.tables 
            WHERE table_catalog = '{catalog}' 
            AND table_schema = '{schema}' 
            AND table_name = '{table}'"""

            results = self._execute_sql(sql_query)

            if results and len(results) > 0:
                return results[0].get("comment")

            return None

        except Exception as e:
            logger.error(
                f"Failed to get description for table {catalog}.{schema}.{table}: {str(e)}"
            )
            return None

    def set_column_description(
        self, catalog: str, schema: str, table: str, column: str, description: str
    ) -> bool:
        """
        Set description for a Unity Catalog column using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name
            description: Description text to set

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
            # Use backslash escaping for single quotes
            safe_description = description.replace("'", "\\'")
            sql_command = f"ALTER TABLE {full_table_name} ALTER COLUMN `{column}` COMMENT '{safe_description}'"

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully set description for column {full_table_name}.{column}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to set description for column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return False

    def remove_column_description(
        self, catalog: str, schema: str, table: str, column: str
    ) -> bool:
        """
        Remove description from a Unity Catalog column using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
            sql_command = (
                f"ALTER TABLE {full_table_name} ALTER COLUMN `{column}` COMMENT ''"
            )

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully removed description from column {full_table_name}.{column}"
            )

            return True

        except Exception as e:
            logger.error(
                f"Failed to remove description from column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return False

    def get_column_description(
        self, catalog: str, schema: str, table: str, column: str
    ) -> Optional[str]:
        """
        Get description for a Unity Catalog column using SQL query.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column: Column name

        Returns:
            str: Description text or None if error
        """
        try:
            sql_query = f"""SELECT comment
            FROM `{catalog}`.information_schema.columns 
            WHERE table_catalog = '{catalog}' 
            AND table_schema = '{schema}' 
            AND table_name = '{table}' 
            AND column_name = '{column}'"""

            results = self._execute_sql(sql_query)

            if results and len(results) > 0:
                return results[0].get("comment")

            return None

        except Exception as e:
            logger.error(
                f"Failed to get description for column {catalog}.{schema}.{table}.{column}: {str(e)}"
            )
            return None

    def add_catalog_tags(self, catalog: str, tags: Dict[str, str]) -> bool:
        """
        Add tags to a Unity Catalog using SQL.

        Args:
            catalog: Catalog name
            tags: Dictionary of tag key-value pairs

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            catalog_name = f"`{catalog}`"

            # Build SET TAGS SQL command
            tag_pairs = [f"'{key}' = '{value}'" for key, value in tags.items()]
            tag_string = ", ".join(tag_pairs)

            sql_command = f"ALTER CATALOG {catalog_name} SET TAGS ({tag_string})"

            self._execute_sql(sql_command)
            logger.info(f"Successfully added tags to catalog {catalog_name}: {tags}")

            return True

        except Exception as e:
            logger.error(f"Failed to add tags to catalog {catalog}: {str(e)}")
            return False

    def remove_catalog_tags(self, catalog: str, tag_keys: List[str]) -> bool:
        """
        Remove specific tags from a Unity Catalog using SQL.

        Args:
            catalog: Catalog name
            tag_keys: List of tag keys to remove

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            catalog_name = f"`{catalog}`"

            # Build UNSET TAGS SQL command
            quoted_keys = [f"'{key}'" for key in tag_keys]
            keys_string = ", ".join(quoted_keys)

            sql_command = f"ALTER CATALOG {catalog_name} UNSET TAGS ({keys_string})"

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully removed tags from catalog {catalog_name}: {tag_keys}"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to remove tags from catalog {catalog}: {str(e)}")
            return False

    def add_schema_tags(self, catalog: str, schema: str, tags: Dict[str, str]) -> bool:
        """
        Add tags to a Unity Catalog schema using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            tags: Dictionary of tag key-value pairs

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_schema_name = f"`{catalog}`.`{schema}`"

            # Build SET TAGS SQL command
            tag_pairs = [f"'{key}' = '{value}'" for key, value in tags.items()]
            tag_string = ", ".join(tag_pairs)

            sql_command = f"ALTER SCHEMA {full_schema_name} SET TAGS ({tag_string})"

            self._execute_sql(sql_command)
            logger.info(f"Successfully added tags to schema {full_schema_name}: {tags}")

            return True

        except Exception as e:
            logger.error(f"Failed to add tags to schema {catalog}.{schema}: {str(e)}")
            return False

    def remove_schema_tags(
        self, catalog: str, schema: str, tag_keys: List[str]
    ) -> bool:
        """
        Remove specific tags from a Unity Catalog schema using SQL.

        Args:
            catalog: Catalog name
            schema: Schema name
            tag_keys: List of tag keys to remove

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_schema_name = f"`{catalog}`.`{schema}`"

            # Build UNSET TAGS SQL command
            quoted_keys = [f"'{key}'" for key in tag_keys]
            keys_string = ", ".join(quoted_keys)

            sql_command = f"ALTER SCHEMA {full_schema_name} UNSET TAGS ({keys_string})"

            self._execute_sql(sql_command)
            logger.info(
                f"Successfully removed tags from schema {full_schema_name}: {tag_keys}"
            )

            return True

        except Exception as e:
            logger.error(
                f"Failed to remove tags from schema {catalog}.{schema}: {str(e)}"
            )
            return False
