import logging
from typing import Any, Callable, Optional, Tuple

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn

logger = logging.getLogger(__name__)


class URNParser:
    """Handles parsing of DataHub URNs to extract database components."""

    @staticmethod
    def parse_dataset_urn(dataset_urn: DatasetUrn) -> Optional[Tuple[str, str, str]]:
        """
        Extract database, schema, and table from a DatasetUrn.

        Returns:
            Tuple of (database, schema, table) or None if parsing fails
        """
        dataset_name = dataset_urn.get_dataset_name()
        parts = dataset_name.split(".")

        if len(parts) < 3:
            logger.warning(f"Could not parse dataset URN: {dataset_name}")
            return None

        return parts[0], parts[1], parts[2]

    @staticmethod
    def parse_field_urn(
        field_urn: SchemaFieldUrn,
    ) -> Optional[Tuple[str, str, str, str]]:
        """
        Extract database, schema, table, and column from a SchemaFieldUrn.

        Returns:
            Tuple of (database, schema, table, column) or None if parsing fails
        """
        parent_urn = Urn.create_from_string(field_urn.parent)

        if not isinstance(parent_urn, DatasetUrn):
            logger.warning(
                f"Parent of schema field is not a dataset: {field_urn.parent}"
            )
            return None

        dataset_info = URNParser.parse_dataset_urn(parent_urn)
        if not dataset_info:
            return None

        database, schema, table = dataset_info
        return database, schema, table, field_urn.field_path


class SQLIdentifierFormatter:
    """Handles formatting and quoting of SQL identifiers."""

    @staticmethod
    def format_identifier(identifier: str) -> str:
        """
        Format a Snowflake identifier with smart quoting.
        Only quotes when necessary (special characters, spaces, reserved words).
        """
        if not identifier:
            return identifier

        # Strip existing quotes to avoid double-quoting
        clean_identifier = identifier.strip('"')

        # Check if quoting is needed
        if SQLIdentifierFormatter._should_quote_identifier(clean_identifier):
            return f'"{clean_identifier}"'

        return clean_identifier

    @staticmethod
    def _should_quote_identifier(identifier: str) -> bool:
        """Determine if an identifier needs to be quoted."""
        if not identifier:
            return False

        # Quote if contains special characters or spaces
        if any(
            char in identifier
            for char in [" ", "-", ".", "/", "\\", "(", ")", "[", "]"]
        ):
            return True

        # Quote if starts with a number
        if identifier[0].isdigit():
            return True

        # Quote if it's a reserved word (basic list)
        reserved_words = {
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TABLE",
            "VIEW",
            "INDEX",
            "DATABASE",
            "SCHEMA",
            "COLUMN",
            "COMMENT",
            "ORDER",
            "GROUP",
            "BY",
            "HAVING",
            "UNION",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "OUTER",
            "ON",
            "AS",
            "AND",
            "OR",
            "NOT",
            "NULL",
            "TRUE",
            "FALSE",
        }

        if identifier.upper() in reserved_words:
            return True

        return False

    @staticmethod
    def escape_description(description: str) -> str:
        """Escape special characters in description for SQL."""
        return description.replace("'", "''").replace("\\", "\\\\")


class ObjectTypeDetector:
    """Handles detection of Snowflake object types (TABLE vs VIEW)."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def detect_object_type(self, database: str, schema: str, table: str) -> str:
        """
        Detect if an object is a TABLE or VIEW by querying Snowflake's information schema.

        Returns:
            'TABLE', 'VIEW', or 'UNKNOWN'
        """
        try:
            formatted_database = SQLIdentifierFormatter.format_identifier(database)

            # Snowflake stores object names in uppercase in information schema
            check_sql = f"""
            SELECT 
                CASE 
                    WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                    WHEN table_type = 'VIEW' THEN 'VIEW'
                    ELSE table_type
                END as object_type
            FROM {formatted_database}.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema = '{schema.upper()}' 
            AND table_name = '{table.upper()}'
            """

            cursor = self.connection.cursor()
            cursor.execute(check_sql)
            result = cursor.fetchone()

            if result:
                return result[0]
            else:
                logger.warning(
                    f"Object not found in information schema: {database}.{schema}.{table}"
                )
                return "UNKNOWN"

        except Exception as e:
            logger.error(
                f"Error detecting object type for {database}.{schema}.{table}: {str(e)}"
            )
            return "UNKNOWN"

    def get_actual_column_name(
        self, database: str, schema: str, table: str, column_name: str
    ) -> Optional[str]:
        """
        Get the actual column name from Snowflake's information schema.
        This handles case sensitivity issues between DataHub and Snowflake.

        Returns:
            The actual column name as stored in Snowflake, or None if not found
        """
        try:
            formatted_database = SQLIdentifierFormatter.format_identifier(database)

            # Query information schema for actual column name
            check_sql = f"""
            SELECT column_name 
            FROM {formatted_database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema = '{schema.upper()}' 
            AND table_name = '{table.upper()}'
            AND UPPER(column_name) = '{column_name.upper()}'
            """

            cursor = self.connection.cursor()
            cursor.execute(check_sql)
            result = cursor.fetchone()

            if result:
                return result[0]
            else:
                logger.warning(
                    f"Column '{column_name}' not found in {database}.{schema}.{table}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Error getting actual column name for {database}.{schema}.{table}.{column_name}: {str(e)}"
            )
            return None

    @staticmethod
    def map_subtype_to_object_type(subtype: str) -> str:
        """Map DataHub subtypes to Snowflake object types."""
        subtype_upper = subtype.upper()
        if subtype_upper in ["VIEW", "MATERIALIZED_VIEW"]:
            return "VIEW"
        elif subtype_upper == "TABLE":
            return "TABLE"
        else:
            logger.warning(f"Unknown subtype '{subtype}', defaulting to TABLE")
            return "TABLE"


class CommentUpdater:
    """Handles the actual execution of comment update SQL statements."""

    def __init__(self, connection: Any, query_executor: Callable[[str], None]) -> None:
        self.connection = connection
        self.query_executor = query_executor

    def update_table_comment(
        self,
        database: str,
        schema: str,
        table: str,
        description: str,
        object_type: str,
    ) -> None:
        """Execute the SQL to update a table or view comment."""
        escaped_description = SQLIdentifierFormatter.escape_description(description)
        formatted_database = SQLIdentifierFormatter.format_identifier(database)
        formatted_schema = SQLIdentifierFormatter.format_identifier(schema)
        formatted_table = SQLIdentifierFormatter.format_identifier(table)

        sql = f"ALTER {object_type} {formatted_database}.{formatted_schema}.{formatted_table} SET COMMENT = '{escaped_description}'"

        self.query_executor(sql)
        logger.info(
            f"Successfully updated {object_type.lower()} comment for {database}.{schema}.{table}"
        )

    def update_column_comment(
        self,
        database: str,
        schema: str,
        table: str,
        column_name: str,
        description: str,
        object_type: str,
    ) -> None:
        """Execute the SQL to update a column comment."""
        escaped_description = SQLIdentifierFormatter.escape_description(description)
        formatted_database = SQLIdentifierFormatter.format_identifier(database)
        formatted_schema = SQLIdentifierFormatter.format_identifier(schema)
        formatted_table = SQLIdentifierFormatter.format_identifier(table)
        formatted_column_name = SQLIdentifierFormatter.format_identifier(column_name)

        # Try to update column comment - different syntax for TABLE vs VIEW
        if object_type == "TABLE":
            sql = f"ALTER TABLE {formatted_database}.{formatted_schema}.{formatted_table} MODIFY COLUMN {formatted_column_name} COMMENT '{escaped_description}'"
        else:  # VIEW
            sql = f"ALTER VIEW {formatted_database}.{formatted_schema}.{formatted_table} MODIFY COLUMN {formatted_column_name} COMMENT '{escaped_description}'"

        try:
            self.query_executor(sql)
            logger.info(
                f"Successfully updated {object_type.lower()} column comment for {database}.{schema}.{table}.{column_name}"
            )
        except Exception as e:
            if object_type == "VIEW":
                # Try alternative syntax for views
                self._try_alternative_view_column_comment(
                    formatted_database,
                    formatted_schema,
                    formatted_table,
                    formatted_column_name,
                    escaped_description,
                    database,
                    schema,
                    table,
                    column_name,
                    e,
                )
            else:
                raise e

    def _try_alternative_view_column_comment(
        self,
        formatted_database: str,
        formatted_schema: str,
        formatted_table: str,
        formatted_column_name: str,
        escaped_description: str,
        database: str,
        schema: str,
        table: str,
        column_name: str,
        original_error: Exception,
    ) -> None:
        """Try alternative COMMENT ON COLUMN syntax for view column comments."""
        try:
            alt_sql = f"COMMENT ON COLUMN {formatted_database}.{formatted_schema}.{formatted_table}.{formatted_column_name} IS '{escaped_description}'"
            self.query_executor(alt_sql)
            logger.info(
                f"Successfully updated view column comment using COMMENT ON COLUMN for {database}.{schema}.{table}.{column_name}"
            )
        except Exception as alt_e:
            logger.warning(
                f"Failed to update view column comment with both syntaxes. ALTER VIEW error: {original_error}, COMMENT ON COLUMN error: {alt_e}"
            )
            raise alt_e

    def try_table_then_view_comment(
        self, database: str, schema: str, table: str, description: str
    ) -> None:
        """Try to update comment as TABLE first, then as VIEW if that fails."""
        try:
            self.update_table_comment(database, schema, table, description, "TABLE")
            logger.info(
                f"Successfully updated TABLE comment for {database}.{schema}.{table}"
            )
        except Exception as table_error:
            try:
                self.update_table_comment(database, schema, table, description, "VIEW")
                logger.info(
                    f"Successfully updated VIEW comment for {database}.{schema}.{table}"
                )
            except Exception as view_error:
                logger.error(
                    f"Both TABLE and VIEW attempts failed for {database}.{schema}.{table}"
                )
                logger.debug(f"TABLE error: {table_error}, VIEW error: {view_error}")
                raise view_error


class SnowflakeCommentManager:
    """
    High-level manager that orchestrates comment updates using the focused classes.
    This replaces the complex apply_description method with a clean, testable interface.
    """

    def __init__(self, connection: Any, query_executor: Callable[[str], None]) -> None:
        self.connection = connection
        self.query_executor = query_executor
        self.object_detector = ObjectTypeDetector(connection)
        self.comment_updater = CommentUpdater(connection, query_executor)

    def apply_description(
        self, entity_urn: str, docs: str, subtype: Optional[str] = None
    ) -> None:
        """
        Apply a description to a Snowflake table or column as a comment.

        Args:
            entity_urn: The URN of the entity (dataset or schema field)
            docs: The description to apply
            subtype: The object subtype (TABLE, VIEW, etc.) - if None, will auto-detect
        """
        logger.debug(f"Applying description to {entity_urn}")

        try:
            parsed_entity_urn = Urn.create_from_string(entity_urn)

            if isinstance(parsed_entity_urn, DatasetUrn):
                self._update_table_comment(parsed_entity_urn, docs, subtype)
            elif isinstance(parsed_entity_urn, SchemaFieldUrn):
                self._update_column_comment(parsed_entity_urn, docs)
            else:
                raise ValueError(
                    f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns. Got: {type(parsed_entity_urn)}"
                )

        except Exception as e:
            logger.error(f"Failed to apply description to {entity_urn}: {str(e)}")
            raise

    def _update_table_comment(
        self, dataset_urn: DatasetUrn, description: str, subtype: Optional[str] = None
    ) -> None:
        """Update a Snowflake table or view comment."""
        logger.debug(f"Updating table comment for: {dataset_urn}")

        try:
            # Parse the dataset URN
            db_info = URNParser.parse_dataset_urn(dataset_urn)
            if not db_info:
                return

            database, schema, table = db_info

            if subtype is None:
                # Auto-detect object type by trying TABLE first, then VIEW
                self.comment_updater.try_table_then_view_comment(
                    database, schema, table, description
                )
            else:
                # Use the provided subtype
                object_type = ObjectTypeDetector.map_subtype_to_object_type(subtype)
                self.comment_updater.update_table_comment(
                    database, schema, table, description, object_type
                )

        except Exception as e:
            logger.error(f"Failed to update table comment: {str(e)}")
            raise

    def _update_column_comment(
        self, field_urn: SchemaFieldUrn, description: str
    ) -> None:
        """Update a Snowflake column comment."""
        logger.debug(f"Updating column comment for: {field_urn}")

        try:
            # Parse the field URN
            db_info = URNParser.parse_field_urn(field_urn)
            if not db_info:
                return

            database, schema, table, column = db_info

            # Detect object type and validate
            object_type = self.object_detector.detect_object_type(
                database, schema, table
            )
            if object_type not in ["TABLE", "VIEW"]:
                logger.warning(
                    f"Unknown object type '{object_type}' for {database}.{schema}.{table}. Skipping column comment update."
                )
                return

            # Get the actual column name to handle case sensitivity
            actual_column_name = self.object_detector.get_actual_column_name(
                database, schema, table, column
            )
            if not actual_column_name:
                logger.warning(
                    f"Column '{column}' not found in {database}.{schema}.{table}. Skipping column comment update."
                )
                return

            # Execute the column comment update
            self.comment_updater.update_column_comment(
                database, schema, table, actual_column_name, description, object_type
            )

        except Exception as e:
            logger.error(f"Failed to update column comment: {str(e)}")
            raise
