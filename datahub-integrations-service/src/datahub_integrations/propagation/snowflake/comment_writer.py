import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnowflakeTable:
    """Represents a Snowflake table or view location."""

    database: str
    schema: str
    table_name: str


@dataclass(frozen=True)
class SnowflakeColumn:
    """Represents a Snowflake column location."""

    table: SnowflakeTable
    column_name: str


class URNParser:
    """Handles parsing of DataHub URNs to extract database components."""

    @staticmethod
    def parse_dataset_urn(dataset_urn: DatasetUrn) -> Optional[SnowflakeTable]:
        """
        Extract database, schema, and table from a DatasetUrn.

        Returns:
            SnowflakeTable or None if parsing fails
        """
        dataset_name = dataset_urn.get_dataset_name()
        parts = dataset_name.split(".")

        if len(parts) < 3:
            logger.warning(f"Could not parse dataset URN: {dataset_name}")
            return None

        return SnowflakeTable(database=parts[0], schema=parts[1], table_name=parts[2])

    @staticmethod
    def parse_field_urn(field_urn: SchemaFieldUrn) -> Optional[SnowflakeColumn]:
        """
        Extract database, schema, table, and column from a SchemaFieldUrn.

        Returns:
            SnowflakeColumn or None if parsing fails
        """
        parent_urn = Urn.create_from_string(field_urn.parent)

        if not isinstance(parent_urn, DatasetUrn):
            logger.warning(
                f"Parent of schema field is not a dataset: {field_urn.parent}"
            )
            return None

        table = URNParser.parse_dataset_urn(parent_urn)
        if not table:
            return None

        return SnowflakeColumn(table=table, column_name=field_urn.field_path)


class SQLIdentifierFormatter:
    """Handles formatting and quoting of SQL identifiers."""

    # Characters that require an identifier to be quoted
    SPECIAL_CHARS_REQUIRING_QUOTES = {" ", "-", ".", "/", "\\", "(", ")", "[", "]"}

    # SQL reserved words that require quoting
    RESERVED_WORDS = {
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
            for char in SQLIdentifierFormatter.SPECIAL_CHARS_REQUIRING_QUOTES
        ):
            return True

        # Quote if starts with a number
        if identifier[0].isdigit():
            return True

        # Quote if it's a reserved word
        if identifier.upper() in SQLIdentifierFormatter.RESERVED_WORDS:
            return True

        return False

    @staticmethod
    def escape_description(description: str) -> str:
        """
        Prepare description for SQL by escaping single quotes.

        In Snowflake SQL, single quotes are escaped by doubling them.

        Args:
            description: The description text to prepare

        Returns:
            Description ready for use in SQL with single quotes
        """
        # In SQL, single quotes are escaped by doubling them: ' becomes ''
        return description.replace("'", "''")


class ObjectTypeDetector:
    """Handles detection of Snowflake object types (TABLE vs VIEW)."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def detect_object_type(self, table: SnowflakeTable) -> str:
        """
        Detect if an object is a TABLE or VIEW by querying Snowflake's information schema.

        Returns:
            'TABLE', 'VIEW', or 'UNKNOWN'
        """
        try:
            formatted_database = SQLIdentifierFormatter.format_identifier(
                table.database
            )

            # Snowflake stores object names in uppercase in information schema
            check_sql = f"""
            SELECT 
                CASE 
                    WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                    WHEN table_type = 'VIEW' THEN 'VIEW'
                    ELSE table_type
                END as object_type
            FROM {formatted_database}.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema = '{table.schema.upper()}' 
            AND table_name = '{table.table_name.upper()}'
            """

            with self.connection.cursor() as cursor:
                cursor.execute(check_sql)
                result = cursor.fetchone()

                if result:
                    return result[0]
                else:
                    logger.warning(
                        f"Object not found in information schema: {table.database}.{table.schema}.{table.table_name}"
                    )
                    return "UNKNOWN"

        except Exception as e:
            logger.error(
                f"Error detecting object type for {table.database}.{table.schema}.{table.table_name}: {str(e)}"
            )
            return "UNKNOWN"

    def get_actual_column_name(self, column: SnowflakeColumn) -> Optional[str]:
        """
        Get the actual column name from Snowflake's information schema.
        This handles case sensitivity issues between DataHub and Snowflake.

        Returns:
            The actual column name as stored in Snowflake, or None if not found
        """
        try:
            formatted_database = SQLIdentifierFormatter.format_identifier(
                column.table.database
            )

            # Query information schema for actual column name
            check_sql = f"""
            SELECT column_name 
            FROM {formatted_database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema = '{column.table.schema.upper()}' 
            AND table_name = '{column.table.table_name.upper()}'
            AND UPPER(column_name) = '{column.column_name.upper()}'
            """

            with self.connection.cursor() as cursor:
                cursor.execute(check_sql)
                result = cursor.fetchone()

                if result:
                    return result[0]
                else:
                    logger.warning(
                        f"Column '{column.column_name}' not found in {column.table.database}.{column.table.schema}.{column.table.table_name}"
                    )
                    return None

        except Exception as e:
            logger.error(
                f"Error getting actual column name for {column.table.database}.{column.table.schema}.{column.table.table_name}.{column.column_name}: {str(e)}"
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
        self, table: SnowflakeTable, description: str, object_type: str
    ) -> None:
        """Execute the SQL to update a table or view comment."""
        escaped_description = SQLIdentifierFormatter.escape_description(description)
        formatted_database = SQLIdentifierFormatter.format_identifier(table.database)
        formatted_schema = SQLIdentifierFormatter.format_identifier(table.schema)
        formatted_table_name = SQLIdentifierFormatter.format_identifier(
            table.table_name
        )

        sql = f"ALTER {object_type} {formatted_database}.{formatted_schema}.{formatted_table_name} SET COMMENT = '{escaped_description}'"

        self.query_executor(sql)
        logger.info(
            f"Successfully updated {object_type.lower()} comment for {table.database}.{table.schema}.{table.table_name}"
        )

    def update_column_comment(
        self, column: SnowflakeColumn, description: str, object_type: str
    ) -> None:
        """Execute the SQL to update a column comment."""
        escaped_description = SQLIdentifierFormatter.escape_description(description)
        formatted_database = SQLIdentifierFormatter.format_identifier(
            column.table.database
        )
        formatted_schema = SQLIdentifierFormatter.format_identifier(column.table.schema)
        formatted_table_name = SQLIdentifierFormatter.format_identifier(
            column.table.table_name
        )
        formatted_column_name = SQLIdentifierFormatter.format_identifier(
            column.column_name
        )

        # Try to update column comment - different syntax for TABLE vs VIEW
        if object_type == "TABLE":
            sql = f"ALTER TABLE {formatted_database}.{formatted_schema}.{formatted_table_name} MODIFY COLUMN {formatted_column_name} COMMENT '{escaped_description}'"
        else:  # VIEW
            sql = f"ALTER VIEW {formatted_database}.{formatted_schema}.{formatted_table_name} MODIFY COLUMN {formatted_column_name} COMMENT '{escaped_description}'"

        try:
            self.query_executor(sql)
            logger.info(
                f"Successfully updated {object_type.lower()} column comment for {column.table.database}.{column.table.schema}.{column.table.table_name}.{column.column_name}"
            )
        except Exception as e:
            if object_type == "VIEW":
                # Try alternative syntax for views
                self._try_alternative_view_column_comment(
                    column, escaped_description, e
                )
            else:
                raise e

    def _try_alternative_view_column_comment(
        self,
        column: SnowflakeColumn,
        escaped_description: str,
        original_error: Exception,
    ) -> None:
        """Try alternative COMMENT ON COLUMN syntax for view column comments."""
        try:
            formatted_database = SQLIdentifierFormatter.format_identifier(
                column.table.database
            )
            formatted_schema = SQLIdentifierFormatter.format_identifier(
                column.table.schema
            )
            formatted_table_name = SQLIdentifierFormatter.format_identifier(
                column.table.table_name
            )
            formatted_column_name = SQLIdentifierFormatter.format_identifier(
                column.column_name
            )

            alt_sql = f"COMMENT ON COLUMN {formatted_database}.{formatted_schema}.{formatted_table_name}.{formatted_column_name} IS '{escaped_description}'"
            self.query_executor(alt_sql)
            logger.info(
                f"Successfully updated view column comment using COMMENT ON COLUMN for {column.table.database}.{column.table.schema}.{column.table.table_name}.{column.column_name}"
            )
        except Exception as alt_e:
            logger.warning(
                f"Failed to update view column comment with both syntaxes. ALTER VIEW error: {original_error}, COMMENT ON COLUMN error: {alt_e}"
            )
            raise alt_e

    def try_table_then_view_comment(
        self, table: SnowflakeTable, description: str
    ) -> None:
        """Try to update comment as TABLE first, then as VIEW if that fails."""
        try:
            self.update_table_comment(table, description, "TABLE")
            logger.info(
                f"Successfully updated TABLE comment for {table.database}.{table.schema}.{table.table_name}"
            )
        except Exception as table_error:
            try:
                self.update_table_comment(table, description, "VIEW")
                logger.info(
                    f"Successfully updated VIEW comment for {table.database}.{table.schema}.{table.table_name}"
                )
            except Exception as view_error:
                logger.error(
                    f"Both TABLE and VIEW attempts failed for {table.database}.{table.schema}.{table.table_name}"
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

        table = None
        try:
            # Parse the dataset URN
            table = URNParser.parse_dataset_urn(dataset_urn)
            if not table:
                return

            if subtype is None:
                # Auto-detect object type by trying TABLE first, then VIEW
                self.comment_updater.try_table_then_view_comment(table, description)
            else:
                # Use the provided subtype
                object_type = ObjectTypeDetector.map_subtype_to_object_type(subtype)
                self.comment_updater.update_table_comment(
                    table, description, object_type
                )

        except Exception as e:
            table_info = (
                f"{table.database}.{table.schema}.{table.table_name}"
                if table
                else "unknown"
            )
            logger.error(f"Failed to update table comment for {table_info}: {str(e)}")
            raise

    def _update_column_comment(
        self, field_urn: SchemaFieldUrn, description: str
    ) -> None:
        """Update a Snowflake column comment."""
        logger.debug(f"Updating column comment for: {field_urn}")

        column = None
        try:
            # Parse the field URN
            column = URNParser.parse_field_urn(field_urn)
            if not column:
                return

            # Detect object type and validate
            object_type = self.object_detector.detect_object_type(column.table)
            if object_type not in ["TABLE", "VIEW"]:
                logger.warning(
                    f"Unknown object type '{object_type}' for {column.table.database}.{column.table.schema}.{column.table.table_name}. Skipping column comment update."
                )
                return

            # Get the actual column name to handle case sensitivity
            actual_column_name = self.object_detector.get_actual_column_name(column)
            if not actual_column_name:
                logger.warning(
                    f"Column '{column.column_name}' not found in {column.table.database}.{column.table.schema}.{column.table.table_name}. Skipping column comment update."
                )
                return

            # Execute the column comment update
            actual_column = SnowflakeColumn(
                table=column.table, column_name=actual_column_name
            )
            self.comment_updater.update_column_comment(
                actual_column, description, object_type
            )

        except Exception as e:
            column_info = (
                f"{column.table.database}.{column.table.schema}.{column.table.table_name}.{column.column_name}"
                if column
                else "unknown"
            )
            logger.error(f"Failed to update column comment for {column_info}: {str(e)}")
            raise
