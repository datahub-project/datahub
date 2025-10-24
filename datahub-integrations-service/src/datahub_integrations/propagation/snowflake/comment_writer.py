import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from sqlglot import exp

logger = logging.getLogger(__name__)

# Snowflake dialect for SQL generation and parsing
_SNOWFLAKE_DIALECT = "snowflake"


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

        Handles platform instances - if present, the dataset name format is:
        platform_instance.database.schema.table

        If no platform instance:
        database.schema.table

        Returns:
            SnowflakeTable or None if parsing fails
        """
        dataset_name = dataset_urn.get_dataset_name()
        parts = dataset_name.split(".")

        if len(parts) < 3:
            logger.warning(
                f"Could not parse dataset URN: {dataset_name} - expected at least 3 parts, got {len(parts)}"
            )
            return None

        # Always take the last 3 parts: database, schema, table
        # This safely handles platform instances with any number of dots
        # Examples:
        #   "db.schema.table" -> db, schema, table (no platform instance)
        #   "prod.db.schema.table" -> db, schema, table (platform instance: "prod")
        #   "prod.us-west-2.db.schema.table" -> db, schema, table (platform instance: "prod.us-west-2")
        return SnowflakeTable(
            database=parts[-3], schema=parts[-2], table_name=parts[-1]
        )

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
    """Handles formatting and quoting of SQL identifiers using sqlglot."""

    @staticmethod
    def format_identifier(identifier: str) -> str:
        """
        Format a Snowflake identifier with smart quoting using sqlglot.

        sqlglot automatically handles:
        - Reserved keyword detection
        - Special character detection
        - Proper quoting rules for the Snowflake dialect

        Args:
            identifier: The identifier to format

        Returns:
            Properly formatted and quoted identifier
        """
        if not identifier:
            return identifier

        # Use sqlglot to create an identifier expression
        # It will automatically determine if quoting is needed
        identifier_expr = exp.to_identifier(identifier, quoted=None)

        # Generate SQL for Snowflake dialect
        return identifier_expr.sql(dialect=_SNOWFLAKE_DIALECT)


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
            # Build the query using sqlglot with literal values (safely escaped)
            query = (
                exp.select(
                    exp.Case(
                        ifs=[
                            exp.If(
                                this=exp.EQ(
                                    this=exp.column("table_type"),
                                    expression=exp.Literal.string("BASE TABLE"),
                                ),
                                true=exp.Literal.string("TABLE"),
                            ),
                            exp.If(
                                this=exp.EQ(
                                    this=exp.column("table_type"),
                                    expression=exp.Literal.string("VIEW"),
                                ),
                                true=exp.Literal.string("VIEW"),
                            ),
                        ],
                        default=exp.column("table_type"),
                    ).as_("object_type")
                )
                .from_(
                    exp.Table(
                        this=exp.to_identifier("TABLES"),
                        db=exp.to_identifier("INFORMATION_SCHEMA"),
                        catalog=exp.to_identifier(table.database),
                    )
                )
                .where(
                    exp.and_(
                        exp.EQ(
                            this=exp.column("table_schema"),
                            expression=exp.Literal.string(table.schema.upper()),
                        ),
                        exp.EQ(
                            this=exp.column("table_name"),
                            expression=exp.Literal.string(table.table_name.upper()),
                        ),
                    )
                )
            )

            check_sql = query.sql(dialect=_SNOWFLAKE_DIALECT)

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
            # Build the query using sqlglot with literal values (safely escaped)
            query = (
                exp.select(exp.column("column_name"))
                .from_(
                    exp.Table(
                        this=exp.to_identifier("COLUMNS"),
                        db=exp.to_identifier("INFORMATION_SCHEMA"),
                        catalog=exp.to_identifier(column.table.database),
                    )
                )
                .where(
                    exp.and_(
                        exp.EQ(
                            this=exp.column("table_schema"),
                            expression=exp.Literal.string(column.table.schema.upper()),
                        ),
                        exp.EQ(
                            this=exp.column("table_name"),
                            expression=exp.Literal.string(
                                column.table.table_name.upper()
                            ),
                        ),
                        exp.EQ(
                            this=exp.Upper(this=exp.column("column_name")),
                            expression=exp.Literal.string(column.column_name.upper()),
                        ),
                    )
                )
            )

            check_sql = query.sql(dialect=_SNOWFLAKE_DIALECT)

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
        # Build ALTER TABLE/VIEW SET COMMENT statement using sqlglot for identifier formatting
        table_ref = exp.Table(
            this=exp.to_identifier(table.table_name),
            db=exp.to_identifier(table.schema),
            catalog=exp.to_identifier(table.database),
        )

        table_sql = table_ref.sql(dialect=_SNOWFLAKE_DIALECT)
        comment_sql = exp.Literal.string(description).sql(dialect=_SNOWFLAKE_DIALECT)

        # Build the ALTER statement - sqlglot doesn't have a direct expression for this
        sql = f"ALTER {object_type} {table_sql} SET COMMENT = {comment_sql}"

        self.query_executor(sql)
        logger.info(
            f"Successfully updated {object_type.lower()} comment for {table.database}.{table.schema}.{table.table_name}"
        )

    def update_column_comment(
        self, column: SnowflakeColumn, description: str, object_type: str
    ) -> None:
        """Execute the SQL to update a column comment."""
        # Build ALTER TABLE/VIEW MODIFY COLUMN statement using sqlglot
        # Note: We build this as a raw string since sqlglot doesn't have explicit support
        # for MODIFY COLUMN ... COMMENT syntax
        table_ref = exp.Table(
            this=exp.to_identifier(column.table.table_name),
            db=exp.to_identifier(column.table.schema),
            catalog=exp.to_identifier(column.table.database),
        )

        table_sql = table_ref.sql(dialect=_SNOWFLAKE_DIALECT)
        column_sql = exp.to_identifier(column.column_name).sql(
            dialect=_SNOWFLAKE_DIALECT
        )
        comment_sql = exp.Literal.string(description).sql(dialect=_SNOWFLAKE_DIALECT)

        # Different syntax for TABLE vs VIEW
        if object_type == "TABLE":
            sql = f"ALTER TABLE {table_sql} MODIFY COLUMN {column_sql} COMMENT {comment_sql}"
        else:  # VIEW
            sql = f"ALTER VIEW {table_sql} MODIFY COLUMN {column_sql} COMMENT {comment_sql}"

        try:
            self.query_executor(sql)
            logger.info(
                f"Successfully updated {object_type.lower()} column comment for {column.table.database}.{column.table.schema}.{column.table.table_name}.{column.column_name}"
            )
        except Exception as e:
            if object_type == "VIEW":
                # Try alternative syntax for views
                self._try_alternative_view_column_comment(column, description, e)
            else:
                raise e

    def _try_alternative_view_column_comment(
        self,
        column: SnowflakeColumn,
        description: str,
        original_error: Exception,
    ) -> None:
        """Try alternative COMMENT ON COLUMN syntax for view column comments."""
        try:
            # Build COMMENT ON COLUMN statement using sqlglot
            # Build the fully qualified column reference
            table_ref = exp.Table(
                this=exp.to_identifier(column.table.table_name),
                db=exp.to_identifier(column.table.schema),
                catalog=exp.to_identifier(column.table.database),
            )

            column_ref = exp.Column(
                this=exp.to_identifier(column.column_name), table=table_ref
            )

            # Build the parts using sqlglot
            column_sql = column_ref.sql(dialect=_SNOWFLAKE_DIALECT)
            comment_sql = exp.Literal.string(description).sql(
                dialect=_SNOWFLAKE_DIALECT
            )

            # COMMENT ON COLUMN doesn't have explicit sqlglot support, so we construct it
            alt_sql = f"COMMENT ON COLUMN {column_sql} IS {comment_sql}"

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
