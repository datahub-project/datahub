import logging
import os
import re
from collections import deque
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Deque, Dict, List, Optional

if TYPE_CHECKING:
    import snowflake.connector

import cachetools
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import GlossaryTermInfoClass
from datahub.metadata.urns import (
    DatasetUrn,
    GlossaryTermUrn,
    SchemaFieldUrn,
    TagUrn,
    Urn,
)
from datahub_actions.api.action_graph import AcrylDataHubGraph
from sqlalchemy.exc import ProgrammingError

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)

logger: logging.Logger = logging.getLogger(__name__)

MAX_ERRORS_PER_HOUR = int(
    os.getenv("MAX_SNOWFLAKE_ERRORS_PER_HOUR", 15)
)  # To Prevent Locking Out of Snowflake Account.


def is_snowflake_urn(urn: str) -> bool:
    parsed_urn = Urn.create_from_string(urn)
    if isinstance(parsed_urn, SchemaFieldUrn):
        parsed_urn = Urn.create_from_string(parsed_urn.parent)

    return (
        isinstance(parsed_urn, DatasetUrn)
        and parsed_urn.get_data_platform_urn().platform_name == "snowflake"
    )


class SnowflakeTagHelper(Closeable):
    """
    Helper class for applying tags, terms, and descriptions to Snowflake objects.

    Provides methods to interact with Snowflake using native connectors to apply
    metadata changes including tags, glossary terms, and descriptions as comments.
    """

    def __init__(self, config: SnowflakeConnectionConfigPermissive):
        self.config: SnowflakeConnectionConfigPermissive = config

        # Use lazy connection - don't connect during initialization to avoid bootstrap failures
        self._connection: Optional["snowflake.connector.SnowflakeConnection"] = None
        self.error_timestamps: Deque[datetime] = (
            deque()
        )  # To store timestamps of errors
        self.error_threshold = MAX_ERRORS_PER_HOUR  # Max errors per hour before dropping. To prevent getting locked out.

    @property
    def connection(self) -> "snowflake.connector.SnowflakeConnection":
        """Lazy connection property - connects only when needed."""
        if self._connection is None:
            logger.debug("Creating new Snowflake connection")
            self._connection = self._get_native_connection()
        return self._connection

    def _get_native_connection(self) -> "snowflake.connector.SnowflakeConnection":
        """Get native Snowflake connection using the same approach as ingestion service."""
        import snowflake.connector
        from datahub.ingestion.source.snowflake.constants import (
            DEFAULT_SNOWFLAKE_DOMAIN,
        )

        connect_args = self.config.get_connect_args()

        if self.config.authentication_type == "KEY_PAIR_AUTHENTICATOR":
            return snowflake.connector.connect(
                user=self.config.username,
                account=self.config.account_id,
                warehouse=self.config.warehouse,
                role=self.config.role,
                authenticator="KEY_PAIR_AUTHENTICATOR",
                application="acryl_datahub",
                host=f"{self.config.account_id}.{getattr(self.config, 'snowflake_domain', DEFAULT_SNOWFLAKE_DOMAIN)}",
                **connect_args,
            )
        else:
            # Default authenticator
            return snowflake.connector.connect(
                user=self.config.username,
                password=self.config.password.get_secret_value()
                if self.config.password
                else None,
                account=self.config.account_id,
                warehouse=self.config.warehouse,
                role=self.config.role,
                application="acryl_datahub",
                host=f"{self.config.account_id}.{getattr(self.config, 'snowflake_domain', DEFAULT_SNOWFLAKE_DOMAIN)}",
                **connect_args,
            )

    @staticmethod
    def get_term_name_from_id(term_urn: str, graph: AcrylDataHubGraph) -> str:
        term_id = Urn.create_from_string(term_urn).get_entity_id_as_string()
        # needs resolution
        term_info = graph.graph.get_aspect(term_urn, GlossaryTermInfoClass)
        if not term_info or not term_info.name:
            return term_id

        return term_info.name

    @staticmethod
    def get_label_urn_to_tag(label_urn: str, graph: AcrylDataHubGraph) -> str:
        label_urn_parsed = Urn.from_string(label_urn)
        if isinstance(label_urn_parsed, TagUrn):
            return label_urn_parsed.name
        elif isinstance(label_urn_parsed, GlossaryTermUrn):
            # if this looks like a guid, we want to resolve to human friendly names
            term_name = SnowflakeTagHelper.get_term_name_from_id(label_urn, graph)
            if term_name is not None:
                return term_name
            else:
                raise ValueError(f"Invalid tag or term urn {label_urn}")
        else:
            raise Exception(
                f"Unexpected label type: neither tag or term {label_urn_parsed.get_type()}"
            )

    def has_special_chars(self, text: str) -> bool:
        return bool(re.search(r"[^a-zA-Z0-9_]", text))

    def _should_quote_identifier(self, identifier: str) -> bool:
        """Check if a Snowflake identifier needs quoting."""
        clean_identifier = identifier.strip('"')

        # Quote if it contains special characters
        if self.has_special_chars(clean_identifier):
            return True

        # Quote if it's a reserved word
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
            "PRIMARY",
            "KEY",
            "FOREIGN",
            "REFERENCES",
            "CONSTRAINT",
            "UNIQUE",
            "NOT",
            "NULL",
            "DEFAULT",
            "CHECK",
            "GRANT",
            "REVOKE",
            "COMMIT",
            "ROLLBACK",
            "TRANSACTION",
            "BEGIN",
            "END",
        }
        if clean_identifier.upper() in reserved_words:
            return True

        # Quote if it starts with a number
        if clean_identifier and clean_identifier[0].isdigit():
            return True

        return False

    def _format_identifier(self, identifier: str) -> str:
        """Format a Snowflake identifier, adding quotes only when necessary."""
        clean_identifier = identifier.strip('"')

        if self._should_quote_identifier(clean_identifier):
            return f'"{clean_identifier}"'
        else:
            return clean_identifier

    def apply_tag_or_term(
        self, entity_urn: str, tag_or_term_urn: str, graph: AcrylDataHubGraph
    ) -> None:
        if not is_snowflake_urn(entity_urn):
            return
        tag = self.get_label_urn_to_tag(tag_or_term_urn, graph)
        assert tag is not None

        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            dataset_urn = parsed_entity_urn
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )
        database, schema, table = dataset_urn.name.split(".")
        self._create_tag(database, schema, tag, tag_or_term_urn)

        if isinstance(parsed_entity_urn, DatasetUrn):
            query = 'ALTER TABLE {table} SET TAG "{tag}"="{tag_or_term_urn}";'
            if not self.has_special_chars(table):
                try:
                    self._run_query(
                        database,
                        schema,
                        query.format(
                            table=table, tag=tag, tag_or_term_urn=tag_or_term_urn
                        ),
                    )
                    return
                except ValueError as e:
                    # Normally this should not happen as without special characters, the table name should be found. But just in case we try to run the query quoted
                    logger.debug(
                        f"Failed to execute query {query}. Error: {e}. Trying to check if colum and table name exists with different casing.."
                    )
            query_table = self.find_table_name(database, schema, table)
            self._run_query(
                database,
                schema,
                query.format(
                    table=f'"{query_table}"', tag=tag, tag_or_term_urn=tag_or_term_urn
                ),
            )
            return

        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            # Note we currently do NOT support nested columns. This will be a future improvement.
            # For now we should likely log a warning if the table column is multipart
            if len(parsed_entity_urn.field_path.split(".")) > 1:
                logger.warning(
                    f"Failed to resolve column with path {parsed_entity_urn.field_path} to Snowflake column. Nested columns not yet supported! Skipping attempt to remove tag..."
                )
                return None
            query = """
                        ALTER TABLE {table} MODIFY COLUMN {column} SET TAG "{tag}"="{tag_or_term_urn}";
            """
            if not self.has_special_chars(
                parsed_entity_urn.field_path
            ) and not self.has_special_chars(table):
                try:
                    self._run_query(
                        database,
                        schema,
                        query.format(
                            table=table,
                            column=parsed_entity_urn.field_path,
                            tag=tag,
                            tag_or_term_urn=tag_or_term_urn,
                        ),
                    )
                    return
                except ValueError as e:
                    # Normally this should not happen as without special characters, the table name should be found. But just in case we try to run the query quoted
                    logger.debug(
                        f"Failed to execute query {query}. Error: {e}. Trying to check if colum and table name exists quoting.."
                    )

            query_table = self.find_table_name(database, schema, table)
            query_col = self.find_column_name(
                database, schema, table, parsed_entity_urn.field_path
            )
            self._run_query(
                database,
                schema,
                query.format(
                    table=f'"{query_table}"',
                    column=f'"{query_col}"',
                    tag=tag,
                    tag_or_term_urn=tag_or_term_urn,
                ),
            )

    def find_table_name(self, database: str, schema: str, table: str) -> str:
        cols = self._get_columns(
            database,
            schema,
            "SHOW COLUMNS;",
        )
        for table_name in cols.keys():
            if table.upper() == table_name.upper():
                return table_name
        return table

    def find_column_name(
        self, database: str, schema: str, table: str, column: str
    ) -> str:
        table_cols: List[str] = []
        cols = self._get_columns(
            database,
            schema,
            "SHOW COLUMNS;",
        )

        for table_name in cols.keys():
            if table_name.upper() == table.upper():
                table_cols = cols[table_name]
                break

        if table_cols:
            for col in table_cols:
                if col.upper() == column.upper():
                    logger.info(f"Found column `{column}` as `{col}` in table {table}")
                    return col
        return column

    def remove_tag_or_term(
        self, entity_urn: str, tag_urn: str, graph: AcrylDataHubGraph
    ) -> None:
        if not is_snowflake_urn(entity_urn):
            return
        tag = self.get_label_urn_to_tag(tag_urn, graph)
        assert tag is not None

        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            dataset_urn = parsed_entity_urn
            database, schema, table = dataset_urn.name.split(".")
            # Since when removing a tag, it might not exist on Snowflake (just datahub), we need to handle the exception
            # internally to prevent getting locked out of the account.
            query = """
                            BEGIN
                                -- Attempt to remove the tag from the table
                                ALTER TABLE {table} UNSET TAG "{tag}";
                                EXCEPTION
                                WHEN STATEMENT_ERROR THEN
                                    IF (SQLCODE = 2003) THEN
                                        RETURN 'Tag does not exist or unauthorized';
                                    ELSE
                                        RAISE;
                                    END IF;
                            END;
                        """

            if not self.has_special_chars(table):
                try:
                    self._run_query(
                        database,
                        schema,
                        query.format(table=table, tag=tag),
                    )

                    return
                except ValueError as e:
                    logger.debug(
                        f"Failed to execute query {query}. Error: {e}. Trying to check if table name exists with quoting.."
                    )
            query_table = self.find_table_name(database, schema, table)
            self._run_query(
                database,
                schema,
                query.format(table=f'"{query_table}"', tag=tag),
            )

        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            # Note we currently do NOT support nested columns. This will be a future improvement.
            # For now we should likely log a warning if the table column is multipart
            if len(parsed_entity_urn.field_path.split(".")) > 1:
                logger.warning(
                    f"Failed to resolve column with path {parsed_entity_urn.field_path} to Snowflake column. Nested columns not yet supported! Skipping attempt to apply tags..."
                )
                return None
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
            database, schema, table = dataset_urn.name.split(".")
            # Since when removing a tag, it might not exist on Snowflake (just datahub), we need to handle the exception
            # internally to prevent getting locked out of the account.
            query = """
                BEGIN
                    -- Your SQL statement
                    ALTER TABLE {table} MODIFY COLUMN {col} UNSET TAG "{tag}";       
                    EXCEPTION
                        WHEN STATEMENT_ERROR THEN
                            IF (SQLCODE = 2003) THEN
                                RETURN 'Tag does not exist or unauthorized';
                            ELSE
                                RAISE;
                            END IF;
                END;
            """
            if not self.has_special_chars(
                parsed_entity_urn.field_path
            ) and not self.has_special_chars(table):
                try:
                    self._run_query(
                        database,
                        schema,
                        query.format(
                            table=table, col=parsed_entity_urn.field_path, tag=tag
                        ),
                    )
                    return
                except ValueError as e:
                    logger.debug(
                        f"Failed to execute query {query}. Error: {e}. Trying to check if colum and table name exists with quoting.."
                    )
            query_table = self.find_table_name(database, schema, table)

            query_col = self.find_column_name(
                database, schema, table, parsed_entity_urn.field_path
            )

            self._run_query(
                database,
                schema,
                query.format(table=f'"{query_table}"', col=f'"{query_col}"', tag=tag),
            )

        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )

    def _create_tag(
        self, database: str, schema: str, tag_name: str, tag_or_term_urn: str
    ) -> None:
        self._run_query(
            database,
            schema,
            f'CREATE TAG IF NOT EXISTS "{tag_name}" COMMENT = "Replicated Tag {tag_or_term_urn} from DataHub";',
        )

    @cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=60 * 5))
    def _get_columns(
        self, database: str, schema: str, query: str, batch_size: int = 1000
    ) -> Dict[str, List[str]]:
        """
        Execute a Snowflake query with pagination using cursor-based fetching.

        Args:
            database (str): Database name
            schema (str): Schema name
            query (str): SQL query to execute
            batch_size (int): Number of records to fetch per batch

        Returns:
            list: List of query results
        """
        if self._too_many_errors():
            logger.warning(
                f"Too many errors have occurred in the past hour; skipping issuing query to Snowflake to avoid account lockout! {query}"
            )
            return {}

        results: Dict[str, List[str]] = {}
        try:
            # Use native Snowflake connection
            cursor = self.connection.cursor()
            try:
                # Set the database and schema - use smart quoting
                formatted_database = self._format_identifier(database)
                formatted_schema = self._format_identifier(schema)
                cursor.execute(f"USE {formatted_database}.{formatted_schema}")

                # Execute the main query
                cursor.execute(query)

                # Get column names from cursor description
                columns = [col[0] for col in cursor.description]

                # Fetch results in batches
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break

                    # Convert each row to a dictionary with column names
                    named_batch = [
                        dict(zip(columns, row, strict=False)) for row in batch
                    ]
                    for row in named_batch:
                        table_name = row.get("table_name")
                        if not table_name:
                            continue

                        if results.get(table_name) is None:
                            results[table_name] = []

                        column_name = row.get("column_name")
                        if column_name:
                            results[table_name].append(column_name)

                return results
            finally:
                cursor.close()

        except Exception as e:
            logger.exception(
                f"Failed to execute snowflake query: {query}. Error: {e!s}. Total errors: {len(self.error_timestamps)}"
            )
            self._log_error()
            return {}

    def _run_query(self, database: str, schema: str, query: str) -> None:
        # If we hit too many errors in the past 1 hour, then we simply start to drop.
        if self._too_many_errors():
            logger.warning(
                f"Too many errors have occurred in the past hour; skipping issuing query to Snowflake to avoid account lockout! {query}"
            )
            return

        try:
            # Use native Snowflake connection instead of SQLAlchemy
            cursor = self.connection.cursor()

            # Use smart quoting for database and schema names
            formatted_database = self._format_identifier(database)
            formatted_schema = self._format_identifier(schema)
            use_statement = f"USE {formatted_database}.{formatted_schema}"

            cursor.execute(use_statement)
            cursor.execute(query)
            cursor.close()
        except ProgrammingError as e:
            self._log_error()
            logger.error(f"ProgrammingError executing query: {query}. Error: {e}")
            raise ValueError(
                f"Failed to execute snowflake query: {query}. Exception: {e}"
            ) from e
        except Exception as e:
            logger.exception(f"Failed to execute snowflake query: {query}. Error: {e}")
            self._log_error()
            raise

    def _run_query_direct(self, query: str) -> None:
        """
        Execute a query directly without USE statement (for fully qualified queries).
        """
        # If we hit too many errors in the past 1 hour, then we simply start to drop.
        if self._too_many_errors():
            logger.warning(
                f"Too many errors have occurred in the past hour; skipping issuing query to Snowflake to avoid account lockout! {query}"
            )
            return

        try:
            # Use native Snowflake connection instead of SQLAlchemy
            cursor = self.connection.cursor()

            cursor.execute(query)
            cursor.close()
        except ProgrammingError as e:
            self._log_error()
            logger.error(
                f"ProgrammingError executing direct query: {query}. Error: {e}"
            )
            raise ValueError(
                f"Failed to execute snowflake query: {query}. Exception: {e}"
            ) from e
        except Exception as e:
            logger.exception(
                f"Failed to execute snowflake direct query: {query}. Error: {e}"
            )
            self._log_error()
            raise

    def _cleanup_old_errors(self) -> None:
        one_hour_ago = datetime.now() - timedelta(hours=1)
        while self.error_timestamps and self.error_timestamps[0] < one_hour_ago:
            self.error_timestamps.popleft()

    def _log_error(self) -> None:
        self.error_timestamps.append(datetime.now())
        self._cleanup_old_errors()

    def _too_many_errors(self) -> bool:
        self._cleanup_old_errors()
        return len(self.error_timestamps) >= self.error_threshold

    def apply_description(
        self, entity_urn: str, docs: str, subtype: Optional[str] = None
    ) -> None:
        """
        Apply a description to a Snowflake table or column as a comment.

        Args:
            entity_urn: The URN of the entity (dataset or schema field)
            docs: The description to apply
            subtype: The object subtype (TABLE, VIEW, etc.) - if None, will default to TABLE
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
            # Parse the dataset URN to get database, schema, and table info
            db_info = self._parse_dataset_urn(dataset_urn)
            if not db_info:
                return

            database, schema, table = db_info
            escaped_description = self._escape_description(description)

            if subtype is None:
                # Auto-detect object type by trying TABLE first, then VIEW
                self._try_table_then_view_comment(
                    database, schema, table, escaped_description
                )
            else:
                # Use the provided subtype
                object_type = self._map_subtype_to_object_type(subtype)
                self._execute_table_comment_update(
                    database, schema, table, escaped_description, object_type
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
            # Parse the field URN to get database, schema, table, and column info
            db_info = self._parse_field_urn(field_urn)
            if not db_info:
                return

            database, schema, table, column = db_info
            escaped_description = self._escape_description(description)

            # Detect object type and validate
            object_type = self._detect_object_type(database, schema, table)
            if object_type not in ["TABLE", "VIEW"]:
                logger.warning(
                    f"Unknown object type '{object_type}' for {database}.{schema}.{table}. Skipping column comment update."
                )
                return

            # Get the actual column name and format identifiers
            actual_column_name = self._get_actual_column_name(
                database, schema, table, column
            )
            if not actual_column_name:
                logger.warning(
                    f"Column '{column}' not found in {database}.{schema}.{table}. Skipping column comment update."
                )
                return

            # Execute the column comment update
            self._execute_column_comment_update(
                database,
                schema,
                table,
                actual_column_name,
                escaped_description,
                object_type,
            )

        except Exception as e:
            logger.error(f"Failed to update column comment: {str(e)}")
            raise

    def _parse_field_urn(
        self, field_urn: SchemaFieldUrn
    ) -> Optional[tuple[str, str, str, str]]:
        """Extract database, schema, table, and column from a SchemaFieldUrn."""
        parent_urn = Urn.create_from_string(field_urn.parent)

        if not isinstance(parent_urn, DatasetUrn):
            logger.warning(
                f"Parent of schema field is not a dataset: {field_urn.parent}"
            )
            return None

        dataset_name = parent_urn.get_dataset_name()
        parts = dataset_name.split(".")

        if len(parts) < 3:
            logger.warning(f"Could not parse dataset URN: {dataset_name}")
            return None

        return parts[0], parts[1], parts[2], field_urn.field_path

    def _parse_dataset_urn(
        self, dataset_urn: DatasetUrn
    ) -> Optional[tuple[str, str, str]]:
        """Extract database, schema, and table from a DatasetUrn."""
        dataset_name = dataset_urn.get_dataset_name()
        parts = dataset_name.split(".")

        if len(parts) < 3:
            logger.warning(f"Could not parse dataset URN: {dataset_name}")
            return None

        return parts[0], parts[1], parts[2]

    def _escape_description(self, description: str) -> str:
        """Escape special characters in description for SQL."""
        return description.replace("'", "''").replace("\\", "\\\\")

    def _map_subtype_to_object_type(self, subtype: str) -> str:
        """Map DataHub subtypes to Snowflake object types."""
        subtype_upper = subtype.upper()
        if subtype_upper in ["VIEW", "MATERIALIZED_VIEW"]:
            return "VIEW"
        elif subtype_upper == "TABLE":
            return "TABLE"
        else:
            logger.warning(f"Unknown subtype '{subtype}', defaulting to TABLE")
            return "TABLE"

    def _try_table_then_view_comment(
        self, database: str, schema: str, table: str, escaped_description: str
    ) -> None:
        """Try to update comment as TABLE first, then as VIEW if that fails."""
        try:
            self._execute_table_comment_update(
                database, schema, table, escaped_description, "TABLE"
            )
            logger.info(
                f"Successfully updated TABLE comment for {database}.{schema}.{table}"
            )
        except Exception as table_error:
            try:
                self._execute_table_comment_update(
                    database, schema, table, escaped_description, "VIEW"
                )
                logger.info(
                    f"Successfully updated VIEW comment for {database}.{schema}.{table}"
                )
            except Exception as view_error:
                logger.error(
                    f"Both TABLE and VIEW attempts failed for {database}.{schema}.{table}"
                )
                logger.debug(f"TABLE error: {table_error}, VIEW error: {view_error}")
                raise view_error

    def _execute_table_comment_update(
        self,
        database: str,
        schema: str,
        table: str,
        escaped_description: str,
        object_type: str,
    ) -> None:
        """Execute the SQL to update a table or view comment."""
        formatted_database = self._format_identifier(database)
        formatted_schema = self._format_identifier(schema)
        formatted_table = self._format_identifier(table)

        sql = f"ALTER {object_type} {formatted_database}.{formatted_schema}.{formatted_table} SET COMMENT = '{escaped_description}'"
        self._run_query_direct(sql)
        logger.info(
            f"Successfully updated {object_type.lower()} comment for {database}.{schema}.{table}"
        )

    def _execute_column_comment_update(
        self,
        database: str,
        schema: str,
        table: str,
        column_name: str,
        escaped_description: str,
        object_type: str,
    ) -> None:
        """Execute the SQL to update a column comment."""
        # Format identifiers with smart quoting
        formatted_database = self._format_identifier(database)
        formatted_schema = self._format_identifier(schema)
        formatted_table = self._format_identifier(table)
        formatted_column_name = self._format_identifier(column_name)

        # Try to update column comment - different syntax for TABLE vs VIEW
        if object_type == "TABLE":
            sql = f"ALTER TABLE {formatted_database}.{formatted_schema}.{formatted_table} MODIFY COLUMN {formatted_column_name} COMMENT '{escaped_description}'"
        else:  # VIEW
            sql = f"ALTER VIEW {formatted_database}.{formatted_schema}.{formatted_table} MODIFY COLUMN {formatted_column_name} COMMENT '{escaped_description}'"

        try:
            self._run_query_direct(sql)
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
            self._run_query_direct(alt_sql)
            logger.info(
                f"Successfully updated view column comment using COMMENT ON COLUMN for {database}.{schema}.{table}.{column_name}"
            )
        except Exception as alt_e:
            logger.warning(
                f"Failed to update view column comment with both syntaxes. ALTER VIEW error: {original_error}, COMMENT ON COLUMN error: {alt_e}"
            )
            raise alt_e

    def _detect_object_type(self, database: str, schema: str, table: str) -> str:
        """Detect if an object is a TABLE or VIEW by querying Snowflake's information schema."""
        try:
            formatted_database = self._format_identifier(database)

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
            cursor.close()

            if result:
                return result[0]
            else:
                logger.warning(
                    f"Object not found in information schema: {database}.{schema}.{table}"
                )
                return "UNKNOWN"

        except Exception as e:
            logger.warning(
                f"Failed to detect object type for {database}.{schema}.{table}: {str(e)}"
            )
            return "UNKNOWN"

    def _get_actual_column_name(
        self, database: str, schema: str, table: str, column_name: str
    ) -> Optional[str]:
        """Get the actual column name from Snowflake to handle case sensitivity."""
        try:
            formatted_database = self._format_identifier(database)

            # Case-insensitive matching since DataHub might have different case
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
            cursor.close()

            if result:
                return result[0]
            else:
                logger.warning(
                    f"Column '{column_name}' not found in {database}.{schema}.{table}"
                )
                return None

        except Exception as e:
            logger.warning(
                f"Failed to get actual column name for {database}.{schema}.{table}.{column_name}: {str(e)}"
            )
            # Fallback to uppercase if query fails
            return column_name.upper()

    def close(self) -> None:
        if self._connection and not self._connection.is_closed():
            self._connection.close()
        logger.info("SnowflakeTagHelper closed.")
