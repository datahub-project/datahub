# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import re
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List

import cachetools
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata._urns.urn_defs import (
    DatasetUrn,
    GlossaryTermUrn,
    SchemaFieldUrn,
    TagUrn,
)
from datahub.metadata.schema_classes import GlossaryTermInfoClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from sqlalchemy import create_engine
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
    def __init__(self, config: SnowflakeConnectionConfigPermissive):
        self.config: SnowflakeConnectionConfigPermissive = config
        url = self.config.get_sql_alchemy_url()
        self.engine = create_engine(url, **self.config.get_options())
        self.error_timestamps: Deque[datetime] = (
            deque()
        )  # To store timestamps of errors
        self.error_threshold = MAX_ERRORS_PER_HOUR  # Max errors per hour before dropping. To prevent getting locked out.

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
            # Get a connection and cursor
            with self.engine.connect() as connection:
                raw_connection = connection.connection
                with raw_connection.cursor() as cursor:
                    # Set the database and schema
                    cursor.execute(f"USE {database}.{schema}")

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
            self.engine.execute(f"USE {database}.{schema};")
            self.engine.execute(query)

            logger.info(f"Successfully executed query {query}")
        except ProgrammingError as e:
            self._log_error()
            raise ValueError(
                f"Failed to execute snowflake query: {query}. Exception: {e}"
            ) from e
        except Exception:
            logger.exception(
                f"Failed to execute snowflake query: {query}. Total errors: {len(self.error_timestamps)}"
            )
            self._log_error()

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

    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
        logger.info("SnowflakeTagHelper closed.")
