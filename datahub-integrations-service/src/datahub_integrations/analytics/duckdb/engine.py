import os
import pathlib
import re
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import duckdb
from jinja2 import Template
from loguru import logger
from pydantic.main import BaseModel

from datahub_integrations.analytics.config import AnalyticsConfig
from datahub_integrations.analytics.engine import (
    AggregationSpec,
    AnalyticsEngine,
    DataFormat,
    Predicate,
    Row,
    RowType,
)


class DuckDBEngineConfig(BaseModel):
    """Configuration for the DuckDB engine"""

    local_cache_path: pathlib.Path = AnalyticsConfig.get_instance().cache_dir
    memory_backed: bool = False


class DuckDBAnalyticsEngine(AnalyticsEngine):
    write_lock = threading.Lock()

    def load_extensions(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        Extend this method to load any required extensions.
        Extensions should be installed as part of the Dockerfile, and should not be installed at runtime.
        This method will be called every time a new connection is created.
        """
        pass

    def get_location_and_credential_query_fragment(
        self, params: Dict[str, Any]
    ) -> Tuple[str, str]:
        raise NotImplementedError(
            "get_location_and_credential_query_fragment not implemented"
        )

    def __init__(self, config: DuckDBEngineConfig) -> None:
        self.config = config
        # ensure the local cache path exists
        os.makedirs(self.config.local_cache_path, exist_ok=True)
        self.fpath = self.config.local_cache_path / "data.db"
        self.base_connection = duckdb.connect(
            str(self.fpath), read_only=False, config={}
        )
        self.load_extensions(self.base_connection)

    def close(self) -> None:
        self.base_connection.close()
        # remove the temp dir and all its contents
        temp_dir = self.config.local_cache_path
        for f in temp_dir.iterdir():
            f.unlink()
        temp_dir.rmdir()

    def __exit__(self) -> None:
        self.close()

    @classmethod
    def create(cls, config: DuckDBEngineConfig) -> "DuckDBAnalyticsEngine":
        return cls(config)

    @property
    def duckdb_client(self) -> duckdb.DuckDBPyConnection:
        """
        Organized as a property to allow for multi-threaded execution
        """
        con = self.base_connection.cursor()
        self.load_extensions(con)
        return con

    def get_schema(self, params: Dict[str, Any]) -> Dict[str, str]:
        """Return the schema of the data at the physical location"""

        (
            location,
            credentials_fragment,
        ) = self.get_location_and_credential_query_fragment(params)

        query = f"{credentials_fragment} SELECT * from '{location}' LIMIT 0"
        logger.info(f"Executing query: {query}")
        result = self.duckdb_client.execute(query)
        return {x[0]: x[1] for x in result.description} if result.description else {}  # type: ignore

    def _get_rows_from_result(self, result: duckdb.DuckDBPyConnection) -> Iterator[Row]:
        schema: List[Optional[str]] = (
            [str(x[0]) for x in result.description] if result.description else []
        )
        logger.info(schema)
        yield Row(type=RowType.HEADER, values=schema)
        for row in result.fetchall():
            yield Row(
                type=RowType.DATA,
                values=[str(col) if col is not None else None for col in row],
            )

    def get_preview(
        self, params: Dict[str, Any], limit: int, format: DataFormat
    ) -> Iterator[Row]:
        (
            location,
            credential_query_fragment,
        ) = self.get_location_and_credential_query_fragment(params)
        # if this is a parquet file, use duckdb parquet reader
        query = f"{credential_query_fragment} SELECT * from '{location}' USING SAMPLE {limit}"
        logger.info(f"Executing query: {query}")
        result = self.duckdb_client.execute(query)
        yield from self._get_rows_from_result(result)

    # Function to check if the table exists
    def _table_exists(self, table_name: str) -> bool:
        query = f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        result = self.duckdb_client.execute(query).fetchone()
        if not result:
            return False
        return result[0] > 0

    # Function to create a table from a Parquet file if it does not exist
    def create_table_from_parquet_if_not_exists(
        self, table_name: str, remote_location: str, credential_query_fragment: str
    ) -> None:
        start_time = time.time()
        with self.write_lock:
            logger.info(f"Checking for existence of table {table_name}")
            if not self._table_exists(table_name):
                logger.info(f"Table {table_name} does not exist.")
                # Use the DuckDB read_parquet function to create a table directly from the Parquet file
                con = self.duckdb_client
                # con.execute(credential_query_fragment)
                create_query = f"{credential_query_fragment} CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{remote_location}')"
                logger.info(f"Executing query: {create_query}")
                con.execute(create_query)
                print(f"Table {table_name} created.")
            else:
                print(f"Table {table_name} already exists.")
        end_time = time.time()
        print(f"Time taken to create table: {end_time - start_time} seconds")

    def create_table_name_from_remote_url(self, remote_url: str) -> str:
        # Extract the bucket and key from the remote URL
        if remote_url.startswith("s3://"):
            s3_url_base = remote_url.replace("s3://", "")
            bucket, key = s3_url_base.split("/", 1)
            logger.info(f"Bucket: {bucket}")
            logger.info(f"Key: {key}")
            # Create a table name from the bucket and key
            table_name = f"{bucket.replace('-', '_')}_{key.replace('/', '_').replace('.', '_').replace('-', '_').replace('*', '_')}"
        else:
            raise ValueError("Only S3 URLs are supported")
        logger.info(f"Table name: {table_name}")
        return table_name

    def check_sql_injection(self, query: str) -> bool:
        # Pattern to match words followed by potentially dangerous operations
        # This regex looks for SQL keywords followed by parentheses, semicolons, or SQL comment syntax (--)
        # It's a basic and not foolproof detection method.
        patterns = [
            r"\b(drop|delete|update|insert|alter|create)\b\s*(--|;|\(|')",
        ]

        # Check if any of the patterns match the query
        if any(re.search(pattern, query, re.IGNORECASE) for pattern in patterns):
            raise ValueError("Potential SQL injection detected")

        return False

    def is_remote(self, location: str) -> bool:
        return not (
            location.startswith("/") or location.startswith("file://")
        )  # TODO: Add more remote locations

    def get_data(
        self,
        params: Dict[str, Any],
        format: DataFormat,
        project: Optional[List[str]],
        filter: Optional[Predicate],
        aggregation: Optional[AggregationSpec],
        sql_query_fragment: Optional[str],
    ) -> Iterator[Row]:
        """Return the data at the physical location"""

        def remove_whitespace(query: str) -> str:
            # Remove leading/trailing whitespace
            query = query.strip()
            # Remove extra whitespace within the string
            query = re.sub(r"\s+", " ", query)
            return query

        (
            location,
            credential_query_fragment,
        ) = self.get_location_and_credential_query_fragment(params)

        logger.info(f"Location: {location}")

        if aggregation and project:
            raise ValueError("Aggregation and project cannot be used together")

        table_name = location

        if not sql_query_fragment:
            logger.info("No SQL query fragment provided")
            query = self.create_query_from_structured_params(
                project, filter, aggregation, table_name
            )
        else:
            # first sanitize the sql_query_fragment to prevent SQL injection
            self.check_sql_injection(sql_query_fragment)
            template = Template(sql_query_fragment)
            query = template.render(table=table_name)

        query_with_creds = f"{credential_query_fragment} {query}"
        query_with_creds = remove_whitespace(query_with_creds)
        logger.info(f"Executing query: {query_with_creds}")
        try:
            result = self.duckdb_client.execute(query_with_creds)
            yield from self._get_rows_from_result(result)
        except Exception as e:
            logger.error(f"Error executing query: {query_with_creds}", e)
            yield Row(type=RowType.ERROR, values=[str(e)])

    def create_query_from_structured_params(
        self,
        project: Optional[list[str]],
        filter: Optional[Predicate],
        aggregation: Optional[AggregationSpec],
        table_name: str,
    ) -> str:
        projected_columns = []
        if project:
            projected_columns = project
        if aggregation and aggregation.aggregations:
            aggregation_cols = [
                (
                    f"{agg.type}({agg.column})"
                    if not agg.alias
                    else f"{agg.type}({agg.column}) AS {agg.alias}"
                )
                for agg in aggregation.aggregations
            ]
            if aggregation.groupBy:
                projected_agg_columns = aggregation.groupBy + aggregation_cols
                projected_columns = projected_agg_columns

        if projected_columns:
            project_str = ", ".join(projected_columns)
        else:
            project_str = "*"

        query = f"SELECT {project_str} from {table_name} "

        if filter and filter.andFilters:
            filter_str = "WHERE " + " AND ".join(
                [f"{f.column} {f.condition} '{f.value}'" for f in filter.andFilters]
            )
            query += filter_str

        if aggregation and aggregation.groupBy:
            group_by_str = ", ".join(aggregation.groupBy)
            query = query + f" GROUP BY {group_by_str}"
            query = query + f" ORDER BY {group_by_str}"
        return query
