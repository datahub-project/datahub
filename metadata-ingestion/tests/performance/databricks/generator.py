import logging
import random
import string
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from typing import Callable, List, TypeVar, Union
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import ColumnTypeName
from performance.data_generation import Distribution, LomaxDistribution, SeedMetadata
from performance.data_model import ColumnType, Container, Table, View
from performance.databricks.unity_proxy_mock import _convert_column_type
from sqlalchemy import create_engine

from datahub.ingestion.source.sql.sql_config import make_sqlalchemy_uri

logger = logging.getLogger(__name__)
T = TypeVar("T")

MAX_WORKERS = 200


class DatabricksDataGenerator:
    def __init__(self, host: str, token: str, warehouse_id: str):
        self.client = WorkspaceClient(host=host, token=token)
        self.warehouse_id = warehouse_id
        url = make_sqlalchemy_uri(
            scheme="databricks",
            username="token",
            password=token,
            at=urlparse(host).netloc,
            db=None,
            uri_opts={"http_path": f"/sql/1.0/warehouses/{warehouse_id}"},
        )
        engine = create_engine(
            url, connect_args={"timeout": 600}, pool_size=MAX_WORKERS
        )
        self.connection = engine.connect()

    def clear_data(self, seed_metadata: SeedMetadata) -> None:
        for container in seed_metadata.containers[0]:
            try:
                self.client.catalogs.delete(container.name, force=True)
            except DatabricksError:
                pass

    def create_data(
        self,
        seed_metadata: SeedMetadata,
        # Percentiles: 1st=0, 10th=7, 25th=21, 50th=58, 75th=152, 90th=364, 99th=2063, 99.99th=46316
        num_rows_distribution: Distribution = LomaxDistribution(scale=100, shape=1.5),
    ) -> None:
        """Create data in Databricks based on SeedMetadata."""
        for container in seed_metadata.containers[0]:
            self._create_catalog(container)
        for container in seed_metadata.containers[1]:
            self._create_schema(container)

        _thread_pool_execute("create tables", seed_metadata.tables, self._create_table)
        _thread_pool_execute("create views", seed_metadata.views, self._create_view)
        _thread_pool_execute(
            "populate tables",
            seed_metadata.tables,
            lambda t: self._populate_table(
                t, num_rows_distribution.sample(ceiling=1_000_000)
            ),
        )
        _thread_pool_execute(
            "create table lineage", seed_metadata.tables, self._create_table_lineage
        )

    def _create_catalog(self, catalog: Container) -> None:
        try:
            self.client.catalogs.get(catalog.name)
        except DatabricksError:
            self.client.catalogs.create(catalog.name)

    def _create_schema(self, schema: Container) -> None:
        try:
            self.client.schemas.get(f"{schema.parent.name}.{schema.name}")
        except DatabricksError:
            self.client.schemas.create(schema.name, schema.parent.name)

    def _create_table(self, table: Table) -> None:
        try:
            self.client.tables.delete(".".join(table.name_components))
        except DatabricksError:
            pass

        columns = ", ".join(
            f"{name} {_convert_column_type(column.type).value}"
            for name, column in table.columns.items()
        )
        self._execute_sql(f"CREATE TABLE {_quote_table(table)} ({columns})")
        self._assert_table_exists(table)

    def _create_view(self, view: View) -> None:
        self._execute_sql(_generate_view_definition(view))
        self._assert_table_exists(view)

    def _assert_table_exists(self, table: Table) -> None:
        self.client.tables.get(".".join(table.name_components))

    def _populate_table(self, table: Table, num_rows: int) -> None:
        values = [
            ", ".join(
                str(_generate_value(column.type)) for column in table.columns.values()
            )
            for _ in range(num_rows)
        ]
        values_str = ", ".join(f"({value})" for value in values)
        self._execute_sql(f"INSERT INTO {_quote_table(table)} VALUES {values_str}")

    def _create_table_lineage(self, table: Table) -> None:
        for upstream in table.upstreams:
            self._execute_sql(_generate_insert_lineage(table, upstream))

    def _execute_sql(self, sql: str) -> None:
        print(sql)
        self.connection.execute(sql)


def _thread_pool_execute(desc: str, lst: List[T], fn: Callable[[T], None]) -> None:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fn, item) for item in lst]
        wait(futures)
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error executing '{desc}': {e}", exc_info=True)


def _generate_value(t: ColumnType) -> Union[int, float, str, bool]:
    ctn = _convert_column_type(t)
    if ctn == ColumnTypeName.INT:
        return random.randint(-(2**31), 2**31 - 1)
    elif ctn == ColumnTypeName.DOUBLE:
        return random.uniform(-(2**31), 2**31 - 1)
    elif ctn == ColumnTypeName.STRING:
        return (
            "'" + "".join(random.choice(string.ascii_letters) for _ in range(8)) + "'"
        )
    elif ctn == ColumnTypeName.BOOLEAN:
        return random.choice([True, False])
    elif ctn == ColumnTypeName.TIMESTAMP:
        return random.randint(0, int(datetime.now().timestamp()))
    else:
        raise NotImplementedError(f"Unsupported type {ctn}")


def _generate_insert_lineage(table: Table, upstream: Table) -> str:
    select = []
    for column in table.columns.values():
        matching_cols = [c for c in upstream.columns.values() if c.type == column.type]
        if matching_cols:
            upstream_col = random.choice(matching_cols)
            select.append(f"{upstream_col.name} AS {column.name}")
        else:
            select.append(f"{_generate_value(column.type)} AS {column.name}")

    return f"INSERT INTO {_quote_table(table)} SELECT {', '.join(select)} FROM {_quote_table(upstream)}"


def _generate_view_definition(view: View) -> str:
    from_statement = f"FROM {_quote_table(view.upstreams[0])} t0"
    join_statement = " ".join(
        f"JOIN {_quote_table(upstream)} t{i+1} ON t0.id = t{i+1}.id"
        for i, upstream in enumerate(view.upstreams[1:])
    )
    return f"CREATE VIEW {_quote_table(view)} AS SELECT * {from_statement} {join_statement} {view.definition}"


def _quote_table(table: Table) -> str:
    return ".".join(f"`{component}`" for component in table.name_components)
