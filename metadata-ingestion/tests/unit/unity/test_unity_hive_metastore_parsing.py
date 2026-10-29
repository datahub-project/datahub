"""HiveMetastoreProxy turns SHOW TABLES / SHOW VIEWS / DESCRIBE EXTENDED output
into Table objects and profiles. ``_execute_sql`` is the boundary: it is
replaced by a lookup keyed on the exact SQL the proxy issues, so the tests pin
both the statements and how their rows are parsed."""

from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple, Union

import pytest
from databricks.sdk.service.catalog import ColumnTypeName, DataSourceFormat

from datahub.ingestion.source.unity.hive_metastore_proxy import (
    HIVE_METASTORE,
    HiveMetastoreProxy,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    CustomCatalogType,
    HiveTableType,
    Schema,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from tests.unit.unity.conftest import Rows, sa_rows, warning_messages

Responses = Dict[str, Union[Rows, Exception]]
HiveProxyBuilder = Callable[[Responses], Tuple[HiveMetastoreProxy, List[str]]]

SCHEMA = Schema(
    id=f"{HIVE_METASTORE}.sales",
    name="sales",
    catalog=Catalog(
        id=HIVE_METASTORE,
        name=HIVE_METASTORE,
        comment=None,
        metastore=None,
        owner=None,
        type=CustomCatalogType.HIVE_METASTORE_CATALOG,
    ),
    comment=None,
    owner=None,
)
ORDERS_REF = TableReference(
    metastore=None, catalog=HIVE_METASTORE, schema="sales", table="orders"
)

SHOW_VIEWS = "SHOW VIEWS FROM `sales`"
SHOW_TABLES = "SHOW TABLES FROM `sales`"
DESCRIBE_ORDERS = "DESCRIBE EXTENDED `sales`.`orders`"
DESCRIBE_VIEW = "DESCRIBE EXTENDED `sales`.`v_orders`"
SHOW_CREATE_VIEW = "SHOW CREATE TABLE `sales`.`v_orders`"

VIEW_COLUMNS = ["namespace", "viewName", "isTemporary", "isMaterialized"]
TABLE_COLUMNS = ["database", "tableName", "isTemporary"]


def _describe(values: List[Tuple[Any, Any, Any]]) -> Rows:
    return sa_rows(["col_name", "data_type", "comment"], values)


def _column_stats(values: List[Tuple[str, str]]) -> Rows:
    return sa_rows(["info_name", "info_value"], values)


@pytest.fixture
def orders_describe() -> Rows:
    return _describe(
        [
            ("col_name", "data_type", "comment"),
            ("id", "int", None),
            ("name", "string", "customer name"),
            ("# Partition Information", "", ""),
            ("# col_name", "data_type", "comment"),
            ("dt", "string", None),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Database", "sales", ""),
            ("Table", "orders", ""),
            ("Owner", "root", ""),
            ("Created Time", "Mon Jan 15 10:30:00 UTC 2024", ""),
            ("Type", "EXTERNAL", ""),
            ("Comment", "orders table", ""),
            ("Provider", "delta", ""),
            ("Location", "s3://my-bucket/orders", ""),
            ("Statistics", "1382 bytes, 2 rows", ""),
        ]
    )


@pytest.fixture
def view_describe() -> Rows:
    return _describe(
        [
            ("id", "int", None),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Type", "VIEW", ""),
            ("Provider", "not-a-real-format", ""),
        ]
    )


@pytest.fixture
def hive_proxy() -> HiveProxyBuilder:
    """A HiveMetastoreProxy whose _execute_sql answers from ``responses`` (an
    Exception value is raised) and records every statement it was asked for."""

    def build(responses: Responses) -> Tuple[HiveMetastoreProxy, List[str]]:
        proxy = HiveMetastoreProxy.__new__(HiveMetastoreProxy)
        proxy.report = UnityCatalogReport()
        executed: List[str] = []

        def execute(sql: str) -> Rows:
            executed.append(sql)
            response = responses[sql]
            if isinstance(response, Exception):
                raise response
            return response

        proxy._execute_sql = execute  # type: ignore[method-assign]
        return proxy, executed

    return build


def test_hive_metastore_tables_yields_views_then_tables_with_parsed_details(
    hive_proxy: HiveProxyBuilder, orders_describe: Rows, view_describe: Rows
) -> None:
    """SHOW TABLES lists views too; each view must be yielded once, from the
    SHOW VIEWS pass, and the DESCRIBE EXTENDED detail section must land on
    the typed Table fields rather than only in properties."""
    proxy, _ = hive_proxy(
        {
            SHOW_VIEWS: sa_rows(
                VIEW_COLUMNS, [("sales", "v_orders", "false", "false")]
            ),
            SHOW_TABLES: sa_rows(
                TABLE_COLUMNS,
                [("sales", "orders", "false"), ("sales", "v_orders", "false")],
            ),
            DESCRIBE_ORDERS: orders_describe,
            DESCRIBE_VIEW: view_describe,
            SHOW_CREATE_VIEW: sa_rows(
                ["createtab_stmt"], [("CREATE VIEW v_orders AS SELECT 1",)]
            ),
        }
    )

    view, orders = list(proxy.hive_metastore_tables(SCHEMA))

    assert view.name == "v_orders"
    assert view.table_type == HiveTableType.HIVE_VIEW
    assert view.is_view
    assert view.view_definition == "CREATE VIEW v_orders AS SELECT 1"
    assert view.data_source_format is None

    assert orders.name == "orders"
    assert orders.ref == ORDERS_REF
    assert [(c.name, c.type_name) for c in orders.columns] == [
        ("id", ColumnTypeName.INT),
        ("name", ColumnTypeName.STRING),
    ]
    assert orders.columns[1].comment == "customer name"
    assert orders.table_type == HiveTableType.HIVE_EXTERNAL_TABLE
    assert orders.comment == "orders table"
    assert orders.storage_location == "s3://my-bucket/orders"
    assert orders.data_source_format == DataSourceFormat.DELTA
    assert orders.created_at == datetime(2024, 1, 15, 10, 30)
    assert orders.view_definition is None
    assert orders.properties["Owner"] == "root"
    assert "Comment" not in orders.properties


def test_detailed_info_sub_sections_are_flattened_into_properties(
    hive_proxy: HiveProxyBuilder,
) -> None:
    """DESCRIBE EXTENDED nests e.g. "Storage Properties" as a heading row
    followed by rows whose col_name is blank; those become
    "<heading> <key>" properties and a later heading resets the prefix."""
    describe = _describe(
        [
            ("id", "int", None),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Type", "MANAGED", ""),
            ("Storage Properties", None, None),
            ("", "path", "s3://my-bucket/orders"),
            ("", "compression", "snappy"),
            ("", None, None),
            ("# Partition Information", None, None),
            ("Owner", "root", ""),
        ]
    )
    proxy, _ = hive_proxy(
        {
            SHOW_VIEWS: sa_rows(VIEW_COLUMNS, []),
            SHOW_TABLES: sa_rows(TABLE_COLUMNS, [("sales", "orders", "false")]),
            DESCRIBE_ORDERS: describe,
        }
    )

    (orders,) = list(proxy.hive_metastore_tables(SCHEMA))

    assert orders.table_type == HiveTableType.HIVE_MANAGED_TABLE
    assert orders.properties == {
        "Storage Properties path": "s3://my-bucket/orders",
        "Storage Properties compression": "snappy",
        "Owner": "root",
    }


def test_table_without_a_detailed_section_has_no_table_info(
    hive_proxy: HiveProxyBuilder,
) -> None:
    proxy, _ = hive_proxy({DESCRIBE_ORDERS: _describe([("id", "int", None)])})

    assert proxy._get_table_info("sales", "orders") == {}
    assert warning_messages(proxy.report) == {"Failed to get detailed info for table"}


def test_listing_failures_are_reported_and_yield_no_tables(
    hive_proxy: HiveProxyBuilder,
) -> None:
    proxy, _ = hive_proxy(
        {
            SHOW_VIEWS: PermissionError("no SELECT on sales"),
            SHOW_TABLES: PermissionError("no SELECT on sales"),
        }
    )

    assert list(proxy.hive_metastore_tables(SCHEMA)) == []
    assert warning_messages(proxy.report) == {
        "Failed to get tables for schema",
        "Failed to get views for schema",
    }


def test_view_definition_failure_keeps_the_view(
    hive_proxy: HiveProxyBuilder, view_describe: Rows
) -> None:
    proxy, _ = hive_proxy(
        {
            SHOW_VIEWS: sa_rows(
                VIEW_COLUMNS, [("sales", "v_orders", "false", "false")]
            ),
            SHOW_TABLES: sa_rows(TABLE_COLUMNS, []),
            DESCRIBE_VIEW: view_describe,
            SHOW_CREATE_VIEW: RuntimeError("SHOW CREATE TABLE not permitted"),
        }
    )

    (view,) = list(proxy.hive_metastore_tables(SCHEMA))

    assert view.view_definition is None
    assert warning_messages(proxy.report) == {"Failed to get view definition for table"}


def test_table_profile_parses_table_statistics_and_column_stats(
    hive_proxy: HiveProxyBuilder, orders_describe: Rows
) -> None:
    """Row count and size come from the "Statistics" detail row; a column whose
    stats query fails is simply absent from column_profiles."""
    proxy, _ = hive_proxy(
        {
            DESCRIBE_ORDERS: orders_describe,
            f"{DESCRIBE_ORDERS} id": _column_stats(
                [
                    ("col_name", "id"),
                    ("data_type", "int"),
                    ("min", "1"),
                    ("max", "2"),
                    ("num_nulls", "0"),
                    ("distinct_count", "2"),
                    ("avg_col_len", "4"),
                    ("max_col_len", "4"),
                ]
            ),
            f"{DESCRIBE_ORDERS} name": RuntimeError("no stats"),
        }
    )

    profile = proxy.get_table_profile(ORDERS_REF, include_column_stats=True)

    assert profile is not None
    assert (profile.num_rows, profile.total_size, profile.num_columns) == (
        2,
        1382,
        2,
    )
    (id_profile,) = profile.column_profiles
    assert id_profile.name == "id"
    assert (id_profile.null_count, id_profile.distinct_count) == (0, 2)
    assert (id_profile.min, id_profile.max) == ("1", "2")
    assert (id_profile.avg_len, id_profile.max_len) == ("4", "4")


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "_column_describe_extended interpolates the column name unquoted, so a "
        "column that needs quoting (space, reserved word, dot) is a syntax error "
        "on the warehouse instead of a DESCRIBE of that column"
    ),
)
def test_column_stats_are_requested_with_the_column_name_quoted(
    hive_proxy: HiveProxyBuilder,
) -> None:
    proxy, executed = hive_proxy(
        {
            DESCRIBE_ORDERS: _describe(
                [
                    ("order id", "int", None),
                    ("", "", ""),
                    ("# Detailed Table Information", "", ""),
                    ("Type", "MANAGED", ""),
                ]
            ),
            f"{DESCRIBE_ORDERS} `order id`": _column_stats([("col_name", "order id")]),
        }
    )

    try:
        proxy.get_table_profile(ORDERS_REF, include_column_stats=True)
    except KeyError:
        pass  # the unquoted statement is not one the fake can answer

    assert executed[-1] == f"{DESCRIBE_ORDERS} `order id`"


def test_table_profile_without_statistics_or_column_stats(
    hive_proxy: HiveProxyBuilder, view_describe: Rows
) -> None:
    proxy, executed = hive_proxy({DESCRIBE_VIEW: view_describe})
    ref = TableReference(
        metastore=None, catalog=HIVE_METASTORE, schema="sales", table="v_orders"
    )

    profile = proxy.get_table_profile(ref, include_column_stats=False)

    assert profile is not None
    assert (profile.num_rows, profile.total_size, profile.num_columns) == (
        None,
        None,
        1,
    )
    assert profile.column_profiles == []
    assert executed == [DESCRIBE_VIEW]


def test_undescribable_table_has_no_profile(hive_proxy: HiveProxyBuilder) -> None:
    proxy, _ = hive_proxy({DESCRIBE_ORDERS: RuntimeError("table not found")})

    assert proxy.get_table_profile(ORDERS_REF, include_column_stats=True) is None
    assert warning_messages(proxy.report) == {
        "Failed to get columns for table",
        "Failed to get detailed info for table",
    }
