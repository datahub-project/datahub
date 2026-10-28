from datetime import datetime
from typing import Any, Dict, List, Sequence, Tuple, Union

import sqlalchemy as sa
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

Rows = Sequence[Any]  # Row objects; untyped so both SA 1.x stubs and SA 2 accept it
Responses = Dict[str, Union[Rows, Exception]]

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


def _rows(columns: List[str], values: List[Tuple[Any, ...]]) -> Rows:
    """Real SQLAlchemy Rows with named columns, as _execute_sql returns them."""
    engine = sa.create_engine("sqlite://")
    with engine.connect() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE out (" + ", ".join(f'"{c}" TEXT' for c in columns) + ")"
        )
        placeholders = ", ".join("?" for _ in columns)
        for row in values:
            conn.exec_driver_sql(f"INSERT INTO out VALUES ({placeholders})", row)
        return conn.execute(sa.text("SELECT * FROM out")).fetchall()


def _describe(values: List[Tuple[Any, Any, Any]]) -> Rows:
    return _rows(["col_name", "data_type", "comment"], values)


ORDERS_DESCRIBE = _describe(
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

VIEW_DESCRIBE = _describe(
    [
        ("id", "int", None),
        ("", "", ""),
        ("# Detailed Table Information", "", ""),
        ("Type", "VIEW", ""),
        ("Provider", "not-a-real-format", ""),
    ]
)


def _proxy(responses: Responses) -> Tuple[HiveMetastoreProxy, List[str]]:
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


def test_hive_metastore_tables_yields_views_then_tables_with_parsed_details() -> None:
    proxy, _ = _proxy(
        {
            SHOW_VIEWS: _rows(
                ["namespace", "viewName", "isTemporary", "isMaterialized"],
                [("sales", "v_orders", "false", "false")],
            ),
            # SHOW TABLES also lists views; they must not be yielded twice.
            SHOW_TABLES: _rows(
                ["database", "tableName", "isTemporary"],
                [("sales", "orders", "false"), ("sales", "v_orders", "false")],
            ),
            DESCRIBE_ORDERS: ORDERS_DESCRIBE,
            DESCRIBE_VIEW: VIEW_DESCRIBE,
            SHOW_CREATE_VIEW: _rows(
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


def test_detailed_info_sub_sections_are_flattened_into_properties() -> None:
    # DESCRIBE EXTENDED nests e.g. "Storage Properties" / "Table Properties" as a
    # heading row followed by rows whose col_name is blank.
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
    proxy, _ = _proxy(
        {
            SHOW_VIEWS: _rows(
                ["namespace", "viewName", "isTemporary", "isMaterialized"], []
            ),
            SHOW_TABLES: _rows(
                ["database", "tableName", "isTemporary"], [("sales", "orders", "false")]
            ),
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


def test_listing_failures_are_reported_and_yield_no_tables() -> None:
    proxy, _ = _proxy(
        {
            SHOW_VIEWS: PermissionError("no SELECT on sales"),
            SHOW_TABLES: PermissionError("no SELECT on sales"),
        }
    )

    assert list(proxy.hive_metastore_tables(SCHEMA)) == []
    assert len(proxy.report.warnings) == 2


def test_view_definition_failure_keeps_the_view() -> None:
    proxy, _ = _proxy(
        {
            SHOW_VIEWS: _rows(
                ["namespace", "viewName", "isTemporary", "isMaterialized"],
                [("sales", "v_orders", "false", "false")],
            ),
            SHOW_TABLES: _rows(["database", "tableName", "isTemporary"], []),
            DESCRIBE_VIEW: VIEW_DESCRIBE,
            SHOW_CREATE_VIEW: RuntimeError("SHOW CREATE TABLE not permitted"),
        }
    )

    (view,) = list(proxy.hive_metastore_tables(SCHEMA))

    assert view.view_definition is None
    assert len(proxy.report.warnings) == 1


def _column_stats(values: List[Tuple[str, str]]) -> Rows:
    return _rows(["info_name", "info_value"], values)


def test_table_profile_parses_table_statistics_and_column_stats() -> None:
    proxy, _ = _proxy(
        {
            DESCRIBE_ORDERS: ORDERS_DESCRIBE,
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
            # A column ANALYZE never covered: its stats query fails.
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


def test_table_profile_without_statistics_or_column_stats() -> None:
    proxy, executed = _proxy({DESCRIBE_VIEW: VIEW_DESCRIBE})
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


def test_undescribable_table_has_no_profile() -> None:
    proxy, _ = _proxy({DESCRIBE_ORDERS: RuntimeError("table not found")})

    assert proxy.get_table_profile(ORDERS_REF, include_column_stats=True) is None
    assert len(proxy.report.warnings) >= 1
