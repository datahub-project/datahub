from typing import Any, List, Sequence, Tuple
from unittest.mock import MagicMock

import sqlalchemy as sa
from sqlalchemy.engine import Row

from datahub.ingestion.source.unity.hive_metastore_proxy import HiveMetastoreProxy


def _sa_rows(values: List[Tuple[Any, Any, Any]]) -> Sequence[Row[Any]]:
    # Real SA 2.0 Rows (not tuples or databricks Rows), as _execute_sql returns.
    engine = sa.create_engine("sqlite://")
    with engine.connect() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE describe_out (col_name TEXT, data_type TEXT, comment TEXT)"
        )
        for row in values:
            conn.execute(
                sa.text("INSERT INTO describe_out VALUES (:a, :b, :c)"),
                {"a": row[0], "b": row[1], "c": row[2]},
            )
        return conn.execute(sa.text("SELECT * FROM describe_out")).fetchall()


def _proxy(rows: Sequence[Row[Any]]) -> HiveMetastoreProxy:
    proxy = HiveMetastoreProxy.__new__(HiveMetastoreProxy)
    proxy.report = MagicMock()
    proxy._describe_extended = lambda schema, table: rows  # type: ignore[method-assign]
    return proxy


def test_table_info_parses_detailed_section_from_sqlalchemy_rows():
    rows = _sa_rows(
        [
            ("col_a", "int", None),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Owner", "root", ""),
            ("Type", "MANAGED", ""),
        ]
    )

    info = _proxy(rows)._get_table_info("my_schema", "events")

    assert info["Owner"] == "root"
    assert info["Type"] == "MANAGED"


def test_table_info_without_detailed_section_is_reported():
    proxy = _proxy(_sa_rows([("col_a", "int", None)]))

    assert proxy._get_table_info("my_schema", "events_no_details") == {}
    proxy.report.warning.assert_called_once()
