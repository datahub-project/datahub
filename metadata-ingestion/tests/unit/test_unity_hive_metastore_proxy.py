from typing import Any, Dict, List, Sequence, Tuple
from unittest.mock import MagicMock, patch

import sqlalchemy as sa
from sqlalchemy.engine import Row

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.hive_metastore_proxy import HiveMetastoreProxy
from datahub.ingestion.source.unity.source import UnityCatalogSource


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


def _proxy(rows: Sequence[Row[Any]], report: MagicMock) -> HiveMetastoreProxy:
    proxy = HiveMetastoreProxy.__new__(HiveMetastoreProxy)
    proxy.report = report
    proxy._describe_extended = lambda schema, table: rows  # type: ignore[method-assign,assignment]
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

    info = _proxy(rows, MagicMock())._get_table_info("my_schema", "events")

    assert info["Owner"] == "root"
    assert info["Type"] == "MANAGED"


def test_table_info_without_detailed_section_is_reported():
    report = MagicMock()
    proxy = _proxy(_sa_rows([("col_a", "int", None)]), report)

    assert proxy._get_table_info("my_schema", "events_no_details") == {}
    report.warning.assert_called_once()


def _hive_source(options: Dict[str, Any]) -> MagicMock:
    """Build the source with the Databricks DBAPI patched; return the patched connect."""
    config = UnityCatalogSourceConfig.model_validate(
        {
            "workspace_url": "https://my-workspace.cloud.databricks.com",
            "token": "my-pat",
            "warehouse_id": "my_warehouse",
            "include_hive_metastore": True,
            "options": options,
        }
    )
    with patch("databricks.sql.connect") as connect:
        UnityCatalogSource(PipelineContext(run_id="hive-auth-test"), config)
    return connect


def test_hive_metastore_connection_uses_recipe_auth():
    connect = _hive_source({})

    kwargs = connect.call_args.kwargs
    assert kwargs["http_path"] == "/sql/1.0/warehouses/my_warehouse"
    headers = kwargs["credentials_provider"]()()
    assert headers["Authorization"] == "Bearer my-pat"


def test_hive_metastore_user_connect_args_take_precedence():
    connect = _hive_source(
        {"connect_args": {"http_path": "/sql/1.0/warehouses/user_warehouse"}}
    )

    kwargs = connect.call_args.kwargs
    assert kwargs["http_path"] == "/sql/1.0/warehouses/user_warehouse"
    assert "credentials_provider" in kwargs
