import datetime as dt
import pathlib
from typing import Any, Dict, List, Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.testing import mce_helpers
from tests.integration.hana._xml_fixtures import (
    PROJECTION_VIEW_XML,
    SQL_SCRIPT_VIEW_XML,
)

pytestmark = pytest.mark.integration_batch_5
FROZEN_TIME = "2025-01-15 12:00:00+00:00"


def _make_row(mapping: Dict[str, Any]) -> MagicMock:
    row = MagicMock()
    row._mapping = mapping
    return row


def _calc_view_rows() -> List[MagicMock]:
    return [
        _make_row(
            {
                "PACKAGE_ID": "acme.analytics",
                "OBJECT_NAME": "SalesOverview",
                "CDATA": PROJECTION_VIEW_XML,
            }
        ),
        _make_row(
            {
                "PACKAGE_ID": "acme.scripts",
                "OBJECT_NAME": "SalesScript",
                "CDATA": SQL_SCRIPT_VIEW_XML,
            }
        ),
    ]


def _column_rows_for(view_name: str) -> List[MagicMock]:
    if view_name == "acme.analytics/SalesOverview":
        return [
            _make_row(
                {
                    "COLUMN_NAME": "CUSTOMER_ID",
                    "COMMENTS": None,
                    "DATA_TYPE_NAME": "INTEGER",
                    "IS_NULLABLE": "TRUE",
                    "POSITION": 1,
                    "LENGTH": None,
                    "SCALE": None,
                }
            ),
            _make_row(
                {
                    "COLUMN_NAME": "CUSTOMER_NAME",
                    "COMMENTS": "Customer display name",
                    "DATA_TYPE_NAME": "NVARCHAR",
                    "IS_NULLABLE": "TRUE",
                    "POSITION": 2,
                    "LENGTH": 100,
                    "SCALE": None,
                }
            ),
        ]
    if view_name == "acme.scripts/SalesScript":
        return [
            _make_row(
                {
                    "COLUMN_NAME": "CUST_ID",
                    "COMMENTS": None,
                    "DATA_TYPE_NAME": "INTEGER",
                    "IS_NULLABLE": "TRUE",
                    "POSITION": 1,
                    "LENGTH": None,
                    "SCALE": None,
                }
            ),
            _make_row(
                {
                    "COLUMN_NAME": "REVENUE",
                    "COMMENTS": None,
                    "DATA_TYPE_NAME": "DECIMAL",
                    "IS_NULLABLE": "TRUE",
                    "POSITION": 2,
                    "LENGTH": 15,
                    "SCALE": 2,
                }
            ),
        ]
    return []


def _stored_procedure_rows() -> List[MagicMock]:
    return [
        _make_row(
            {
                "SCHEMA_NAME": "REPORTING",
                "PROCEDURE_NAME": "UPDATE_DAILY_SALES",
                "DEFINITION": (
                    "BEGIN\n"
                    '  INSERT INTO "REPORTING"."DAILY_SALES"\n'
                    '    SELECT * FROM "REPORTING"."SALES" '
                    "WHERE SALE_DATE = CURRENT_DATE;\n"
                    "END"
                ),
                "PROCEDURE_TYPE": "PROCEDURE",
                "CREATE_TIME": dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
                "LANGUAGE": "SQLSCRIPT",
                "ARGUMENT_SIGNATURE": "(IN START_DATE DATE, IN END_DATE DATE)",
            }
        ),
    ]


def _observed_query_rows() -> List[MagicMock]:
    # All timestamps within the [start_time, end_time] window declared
    # in the pipeline config below.
    ts = dt.datetime(2025, 1, 15, 9, 30, 0, tzinfo=dt.timezone.utc)
    return [
        _make_row(
            {
                "STATEMENT_HASH": "h1",
                "STATEMENT_STRING": (
                    'SELECT * FROM "REPORTING"."CUSTOMERS" WHERE "ID" > 100'
                ),
                "USER_NAME": "ALICE",
                "SCHEMA_NAME": "REPORTING",
                "APPLICATION_NAME": "HANA_STUDIO",
                "LAST_EXECUTION_TIMESTAMP": ts,
            }
        ),
        _make_row(
            {
                "STATEMENT_HASH": "h2",
                "STATEMENT_STRING": ('SELECT COUNT(*) FROM "REPORTING"."SALES"'),
                "USER_NAME": "BOB",
                "SCHEMA_NAME": "REPORTING",
                "APPLICATION_NAME": "ETL_JOB",
                "LAST_EXECUTION_TIMESTAMP": ts + dt.timedelta(minutes=5),
            }
        ),
    ]


def _build_mock_engine() -> MagicMock:
    """Engine whose `.connect()` returns a connection dispatching on SQL text."""

    def execute(stmt, params=None):
        text = str(stmt).lower()
        result = MagicMock()
        if "_sys_repo.active_object" in text:
            rows = _calc_view_rows()
            result.__iter__ = lambda self: iter(rows)
            result.all.return_value = rows
        elif "sys.view_columns" in text:
            assert params is not None
            rows = _column_rows_for(params["view_name"])
            result.all.return_value = rows
        elif "_sys_statistics.host_sql_plan_cache" in text:
            rows = _observed_query_rows()
            result.all.return_value = rows
        elif "sys.procedures" in text:
            rows = _stored_procedure_rows()
            result.all.return_value = rows
        else:
            raise AssertionError(f"Unexpected query: {stmt}")
        return result

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn.__enter__ = lambda self: conn
    conn.__exit__ = lambda self, *args: None

    engine = MagicMock()
    engine.connect.return_value = conn
    engine.url.database = None
    engine.dispose = MagicMock()
    return engine


def _build_empty_inspector(
    engine: MagicMock, *, schemas: Optional[List[str]] = None
) -> MagicMock:
    """Inspector reporting no tables/views; ``schemas`` controls which
    schemas the procedure-discovery loop iterates.

    The standard SQLAlchemy reflection path (tables / views / columns) is
    exercised by the Docker-based test; here we keep the golden focused
    on calc-view + usage + stored-procedures output.
    """
    inspector = MagicMock()
    inspector.engine = engine
    inspector.get_schema_names.return_value = schemas or []
    inspector.get_table_names.return_value = []
    inspector.get_view_names.return_value = []
    inspector.get_columns.return_value = []
    inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    inspector.get_foreign_keys.return_value = []
    inspector.get_indexes.return_value = []
    return inspector


def _run_pipeline(
    *,
    tmp_path: pathlib.Path,
    output_filename: str,
    inspector: MagicMock,
    engine: MagicMock,
    extra_config: Dict[str, Any],
) -> pathlib.Path:
    output_file = tmp_path / output_filename
    with (
        mock.patch(
            "datahub.ingestion.source.sql.hana.hana.create_engine",
            return_value=engine,
        ),
        mock.patch(
            "datahub.ingestion.source.sql.sql_common.create_engine",
            return_value=engine,
        ),
        mock.patch(
            "datahub.ingestion.source.sql.sql_common.inspect",
            return_value=inspector,
        ),
    ):
        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="hana",
                    config={
                        "username": "user",
                        "password": "pwd",
                        "host_port": "localhost:39041",
                        "scheme": "hana+hdbcli",
                        "include_calculation_views": True,
                        **extra_config,
                    },
                ),
                sink={
                    "type": "file",
                    "config": {"filename": str(output_file)},
                },
            )
        )
        pipeline.run()
        pipeline.raise_from_status()
    return output_file


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hana_calc_views_and_usage_mock(
    pytestconfig: pytest.Config,
    tmp_path: pathlib.Path,
) -> None:
    # NOTE: do NOT add ``mock_time`` from the global conftest fixture — it
    # invokes ``time_machine.travel`` to a different epoch and nesting two
    # travel contexts produces undefined behaviour. Use the decorator above
    # as the single source of frozen time for this test.
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hana"
    golden_file = test_resources_dir / "hana_mock_calc_views_golden.json"

    engine = _build_mock_engine()
    inspector = _build_empty_inspector(engine)

    output_file = _run_pipeline(
        tmp_path=tmp_path,
        output_filename="hana_mock_mces.json",
        inspector=inspector,
        engine=engine,
        extra_config={
            "include_stored_procedures": False,
            "include_query_usage": True,
            "include_usage_stats": True,
            "start_time": "2025-01-15T00:00:00+00:00",
            "end_time": "2025-01-15T23:59:59+00:00",
        },
    )

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hana_stored_procedures_mock(
    pytestconfig: pytest.Config,
    tmp_path: pathlib.Path,
) -> None:
    """Cover the ``include_stored_procedures=True`` (default) path that
    the calc-view + usage test deliberately keeps off."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hana"
    golden_file = test_resources_dir / "hana_mock_stored_procedures_golden.json"

    engine = _build_mock_engine()
    # ``REPORTING`` is the only schema we serve stored-procedure rows for;
    # the empty inspector reports no tables/views so the golden stays
    # focused on the procedure path.
    inspector = _build_empty_inspector(engine, schemas=["REPORTING"])

    output_file = _run_pipeline(
        tmp_path=tmp_path,
        output_filename="hana_mock_stored_procedures.json",
        inspector=inspector,
        engine=engine,
        extra_config={
            "include_stored_procedures": True,
            "include_query_usage": False,
            "include_usage_stats": False,
        },
    )

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )
