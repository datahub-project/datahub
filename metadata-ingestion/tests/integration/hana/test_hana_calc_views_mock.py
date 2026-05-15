"""Mock-driven integration test for the HANA calc-view + usage paths.

Stands in for the Docker-based ``test_hana_ingest`` whenever HANA Express
isn't available (e.g. aarch64 CI runners, or when the SAP image gate
prevents the pull). Runs unconditionally and snapshots a golden MCE file.

Scope:
- Calculation-view discovery via mocked ``_SYS_REPO.ACTIVE_OBJECT`` rows
  (one ProjectionView and one SqlScriptView).
- Column- and table-level lineage emission through ``SqlParsingAggregator``.
- Observed-query usage extraction via mocked
  ``_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`` rows.

The SQLAlchemy reflection path is short-circuited (empty inspector) so the
golden stays focused on the HANA-specific code paths; standard table /
view / profiling extraction is covered by the Docker-based test.
"""

import datetime as dt
from typing import Any, Dict, List
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration_batch_5
FROZEN_TIME = "2025-01-15 12:00:00+00:00"

_PROJECTION_VIEW_XML = """\
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="CUSTOMERS" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="CUSTOMERS"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:ProjectionView" id="Projection_1">
      <input node="#CUSTOMERS">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="ID" target="CUST_ID"/>
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="NAME" target="CUST_NAME"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <attributes>
      <attribute id="CUSTOMER_ID">
        <keyMapping columnObjectName="Projection_1" columnName="CUST_ID"/>
      </attribute>
      <attribute id="CUSTOMER_NAME">
        <keyMapping columnObjectName="Projection_1" columnName="CUST_NAME"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""

_SCRIPT_VIEW_XML = """\
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                      calculationScenarioType="SCRIPT_BASED">
  <dataSources/>
  <calculationViews>
    <calculationView xsi:type="Calculation:SqlScriptView" id="Script_View">
      <viewAttributes>
        <viewAttribute id="CUST_ID"/>
        <viewAttribute id="REVENUE"/>
      </viewAttributes>
      <definition>BEGIN
  RESULT_SET = SELECT "ID" AS "CUST_ID", "TOTAL" AS "REVENUE"
               FROM "REPORTING"."SALES"
               JOIN "REPORTING"."CUSTOMERS"
                 ON "SALES"."CUST_ID" = "CUSTOMERS"."ID";
END</definition>
    </calculationView>
  </calculationViews>
  <logicalModel id="Script_View">
    <attributes>
      <attribute id="CUST_ID">
        <keyMapping columnObjectName="Script_View" columnName="CUST_ID"/>
      </attribute>
    </attributes>
    <baseMeasures>
      <measure id="REVENUE">
        <measureMapping columnObjectName="Script_View" columnName="REVENUE"/>
      </measure>
    </baseMeasures>
  </logicalModel>
</Calculation:scenario>
"""


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
                "CDATA": _PROJECTION_VIEW_XML,
            }
        ),
        _make_row(
            {
                "PACKAGE_ID": "acme.scripts",
                "OBJECT_NAME": "SalesScript",
                "CDATA": _SCRIPT_VIEW_XML,
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


def _build_empty_inspector(engine: MagicMock) -> MagicMock:
    """Inspector that reports no schemas / tables / views.

    Keeps the golden focused on calc-view + usage output; the standard
    SQLAlchemy reflection path is exercised by the Docker-based test.
    """
    inspector = MagicMock()
    inspector.engine = engine
    inspector.get_schema_names.return_value = []
    inspector.get_table_names.return_value = []
    inspector.get_view_names.return_value = []
    inspector.get_columns.return_value = []
    inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    inspector.get_foreign_keys.return_value = []
    inspector.get_indexes.return_value = []
    return inspector


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hana_calc_views_and_usage_mock(
    pytestconfig: pytest.Config,
    tmp_path,
    mock_time,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hana"
    output_file = tmp_path / "hana_mock_mces.json"
    golden_file = test_resources_dir / "hana_mock_calc_views_golden.json"

    mock_engine = _build_mock_engine()
    mock_inspector = _build_empty_inspector(mock_engine)

    with (
        mock.patch(
            "datahub.ingestion.source.sql.hana.hana.create_engine",
            return_value=mock_engine,
        ),
        mock.patch(
            "datahub.ingestion.source.sql.sql_common.create_engine",
            return_value=mock_engine,
        ),
        mock.patch(
            "datahub.ingestion.source.sql.sql_common.inspect",
            return_value=mock_inspector,
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
                        "include_stored_procedures": False,
                        "include_query_usage": True,
                        "include_usage_stats": True,
                        "start_time": "2025-01-15T00:00:00+00:00",
                        "end_time": "2025-01-15T23:59:59+00:00",
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

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )
