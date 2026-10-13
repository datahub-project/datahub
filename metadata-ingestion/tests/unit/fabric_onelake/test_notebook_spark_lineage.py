"""Lineage recovered from executed Spark physical plans."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from datahub.ingestion.source.fabric.common.urn_generator import make_onelake_urn
from datahub.ingestion.source.fabric.onelake.client import (
    _SPARK_SQL_PAGE_SIZE,
    OneLakeClient,
)
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
    extract_spark_lineage,
)

WS = "11111111-1111-1111-1111-111111111111"
LH = "22222222-2222-2222-2222-222222222222"


def _onelake(path: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH}/{path}"


# A write (SaveIntoDataSourceCommand exposes the sink via path=) that reads two
# silver tables, plus an internal Spark warmup path on the capacity's own
# storage host that must be ignored.
_PLAN = f"""== Physical Plan ==
Execute SaveIntoDataSourceCommand (1)
   +- Project (4)

(2) SaveIntoDataSourceCommand
Arguments: DeltaDataSource, [overwriteschema=true, path={_onelake("Tables/gold/gold_dim")}], Overwrite

(5) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/silver_a/_delta_log/00000000000000000000.json")}]

(6) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/silver_b/_delta_log/00000000000000000000.json")}]

(7) Scan parquet
Location: abfss://9a80d3dc@spark2triprodne.dfs.core.windows.net/app-logs/lakehouse-warmup/DummyDeltaTable/_delta_log
"""


_GOLD_DIM = SparkTableRef(workspace_id=WS, item_id=LH, table="gold_dim", schema="gold")


def test_extract_spark_lineage_maps_output_to_its_inputs() -> None:
    lineage = extract_spark_lineage([_PLAN])

    assert set(lineage) == {_GOLD_DIM}
    assert lineage[_GOLD_DIM] == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="silver_a", schema="silver"),
        SparkTableRef(workspace_id=WS, item_id=LH, table="silver_b", schema="silver"),
    }


def test_extract_spark_lineage_ignores_non_onelake_paths() -> None:
    lineage = extract_spark_lineage([_PLAN])
    all_tables = {ref.table for inputs in lineage.values() for ref in inputs} | {
        ref.table for ref in lineage
    }
    assert "DummyDeltaTable" not in all_tables


def test_sequential_pipelines_do_not_cross_contaminate() -> None:
    # Two self-contained executions: X<-A then Y<-B. The result must be precise,
    # not the cross-product {A,B} -> {X,Y} a whole-run union would produce. The
    # reset-after-write rule keeps them separate even in one run.
    plan_x = f"""(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/x")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/a/_delta_log/0.json")}]
"""
    plan_y = f"""(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/y")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/b/_delta_log/0.json")}]
"""
    lineage = extract_spark_lineage([plan_x, plan_y])

    assert lineage == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="x", schema="gold"): {
            SparkTableRef(workspace_id=WS, item_id=LH, table="a", schema="silver")
        },
        SparkTableRef(workspace_id=WS, item_id=LH, table="y", schema="gold"): {
            SparkTableRef(workspace_id=WS, item_id=LH, table="b", schema="silver")
        },
    }


def test_read_in_earlier_execution_attaches_to_later_write() -> None:
    # The real-world pattern: read A in one execution, then write X in a later
    # execution whose plan references a cached relation, not A's path. Strict
    # per-execution grouping would drop A -> X; order-aware attribution recovers
    # it from the preceding read.
    plan_read = f"""(1) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/a/_delta_log/0.json")}]
"""
    plan_write = f"""(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/x")}], Overwrite
(2) InMemoryTableScan
"""
    lineage = extract_spark_lineage([plan_read, plan_write])

    assert lineage == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="x", schema="gold"): {
            SparkTableRef(workspace_id=WS, item_id=LH, table="a", schema="silver")
        },
    }


def test_write_consumes_preceding_reads_then_resets() -> None:
    # read A -> write X -> read B -> write Y. X must get A only and Y must get B
    # only; the reset after X's write stops A leaking into Y.
    plan_read_a = f"""(1) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/a/_delta_log/0.json")}]
"""
    plan_write_x = f"""(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/x")}], Overwrite
"""
    plan_read_b = f"""(1) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/b/_delta_log/0.json")}]
"""
    plan_write_y = f"""(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/y")}], Overwrite
"""
    lineage = extract_spark_lineage(
        [plan_read_a, plan_write_x, plan_read_b, plan_write_y]
    )

    assert lineage == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="x", schema="gold"): {
            SparkTableRef(workspace_id=WS, item_id=LH, table="a", schema="silver")
        },
        SparkTableRef(workspace_id=WS, item_id=LH, table="y", schema="gold"): {
            SparkTableRef(workspace_id=WS, item_id=LH, table="b", schema="silver")
        },
    }


def test_write_target_is_not_also_counted_as_input() -> None:
    # A delta write reads the target's own _delta_log; it must stay output-only.
    plan = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/gold_dim")}], Append

(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/gold/gold_dim/_delta_log/0.json")}]
"""
    assert extract_spark_lineage([plan]) == {_GOLD_DIM: set()}


def test_read_only_execution_yields_no_lineage() -> None:
    # No write command -> no transformation to model.
    plan = f"""(1) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/a/_delta_log/0.json")}]
"""
    assert extract_spark_lineage([plan]) == {}


def test_schemaless_lakehouse_table_output() -> None:
    plan = f"""(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/orders")}], Overwrite
"""
    assert extract_spark_lineage([plan]) == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="orders", schema=None): set()
    }


def test_ref_to_urn_matches_connector_dataset_urn() -> None:
    ref = SparkTableRef(workspace_id=WS, item_id=LH, table="gold_dim", schema="gold")
    assert ref.to_urn(env="PROD", platform_instance=None) == make_onelake_urn(
        workspace_id=WS,
        item_id=LH,
        table_name="gold_dim",
        schema_name="gold",
        env="PROD",
        platform_instance=None,
    )


def test_ref_to_urn_applies_name_normalization() -> None:
    # convert_urns_to_lowercase: a plan's `dim_User` must normalize to the same
    # URN the source registered for the ingested `dim_user` table.
    ref = SparkTableRef(workspace_id=WS, item_id=LH, table="dim_User", schema="Gold")
    assert ref.to_urn("PROD", None, str.lower) == make_onelake_urn(
        workspace_id=WS,
        item_id=LH,
        table_name="dim_user",
        schema_name="gold",
        env="PROD",
        platform_instance=None,
    )


def test_empty_and_blank_plans_yield_nothing() -> None:
    assert extract_spark_lineage(["", None]) == {}  # type: ignore[list-item]


def _response(payload: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_list_spark_sessions_filters_by_window_and_parses_fields() -> None:
    client = OneLakeClient.__new__(OneLakeClient)

    def get(endpoint: str, params: object = None) -> MagicMock:
        return _response(
            {
                "value": [
                    {
                        "sparkApplicationId": "app-recent",
                        "livyId": "livy-recent",
                        "state": "Succeeded",
                        "startDateTime": "2026-10-12T07:00:00Z",
                        "endDateTime": "2026-10-12T07:40:00Z",
                        "item": {"itemId": "nb-1"},
                    },
                    {
                        "sparkApplicationId": "app-old",
                        "livyId": "livy-old",
                        "startDateTime": "2026-01-01T00:00:00Z",
                    },
                    {"state": "Succeeded"},  # no app/livy id -> skipped
                ]
            }
        )

    client.get = get  # type: ignore[method-assign]

    sessions = client.list_notebook_spark_sessions(
        "ws-1", "nb-1", datetime(2026, 10, 1, tzinfo=timezone.utc)
    )

    assert [s.application_id for s in sessions] == ["app-recent"]
    assert sessions[0].livy_id == "livy-recent"
    assert sessions[0].item_id == "nb-1"
    assert sessions[0].state == "Succeeded"


def test_get_spark_sql_plans_extracts_plan_descriptions() -> None:
    client = OneLakeClient.__new__(OneLakeClient)
    client.get = MagicMock(  # type: ignore[method-assign]
        return_value=_response(
            [
                {"id": 0, "planDescription": "== Physical Plan ==\nA"},
                {"id": 1},  # no planDescription -> skipped
                {"id": 2, "planDescription": "== Physical Plan ==\nB"},
            ]
        )
    )

    plans = client.get_spark_sql_plans("ws-1", "nb-1", "livy-1", "app-1")

    assert plans == ["== Physical Plan ==\nA", "== Physical Plan ==\nB"]


def test_get_spark_sql_plans_orders_by_execution_id() -> None:
    # The SQL API returns executions newest-first; order-aware lineage needs them
    # chronological, so the client must sort by execution id ascending.
    client = OneLakeClient.__new__(OneLakeClient)
    client.get = MagicMock(  # type: ignore[method-assign]
        return_value=_response(
            [
                {"id": 2, "planDescription": "write"},
                {"id": 0, "planDescription": "read-a"},
                {"id": 1, "planDescription": "read-b"},
            ]
        )
    )

    plans = client.get_spark_sql_plans("ws-1", "nb-1", "livy-1", "app-1")

    assert plans == ["read-a", "read-b", "write"]


def test_get_spark_sql_plans_pages_past_the_default_limit() -> None:
    # The SQL endpoint returns a bare array capped at 20 by default; late write
    # executions only appear once paging advances past the first page. A full
    # page must trigger another fetch; a short page ends it.
    full_page = [
        {"id": i, "planDescription": f"page0-{i}"} for i in range(_SPARK_SQL_PAGE_SIZE)
    ]
    short_page = [{"id": 999, "planDescription": "page1-write"}]
    client = OneLakeClient.__new__(OneLakeClient)
    client.get = MagicMock(  # type: ignore[method-assign]
        side_effect=[_response(full_page), _response(short_page)]
    )

    plans = client.get_spark_sql_plans("ws-1", "nb-1", "livy-1", "app-1")

    assert len(plans) == _SPARK_SQL_PAGE_SIZE + 1
    assert plans[-1] == "page1-write"
    offsets = [call.kwargs["params"]["offset"] for call in client.get.call_args_list]
    assert offsets == ["0", str(_SPARK_SQL_PAGE_SIZE)]
