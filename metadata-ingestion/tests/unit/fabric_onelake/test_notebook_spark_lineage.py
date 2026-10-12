"""Lineage recovered from executed Spark physical plans."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from datahub.ingestion.source.fabric.common.urn_generator import make_onelake_urn
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
    extract_spark_io,
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


def test_extract_spark_io_classifies_reads_and_writes() -> None:
    io = extract_spark_io([_PLAN])

    assert io.outputs == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="gold_dim", schema="gold")
    }
    assert io.inputs == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="silver_a", schema="silver"),
        SparkTableRef(workspace_id=WS, item_id=LH, table="silver_b", schema="silver"),
    }


def test_extract_spark_io_ignores_non_onelake_paths() -> None:
    io = extract_spark_io([_PLAN])
    all_tables = {ref.table for ref in io.inputs | io.outputs}
    assert "DummyDeltaTable" not in all_tables


def test_write_target_is_not_also_counted_as_input() -> None:
    # A delta write reads the target's own _delta_log; it must stay output-only.
    plan = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/gold_dim")}], Append

(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/gold/gold_dim/_delta_log/0.json")}]
"""
    io = extract_spark_io([plan])
    assert io.inputs == set()
    assert io.outputs == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="gold_dim", schema="gold")
    }


def test_schemaless_lakehouse_table() -> None:
    plan = f"""(1) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/orders/_delta_log/0.json")}]
"""
    io = extract_spark_io([plan])
    assert io.inputs == {
        SparkTableRef(workspace_id=WS, item_id=LH, table="orders", schema=None)
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


def test_empty_and_blank_plans_yield_nothing() -> None:
    io = extract_spark_io(["", None])  # type: ignore[list-item]
    assert io.is_empty()


def _response(payload: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    return response


def test_list_spark_sessions_filters_by_window_and_parses_fields() -> None:
    client = OneLakeClient.__new__(OneLakeClient)

    def get(endpoint: str, params=None):
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
