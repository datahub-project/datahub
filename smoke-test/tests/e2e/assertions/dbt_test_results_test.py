"""dbt ingestion must not replay superseded test results as assertion run events.

Every AssertionRunEvent written to GMS can fan out into an assertion
notification. When several run_results files cover the same dbt test (a glob
over historical runs, or `dbt build` followed by `dbt retry`), ingesting all of
them emitted a stale FAILURE followed by the current SUCCESS on every
ingestion — a spurious "failed" alert immediately followed by "passed".
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pytest

from datahub.emitter import mce_builder
from datahub.ingestion.run.pipeline import Pipeline
from tests.e2e.utils import delete_urns, execute_graphql, unique_suffix, with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.OBSERVE, Domain.INGESTION)

_PROJECT = "my_project"

_RUN_EVENTS_QUERY = """
query assertionRunEvents($urn: String!) {
  assertion(urn: $urn) {
    runEvents(limit: 10) {
      total
      runEvents {
        timestampMillis
        runId
        result {
          type
        }
      }
    }
  }
}
"""


def _manifest(model_name: str, test_id: str, model_id: str) -> Dict[str, Any]:
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
            "dbt_version": "1.7.3",
            "adapter_type": "postgres",
            "project_name": _PROJECT,
        },
        "nodes": {
            model_id: {
                "unique_id": model_id,
                "name": model_name,
                "resource_type": "model",
                "database": "my_db",
                "schema": "my_schema",
                "alias": model_name,
                "original_file_path": f"models/{model_name}.sql",
                "config": {"materialized": "table"},
                "depends_on": {"nodes": []},
                "columns": {},
                "tags": [],
                "meta": {},
                "raw_code": "select 1 as id",
            },
            test_id: {
                "unique_id": test_id,
                "name": f"not_null_{model_name}_id",
                "resource_type": "test",
                "database": "my_db",
                "schema": "my_schema",
                "original_file_path": f"models/{model_name}.yml",
                "config": {"materialized": "test", "severity": "ERROR"},
                "depends_on": {"nodes": [model_id]},
                "test_metadata": {
                    "name": "not_null",
                    "namespace": None,
                    "kwargs": {"column_name": "id", "model": model_name},
                },
                "column_name": "id",
                "columns": {},
                "tags": [],
                "meta": {},
            },
        },
        "sources": {},
        "exposures": {},
        "metrics": {},
    }


def _run_results(test_id: str, day: str, invocation_id: str, status: str) -> Dict:
    ts = f"{day}T06:00:00.000000Z"
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
            "dbt_version": "1.7.3",
            "generated_at": ts,
            "invocation_id": invocation_id,
        },
        "args": {"which": "build"},
        "results": [
            {
                "unique_id": test_id,
                "status": status,
                "failures": 3 if status == "fail" else 0,
                "message": "Got 3 results" if status == "fail" else None,
                "timing": [{"name": "execute", "started_at": ts, "completed_at": ts}],
            }
        ],
    }


@with_test_retry()
def _wait_for_run_events(auth_session, assertion_urn: str) -> List[Dict[str, Any]]:
    res = execute_graphql(
        auth_session, _RUN_EVENTS_QUERY, variables={"urn": assertion_urn}
    )
    run_events = res["data"]["assertion"]["runEvents"]
    assert run_events["total"] > 0
    return run_events["runEvents"]


def test_dbt_ingestion_emits_only_latest_test_result(
    auth_session, graph_client, tmp_path: Path
) -> None:
    model_name = f"events_{unique_suffix()}"
    model_id = f"model.{_PROJECT}.{model_name}"
    test_id = f"test.{_PROJECT}.not_null_{model_name}_id.0a1b2c3d4e"

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest(model_name, test_id, model_id)))
    # Lexically-ordered run directories, as produced by archiving each dbt
    # invocation: yesterday's run failed, today's run passed.
    for day, invocation_id, status in [
        ("2026-01-01", "inv-previous", "fail"),
        ("2026-01-02", "inv-current", "pass"),
    ]:
        run_dir = tmp_path / "runs" / day
        run_dir.mkdir(parents=True)
        (run_dir / "run_results.json").write_text(
            json.dumps(_run_results(test_id, day, invocation_id, status))
        )

    # Mirrors the guid dbt ingestion assigns to single-upstream PROD tests.
    assertion_urn = mce_builder.make_assertion_urn(
        mce_builder.datahub_guid({"platform": "dbt", "name": test_id})
    )
    target_urn = mce_builder.make_dataset_urn(
        "postgres", f"my_db.my_schema.{model_name}"
    )
    dbt_urn = mce_builder.make_dataset_urn("dbt", f"my_db.my_schema.{model_name}")

    try:
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "dbt",
                    "config": {
                        "manifest_path": str(manifest_path),
                        "run_results_paths": [
                            str(tmp_path / "runs" / "*" / "run_results.json")
                        ],
                        "target_platform": "postgres",
                    },
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": auth_session.gms_url(),
                        "token": auth_session.gms_token(),
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        wait_for_writes_to_sync()

        run_events = _wait_for_run_events(auth_session, assertion_urn)

        # One run event per ingestion: the current pass. Before the fix the
        # superseded failure was written too, producing a FAILURE -> SUCCESS
        # notification pair on every ingestion.
        assert [(e["runId"], e["result"]["type"]) for e in run_events] == [
            ("inv-current", "SUCCESS")
        ]
    finally:
        try:
            delete_urns(graph_client, [assertion_urn, target_urn, dbt_urn])
        except Exception:
            logger.exception("Cleanup of dbt smoke-test entities failed")
