"""E2E: GX Core 1.x Fluent Checkpoint -> DataHubValidationAction -> local GMS.

Run with the gx-plugin venv-gx1 environment:
  source venv-gx1/bin/activate
  python tests/e2e/run_gx1_fluent_e2e.py

Auth: set DATAHUB_GMS_TOKEN, or leave unset to log in via frontend
(DATAHUB_FRONTEND_URL, default http://localhost:9002) as datahub/datahub.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import uuid
from typing import Any, Dict, Optional

import great_expectations as gx
import packaging.version
import pandas as pd
import requests

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionInfo
from datahub_gx_plugin.action_v1 import DataHubValidationAction

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080").rstrip("/")
FRONTEND_URL = os.environ.get("DATAHUB_FRONTEND_URL", "http://localhost:9002").rstrip(
    "/"
)
DOCS_URL = "https://example.com/gx-data-docs/index.html"


def _require_gx1() -> None:
    version = packaging.version.parse(gx.__version__)
    if version.major < 1:
        raise SystemExit(f"Need great-expectations>=1.0, got {gx.__version__}")


def _gms_ready() -> None:
    r = requests.get(f"{GMS_URL}/health", timeout=10)
    r.raise_for_status()


def _token_from_play_session(play_session: str) -> str:
    payload = play_session.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    data = json.loads(base64.urlsafe_b64decode(payload))
    return data["data"]["token"]


def _resolve_token() -> str:
    env_token = os.environ.get("DATAHUB_GMS_TOKEN") or os.environ.get("DATAHUB_TOKEN")
    if env_token:
        return env_token

    username = os.environ.get("DATAHUB_USERNAME", "datahub")
    password = os.environ.get("DATAHUB_PASSWORD", "datahub")
    r = requests.post(
        f"{FRONTEND_URL}/logIn",
        json={"username": username, "password": password},
        timeout=30,
    )
    r.raise_for_status()
    play = r.cookies.get("PLAY_SESSION")
    if not play:
        raise SystemExit(
            "Frontend login succeeded but PLAY_SESSION cookie missing; "
            "set DATAHUB_GMS_TOKEN explicitly."
        )
    return _token_from_play_session(play)


def run_checkpoint(token: str) -> Dict[str, Any]:
    run_suffix = uuid.uuid4().hex[:8]
    # Asset name is the Fluent data asset; action omits dataset_name so asset_name
    # resolution is exercised.
    asset_name = f"gx1_e2e_orders_{run_suffix}"
    suite_name = f"gx1_e2e_suite_{run_suffix}"
    checkpoint_name = f"checkpoint_{run_suffix}"
    validation_name = f"validation_{run_suffix}"

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, None],  # null triggers not-null failure
            "score": [10, 20, 30, 40, 50],
            "status": ["a", "b", "a", "b", "a"],
        }
    )

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=f"pandas_{run_suffix}")
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")

    suite = gx.ExpectationSuite(name=suite_name)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="id",
            description="id must never be null",
            severity="critical",
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToEqual(
            value=5,
            description="orders batch must have 5 rows",
            severity="warning",
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="score",
            min_value=0,
            max_value=100,
            description="score is within 0-100",
            severity="info",
        )
    )
    suite = context.suites.add(suite)
    expectation_ids = {}
    for exp in suite.expectations:
        exp_type = getattr(exp, "expectation_type", None) or getattr(
            getattr(exp, "configuration", None), "type", None
        )
        exp_id = getattr(exp, "id", None) or getattr(
            getattr(exp, "configuration", None), "id", None
        )
        if exp_type and exp_id:
            expectation_ids[exp_type] = exp_id

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=validation_name,
            data=batch_definition,
            suite=suite,
        )
    )

    action = DataHubValidationAction(
        name="datahub_action",
        server_url=GMS_URL,
        token=token,
        platform="pandas",
        # Intentionally omit dataset_name — rely on GX asset_name.
        graceful_exceptions=False,
        emit_mode="SYNC_WAIT",
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_definition],
            actions=[action],
        )
    )

    print(f"Running GX {gx.__version__} checkpoint against {GMS_URL} ...")
    # Avoid subclassing ValidationAction (GX registers by type) and Pydantic
    # forbids assigning methods on the instance — patch the class for this run.
    from unittest import mock

    with mock.patch.object(
        DataHubValidationAction,
        "_docs_link_from_action_context",
        return_value=DOCS_URL,
    ):
        result = checkpoint.run(batch_parameters={"dataframe": df})
    print(f"Checkpoint success={result.success}")
    print(f"Action results keys: {list(result.run_results.keys())}")

    return {
        "asset_name": asset_name,
        "suite_name": suite_name,
        "checkpoint_name": checkpoint_name,
        "checkpoint_id": checkpoint.id,
        "validation_name": validation_name,
        "validation_id": validation_definition.id,
        "expectation_ids": expectation_ids,
        "result": result,
    }


def _graphql(token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-DataHub-Actor": "urn:li:corpuser:datahub",
    }
    resp = requests.post(
        f"{GMS_URL}/api/graphql",
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise SystemExit(f"GraphQL errors: {payload['errors']}")
    return payload["data"]


def _verify_run_events(
    assertion_by_native_type: Dict[str, Dict[str, Any]], token: str
) -> None:
    query = """
    query($urn: String!) {
      assertion(urn: $urn) {
        urn
        runEvents(status: COMPLETE, limit: 3) {
          total
          runEvents {
            status
            result { type rowCount unexpectedCount severity externalUrl }
          }
        }
      }
    }
    """
    for native_type, meta in assertion_by_native_type.items():
        urn = meta["urn"]
        data = _graphql(token, query, {"urn": urn})
        run_events = data["assertion"]["runEvents"]
        events = run_events["runEvents"] or []
        if run_events["total"] < 1 or not events:
            raise SystemExit(f"No COMPLETE run events for {urn}")
        event = events[0]
        result = event["result"]
        expected_type = meta["expected_result_type"]
        print(
            f"  runEvent {native_type}: status={event['status']} "
            f"result={result['type']} severity={result.get('severity')} "
            f"externalUrl={result.get('externalUrl')}"
        )
        if result["type"] != expected_type:
            raise SystemExit(
                f"Expected {expected_type} for {native_type}, got {result['type']}"
            )
        if result.get("externalUrl") != DOCS_URL:
            raise SystemExit(
                f"Expected externalUrl={DOCS_URL} for {urn}, got {result.get('externalUrl')}"
            )
        expected_severity = meta.get("expected_severity")
        if expected_severity is not None:
            if result.get("severity") != expected_severity:
                raise SystemExit(
                    f"Expected severity={expected_severity} for {native_type}, "
                    f"got {result.get('severity')}"
                )
        elif result.get("severity") is not None:
            raise SystemExit(
                f"Expected no severity on SUCCESS for {native_type}, "
                f"got {result.get('severity')}"
            )


def _assert_custom_props(
    custom: Dict[str, Any],
    *,
    suite_name: str,
    checkpoint_name: str,
    checkpoint_id: Optional[str],
    validation_name: str,
    validation_id: Optional[str],
    expectation_id: Optional[str],
) -> None:
    required = {
        "expectation_suite_name": suite_name,
        "checkpoint_name": checkpoint_name,
        "validation_definition_name": validation_name,
    }
    if checkpoint_id:
        required["checkpoint_id"] = str(checkpoint_id)
    if validation_id:
        required["validation_id"] = str(validation_id)
    if expectation_id:
        required["expectation_id"] = str(expectation_id)

    for key, expected in required.items():
        actual = custom.get(key)
        if actual != expected:
            raise SystemExit(
                f"customProperties.{key}: expected {expected!r}, got {actual!r} "
                f"(all={custom})"
            )


def verify_in_datahub(ctx: Dict[str, Any], token: str) -> None:
    asset_name = ctx["asset_name"]
    suite_name = ctx["suite_name"]
    dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:pandas,{asset_name},PROD)"
    graph = DataHubGraph(DatahubClientConfig(server=GMS_URL, token=token))

    expected_by_type = {
        "expect_column_values_to_not_be_null": {
            "description": "id must never be null",
            "expected_result_type": "FAILURE",
            "expected_severity": "HIGH",
            "expectation_id": ctx["expectation_ids"].get(
                "expect_column_values_to_not_be_null"
            ),
        },
        "expect_table_row_count_to_equal": {
            "description": "orders batch must have 5 rows",
            "expected_result_type": "SUCCESS",
            "expected_severity": None,
            "expectation_id": ctx["expectation_ids"].get(
                "expect_table_row_count_to_equal"
            ),
        },
        "expect_column_values_to_be_between": {
            "description": "score is within 0-100",
            "expected_result_type": "SUCCESS",
            "expected_severity": None,
            "expectation_id": ctx["expectation_ids"].get(
                "expect_column_values_to_be_between"
            ),
        },
    }

    deadline = time.time() + 90
    last_error: Optional[str] = None
    while time.time() < deadline:
        try:
            related = list(
                graph.get_related_entities(
                    entity_urn=dataset_urn,
                    relationship_types=["Asserts"],
                    direction=DataHubGraph.RelationshipDirection.INCOMING,
                )
            )
            assertion_urns = [r.urn for r in related]
            if not assertion_urns:
                last_error = f"No Asserts relationships yet for {dataset_urn}"
                time.sleep(2)
                continue

            print(f"Found {len(assertion_urns)} assertion(s) on {dataset_urn}")
            found: Dict[str, Dict[str, Any]] = {}
            for urn in assertion_urns:
                info = graph.get_aspect(urn, AssertionInfo)
                if info is None:
                    continue
                info_obj = info.to_obj() if hasattr(info, "to_obj") else info
                print(f"  {urn}")
                print(f"    info={json.dumps(info_obj, default=str)[:700]}")
                custom = info_obj.get("customProperties") or {}
                native_type = (info_obj.get("customAssertion") or {}).get("nativeType")
                if native_type not in expected_by_type:
                    last_error = f"Unexpected nativeType {native_type}"
                    continue

                expected = expected_by_type[native_type]
                if info_obj.get("description") != expected["description"]:
                    raise SystemExit(
                        f"description mismatch for {native_type}: "
                        f"{info_obj.get('description')!r} != {expected['description']!r}"
                    )
                _assert_custom_props(
                    custom,
                    suite_name=suite_name,
                    checkpoint_name=ctx["checkpoint_name"],
                    checkpoint_id=ctx["checkpoint_id"],
                    validation_name=ctx["validation_name"],
                    validation_id=ctx["validation_id"],
                    expectation_id=expected["expectation_id"],
                )
                print(f"    matched {native_type}: description + customProperties OK")
                found[native_type] = {
                    "urn": urn,
                    "expected_result_type": expected["expected_result_type"],
                    "expected_severity": expected["expected_severity"],
                }

            if len(found) >= 3:
                _verify_run_events(found, token)
                return

            last_error = (
                f"Have {len(assertion_urns)} assertion(s), "
                f"matched_types={sorted(found)}; waiting for full set"
            )
        except SystemExit:
            raise
        except Exception as e:
            last_error = str(e)
        time.sleep(2)

    print("Relationship lookup timed out; trying entity exists check...")
    if graph.exists(dataset_urn):
        print(f"Dataset entity exists: {dataset_urn}")
    else:
        print(f"WARNING: dataset entity not found: {dataset_urn}")
    raise SystemExit(f"E2E verification failed: {last_error}")


def main() -> int:
    _require_gx1()
    _gms_ready()
    token = _resolve_token()
    ctx = run_checkpoint(token)
    if ctx["result"].success:
        print(
            "WARNING: expected checkpoint failure from null id values; "
            "continuing verification"
        )
    verify_in_datahub(ctx, token)
    print("E2E PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
