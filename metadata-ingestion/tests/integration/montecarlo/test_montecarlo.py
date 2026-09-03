"""Golden-file integration test for the Monte Carlo source.

Monte Carlo has no public Docker image, so this test injects a fake pycarlo
client that replays recorded GraphQL responses. The real source, client
(parsing + pagination), MCON resolver and assertion builder all run end to end.

Fixture keys below are snake_case, not the camelCase GraphQL field names — the
real pycarlo Client normalizes every response key to snake_case (Box with
camel_killer_box=True) before MonteCarloClient ever sees it, so the fake must
match that shape rather than the raw wire format.
"""

import json
import pathlib
from typing import Any, Dict, List

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.montecarlo.source import MonteCarloSource
from datahub.testing import mce_helpers

FROZEN_TIME = "2026-06-01 00:00:00"

# Two monitored warehouses resolved via getTable.
TABLES: Dict[str, Dict[str, Any]] = {
    "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS": {
        "mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS",
        "full_table_id": "analytics.public.orders",
        "warehouse": {"connection_type": "snowflake"},
    },
    "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.CUSTOMERS": {
        "mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.CUSTOMERS",
        "full_table_id": "analytics.public.customers",
        "warehouse": {"connection_type": "snowflake"},
    },
    "MCON++acct++wh-bq++table++proj.dataset.events": {
        "mcon": "MCON++acct++wh-bq++table++proj.dataset.events",
        "full_table_id": "proj.dataset.events",
        "warehouse": {"connection_type": "bigquery"},
    },
}

MONITORS: List[Dict[str, Any]] = [
    {
        "uuid": "mon-fresh-orders",
        "name": "Freshness - orders",
        "description": "orders should update hourly",
        "monitor_type": "FRESHNESS",
        "entity_mcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
        "resource_id": "wh-snow",
        "severity": "SEV-2",
        "is_paused": False,
        "data_quality_dimension": "FRESHNESS",
        "comparisons": [
            {
                "comparison_type": "FRESHNESS",
                "operator": "GT",
                "metric": "freshness",
                "threshold": 24,
            }
        ],
    },
    {
        "uuid": "mon-vol-orders",
        "name": "Volume - orders",
        "description": "orders row count should be stable",
        "monitor_type": "VOLUME",
        "entity_mcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
        "resource_id": "wh-snow",
        "severity": "SEV-3",
        "is_paused": False,
        "data_quality_dimension": "VOLUME",
        "comparisons": [
            {
                "comparison_type": "ABSOLUTE_VOLUME",
                "operator": "GT",
                "metric": "row_count",
                "threshold": 1000,
            }
        ],
    },
    {
        "uuid": "mon-fresh-events",
        "name": "Freshness - events",
        "description": "events should be fresh",
        "monitor_type": "FRESHNESS",
        "entity_mcons": ["MCON++acct++wh-bq++table++proj.dataset.events"],
        "resource_id": "wh-bq",
        "severity": "SEV-2",
        "is_paused": False,
        "data_quality_dimension": "FRESHNESS",
        "comparisons": [
            {
                "comparison_type": "THRESHOLD",
                "operator": "INSIDE_RANGE",
                "metric": "row_count",
                "lower_threshold": 100,
                "upper_threshold": 5000,
            }
        ],
    },
    {
        # A monitor with no comparisons exercises the fallback path: native fields
        # move to nativeType/nativeParameters, customProperties keeps only
        # mc_monitor_uuid, and no scope is set (structured rendering does not fire).
        "uuid": "mon-schema-customers",
        "name": "Schema - customers",
        "description": "customers schema should not drift",
        "monitor_type": "SCHEMA_CHANGE",
        "entity_mcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.CUSTOMERS"],
        "resource_id": "wh-snow",
        "severity": "SEV-4",
        "is_paused": True,
        "data_quality_dimension": "SCHEMA",
    },
]

CUSTOM_RULES: List[Dict[str, Any]] = [
    {
        "uuid": "rule-orders-not-null",
        "rule_type": "CUSTOM_SQL",
        "description": "orders.total must be non-negative",
        "custom_sql": "SELECT count(*) FROM analytics.public.orders WHERE total < 0",
        "entity_mcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
        "severity": "SEV-2",
        "comparisons": [
            {
                "comparison_type": "THRESHOLD",
                "operator": "LTE",
                "metric": "max",
                "field": "total",
                "threshold": 0,
                "custom_metric": {"uuid": "cm-max-total", "metric_name": "max_total"},
            }
        ],
    },
    {
        "uuid": "rule-events-freshness",
        "rule_type": "FRESHNESS",
        "description": "events freshness SLA",
        "custom_sql": None,
        "entity_mcons": ["MCON++acct++wh-bq++table++proj.dataset.events"],
        "severity": "SEV-3",
        "comparisons": [
            {
                "comparison_type": "FRESHNESS",
                "operator": "GT",
                "metric": "freshness",
                "threshold": 24,
            }
        ],
    },
]

ALERTS: List[Dict[str, Any]] = [
    {
        "id": "alert-1",
        "type": "ANOMALY",
        "sub_types": ["FRESHNESS_ANOMALY"],
        "severity": "SEV-2",
        "priority": "P1",
        "status": "TRIGGERED",
        "created_time": "2026-05-20T08:00:00+00:00",
        "monitor_uuids": ["mon-fresh-orders"],
        "assets": [{"mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"}],
    },
    {
        "id": "alert-2",
        "type": "ANOMALY",
        "sub_types": ["VOLUME_ANOMALY"],
        "severity": "SEV-3",
        "priority": "P2",
        "status": "TRIGGERED",
        "created_time": "2026-05-21T09:30:00+00:00",
        "monitor_uuids": ["mon-vol-orders"],
        "assets": [{"mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"}],
    },
    {
        "id": "alert-3",
        "type": "ANOMALY",
        "sub_types": ["CUSTOM_RULE"],
        "severity": "SEV-2",
        "priority": "P1",
        "status": "TRIGGERED",
        "created_time": "2026-05-22T10:00:00+00:00",
        "monitor_uuids": ["rule-orders-not-null"],
        "assets": [{"mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"}],
    },
    {
        # Orphan alert for a monitor we never ingested -> should be skipped.
        "id": "alert-orphan",
        "type": "ANOMALY",
        "sub_types": ["FRESHNESS_ANOMALY"],
        "severity": "SEV-4",
        "status": "TRIGGERED",
        "created_time": "2026-05-23T11:00:00+00:00",
        "monitor_uuids": ["ghost-monitor"],
        "assets": [{"mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"}],
    },
]

# Run history for the volume monitor (mon-vol-orders). Most-recent-first.
# The volume monitor's comparisons carry metric="row_count", which is a
# standard getMetricsV4 metricName that returns points.
JOB_EXECUTIONS: Dict[str, List[Dict[str, Any]]] = {
    "mon-vol-orders": [
        {
            "job_execution_uuid": "je-vol-1",
            "start_time": "2026-05-31T08:00:00+00:00",
            "end_time": "2026-05-31T08:01:00+00:00",
            "status": "SUCCESS",
            "exceptions": None,
            "total_result_count": 1,
            "evaluated_record_count": 500,
        },
        {
            "job_execution_uuid": "je-vol-2",
            "start_time": "2026-05-31T07:00:00+00:00",
            "end_time": "2026-05-31T07:01:00+00:00",
            "status": "SUCCESS",
            "exceptions": None,
            "total_result_count": 1,
            "evaluated_record_count": 499,
        },
    ],
    # A monitor whose only recent run failed — should produce no SUCCESS run event.
    "mon-fresh-orders": [
        {
            "job_execution_uuid": "je-fresh-fail",
            "start_time": "2026-05-31T08:00:00+00:00",
            "status": "FAILED",
            "exceptions": "timeout",
        },
    ],
}

# Measured metric values from getMetricsV4. Keyed by (mcon, metricName).
# jobExecutionUuid is null for table-level metrics (matches the live API).
METRICS_V4: Dict[str, List[Dict[str, Any]]] = {
    "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS:row_count": [
        {
            "metric": "row_count",
            "value": 500.0,
            "field": None,
            "measurement_timestamp": "2026-05-31T08:00:30+00:00",
            "thresholds": [],
            "job_execution_uuid": None,
        }
    ],
}


class _FakeBox(dict):
    pass


class _FakePycarloClient:
    """Replays recorded GraphQL responses keyed by the operation in the query."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __call__(self, query: str, variables: Dict[str, Any]) -> _FakeBox:
        if "getMonitors" in query:
            offset = variables.get("offset") or 0
            limit = variables.get("limit") or len(MONITORS)
            return _FakeBox(get_monitors=MONITORS[offset : offset + limit])
        if "getCustomRules" in query:
            return _FakeBox(
                get_custom_rules={
                    "edges": [{"node": r} for r in CUSTOM_RULES],
                    "page_info": {"has_next_page": False, "end_cursor": None},
                }
            )
        if "getAlerts" in query:
            return _FakeBox(
                get_alerts={
                    "edges": [{"node": a} for a in ALERTS],
                    "page_info": {"has_next_page": False, "end_cursor": None},
                }
            )
        if "getTable" in query:
            mcon = variables.get("mcon")
            if isinstance(mcon, str):
                return _FakeBox(get_table=TABLES.get(mcon))
            full_table_id = variables.get("fullTableId")
            table = next(
                (t for t in TABLES.values() if t["full_table_id"] == full_table_id),
                None,
            )
            return _FakeBox(get_table=table)
        if "getJobExecutions" in query:
            monitor_uuid = variables.get("monitorUuid")
            runs = JOB_EXECUTIONS.get(monitor_uuid, []) if monitor_uuid else []
            return _FakeBox(
                get_job_executions={
                    "edges": [{"node": r} for r in runs],
                    "page_info": {"has_next_page": False, "end_cursor": None},
                }
            )
        if "getMetricsV4" in query:
            metrics_filter = variables.get("metricsFilter") or {}
            mcon = (
                metrics_filter.get("mcon") if isinstance(metrics_filter, dict) else None
            )
            metric_name = variables.get("metricName")
            key = f"{mcon}:{metric_name}"
            return _FakeBox(get_metrics_v4={"metrics": METRICS_V4.get(key, [])})
        raise AssertionError(f"Unexpected query: {query}")


class _FakeSession:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


@pytest.fixture
def fake_pycarlo(monkeypatch):
    import sys
    import types

    core = types.ModuleType("pycarlo.core")
    core.Client = _FakePycarloClient  # type: ignore[attr-defined]
    core.Session = _FakeSession  # type: ignore[attr-defined]
    pycarlo = types.ModuleType("pycarlo")
    pycarlo.core = core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pycarlo", pycarlo)
    monkeypatch.setitem(sys.modules, "pycarlo.core", core)
    yield


CONFIG_DICT: Dict[str, Any] = {
    "api_id": "test-id",
    "api_token": "test-token",
    "connection_to_platform_map": {
        "wh-snow": {
            "platform": "snowflake",
            "platform_instance": "prod",
            "env": "PROD",
        },
        "wh-bq": {"platform": "bigquery", "env": "PROD"},
    },
    "stateful_ingestion": {"enabled": False},
}

IGNORE_PATHS = [
    r"root\[\d+\]\['aspect'\]\['json'\]\['source'\]\['created'\]\['time'\]",
]


def _run_source() -> List[Any]:
    source = MonteCarloSource.create(
        CONFIG_DICT, PipelineContext(run_id="montecarlo-test")
    )
    return [wu.metadata for wu in source.get_workunits()]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_montecarlo_ingestion_oss(pytestconfig, fake_pycarlo, monkeypatch):
    """Deterministic golden using the OSS assertion path (no cloud SDK).

    Forces the cloud-SDK loader to return None so the test produces identical
    output whether or not acryl-datahub-cloud is installed in the test env.
    """
    from datahub.ingestion.source.montecarlo import assertion as mc_assertion

    monkeypatch.setattr(mc_assertion, "_load_cloud_assertion_class", lambda: None)

    golden_path = (
        pytestconfig.rootpath
        / "tests/integration/montecarlo/golden/montecarlo_mces_golden.json"
    )
    mce_helpers.check_goldens_stream(
        outputs=_run_source(), golden_path=golden_path, ignore_paths=IGNORE_PATHS
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_montecarlo_ingestion_cloud_entity(pytestconfig, fake_pycarlo):
    """The Cloud Assertion entity path (default when installed) must produce the
    same aspects as the OSS fallback, so it compares against the same golden."""
    pytest.importorskip("acryl_datahub_cloud")

    golden_path = (
        pytestconfig.rootpath
        / "tests/integration/montecarlo/golden/montecarlo_mces_golden.json"
    )
    mce_helpers.check_goldens_stream(
        outputs=_run_source(), golden_path=golden_path, ignore_paths=IGNORE_PATHS
    )


def test_golden_file_is_substantial():
    """Guard the golden file stays a meaningful fixture (>5KB, >15 events)."""
    golden_path = (
        pathlib.Path(__file__).parent / "golden" / "montecarlo_mces_golden.json"
    )
    content = golden_path.read_text()
    assert len(content) > 5 * 1024
    assert len(json.loads(content)) >= 15


RUN_EVENTS_CONFIG: Dict[str, Any] = {
    **CONFIG_DICT,
    "run_events_lookback_days": 7,
}


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_montecarlo_run_events_emits_success_with_measured_value(
    fake_pycarlo, monkeypatch
):
    """With run_events_lookback_days set, the source emits SUCCESS
    AssertionRunEvents carrying the measured metric value on
    AssertionResult. Only the latest SUCCESS run carries the value; older
    SUCCESS runs carry only per-run execution metadata. Monitors with no
    recent SUCCESS run produce no run event (FAILUREs are left to alerts)."""
    from datahub.ingestion.source.montecarlo import assertion as mc_assertion

    # Force the OSS path so the test is deterministic regardless of cloud SDK.
    monkeypatch.setattr(mc_assertion, "_load_cloud_assertion_class", lambda: None)

    source = MonteCarloSource.create(
        RUN_EVENTS_CONFIG, PipelineContext(run_id="montecarlo-run-events-test")
    )
    wus = list(source.get_workunits())

    # Collect AssertionRunEvent aspects grouped by runId.
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import AssertionRunEventClass

    run_events: List[AssertionRunEventClass] = []
    for wu in wus:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, AssertionRunEventClass):
                run_events.append(wu.metadata.aspect)

    # We expect SUCCESS run events from mon-vol-orders (2 SUCCESS runs in the
    # fixture) plus FAILURE run events from the alerts path (3 alerts).
    success_events = [e for e in run_events if e.result and e.result.type == "SUCCESS"]
    failure_events = [e for e in run_events if e.result and e.result.type == "FAILURE"]

    assert len(success_events) == 2, (
        f"expected 2 SUCCESS events, got {len(success_events)}"
    )
    assert len(failure_events) == 3, (
        f"expected 3 FAILURE events, got {len(failure_events)}"
    )

    # The latest SUCCESS run (je-vol-1) carries the measured row_count.
    latest = next(e for e in success_events if e.runId == "je-vol-1")
    assert latest.result is not None
    assert latest.result.rowCount == 500
    assert latest.result.nativeResults is not None
    assert latest.result.nativeResults["evaluatedRecordCount"] == "500"

    # The older SUCCESS run (je-vol-2) carries only execution metadata, no
    # measured value.
    older = next(e for e in success_events if e.runId == "je-vol-2")
    assert older.result is not None
    assert older.result.rowCount is None
    assert older.result.nativeResults is not None
    assert older.result.nativeResults["evaluatedRecordCount"] == "499"

    # mon-fresh-orders had only a FAILED run -> no SUCCESS event for it.
    assert all(e.runId != "je-fresh-fail" for e in success_events)

    # All run events are non-primary-source (stale removal safeguard).
    for wu in wus:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, AssertionRunEventClass
        ):
            assert wu.is_primary_source is False

    # Report counters reflect the work.
    report = source.get_report()
    assert report.job_executions_scanned == 2  # 2 SUCCESS runs emitted
    assert report.metric_points_fetched == 1  # 1 metric point from getMetricsV4
