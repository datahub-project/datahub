"""Golden-file integration test for the Monte Carlo source.

Monte Carlo has no public Docker image, so this test injects a fake pycarlo
client that replays recorded GraphQL responses. The real source, client
(parsing + pagination), MCON resolver and assertion builder all run end to end.
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
        "fullTableId": "analytics.public.orders",
        "warehouse": {"connectionType": "snowflake"},
    },
    "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.CUSTOMERS": {
        "mcon": "MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.CUSTOMERS",
        "fullTableId": "analytics.public.customers",
        "warehouse": {"connectionType": "snowflake"},
    },
    "MCON++acct++wh-bq++table++proj.dataset.events": {
        "mcon": "MCON++acct++wh-bq++table++proj.dataset.events",
        "fullTableId": "proj.dataset.events",
        "warehouse": {"connectionType": "bigquery"},
    },
}

MONITORS: List[Dict[str, Any]] = [
    {
        "uuid": "mon-fresh-orders",
        "name": "Freshness - orders",
        "description": "orders should update hourly",
        "monitorType": "FRESHNESS",
        "entityMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
        "resourceId": "wh-snow",
        "severity": "SEV-2",
        "isPaused": False,
        "dataQualityDimension": "FRESHNESS",
    },
    {
        "uuid": "mon-vol-orders",
        "name": "Volume - orders",
        "description": "orders row count should be stable",
        "monitorType": "VOLUME",
        "entityMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
        "resourceId": "wh-snow",
        "severity": "SEV-3",
        "isPaused": False,
        "dataQualityDimension": "VOLUME",
    },
    {
        "uuid": "mon-fresh-events",
        "name": "Freshness - events",
        "description": "events should be fresh",
        "monitorType": "FRESHNESS",
        "entityMcons": ["MCON++acct++wh-bq++table++proj.dataset.events"],
        "resourceId": "wh-bq",
        "severity": "SEV-2",
        "isPaused": False,
        "dataQualityDimension": "FRESHNESS",
    },
    {
        "uuid": "mon-schema-customers",
        "name": "Schema - customers",
        "description": "customers schema should not drift",
        "monitorType": "SCHEMA_CHANGE",
        "entityMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.CUSTOMERS"],
        "resourceId": "wh-snow",
        "severity": "SEV-4",
        "isPaused": True,
        "dataQualityDimension": "SCHEMA",
    },
]

CUSTOM_RULES: List[Dict[str, Any]] = [
    {
        "uuid": "rule-orders-not-null",
        "ruleType": "CUSTOM_SQL",
        "description": "orders.total must be non-negative",
        "customSql": "SELECT count(*) FROM analytics.public.orders WHERE total < 0",
        "entityMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
        "severity": "SEV-2",
    },
    {
        "uuid": "rule-events-freshness",
        "ruleType": "FRESHNESS",
        "description": "events freshness SLA",
        "customSql": None,
        "entityMcons": ["MCON++acct++wh-bq++table++proj.dataset.events"],
        "severity": "SEV-3",
    },
]

ALERTS: List[Dict[str, Any]] = [
    {
        "id": "alert-1",
        "type": "ANOMALY",
        "subTypes": ["FRESHNESS_ANOMALY"],
        "severity": "SEV-2",
        "priority": "P1",
        "status": "TRIGGERED",
        "createdTime": "2026-05-20T08:00:00+00:00",
        "monitorUuid": "mon-fresh-orders",
        "assetMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
    },
    {
        "id": "alert-2",
        "type": "ANOMALY",
        "subTypes": ["VOLUME_ANOMALY"],
        "severity": "SEV-3",
        "priority": "P2",
        "status": "TRIGGERED",
        "createdTime": "2026-05-21T09:30:00+00:00",
        "monitorUuid": "mon-vol-orders",
        "assetMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
    },
    {
        "id": "alert-3",
        "type": "ANOMALY",
        "subTypes": ["CUSTOM_RULE"],
        "severity": "SEV-2",
        "priority": "P1",
        "status": "TRIGGERED",
        "createdTime": "2026-05-22T10:00:00+00:00",
        "monitorUuid": "rule-orders-not-null",
        "assetMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
    },
    {
        # Orphan alert for a monitor we never ingested -> should be skipped.
        "id": "alert-orphan",
        "type": "ANOMALY",
        "subTypes": ["FRESHNESS_ANOMALY"],
        "severity": "SEV-4",
        "status": "TRIGGERED",
        "createdTime": "2026-05-23T11:00:00+00:00",
        "monitorUuid": "ghost-monitor",
        "assetMcons": ["MCON++acct++wh-snow++table++ANALYTICS.PUBLIC.ORDERS"],
    },
]


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
            return _FakeBox(getMonitors=MONITORS[offset : offset + limit])
        if "getCustomRules" in query:
            return _FakeBox(
                getCustomRules={
                    "edges": [{"node": r} for r in CUSTOM_RULES],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            )
        if "getAlerts" in query:
            return _FakeBox(
                getAlerts={
                    "edges": [{"node": a} for a in ALERTS],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            )
        if "getTable" in query:
            mcon = variables.get("mcon")
            table = TABLES.get(mcon) if isinstance(mcon, str) else None
            return _FakeBox(getTable=table)
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
