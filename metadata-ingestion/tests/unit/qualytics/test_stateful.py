"""Stale-entity removal, end to end: two real pipeline runs against one state file.

The rest of the suite stops at the source's workunits. These run the whole pipeline,
framework processors included, because the guarantees that matter here are about what
the *second* run deletes:

- a check that is genuinely gone takes its assertion with it, and nothing else;
- a dataset is never soft-deleted, because this connector does not own any;
- a run that could not read everything deletes nothing.

The last was a real bug: a 500 on one container's quality-check listing was reported
as a warning, DataHub's stale-entity handler only stands down for failures, and the
next commit soft-deleted every assertion on that container.
"""

import json
from pathlib import Path
from typing import Any

from requests_mock import Mocker

from datahub.ingestion.run.pipeline import Pipeline

BASE = "https://acme.qualytics.io/api"
DATASTORE = {
    "id": 1,
    "name": "warehouse",
    "store_type": "jdbc",
    "type": "snowflake",
    "database": "SALES",
    "schema": "PUBLIC",
}
ORDERS = {"id": 10, "name": "ORDERS", "container_type": "table"}
CUSTOMERS = {"id": 11, "name": "CUSTOMERS", "container_type": "table"}
CHECK = {
    "id": 100,
    "rule_type": "notNull",
    "fields": [{"name": "amount"}],
    "is_passing": True,
    "last_asserted": "2026-09-08T10:00:00Z",
}
ORDERS_CHECKS = [CHECK, {**CHECK, "id": 101}]
CUSTOMERS_CHECK = {**CHECK, "id": 200}


def _page(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"items": items, "page": 1, "pages": 1, "size": 100, "total": len(items)}


def _recipe(tmp_path: Path, run: int, **config: Any) -> dict[str, Any]:
    return {
        "pipeline_name": "qualytics-stateful-test",
        "run_id": f"run-{run}",
        "source": {
            "type": "qualytics",
            "config": {
                "base_url": BASE,
                "token": "t",
                "platform_instance": "acme",
                "emit_profiles": False,
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "state_provider": {
                        "type": "file",
                        "config": {"filename": str(tmp_path / "state.json")},
                    },
                },
                **config,
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(tmp_path / f"out-{run}.json")},
        },
    }


def _deploy(m: Mocker, *, orders_checks: Any = None) -> None:
    m.get(f"{BASE}/openapi.json", json={"info": {"version": "test"}, "paths": {}})
    m.get(f"{BASE}/datastores", json=_page([DATASTORE]))
    m.get(f"{BASE}/containers", json=_page([ORDERS, CUSTOMERS]))
    m.get(f"{BASE}/anomalies", json=_page([]))
    m.get(
        f"{BASE}/quality-checks?container=10",
        **(orders_checks or {"json": _page(ORDERS_CHECKS)}),
    )
    m.get(f"{BASE}/quality-checks?container=11", json=_page([CUSTOMERS_CHECK]))


def _run(tmp_path: Path, run: int, **config: Any) -> Pipeline:
    pipeline = Pipeline.create(_recipe(tmp_path, run, **config))
    pipeline.run()
    return pipeline


def _soft_deleted(tmp_path: Path, run: int) -> list[str]:
    records = json.loads((tmp_path / f"out-{run}.json").read_text())
    return [
        r["entityUrn"]
        for r in records
        if r.get("aspectName") == "status" and r["aspect"]["json"].get("removed")
    ]


def _assertion_urns(tmp_path: Path, run: int) -> set[str]:
    records = json.loads((tmp_path / f"out-{run}.json").read_text())
    return {r["entityUrn"] for r in records if r.get("aspectName") == "assertionInfo"}


def test_a_container_dropped_from_scope_removes_exactly_its_assertions(
    tmp_path: Path,
) -> None:
    with Mocker() as m:
        _deploy(m)
        first = _run(tmp_path, 1)
    first.raise_from_status()
    all_assertions = _assertion_urns(tmp_path, 1)

    with Mocker() as m:
        _deploy(m)
        _run(tmp_path, 2, container_pattern={"deny": ["^ORDERS$"]})

    removed = _soft_deleted(tmp_path, 2)
    kept = _assertion_urns(tmp_path, 2)
    # Exactly once each: two removal processors used to run.
    assert sorted(removed) == sorted(set(removed))
    assert set(removed) == all_assertions - kept
    assert len(removed) == len(ORDERS_CHECKS)
    assert not any(urn.startswith("urn:li:dataset:") for urn in removed)


def test_a_failed_listing_deletes_nothing(tmp_path: Path) -> None:
    with Mocker() as m:
        _deploy(m)
        _run(tmp_path, 1).raise_from_status()

    with Mocker() as m:
        _deploy(m, orders_checks={"status_code": 500, "text": "boom"})
        second = _run(tmp_path, 2)

    assert _soft_deleted(tmp_path, 2) == []
    # Reported as a failure: that is what makes the stale-entity handler stand down,
    # and what makes the run's exit status say it was incomplete.
    assert any(
        "container" in str(f).lower() for f in second.source.get_report().failures
    )


def test_an_unparseable_check_deletes_nothing(tmp_path: Path) -> None:
    with Mocker() as m:
        _deploy(m)
        _run(tmp_path, 1).raise_from_status()

    malformed = {k: v for k, v in CHECK.items() if k != "rule_type"}
    with Mocker() as m:
        _deploy(m, orders_checks={"json": _page([malformed, {**CHECK, "id": 101}])})
        _run(tmp_path, 2)

    assert _soft_deleted(tmp_path, 2) == []
