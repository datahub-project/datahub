"""Behavioural tests for merging dbt Cloud Discovery API rows across jobs.

The job-scoped Discovery query returns the whole manifest for models, seeds
and snapshots. Nodes the run did not execute come back as "padding" rows
with ``status: null`` and no timings; tests are run-scoped and have no
padding. These tests pin the rules by which one copy per node is kept.
"""

from typing import Any, Dict, List, Optional

from datahub.ingestion.source.dbt.dbt_cloud import _merge_raw_nodes, _MergedRawNodes


def _padding(
    unique_id: str,
    *,
    job_id: int,
    run_id: int,
    resource_type: str = "model",
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A non-executed row, shaped exactly as dbt Cloud returns it."""
    return {
        "uniqueId": unique_id,
        "name": unique_id.rsplit(".", 1)[-1],
        "resourceType": resource_type,
        "jobId": job_id,
        "runId": run_id,
        "meta": meta or {},
        "status": None,
        "skip": False,
        "error": None,
        "executeStartedAt": None,
        "executeCompletedAt": None,
        "runGeneratedAt": None,
        "compiledCode": None,
        "materializedType": "table",
    }


def _built(
    unique_id: str,
    *,
    job_id: int,
    run_id: int,
    completed_at: Optional[str] = "2025-01-01T00:00:00Z",
    started_at: Optional[str] = None,
    resource_type: str = "model",
    status: str = "success",
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        **_padding(
            unique_id,
            job_id=job_id,
            run_id=run_id,
            resource_type=resource_type,
            meta=meta,
        ),
        "status": status,
        "executeStartedAt": started_at,
        "executeCompletedAt": completed_at,
        "runGeneratedAt": "2024-12-31T23:00:00Z",
        "compiledCode": "select 1",
    }


def _test_row(
    unique_id: str, *, job_id: int, run_id: int, status: str
) -> Dict[str, Any]:
    return {
        "uniqueId": unique_id,
        "name": unique_id.rsplit(".", 1)[-1],
        "resourceType": "test",
        "jobId": job_id,
        "runId": run_id,
        "status": status,
        "skip": False,
        "executeCompletedAt": "2025-01-01T00:00:00Z",
    }


def _source(
    unique_id: str,
    *,
    job_id: int,
    run_id: int,
    snapshotted_at: Optional[str],
    max_loaded_at: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "uniqueId": unique_id,
        "name": unique_id.rsplit(".", 1)[-1],
        "resourceType": "source",
        "jobId": job_id,
        "runId": run_id,
        "snapshottedAt": snapshotted_at,
        "maxLoadedAt": max_loaded_at,
    }


def _exposure(unique_id: str, *, job_id: int, run_id: int) -> Dict[str, Any]:
    return {
        "uniqueId": unique_id,
        "name": unique_id.rsplit(".", 1)[-1],
        "resourceType": "exposure",
        "jobId": job_id,
        "runId": run_id,
    }


def _merge(
    rows: List[Dict[str, Any]], exposures: Optional[List[Dict[str, Any]]] = None
) -> _MergedRawNodes:
    return _merge_raw_nodes(raw_nodes=rows, raw_exposures=exposures or [])


def _winner_job(merged: _MergedRawNodes, unique_id: str) -> int:
    (node,) = [n for n in merged.nodes if n["uniqueId"] == unique_id]
    return node["jobId"]


def test_built_copy_beats_padding_copy_regardless_of_order() -> None:
    model = "model.my_pkg.orders"
    # The padding copy comes from a newer run and is listed last: both of
    # the things the old last-wins merge got wrong.
    rows = [
        _built(model, job_id=1, run_id=10, meta={"owner": "team_a"}),
        _padding(model, job_id=2, run_id=20, meta={"owner": "stale"}),
    ]

    merged = _merge(rows)

    assert len(merged.nodes) == 1
    assert merged.nodes[0]["meta"] == {"owner": "team_a"}
    assert _winner_job(_merge(list(reversed(rows))), model) == 1


def test_latest_execution_wins_among_builders() -> None:
    model = "model.my_pkg.orders"
    rows = [
        _built(model, job_id=1, run_id=30, completed_at="2025-01-02T00:00:00Z"),
        _built(model, job_id=2, run_id=20, completed_at="2025-01-03T00:00:00Z"),
        _built(model, job_id=3, run_id=10, completed_at="2025-01-01T00:00:00Z"),
    ]

    assert _winner_job(_merge(rows), model) == 2


def test_execute_started_at_is_the_fallback_execution_time() -> None:
    model = "model.my_pkg.orders"
    rows = [
        _built(model, job_id=1, run_id=10, completed_at="2025-01-01T00:00:00Z"),
        _built(
            model,
            job_id=2,
            run_id=20,
            completed_at=None,
            started_at="2025-01-05T00:00:00Z",
        ),
    ]

    assert _winner_job(_merge(rows), model) == 2


def test_all_padding_nodes_survive_and_take_the_newest_run() -> None:
    """Seeds and snapshots are routinely never executed by any job."""
    seed = "seed.my_pkg.countries"
    snapshot = "snapshot.my_pkg.orders_snapshot"
    rows = [
        _padding(seed, job_id=1, run_id=10, resource_type="seed"),
        _padding(seed, job_id=2, run_id=30, resource_type="seed"),
        _padding(seed, job_id=3, run_id=20, resource_type="seed"),
        _padding(snapshot, job_id=1, run_id=10, resource_type="snapshot"),
        _padding(snapshot, job_id=2, run_id=30, resource_type="snapshot"),
    ]

    merged = _merge(rows)

    assert {n["uniqueId"] for n in merged.nodes} == {seed, snapshot}
    assert _winner_job(merged, seed) == 2
    assert _winner_job(merged, snapshot) == 2
    assert sorted(merged.not_built_by_any_job) == sorted([seed, snapshot])


def test_job_with_no_rows_changes_nothing() -> None:
    model = "model.my_pkg.orders"
    only_here = "model.my_pkg.only_in_old_job"
    rows = [
        _built(model, job_id=1, run_id=10),
        # Job 2's whole batch is padding; it is still the only source of
        # one model, which must not be dropped.
        _padding(model, job_id=2, run_id=20),
        _padding(only_here, job_id=2, run_id=20),
    ]

    merged = _merge(rows)

    assert {n["uniqueId"] for n in merged.nodes} == {model, only_here}
    assert _winner_job(merged, model) == 1
    assert merged.not_built_by_any_job == [only_here]


def test_test_rows_keep_every_copy() -> None:
    test_id = "test.my_pkg.not_null_orders_id"
    rows = [
        _test_row(test_id, job_id=1, run_id=10, status="pass"),
        _test_row(test_id, job_id=2, run_id=20, status="fail"),
    ]

    merged = _merge(rows)

    (kept,) = merged.nodes
    assert kept["jobId"] == 2
    assert [c["jobId"] for c in merged.extra_test_copies] == [1]


def test_exposures_are_deduplicated() -> None:
    exposure = "exposure.my_pkg.weekly_report"
    exposures = [
        _exposure(exposure, job_id=1, run_id=10),
        _exposure(exposure, job_id=2, run_id=20),
    ]

    merged = _merge([], exposures)

    assert [e["jobId"] for e in merged.exposures] == [2]
    assert merged.exposures_deduplicated == 1


def test_sources_rank_by_freshness_then_run() -> None:
    fresh = "source.my_pkg.raw.events"
    unchecked = "source.my_pkg.raw.users"
    rows = [
        _source(fresh, job_id=1, run_id=30, snapshotted_at="2025-01-03T00:00:00Z"),
        _source(fresh, job_id=2, run_id=10, snapshotted_at="2025-01-05T00:00:00Z"),
        _source(unchecked, job_id=1, run_id=30, snapshotted_at=None),
        _source(unchecked, job_id=2, run_id=10, snapshotted_at=None),
    ]

    merged = _merge(rows)

    assert _winner_job(merged, fresh) == 2
    assert _winner_job(merged, unchecked) == 1


def test_merge_stats() -> None:
    model = "model.my_pkg.orders"
    seed = "seed.my_pkg.countries"
    rows = [
        _built(model, job_id=1, run_id=10),
        _padding(model, job_id=2, run_id=20),
        _padding(model, job_id=3, run_id=30),
        _padding(seed, job_id=1, run_id=10, resource_type="seed"),
        _padding(seed, job_id=2, run_id=20, resource_type="seed"),
        _padding(seed, job_id=3, run_id=30, resource_type="seed"),
        _built("model.my_pkg.single", job_id=1, run_id=10),
    ]

    merged = _merge(rows)

    assert merged.copies_seen == 7
    assert merged.nodes_merged == 2
    # Two padding copies lost to the built copy, two to the newest padding copy.
    assert merged.padding_dropped == 4
    assert merged.not_built_by_any_job == [seed]


def test_ephemeral_models_are_never_reported_as_unbuilt() -> None:
    """Ephemeral models have no build status in any run."""
    model = "model.my_pkg.stg_orders"
    rows = [
        {**_padding(model, job_id=1, run_id=10), "materializedType": "ephemeral"},
        {**_padding(model, job_id=2, run_id=20), "materializedType": "ephemeral"},
    ]

    merged = _merge(rows)

    assert _winner_job(merged, model) == 2
    assert merged.not_built_by_any_job == []
    assert merged.padding_dropped == 0


def test_skipped_node_is_a_real_result_not_padding() -> None:
    """dbt marks skipped nodes with a status and skip=true; padding has neither."""
    model = "model.my_pkg.orders"
    rows = [
        _built(
            model,
            job_id=1,
            run_id=10,
            status="skipped",
            completed_at=None,
        ),
        _padding(model, job_id=2, run_id=20),
    ]
    rows[0]["skip"] = True

    merged = _merge(rows)

    assert _winner_job(merged, model) == 1
    assert merged.padding_dropped == 1


def test_untimed_test_copies_are_not_merged_with_timed_ones() -> None:
    """A result without a run time cannot be ordered, so it must not be
    merged next to one that can, where it would be stamped with ingestion
    time and pass for the latest."""
    test_id = "test.my_pkg.not_null_orders_id"
    timed = _test_row(test_id, job_id=1, run_id=10, status="fail")
    untimed = _test_row(test_id, job_id=2, run_id=20, status="pass")
    untimed["executeCompletedAt"] = None

    merged = _merge([timed, untimed])

    (kept,) = merged.nodes
    assert kept["jobId"] == 1
    assert merged.extra_test_copies == []
    assert merged.untimed_test_copies_dropped == 1


def test_all_untimed_test_copies_keep_only_the_newest_run() -> None:
    test_id = "test.my_pkg.not_null_orders_id"
    rows = [
        {
            **_test_row(test_id, job_id=1, run_id=10, status="fail"),
            "executeCompletedAt": None,
        },
        {
            **_test_row(test_id, job_id=2, run_id=20, status="pass"),
            "executeCompletedAt": None,
        },
    ]

    merged = _merge(rows)

    (kept,) = merged.nodes
    assert kept["jobId"] == 2
    assert merged.extra_test_copies == []
    assert merged.untimed_test_copies_dropped == 1


def test_execution_times_compare_across_timezone_offsets() -> None:
    model = "model.my_pkg.orders"
    rows = [
        # 02:00+02:00 is 00:00Z, so the UTC copy below is the later one.
        _built(model, job_id=1, run_id=20, completed_at="2025-01-01T02:00:00+02:00"),
        _built(model, job_id=2, run_id=10, completed_at="2025-01-01T01:00:00Z"),
    ]

    assert _winner_job(_merge(rows), model) == 2
