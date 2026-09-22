"""CPU-only tests for loadscope-aware CI batch packing."""

from __future__ import annotations

import pytest

from shard_pack import ModuleShard, pack_module_plans, pack_modules
from utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.PLATFORM_INTERNAL)


def _batch_of(plans, path: str) -> int:
    for i, plan in enumerate(plans):
        if path in plan.module_paths:
            return i
    raise AssertionError(f"{path} was not assigned to any batch")


def test_pack_modules_rejects_empty_batch_count() -> None:
    with pytest.raises(ValueError, match="batch_count"):
        pack_modules([], 0, 3)


def test_n1_matches_greedy_lpt_on_total_seconds() -> None:
    modules = [
        ModuleShard("a", 40.0, 5.0),
        ModuleShard("b", 30.0, 0.0),
        ModuleShard("c", 0.0, 25.0),
        ModuleShard("d", 20.0, 2.0),
        ModuleShard("e", 10.0, 10.0),
        ModuleShard("f", 8.0, 0.0),
    ]
    batch_count = 3
    plans = pack_module_plans(modules, batch_count, xdist_workers=1)

    loads = [0.0] * batch_count
    counts = [0] * batch_count
    expected: list[list[str]] = [[] for _ in range(batch_count)]
    for module in sorted(modules, key=lambda m: (-m.total_seconds, m.path)):
        best_idx = 0
        best_key: tuple[float, int, int] | None = None
        for idx in range(batch_count):
            wall = loads[idx] + module.total_seconds
            key = (wall, counts[idx] + 1, -idx)
            if best_key is None or key < best_key:
                best_key = key
                best_idx = idx
        loads[best_idx] += module.total_seconds
        counts[best_idx] += 1
        expected[best_idx].append(module.path)

    assert [plan.module_paths for plan in plans] == expected
    for plan, load in zip(plans, loads, strict=True):
        assert plan.predicted_wall == pytest.approx(load)


def test_long_parallel_module_not_packed_with_long_serial_mutator() -> None:
    """Regression: run 35331067548 co-located these and 2x'd batch 1."""
    modules = [
        ModuleShard(
            "tests/e2e/entity_graph_cache/test_entity_graph_cache.py", 833.0, 0.0
        ),
        ModuleShard(
            "tests/e2e/authorization/test_domain_scoped_create_entity_auth.py",
            0.0,
            536.0,
        ),
        *[ModuleShard(f"tests/e2e/filler_{i}.py", 40.0, 0.0) for i in range(20)],
    ]
    plans = pack_module_plans(modules, batch_count=7, xdist_workers=3)
    parallel_batch = _batch_of(
        plans, "tests/e2e/entity_graph_cache/test_entity_graph_cache.py"
    )
    serial_batch = _batch_of(
        plans, "tests/e2e/authorization/test_domain_scoped_create_entity_auth.py"
    )
    assert parallel_batch != serial_batch


def test_equal_small_modules_do_not_pile_onto_batch_zero() -> None:
    modules = [ModuleShard(f"m{i:03d}", 10.0, 0.0) for i in range(70)]
    plans = pack_module_plans(modules, batch_count=7, xdist_workers=3)
    walls = [plan.predicted_wall for plan in plans]
    counts = [len(plan.module_paths) for plan in plans]
    assert max(walls) - min(walls) <= 10.0
    assert counts[0] == min(counts)


def test_oversized_module_sets_wall_floor_without_a_second_giant() -> None:
    modules = [
        ModuleShard("giant", 1000.0, 0.0),
        ModuleShard("other_giant", 0.0, 900.0),
        *[ModuleShard(f"small_{i}", 20.0, 0.0) for i in range(15)],
    ]
    plans = pack_module_plans(modules, batch_count=7, xdist_workers=3)
    giant_batch = _batch_of(plans, "giant")
    other_batch = _batch_of(plans, "other_giant")
    assert giant_batch != other_batch
    assert plans[giant_batch].predicted_wall == pytest.approx(1000.0)
    assert max(plan.predicted_wall for plan in plans) == pytest.approx(1000.0)


def test_timeline_classes_pack_by_class_makespan_not_file_sum() -> None:
    """Four classes in one file must not pack as a single 885s leftover shard."""
    path = "tests/e2e/timeline/timeline_change_history_test.py"
    shards = [
        ModuleShard(f"{path}::{cls}", seconds, 0.0)
        for cls, seconds in (
            ("TestDatasetTimeline", 303.2),
            ("TestDataProductTimeline", 295.9),
            ("TestGlossaryTermTimeline", 167.5),
            ("TestDomainTimeline", 118.7),
        )
    ]
    assert sum(s.parallel_seconds for s in shards) == pytest.approx(885.3)
    plans = pack_module_plans(shards, batch_count=7, xdist_workers=3)
    walls = [plan.predicted_wall for plan in plans]
    assert max(walls) == pytest.approx(303.2)
    assigned = {path for plan in plans for path in plan.module_paths}
    assert assigned == {s.path for s in shards}
