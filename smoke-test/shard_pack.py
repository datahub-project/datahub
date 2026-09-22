"""Loadscope-aware packing of smoke-test items into CI batches.

Phase 1 runs under pytest-xdist ``--dist=loadscope``: a class stays on one
worker, and loose functions stay together by module. Phase 2 then runs
mutators serially on that same batch. Predicted wall clock is therefore
``max(N worker loads) + serial_sum``, not ``sum(parallel) / N + serial``.

Each ``ModuleShard.path`` is a loadscope key (``file.py::Class`` or
``file.py``), not necessarily a whole file.

Weight lookup maps pytest nodeids onto the JUnit ``classname::name`` keys in
pytest_test_weights.json so class-based tests do not pack at the default.

``plan_collected_items`` is the shared entry point for live CI batching
(conftest) and the opt-in dump plugin so both cannot drift.
"""

from __future__ import annotations

import json
import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_WEIGHTS_DIR = Path(__file__).resolve().parent
DEFAULT_TEST_WEIGHT = 1.0
# Collection-time skip/skipif tests never run, so they must not take the median
# default (or a historical duration) and inflate a batch.
SKIPPED_TEST_WEIGHT_SECONDS = 0.01


@dataclass(frozen=True)
class ModuleShard:
    path: str
    parallel_seconds: float
    serial_seconds: float

    @property
    def total_seconds(self) -> float:
        return self.parallel_seconds + self.serial_seconds


@dataclass
class BatchPlan:
    module_paths: list[str] = field(default_factory=list)
    phase1_makespan: float = 0.0
    serial_seconds: float = 0.0

    @property
    def predicted_wall(self) -> float:
        return self.phase1_makespan + self.serial_seconds


@dataclass
class _BatchState:
    workers: list[float]
    serial: float = 0.0
    paths: list[str] = field(default_factory=list)

    @property
    def phase1_makespan(self) -> float:
        return max(self.workers) if self.workers else 0.0


def _lightest_worker_index(workers: list[float]) -> int:
    lightest = 0
    for i, load in enumerate(workers):
        if load <= workers[lightest]:
            lightest = i
    return lightest


def _add_parallel(workers: list[float], seconds: float) -> None:
    if seconds:
        workers[_lightest_worker_index(workers)] += seconds


def _predicted_after_add(batch: _BatchState, module: ModuleShard) -> float:
    if not module.parallel_seconds:
        return batch.phase1_makespan + batch.serial + module.serial_seconds
    workers = list(batch.workers)
    _add_parallel(workers, module.parallel_seconds)
    return max(workers) + batch.serial + module.serial_seconds


def _best_batch_index(batches: list[_BatchState], module: ModuleShard) -> int:
    best_idx = 0
    best_key: tuple[float, int, int] | None = None
    for idx, batch in enumerate(batches):
        # Fewest modules, then highest batch index, so leftovers do not pile
        # onto shard 0.
        key = (_predicted_after_add(batch, module), len(batch.paths) + 1, -idx)
        if best_key is None or key < best_key:
            best_key = key
            best_idx = idx
    return best_idx


def pack_module_plans(
    modules: list[ModuleShard],
    batch_count: int,
    xdist_workers: int,
) -> list[BatchPlan]:
    if batch_count < 1:
        raise ValueError(f"batch_count must be >= 1, got {batch_count}")
    worker_count = max(1, xdist_workers)
    batches = [_BatchState(workers=[0.0] * worker_count) for _ in range(batch_count)]

    for module in sorted(modules, key=lambda m: (-m.total_seconds, m.path)):
        chosen = batches[_best_batch_index(batches, module)]
        _add_parallel(chosen.workers, module.parallel_seconds)
        chosen.serial += module.serial_seconds
        chosen.paths.append(module.path)

    return [
        BatchPlan(
            module_paths=list(batch.paths),
            phase1_makespan=batch.phase1_makespan,
            serial_seconds=batch.serial,
        )
        for batch in batches
    ]


def pack_modules(
    modules: list[ModuleShard],
    batch_count: int,
    xdist_workers: int,
) -> list[list[str]]:
    return [
        plan.module_paths
        for plan in pack_module_plans(modules, batch_count, xdist_workers)
    ]


def loadscope_key(nodeid: str) -> str:
    """Return the xdist loadscope id: drop the last ``::`` segment of *nodeid*."""
    return nodeid.replace("\\", "/").rsplit("::", 1)[0]


def nodeid_to_weight_keys(nodeid: str) -> list[str]:
    """Return candidate weight keys for a pytest nodeid, first match wins.

    generate_test_weights.py stores JUnit ``{classname}::{name}``. Class tests
    therefore look like ``tests.foo.BarTest::test_it``, while function tests
    look like ``tests.foo::test_it``. The nodeid uses ``::`` before the class
    name, so both spellings (and a class-stripped fallback) are tried.
    """
    posix = nodeid.replace("\\", "/")
    dotted = posix.replace("/", ".")
    keys = [
        dotted.replace(".py::", "::"),
        dotted.replace(".py::", "."),
    ]
    parts = posix.split("::")
    if len(parts) > 2:
        module = parts[0].replace("/", ".").removesuffix(".py")
        keys.append(f"{module}::{parts[-1]}")
    return list(dict.fromkeys(keys))


def lookup_test_weight(
    nodeid: str, test_weights: dict[str, float], default_weight: float
) -> tuple[float, bool]:
    """Return (seconds, used_default) for *nodeid* against *test_weights*."""
    for test_id in nodeid_to_weight_keys(nodeid):
        if test_id in test_weights:
            return test_weights[test_id], False
    return default_weight, True


def load_pytest_test_weights() -> dict[str, float]:
    """Load pytest_test_weights.json from the smoke-test directory."""
    weights_file = _WEIGHTS_DIR / "pytest_test_weights.json"
    if not weights_file.exists():
        return {}
    try:
        with open(weights_file) as f:
            weights_data = json.load(f)
        return {item["testId"]: float(item["duration"][:-1]) for item in weights_data}
    except Exception as e:
        logger.warning("Failed to load pytest test weights: %s", e)
        return {}


def load_persisted_default_weight() -> float | None:
    """Load the fallback weight generated alongside the pytest weights."""
    meta_file = _WEIGHTS_DIR / "pytest_test_weights_meta.json"
    if not meta_file.exists():
        return None
    try:
        value = float(json.loads(meta_file.read_text())["defaultTestWeightSeconds"])
        return value if value > 0 else None
    except Exception as e:
        logger.warning("Failed to read %s: %s", meta_file.name, e)
        return None


def compute_default_test_weight(test_weights: dict[str, float]) -> float:
    """Return the weight assigned to tests absent from the weights file."""
    persisted = load_persisted_default_weight()
    if persisted is not None:
        return persisted
    if not test_weights:
        return DEFAULT_TEST_WEIGHT
    return statistics.median(test_weights.values())


def is_global_policy_mutator(item: Any) -> bool:
    return item.get_closest_marker("global_policy_mutator") is not None


def _item_will_be_skipped(item: Any) -> bool:
    try:
        from _pytest.skipping import evaluate_skip_marks

        return evaluate_skip_marks(item) is not None
    except Exception:
        return item.get_closest_marker("skip") is not None


def get_pytest_test_weight(
    item: Any, test_weights: dict[str, float], default_weight: float
) -> tuple[float, bool]:
    """Return (seconds, used_default). Collection-time skips use a tiny weight."""
    if _item_will_be_skipped(item):
        return SKIPPED_TEST_WEIGHT_SECONDS, False
    return lookup_test_weight(item.nodeid, test_weights, default_weight)


@dataclass
class PackedSuite:
    """Result of grouping collected items into loadscopes and packing batches."""

    plans: list[BatchPlan]
    items_by_scope: dict[str, list[Any]]
    shards: list[ModuleShard]
    missing_weight_ids: list[str]
    default_weight: float


def plan_collected_items(
    items: Sequence[Any],
    batch_count: int,
    xdist_workers: int,
    test_weights: dict[str, float] | None = None,
) -> PackedSuite:
    """Weight collected pytest items and pack them into ``batch_count`` batches.

    Used by conftest (live CI slice) and the dump plugin (full-plan JSON).
    """
    if test_weights is None:
        test_weights = load_pytest_test_weights()
    default_weight = compute_default_test_weight(test_weights)

    scopes: dict[str, list[Any]] = defaultdict(list)
    for item in items:
        scopes[loadscope_key(item.nodeid)].append(item)

    items_by_scope: dict[str, list[Any]] = {}
    shards: list[ModuleShard] = []
    missing_weight_ids: list[str] = []
    for scope_key, scope_items in scopes.items():
        parallel_seconds = 0.0
        serial_seconds = 0.0
        for item in scope_items:
            weight, used_default = get_pytest_test_weight(
                item, test_weights, default_weight
            )
            if used_default:
                missing_weight_ids.append(item.nodeid)
            if is_global_policy_mutator(item):
                serial_seconds += weight
            else:
                parallel_seconds += weight
        items_by_scope[scope_key] = scope_items
        shards.append(ModuleShard(scope_key, parallel_seconds, serial_seconds))

    if missing_weight_ids:
        logger.info(
            "No recorded duration for %s test(s); packing with %.1fs each. Sample: %s",
            len(missing_weight_ids),
            default_weight,
            ", ".join(missing_weight_ids[:5]),
        )

    plans = pack_module_plans(shards, batch_count, xdist_workers)
    return PackedSuite(
        plans=plans,
        items_by_scope=items_by_scope,
        shards=shards,
        missing_weight_ids=missing_weight_ids,
        default_weight=default_weight,
    )


def items_for_batch(packed: PackedSuite, batch_number: int) -> list[Any]:
    """Return collected items assigned to ``batch_number``."""
    selected: list[Any] = []
    for scope_key in packed.plans[batch_number].module_paths:
        selected.extend(packed.items_by_scope[scope_key])
    return selected
