"""Loadscope-aware packing of smoke-test modules into CI batches.

Phase 1 runs under pytest-xdist ``--dist=loadscope``, so a file stays on one
worker. Phase 2 then runs mutators serially on that same batch. Predicted wall
clock is therefore ``max(N worker loads) + serial_sum``, not
``sum(parallel) / N + serial``.

Weight lookup maps pytest nodeids onto the JUnit ``classname::name`` keys in
pytest_test_weights.json so class-based tests do not pack at the default.
"""

from __future__ import annotations

from dataclasses import dataclass, field


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
