#!/usr/bin/env python3
"""Assign Gradle test tasks to one CI shard, balanced by historical duration.

Vendored (not a third-party dependency) so DataHub owns the shard logic on its critical CI
path. Stdlib-only (py3.9+), no venv. Run from / pass the repo root so globs resolve
repo-relative.

Mirrors the existing pytest sharding convention (smoke-test/conftest.py + #18270): a
committed weights snapshot (gradle_test_weights.json, produced by generate_test_weights.py)
maps FQCN -> median seconds; whole test modules are bin-packed across shards.

HYBRID granularity — the improvement over pure module-level:
  * A normal module runs WHOLE on one shard: `:module:test` (one JVM/Spring init, robust —
    no per-class discovery needed, no failOnNoMatchingTests risk).
  * A "whale" module (weight > ideal shard load) is CLASS-SPLIT across shards:
    `:module:test --tests A --tests B`, so no single 300-class module bounds the slowest
    shard. metadata-io is already isolated in its own workflow; graphql-core is the whale here.
Whole-vs-split is decided by a threshold (ideal_load = total_weight / shards), so it
auto-adapts — no hardcoded whale list to rot.

Within a shard, Gradle's maxParallelForks parallelizes whatever classes land there.

Output:
  * stdout: JSON plan {hasTests, tasks:[{task, tests}], diagnostics}.
  * --output-args FILE: gradle args one-per-line (empty if the shard is empty) for
    `mapfile -t ARGS < FILE; gradle "${ARGS[@]}"` — no shell word-splitting.

Test identity is (gradle-project, class); duplicate class names across modules are both
scheduled. Weight lookup is by FQCN, so two modules sharing a class name share an estimate
(a minor weighting approximation, not a correctness issue).

LIMITATION (v1): whale class discovery assumes conventional one-public-test-class-per-file
(FQCN mirrors path). Whole modules are immune (they run `:module:test` outright).
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass

_SOURCE_SET_MARKERS = ("/src/test/java/", "/src/test/kotlin/", "/src/test/groovy/")


@dataclass(frozen=True, order=True)
class TestId:
    project: str  # gradle project path, e.g. ":metadata-service:services" (":" = root)
    class_name: str

    @property
    def task_path(self) -> str:
        return ":test" if self.project == ":" else f"{self.project}:test"


@dataclass
class Unit:
    """A schedulable unit: a whole module, or one class carved out of a whale."""
    weight: float
    project: str
    classes: list[str]  # class names; empty => run the whole module task
    whole: bool


def _canonical(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def _resolve_glob(pattern: str, repo_root: str) -> str:
    return pattern if os.path.isabs(pattern) else os.path.join(repo_root, pattern)


def _repo_relative(path: str, repo_root: str) -> str | None:
    rel = os.path.relpath(os.path.abspath(path), repo_root)
    if rel == ".." or rel.startswith(".." + os.sep):
        return None
    return rel.replace(os.sep, "/")


def _project_from_module_dir(module_rel: str) -> str:
    module_rel = module_rel.strip("/")
    return ":" if module_rel == "" else ":" + module_rel.replace("/", ":")


def path_to_test(path: str, repo_root: str) -> TestId | None:
    rel = _repo_relative(path, repo_root)
    if rel is None:
        return None
    normalized = "/" + rel.lstrip("/")
    for marker in _SOURCE_SET_MARKERS:
        idx = normalized.find(marker)
        if idx == -1:
            continue
        project = _project_from_module_dir(normalized[:idx])
        fqcn = os.path.splitext(normalized[idx + len(marker):])[0].replace("/", ".")
        return TestId(project, fqcn)
    return None


def discover_tests(globs: list[str], exclude_globs: list[str], repo_root: str) -> set[TestId]:
    excluded = {
        _canonical(p)
        for pattern in exclude_globs
        for p in glob.glob(_resolve_glob(pattern, repo_root), recursive=True)
    }
    tests: set[TestId] = set()
    for pattern in globs:
        for path in glob.glob(_resolve_glob(pattern, repo_root), recursive=True):
            if not os.path.isfile(path) or _canonical(path) in excluded:
                continue
            test_id = path_to_test(path, repo_root)
            if test_id is not None:
                tests.add(test_id)
    return tests


def load_weights(weights_path: str | None) -> dict[str, float]:
    """FQCN -> median seconds, from the committed gradle_test_weights.json."""
    if not weights_path or not os.path.isfile(weights_path):
        return {}
    try:
        with open(weights_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    weights: dict[str, float] = {}
    for item in data:
        test_id = item.get("testId")
        duration = item.get("duration", "")
        if not test_id or not isinstance(duration, str) or not duration.endswith("s"):
            continue
        try:
            weights[test_id] = float(duration[:-1])
        except ValueError:
            continue
    return weights


def build_weight_of(tests: set[TestId], weights: dict[str, float]):
    known = [weights[t.class_name] for t in tests if t.class_name in weights]
    fallback = statistics.median(known) if known else 1.0

    def weight_of(t: TestId) -> float:
        return weights.get(t.class_name, fallback)

    return weight_of


def build_units(tests: set[TestId], weight_of, ideal_load: float) -> list[Unit]:
    by_module: dict[str, list[TestId]] = defaultdict(list)
    for t in tests:
        by_module[t.project].append(t)

    units: list[Unit] = []
    for project, tids in by_module.items():
        module_weight = sum(weight_of(t) for t in tids)
        # Split a module only if it alone would exceed a shard's fair share AND can be split.
        if module_weight > ideal_load and len(tids) > 1:
            for t in tids:
                units.append(Unit(weight_of(t), project, [t.class_name], whole=False))
        else:
            classes = sorted(t.class_name for t in tids)
            units.append(Unit(module_weight, project, classes, whole=True))
    return units


def bin_pack(units: list[Unit], total: int) -> list[list[Unit]]:
    buckets: list[list[Unit]] = [[] for _ in range(total)]
    load = [0.0] * total
    # Longest-processing-time: heaviest unit into the lightest bucket. Deterministic tiebreak.
    for unit in sorted(units, key=lambda u: (-u.weight, u.project, u.classes)):
        target = min(range(total), key=lambda b: load[b])
        buckets[target].append(unit)
        load[target] += unit.weight
    return buckets


def plan_for_shard(bucket: list[Unit]) -> dict:
    whole_modules: set[str] = set()
    partial_classes: dict[str, set[str]] = defaultdict(set)
    for unit in bucket:
        if unit.whole:
            whole_modules.add(unit.project)
        else:
            partial_classes[unit.project].update(unit.classes)

    tasks: list[dict] = []
    for project in sorted(whole_modules):
        # Whole module -> bare `:project:test` (runs everything, no --tests, robust).
        tasks.append({"task": _task_path(project), "tests": []})
    for project in sorted(partial_classes):
        tasks.append({"task": _task_path(project), "tests": sorted(partial_classes[project])})

    classes = sum(len(u.classes) for u in bucket)
    return {
        "hasTests": bool(bucket),
        "tasks": tasks,
        "diagnostics": {
            "classes": classes,
            "modules": len(whole_modules) + len(partial_classes),
            "wholeModules": len(whole_modules),
            "splitModules": len(partial_classes),
            "predictedSeconds": round(sum(u.weight for u in bucket), 1),
        },
    }


def _task_path(project: str) -> str:
    return ":test" if project == ":" else f"{project}:test"


def gradle_args(plan: dict) -> list[str]:
    args: list[str] = []
    for task in plan["tasks"]:
        args.append(task["task"])
        for cls in task["tests"]:
            args += ["--tests", cls]
    return args


def main() -> int:
    parser = argparse.ArgumentParser(description="Assign Gradle test tasks to one CI shard.")
    parser.add_argument("--split-index", "-i", type=int, required=True)
    parser.add_argument("--split-total", "-t", type=int, required=True)
    parser.add_argument("--glob", "-g", action="append", required=True)
    parser.add_argument("--exclude-glob", "-e", action="append", default=[])
    parser.add_argument("--weights", help="Path to committed gradle_test_weights.json.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-args", help="Write gradle args one-per-line to this file.")
    args = parser.parse_args()

    if args.split_total < 1:
        parser.error("--split-total must be >= 1")
    if not (0 <= args.split_index < args.split_total):
        parser.error("--split-index must be in [0, --split-total)")
    repo_root = os.path.abspath(args.repo_root)

    tests = discover_tests(args.glob, args.exclude_glob, repo_root)
    if not tests:
        print("split_gradle_tests: no test classes matched", file=sys.stderr)

    weights = load_weights(args.weights)
    weight_of = build_weight_of(tests, weights)
    total_weight = sum(weight_of(t) for t in tests)
    ideal_load = total_weight / args.split_total if args.split_total else total_weight

    matched = sum(1 for t in tests if t.class_name in weights)
    print(
        f"split_gradle_tests: {len(tests)} classes, {matched} with recorded weights "
        f"({'timing-balanced' if weights else 'even fallback'}); "
        f"ideal shard load {round(ideal_load, 1)}s",
        file=sys.stderr,
    )

    units = build_units(tests, weight_of, ideal_load)
    buckets = bin_pack(units, args.split_total)
    plan = plan_for_shard(buckets[args.split_index])

    d = plan["diagnostics"]
    print(
        f"split_gradle_tests: shard {args.split_index}/{args.split_total} -> "
        f"{d['classes']} classes, {d['wholeModules']} whole + {d['splitModules']} split "
        f"modules, predicted {d['predictedSeconds']}s",
        file=sys.stderr,
    )

    if args.output_args is not None:
        with open(args.output_args, "w", encoding="utf-8") as fh:
            fh.write("\n".join(gradle_args(plan)))
    print(json.dumps(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
