#!/usr/bin/env python3
"""Assign whole Gradle test modules to one CI shard, balanced by historical duration.

Vendored + stdlib-only (py3.9+). Reads a committed weights snapshot (backend_test_weights.json,
FQCN -> median seconds from generate_test_weights.py) and LPT bin-packs whole modules across
shards. Run from / pass the repo root so globs resolve repo-relative.

Module-level by design: each shard runs bare `:module:test` tasks, never `--tests` filters, so
it runs every test Gradle finds regardless of class naming — discovery here only estimates
weight. Falls back to an even split when no weights exist.

Emits a JSON plan on stdout; with --output-args FILE, also writes gradle args one-per-line for
`mapfile -t ARGS < FILE; gradle "${ARGS[@]}"`.

Assumption: a module's directory path maps directly to its Gradle project path
(`metadata-service/services` -> `:metadata-service:services`). A `src/test` dir that is NOT a
registered Gradle project would yield a `:not-a-project:test` task and the shard fails LOUDLY
("task not found") — not silently. True for DataHub today; revisit if that mapping ever diverges.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

_SOURCE_SET_MARKERS = ("/src/test/java/", "/src/test/kotlin/", "/src/test/groovy/")


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


def path_to_module_class(path: str, repo_root: str) -> tuple[str, str] | None:
    """(gradle-project, fqcn) for a test source file, or None if outside repo / not a test src."""
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
        return project, fqcn
    return None


def discover_modules(globs: list[str], exclude_globs: list[str], repo_root: str) -> dict[str, list[str]]:
    """gradle-project -> list of test FQCNs found in it (for weighting).

    A module is included if it has ANY matched test source, so inclusion does not depend on
    class naming — the shard runs `:module:test` whole regardless.
    """
    excluded = {
        _canonical(p)
        for pattern in exclude_globs
        for p in glob.glob(_resolve_glob(pattern, repo_root), recursive=True)
    }
    modules: dict[str, list[str]] = defaultdict(list)
    for pattern in globs:
        for path in glob.glob(_resolve_glob(pattern, repo_root), recursive=True):
            if not os.path.isfile(path) or _canonical(path) in excluded:
                continue
            parsed = path_to_module_class(path, repo_root)
            if parsed is not None:
                project, fqcn = parsed
                modules[project].append(fqcn)
    return modules


def load_weights(weights_path: str | None) -> dict[str, float]:
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


def module_weights(modules: dict[str, list[str]], weights: dict[str, float]) -> dict[str, float]:
    all_classes = [c for classes in modules.values() for c in classes]
    known = [weights[c] for c in all_classes if c in weights]
    fallback = statistics.median(known) if known else 1.0
    return {
        project: sum(weights.get(c, fallback) for c in classes)
        for project, classes in modules.items()
    }


def bin_pack(weighted: dict[str, float], total: int) -> list[list[str]]:
    """LPT: heaviest module into the currently-lightest shard. Deterministic tiebreak by name."""
    buckets: list[list[str]] = [[] for _ in range(total)]
    load = [0.0] * total
    for project in sorted(weighted, key=lambda p: (-weighted[p], p)):
        target = min(range(total), key=lambda b: load[b])
        buckets[target].append(project)
        load[target] += weighted[project]
    return buckets


def plan_for_shard(projects: list[str], weighted: dict[str, float]) -> dict:
    tasks = [{"task": (":test" if p == ":" else f"{p}:test")} for p in sorted(projects)]
    return {
        "hasTests": bool(projects),
        "tasks": tasks,
        "diagnostics": {
            "modules": len(projects),
            "predictedSeconds": round(sum(weighted.get(p, 0.0) for p in projects), 1),
        },
    }


def gradle_args(plan: dict) -> list[str]:
    return [task["task"] for task in plan["tasks"]]


def main() -> int:
    parser = argparse.ArgumentParser(description="Assign whole Gradle test modules to one shard.")
    parser.add_argument("--split-index", "-i", type=int, required=True)
    parser.add_argument("--split-total", "-t", type=int, required=True)
    parser.add_argument("--glob", "-g", action="append", required=True)
    parser.add_argument("--exclude-glob", "-e", action="append", default=[])
    parser.add_argument("--weights", help="Path to committed backend_test_weights.json.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-args", help="Write gradle args one-per-line to this file.")
    args = parser.parse_args()

    if args.split_total < 1:
        parser.error("--split-total must be >= 1")
    if not (0 <= args.split_index < args.split_total):
        parser.error("--split-index must be in [0, --split-total)")
    repo_root = os.path.abspath(args.repo_root)

    modules = discover_modules(args.glob, args.exclude_glob, repo_root)
    if not modules:
        print("split_gradle_tests: no test modules matched", file=sys.stderr)

    weights = load_weights(args.weights)
    weighted = module_weights(modules, weights)
    buckets = bin_pack(weighted, args.split_total)
    plan = plan_for_shard(buckets[args.split_index], weighted)

    total_classes = sum(len(c) for c in modules.values())
    matched = sum(1 for classes in modules.values() for c in classes if c in weights)
    mode = "duration-balanced" if weights else "even fallback"
    d = plan["diagnostics"]
    print(
        f"split_gradle_tests: {len(modules)} modules / {total_classes} classes "
        f"({matched} weighted, {mode}); shard {args.split_index}/{args.split_total} -> "
        f"{d['modules']} modules, predicted {d['predictedSeconds']}s",
        file=sys.stderr,
    )

    if args.output_args is not None:
        with open(args.output_args, "w", encoding="utf-8") as fh:
            fh.write("\n".join(gradle_args(plan)))
    print(json.dumps(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
