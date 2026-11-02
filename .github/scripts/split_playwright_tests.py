#!/usr/bin/env python3
"""Assign individual Playwright spec files to one CI shard, balanced by historical duration.

Vendored + stdlib-only (py3.9+), mirrors split_gradle_tests.py's LPT bin-packing. Reads a
committed weights snapshot (playwright_test_weights.json, spec-file-path -> median seconds
from generate_test_weights.py) and bin-packs individual *.spec.ts files across shards. Run
from / pass the Playwright project root (e2e-test/ui/playwright) so paths resolve correctly.

File-level by design: explicit file args passed to `playwright test` bypass Playwright's
fullyParallel worker-hash grouping entirely, so per-file duration balancing actually controls
shard placement (unlike `--shard=N/M`, which balances by test count only). Falls back to an
even split when no weights exist.

Emits a JSON plan on stdout; with --output-args FILE, also writes spec file paths one-per-line
for `mapfile -t ARGS < FILE; npx playwright test "${ARGS[@]}"`.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
import sys


def _canonical(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def _resolve_glob(pattern: str, repo_root: str) -> str:
    return pattern if os.path.isabs(pattern) else os.path.join(repo_root, pattern)


def _relative_to_test_dir(path: str, test_dir: str) -> str | None:
    rel = os.path.relpath(os.path.abspath(path), test_dir)
    if rel == ".." or rel.startswith(".." + os.sep):
        return None
    return rel.replace(os.sep, "/")


def discover_spec_files(glob_pattern: str, exclude_globs: list[str], test_dir: str) -> list[str]:
    """Spec file paths relative to test_dir, matching the junit reporter's testsuite name."""
    excluded = {
        _canonical(p)
        for pattern in exclude_globs
        for p in glob.glob(_resolve_glob(pattern, test_dir), recursive=True)
    }
    specs = []
    for path in glob.glob(_resolve_glob(glob_pattern, test_dir), recursive=True):
        if not os.path.isfile(path) or _canonical(path) in excluded:
            continue
        rel = _relative_to_test_dir(path, test_dir)
        if rel is not None:
            specs.append(rel)
    return specs


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
        file_path = item.get("filePath")
        duration = item.get("duration", "")
        if not file_path or not isinstance(duration, str) or not duration.endswith("s"):
            continue
        try:
            weights[file_path] = float(duration[:-1])
        except ValueError:
            continue
    return weights


def spec_weights(specs: list[str], weights: dict[str, float]) -> dict[str, float]:
    known = [weights[s] for s in specs if s in weights]
    fallback = statistics.median(known) if known else 1.0
    return {spec: weights.get(spec, fallback) for spec in specs}


def bin_pack(weighted: dict[str, float], total: int) -> list[list[str]]:
    """LPT: heaviest spec file into the currently-lightest shard. Deterministic tiebreak by name."""
    buckets: list[list[str]] = [[] for _ in range(total)]
    load = [0.0] * total
    for spec in sorted(weighted, key=lambda p: (-weighted[p], p)):
        target = min(range(total), key=lambda b: load[b])
        buckets[target].append(spec)
        load[target] += weighted[spec]
    return buckets


def plan_for_shard(specs: list[str], weighted: dict[str, float]) -> dict:
    return {
        "hasTests": bool(specs),
        "specs": sorted(specs),
        "diagnostics": {
            "specFiles": len(specs),
            "predictedSeconds": round(sum(weighted.get(s, 0.0) for s in specs), 1),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Assign individual Playwright spec files to one shard.")
    parser.add_argument("--split-index", "-i", type=int, required=True)
    parser.add_argument("--split-total", "-t", type=int, required=True)
    parser.add_argument("--glob", "-g", default="**/*.spec.ts")
    parser.add_argument("--exclude-glob", "-e", action="append", default=[])
    parser.add_argument("--weights", help="Path to committed playwright_test_weights.json.")
    parser.add_argument("--test-dir", default="tests", help="Playwright testDir (relative to --repo-root).")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-args", help="Write spec file paths one-per-line to this file.")
    args = parser.parse_args()

    if args.split_total < 1:
        parser.error("--split-total must be >= 1")
    if not (0 <= args.split_index < args.split_total):
        parser.error("--split-index must be in [0, --split-total)")
    repo_root = os.path.abspath(args.repo_root)
    test_dir = os.path.join(repo_root, args.test_dir)

    specs = discover_spec_files(args.glob, args.exclude_glob, test_dir)
    if not specs:
        print("split_playwright_tests: no spec files matched", file=sys.stderr)

    weights = load_weights(args.weights)
    weighted = spec_weights(specs, weights)
    buckets = bin_pack(weighted, args.split_total)
    plan = plan_for_shard(buckets[args.split_index], weighted)

    matched = sum(1 for s in specs if s in weights)
    mode = "duration-balanced" if weights else "even fallback"
    d = plan["diagnostics"]
    print(
        f"split_playwright_tests: {len(specs)} spec files "
        f"({matched} weighted, {mode}); shard {args.split_index}/{args.split_total} -> "
        f"{d['specFiles']} files, predicted {d['predictedSeconds']}s",
        file=sys.stderr,
    )

    if args.output_args is not None:
        with open(args.output_args, "w", encoding="utf-8") as fh:
            fh.write("\n".join(plan["specs"]))
    print(json.dumps(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
