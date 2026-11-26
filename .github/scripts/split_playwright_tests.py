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

With --dump-plan FILE (or "-" for stdout), writes the predicted plan for every shard without
needing --split-index, so shard counts can be compared offline:

    python3 .github/scripts/split_playwright_tests.py --split-total 10 --repo-root . \
        --test-dir e2e-test/ui/playwright/tests \
        --weights e2e-test/ui/playwright/playwright_test_weights.json --dump-plan -

The dump and a CI shard's own assignment both come from build_plan(), so they cannot drift.
Predicted seconds are sums of recorded medians and exclude per-shard setup and retries.
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


def scaled_spec_weights(test_counts: dict[str, tuple[int, int]], weights: dict[str, float]) -> dict[str, float]:
    """Seconds per spec for a run that selects only some of each file's tests.

    ``test_counts`` maps a spec to (selected tests, all tests in the file). Weights are
    recorded per whole file, so a file running 1 of its 4 tests counts a quarter of it.
    Shared with plan_playwright_tier.py so its shard count matches what this packs.
    """
    full = spec_weights(list(test_counts), weights)
    return {
        spec: full[spec] * (selected / total if total else 1.0)
        for spec, (selected, total) in test_counts.items()
    }


def bin_pack(weighted: dict[str, float], total: int) -> list[list[str]]:
    """LPT: heaviest spec file into the currently-lightest shard. Deterministic tiebreak by name."""
    buckets: list[list[str]] = [[] for _ in range(total)]
    load = [0.0] * total
    for spec in sorted(weighted, key=lambda p: (-weighted[p], p)):
        target = min(range(total), key=lambda b: load[b])
        buckets[target].append(spec)
        load[target] += weighted[spec]
    return buckets


def build_plan(
    specs: list[str], weights: dict[str, float], total: int, all_specs: list[str] | None = None
) -> dict:
    """Pack every spec into ``total`` shards and describe the result.

    Single entry point for a CI shard's assignment and the --dump-plan output.
    ``all_specs`` is every spec on disk when ``specs`` is a selection, so weights
    for unselected specs are not reported as stale.
    """
    all_specs = specs if all_specs is None else all_specs
    weighted = spec_weights(specs, weights)
    buckets = bin_pack(weighted, total)
    unweighted = sorted(s for s in specs if s not in weights)
    known = [weights[s] for s in specs if s in weights]
    shards = []
    for index, bucket in enumerate(buckets):
        shard = plan_for_shard(bucket, weighted)
        shard["index"] = index
        shard["diagnostics"]["unweightedSpecFiles"] = sum(1 for s in bucket if s not in weights)
        shards.append(shard)
    predicted = [shard["diagnostics"]["predictedSeconds"] for shard in shards]
    return {
        "splitTotal": total,
        "mode": "duration-balanced" if weights else "even fallback",
        "specFiles": len(specs),
        "weightedSpecFiles": len(specs) - len(unweighted),
        "fallbackSeconds": round(statistics.median(known), 1) if known else 1.0,
        "totalPredictedSeconds": round(sum(weighted.values()), 1),
        "maxShardPredictedSeconds": max(predicted, default=0.0),
        "minShardPredictedSeconds": min(predicted, default=0.0),
        "unweightedSpecs": unweighted,
        # Weights for specs that no longer exist; the weekly refresh drops them.
        "staleWeightEntries": sorted(set(weights) - set(all_specs)),
        "shards": shards,
    }


def _write_json(path: str, payload: dict) -> None:
    text = json.dumps(payload, indent=2) + "\n"
    if path == "-":
        sys.stdout.write(text)
        return
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


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
    parser.add_argument("--split-index", "-i", type=int, help="Required unless --dump-plan is set.")
    parser.add_argument("--split-total", "-t", type=int, required=True)
    parser.add_argument("--glob", "-g", default="**/*.spec.ts")
    parser.add_argument("--exclude-glob", "-e", action="append", default=[])
    parser.add_argument("--weights", help="Path to committed playwright_test_weights.json.")
    parser.add_argument("--test-dir", default="tests", help="Playwright testDir (relative to --repo-root).")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-args", help="Write spec file paths one-per-line to this file.")
    parser.add_argument(
        "--tier-plan",
        help="PR P0 tier: pack only the specs in this plan (plan_playwright_tier.py), weighted by "
        "their selected tests.",
    )
    parser.add_argument(
        "--output-test-list",
        help="With --tier-plan: write this shard's tests as a Playwright --test-list file.",
    )
    parser.add_argument(
        "--dump-plan",
        help='Write the predicted plan for every shard as JSON to this file ("-" for stdout).',
    )
    args = parser.parse_args()

    if args.split_total < 1:
        parser.error("--split-total must be >= 1")
    if args.split_index is None and args.dump_plan is None:
        parser.error("--split-index is required unless --dump-plan is set")
    if args.split_index is not None and not (0 <= args.split_index < args.split_total):
        parser.error("--split-index must be in [0, --split-total)")
    if args.split_index is not None and args.dump_plan == "-":
        parser.error('--dump-plan - cannot be combined with --split-index (both write to stdout)')
    repo_root = os.path.abspath(args.repo_root)
    test_dir = os.path.join(repo_root, args.test_dir)

    all_specs = discover_spec_files(args.glob, args.exclude_glob, test_dir)
    specs = all_specs
    weights = load_weights(args.weights)
    tier_specs: dict[str, dict] = {}
    if args.tier_plan is not None:
        with open(args.tier_plan, encoding="utf-8") as fh:
            tier_specs = {spec["path"]: spec for spec in json.load(fh)["specs"]}
        missing = sorted(set(tier_specs) - set(all_specs))
        if missing:
            print(f"split_playwright_tests: plan specs not on disk: {', '.join(missing[:5])}", file=sys.stderr)
        specs = [spec for spec in all_specs if spec in tier_specs]
        counts = {s: (tier_specs[s]["selectedTests"], tier_specs[s]["totalTests"]) for s in specs}
        weights = scaled_spec_weights(counts, weights)
    if not specs:
        print("split_playwright_tests: no spec files matched", file=sys.stderr)

    full = build_plan(specs, weights, args.split_total, all_specs=all_specs)

    print(
        f"split_playwright_tests: {full['specFiles']} spec files "
        f"({full['weightedSpecFiles']} weighted, {full['mode']}); "
        f"{args.split_total} shards, predicted {full['minShardPredictedSeconds']}-"
        f"{full['maxShardPredictedSeconds']}s per shard",
        file=sys.stderr,
    )
    if weights and full["unweightedSpecs"]:
        unweighted = full["unweightedSpecs"]
        print(
            f"split_playwright_tests: {len(unweighted)} spec file(s) have no recorded weight; "
            f"packed at the {full['fallbackSeconds']}s median: {', '.join(unweighted[:5])}"
            + (", ..." if len(unweighted) > 5 else ""),
            file=sys.stderr,
        )

    if args.dump_plan is not None:
        _write_json(args.dump_plan, full)
    if args.split_index is None:
        return 0

    shard = full["shards"][args.split_index]
    plan = {"hasTests": shard["hasTests"], "specs": shard["specs"], "diagnostics": shard["diagnostics"]}
    d = plan["diagnostics"]
    print(
        f"split_playwright_tests: shard {args.split_index}/{args.split_total} -> "
        f"{d['specFiles']} files, predicted {d['predictedSeconds']}s",
        file=sys.stderr,
    )

    if args.output_test_list is not None:
        lines = []
        for spec in plan["specs"]:
            entry = tier_specs.get(spec)
            lines.extend([spec] if entry is None or entry["wholeFile"] else entry["testLines"])
        with open(args.output_test_list, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + ("\n" if lines else ""))
    if args.output_args is not None:
        with open(args.output_args, "w", encoding="utf-8") as fh:
            fh.write("\n".join(plan["specs"]))
    print(json.dumps(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
