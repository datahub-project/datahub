#!/usr/bin/env python3
"""
Bin-pack Playwright spec files into duration-weighted shards.

Playwright's own `--shard=N/M` splits by file *count*, not measured duration,
which lets one heavy spec file dominate a shard's wall clock while other
shards sit idle (see e2e-test/ui/playwright/playwright_test_weights.json for
the harvested per-file durations this consumes). This script computes the
full N-way partition independently in each shard's CI job (a pure function of
the file list + weights, so every job derives the same partition without any
shared state) and prints just the one shard's file list, ready to hand to
`npx playwright test` as positional arguments in place of `--shard`.
"""

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def discover_spec_files(tests_dir: Path) -> List[str]:
    """
    Find all Playwright spec files under tests_dir, relative to tests_dir,
    as posix-style paths. Mirrors playwright.config.ts's discovery: testDir
    is tests_dir, and *.setup.ts files (testIgnore'd there) never match the
    *.spec.ts glob here, so no separate exclusion is needed.
    """
    return sorted(p.relative_to(tests_dir).as_posix() for p in tests_dir.rglob("*.spec.ts"))


def load_weights(weights_file: Path) -> Dict[str, float]:
    with open(weights_file) as f:
        data = json.load(f)
    return {item["filePath"]: float(item["duration"].rstrip("s")) for item in data}


def bin_pack_tasks(tasks: List[Tuple[str, float]], n_buckets: int) -> List[List[str]]:
    """
    Greedy LPT (Longest Processing Time) bin packer: sort tasks descending by
    weight, repeatedly assign to the currently-lightest bucket. Ported from
    smoke-test/conftest.py's bin_pack_tasks (same algorithm, used there for
    Cypress/Pytest batching) rather than imported, since that module is a
    pytest conftest, not a library.
    """
    sorted_tasks = sorted(tasks, key=lambda t: t[1], reverse=True)
    buckets: List[List[str]] = [[] for _ in range(n_buckets)]
    bucket_weights = [0.0] * n_buckets
    for task, weight in sorted_tasks:
        idx = bucket_weights.index(min(bucket_weights))
        buckets[idx].append(task)
        bucket_weights[idx] += weight
    return buckets


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute one shard's duration-weighted Playwright spec file list"
    )
    parser.add_argument(
        "--tests-dir",
        type=Path,
        required=True,
        help="Playwright testDir to glob, e.g. 'tests' when run from e2e-test/ui/playwright "
        "(output paths are prefixed with this same value, ready for the CLI)",
    )
    parser.add_argument(
        "--weights-file",
        type=Path,
        required=True,
        help="Path to the weights JSON ([{filePath, duration}, ...])",
    )
    parser.add_argument("--shard", type=int, required=True, help="1-indexed shard number")
    parser.add_argument("--shard-count", type=int, required=True, help="Total number of shards")
    parser.add_argument(
        "--output",
        type=Path,
        required=False,
        help="Write the shard's file list here (one per line). Prints to stdout if omitted.",
    )

    args = parser.parse_args()

    if not (1 <= args.shard <= args.shard_count):
        parser.error(f"--shard must be in [1, {args.shard_count}], got {args.shard}")

    if not args.tests_dir.is_dir():
        print(f"Error: tests dir does not exist: {args.tests_dir}", file=sys.stderr)
        sys.exit(1)

    if not args.weights_file.exists():
        print(f"Error: weights file does not exist: {args.weights_file}", file=sys.stderr)
        sys.exit(1)

    spec_files = discover_spec_files(args.tests_dir)
    if not spec_files:
        print(f"Error: no *.spec.ts files found under {args.tests_dir}", file=sys.stderr)
        sys.exit(1)

    weights = load_weights(args.weights_file)
    # New files with no harvested history yet default to the median rather
    # than 0 (which would let an unweighted file be "free" to pile onto any
    # bucket) or a small constant (which would starve genuinely heavy new
    # specs of shard budget until the next weekly refresh).
    default_weight = statistics.median(weights.values()) if weights else 1.0
    unweighted = [f for f in spec_files if f not in weights]
    if unweighted:
        print(
            f"Note: {len(unweighted)} spec file(s) have no weight entry, "
            f"defaulting to the median ({default_weight:.1f}s): {', '.join(unweighted[:5])}"
            + (", ..." if len(unweighted) > 5 else ""),
            file=sys.stderr,
        )

    tasks = [(f, weights.get(f, default_weight)) for f in spec_files]
    buckets = bin_pack_tasks(tasks, args.shard_count)
    shard_files = sorted(buckets[args.shard - 1])
    shard_weight = sum(weights.get(f, default_weight) for f in shard_files)

    all_shard_weights = [sum(weights.get(f, default_weight) for f in b) for b in buckets]
    print(
        f"Shard {args.shard}/{args.shard_count}: {len(shard_files)} files, "
        f"~{shard_weight:.1f}s weighted "
        f"(all shards: {[f'{w:.0f}s' for w in all_shard_weights]})",
        file=sys.stderr,
    )

    output_paths = [f"{args.tests_dir}/{f}" for f in shard_files]
    output_text = "\n".join(output_paths)
    if args.output:
        args.output.write_text(output_text + ("\n" if output_text else ""))
        print(f"Wrote {len(shard_files)} file paths to {args.output}", file=sys.stderr)
    else:
        print(output_text)


if __name__ == "__main__":
    main()
