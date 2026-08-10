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

With workers_per_shard > 1 (see resusable-playwright-tests.yml), a shard's
own wall clock is no longer just its total weight: Playwright's fullyParallel
scheduler pulls individual tests onto whichever worker is free, so ordinary
files mix freely across a shard's workers and the shard finishes in roughly
total_weight / workers_per_shard. The one exception is a file using
`test.describe.configure({ mode: 'serial' })`: every test in that block must
run sequentially on a single worker, so the file's own weight is a hard floor
no amount of extra workers can shrink. A shard stuck with several heavy
serial files bottlenecks on that floor even while its other workers sit idle.
bin_pack_tasks therefore places serial files first (spreading the
un-parallelizable floor evenly across shards) before packing the remaining
parallel-friendly files to balance each shard's total.
"""

import argparse
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Tuple

SERIAL_MODE_PATTERN = re.compile(r"""mode:\s*['"]serial['"]""")


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


def find_serial_files(tests_dir: Path, spec_files: List[str]) -> set:
    """
    Spec files containing test.describe.configure({ mode: 'serial' }) anywhere
    in the file. Conservative at file granularity (no per-describe-block
    weights exist to do better): a file with any serial block is treated as
    fully atomic for floor purposes, even if only part of it is serial.
    """
    serial_files = set()
    for f in spec_files:
        content = (tests_dir / f).read_text(encoding="utf-8", errors="ignore")
        if SERIAL_MODE_PATTERN.search(content):
            serial_files.add(f)
    return serial_files


def lpt_pack(
    tasks: List[Tuple[str, float]],
    buckets: List[List[str]],
    bucket_weights: List[float],
) -> None:
    """
    Greedy LPT (Longest Processing Time): sort tasks descending by weight,
    repeatedly assign to the currently-lightest bucket. Mutates buckets /
    bucket_weights in place so callers can run it in two passes (serial tasks
    first, then parallel tasks) over the same running totals. Ported from
    smoke-test/conftest.py's bin_pack_tasks (same algorithm, used there for
    Cypress/Pytest batching) rather than imported, since that module is a
    pytest conftest, not a library.
    """
    for task, weight in sorted(tasks, key=lambda t: t[1], reverse=True):
        idx = bucket_weights.index(min(bucket_weights))
        buckets[idx].append(task)
        bucket_weights[idx] += weight


def bin_pack_tasks(
    tasks: List[Tuple[str, float]],
    n_buckets: int,
    serial_files: set = frozenset(),
) -> List[List[str]]:
    """
    Two-phase LPT: serial-mode files are packed first (their weight is an
    unavoidable single-worker floor, so spreading them evenly across shards
    first matters most), then the remaining files are packed to balance each
    shard's overall total. With no serial files this reduces to a single LPT
    pass, identical to the previous single-phase behavior.
    """
    buckets: List[List[str]] = [[] for _ in range(n_buckets)]
    bucket_weights = [0.0] * n_buckets
    serial_tasks = [t for t in tasks if t[0] in serial_files]
    parallel_tasks = [t for t in tasks if t[0] not in serial_files]
    lpt_pack(serial_tasks, buckets, bucket_weights)
    lpt_pack(parallel_tasks, buckets, bucket_weights)
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
        "--workers-per-shard",
        type=int,
        default=1,
        help="Playwright --workers value each shard will run with. Only affects the projected "
        "per-shard floor logged for visibility -- above 1, spreads mode:'serial' files (an "
        "unavoidable single-worker floor) evenly across shards before balancing shard totals.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=False,
        help="Write the shard's file list here (one per line). Prints to stdout if omitted.",
    )

    args = parser.parse_args()

    if not (1 <= args.shard <= args.shard_count):
        parser.error(f"--shard must be in [1, {args.shard_count}], got {args.shard}")

    if args.workers_per_shard < 1:
        parser.error(f"--workers-per-shard must be >= 1, got {args.workers_per_shard}")

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

    serial_files = find_serial_files(args.tests_dir, spec_files) if args.workers_per_shard > 1 else set()

    tasks = [(f, weights.get(f, default_weight)) for f in spec_files]
    buckets = bin_pack_tasks(tasks, args.shard_count, serial_files)
    shard_files = sorted(buckets[args.shard - 1])
    shard_weight = sum(weights.get(f, default_weight) for f in shard_files)

    all_shard_weights = [sum(weights.get(f, default_weight) for f in b) for b in buckets]
    print(
        f"Shard {args.shard}/{args.shard_count}: {len(shard_files)} files, "
        f"~{shard_weight:.1f}s weighted "
        f"(all shards: {[f'{w:.0f}s' for w in all_shard_weights]})",
        file=sys.stderr,
    )

    if args.workers_per_shard > 1:
        shard_serial_weight = max(
            (weights.get(f, default_weight) for f in shard_files if f in serial_files), default=0.0
        )
        projected_floor = max(shard_serial_weight, shard_weight / args.workers_per_shard)
        all_floors = [
            max(
                max((weights.get(f, default_weight) for f in b if f in serial_files), default=0.0),
                sum(weights.get(f, default_weight) for f in b) / args.workers_per_shard,
            )
            for b in buckets
        ]
        print(
            f"Shard {args.shard}/{args.shard_count}: {len(serial_files & set(shard_files))} serial file(s), "
            f"projected floor ~{projected_floor:.1f}s at {args.workers_per_shard} workers "
            f"(all shards: {[f'{w:.0f}s' for w in all_floors]})",
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
