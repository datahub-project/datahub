#!/usr/bin/env python3
"""
Simulate the selective CI workflow against real git history.

For each recent PR merge commit, computes what the detect job would output
and compares it to the full batch suite that actually ran. This lets you
validate the selective logic before wiring it into the real workflow.

Usage:
    cd metadata-ingestion
    python scripts/test_ci_simulation.py [--count N] [--verbose]

Examples:
    # Simulate last 20 PRs
    python scripts/test_ci_simulation.py --count 20

    # Verbose: show full test matrix for each PR
    python scripts/test_ci_simulation.py --count 5 --verbose
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from selective_ci_checks import build_import_graph, classify, load_connector_registry


def get_recent_pr_commits(repo_root: Path, count: int) -> list[dict]:
    """Get recent commits that touched metadata-ingestion/."""
    result = subprocess.run(
        [
            "git",
            "log",
            "--oneline",
            f"-{count * 3}",
            "--",
            "metadata-ingestion/",
            "metadata-models/",
        ],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    commits = []
    for line in result.stdout.strip().splitlines():
        if not line:
            continue
        sha = line.split()[0]
        message = " ".join(line.split()[1:])
        commits.append({"sha": sha, "message": message})
        if len(commits) >= count:
            break
    return commits


def get_changed_files_for_commit(repo_root: Path, sha: str) -> list[str]:
    """Get files changed in a commit (diff against parent)."""
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{sha}~1", sha],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    if result.returncode != 0:
        return []
    return [f.strip() for f in result.stdout.splitlines() if f.strip()]


def simulate(repo_root: Path, count: int, verbose: bool) -> None:
    """Run simulation against recent PRs."""
    commits = get_recent_pr_commits(repo_root, count)
    if not commits:
        print("No merge commits found touching metadata-ingestion/")
        return

    print(f"Simulating selective CI for {len(commits)} recent PRs...\n")

    # Pre-load registry and import graph (same for all simulations)
    registry = load_connector_registry(repo_root)
    import_graph = build_import_graph(repo_root, registry)
    print(f"Registry: {len(registry)} connectors discovered")
    print(f"Import graph: {sum(len(v) for v in import_graph.values())} edges\n")
    print("=" * 80)

    stats = {
        "total": 0,
        "selective": 0,
        "full_suite": 0,
        "no_tests": 0,
        "saved_jobs": 0,
    }
    FULL_BATCH_COUNT = 7  # 7 integration batch jobs

    for commit in commits:
        sha = commit["sha"]
        msg = commit["message"][:70]
        changed = get_changed_files_for_commit(repo_root, sha)
        if not changed:
            continue

        stats["total"] += 1
        d = classify(changed, repo_root)

        # Count metadata-ingestion changes
        mi_changes = [
            f
            for f in changed
            if f.startswith("metadata-ingestion/") or f.startswith("metadata-models/")
        ]
        source_changes = [
            f
            for f in changed
            if f.startswith("metadata-ingestion/src/datahub/ingestion/source/")
        ]

        if d.run_all_integration:
            status = "FULL SUITE"
            stats["full_suite"] += 1
            jobs = FULL_BATCH_COUNT
        elif d.test_matrix:
            status = f"SELECTIVE ({len(d.test_matrix)} tests)"
            stats["selective"] += 1
            jobs = len(d.test_matrix)
            stats["saved_jobs"] += FULL_BATCH_COUNT - jobs
        else:
            status = "NO INTEGRATION TESTS"
            stats["no_tests"] += 1
            jobs = 0
            stats["saved_jobs"] += FULL_BATCH_COUNT

        print(f"\n{sha[:8]} {msg}")
        print(
            f"  Changed: {len(changed)} files ({len(mi_changes)} MI, {len(source_changes)} source)"
        )
        print(f"  Result:  {status}")

        if verbose:
            if d.test_matrix:
                print("  Matrix:")
                for entry in d.test_matrix:
                    print(f"    - {entry['connector']}: {entry['test_path']}")
            if source_changes:
                print("  Source changes:")
                for f in source_changes[:10]:
                    print(f"    - {f}")
                if len(source_changes) > 10:
                    print(f"    ... and {len(source_changes) - 10} more")

    print("\n" + "=" * 80)
    print(f"\nSimulation Summary ({stats['total']} PRs)")
    print(f"  Full suite:          {stats['full_suite']}")
    print(f"  Selective:           {stats['selective']}")
    print(f"  No integration:      {stats['no_tests']}")
    print(
        f"  Integration jobs saved: {stats['saved_jobs']} / {stats['total'] * FULL_BATCH_COUNT} "
        f"({stats['saved_jobs'] / max(1, stats['total'] * FULL_BATCH_COUNT) * 100:.0f}%)"
    )

    if stats["selective"] > 0:
        avg_selective = (
            stats["total"] * FULL_BATCH_COUNT
            - stats["saved_jobs"]
            - stats["full_suite"] * FULL_BATCH_COUNT
        ) / max(1, stats["selective"])
        print(
            f"  Avg selective jobs:  {avg_selective:.1f} (vs {FULL_BATCH_COUNT} full)"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulate selective CI against git history"
    )
    parser.add_argument(
        "--count", type=int, default=20, help="Number of PRs to simulate"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show full details"
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    simulate(repo_root, args.count, args.verbose)
