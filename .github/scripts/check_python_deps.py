#!/usr/bin/env python3
"""
Compare Python dependency pinning between baseline (master) and PR branches.

This script reads dep-analyzer.py JSON outputs from both branches and identifies:
- New unpinned dependencies (no version constraint)
- New lower-bound only dependencies (e.g., >=2.0 without upper bound)
- Downgraded dependencies (from stricter to looser pinning)

Usage:
    check_python_deps.py --baseline-file /tmp/master-project.json \
                         --pr-file /tmp/pr-project.json \
                         --project-name metadata-ingestion

Exit codes:
    0: No violations found
    1: Violations found
    2: Error reading or parsing files
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any


# Pin levels that should cause failure if newly introduced
VIOLATION_LEVELS = {"unpinned", "lower_bound"}


def load_json_results(file_path: Path) -> dict[str, Any] | None:
    """
    Load and parse dep-analyzer.py JSON output.

    Returns None if file doesn't exist (project added/deleted in PR).
    Raises exception if file exists but is malformed.
    """
    if not file_path.exists():
        return None

    try:
        with open(file_path) as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"Malformed JSON in {file_path}: {e}")


def build_dependency_map(data: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """
    Build a mapping from dependency name to its analysis.

    Returns empty dict if data is None.
    """
    if data is None:
        return {}

    dep_map = {}
    for level, deps in data.get("by_level", {}).items():
        for dep in deps:
            dep_map[dep["name"]] = {
                "level": level,
                "specifier": dep.get("specifier", ""),
                "source": dep.get("source", "unknown")
            }

    return dep_map


def classify_violation(
    name: str,
    baseline_info: dict[str, Any] | None,
    pr_info: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Classify if a dependency represents a violation.

    Returns violation dict if it's a violation, None otherwise.
    """
    pr_level = pr_info["level"]

    # Check if PR has a problematic pin level
    if pr_level not in VIOLATION_LEVELS:
        return None

    # Case 1: New dependency with bad pinning
    if baseline_info is None:
        violation_type = f"new_{pr_level}"
        return {
            "type": violation_type,
            "name": name,
            "old_spec": None,
            "old_level": None,
            "new_spec": pr_info["specifier"],
            "new_level": pr_level,
            "source": pr_info["source"]
        }

    # Case 2: Existing dependency downgraded to bad pinning
    baseline_level = baseline_info["level"]
    if baseline_level not in VIOLATION_LEVELS:
        # Was good, now bad - downgrade violation
        return {
            "type": "downgraded",
            "name": name,
            "old_spec": baseline_info["specifier"],
            "old_level": baseline_level,
            "new_spec": pr_info["specifier"],
            "new_level": pr_level,
            "source": pr_info["source"]
        }

    # Case 3: Was already bad in baseline - grandfathered
    return None


def compare_dependencies(
    baseline_map: dict[str, dict[str, Any]],
    pr_map: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    Compare baseline and PR dependency maps to find violations.

    Returns list of violation dictionaries.
    """
    violations = []

    for name, pr_info in pr_map.items():
        baseline_info = baseline_map.get(name)
        violation = classify_violation(name, baseline_info, pr_info)
        if violation:
            violations.append(violation)

    return violations


def generate_summary(violations: list[dict[str, Any]]) -> dict[str, int]:
    """Generate violation summary statistics."""
    summary = {
        "new_unpinned": 0,
        "new_lower_bound": 0,
        "downgraded": 0
    }

    for v in violations:
        vtype = v["type"]
        if vtype == "new_unpinned":
            summary["new_unpinned"] += 1
        elif vtype == "new_lower_bound":
            summary["new_lower_bound"] += 1
        elif vtype == "downgraded":
            summary["downgraded"] += 1

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Compare Python dependency pinning between branches"
    )
    parser.add_argument(
        "--baseline-file",
        type=Path,
        required=True,
        help="Path to master branch dep-analyzer JSON output"
    )
    parser.add_argument(
        "--pr-file",
        type=Path,
        required=True,
        help="Path to PR branch dep-analyzer JSON output"
    )
    parser.add_argument(
        "--project-name",
        required=True,
        help="Name of the project being compared (for reporting)"
    )

    args = parser.parse_args()

    try:
        # Load both JSON files
        baseline_data = load_json_results(args.baseline_file)
        pr_data = load_json_results(args.pr_file)

        # Handle edge cases
        if pr_data is None:
            # Project deleted in PR - no deps to check
            result = {
                "project": args.project_name,
                "has_violations": False,
                "violations": [],
                "summary": {"new_unpinned": 0, "new_lower_bound": 0, "downgraded": 0},
                "note": "Project deleted in PR"
            }
            print(json.dumps(result, indent=2))
            return 0

        # Build dependency maps
        baseline_map = build_dependency_map(baseline_data)
        pr_map = build_dependency_map(pr_data)

        # Compare and find violations
        violations = compare_dependencies(baseline_map, pr_map)
        summary = generate_summary(violations)

        # Generate output
        result = {
            "project": args.project_name,
            "has_violations": len(violations) > 0,
            "violations": violations,
            "summary": summary
        }

        if baseline_data is None:
            result["note"] = "New project in PR - all dependencies treated as new"

        print(json.dumps(result, indent=2))

        # Exit with appropriate code
        return 1 if violations else 0

    except ValueError as e:
        # JSON parsing or other validation error
        error_result = {
            "project": args.project_name,
            "error": str(e),
            "has_violations": False,
            "violations": []
        }
        print(json.dumps(error_result, indent=2), file=sys.stderr)
        return 2

    except Exception as e:
        # Unexpected error
        error_result = {
            "project": args.project_name,
            "error": f"Unexpected error: {e}",
            "has_violations": False,
            "violations": []
        }
        print(json.dumps(error_result, indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
