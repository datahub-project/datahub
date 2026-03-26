#!/usr/bin/env python3
"""
CLI tool to bump schemaVersion annotations on changed PDL aspect files.

Compares PDL files against a base branch and increments the schemaVersion
annotation on any aspect that has changed — including aspects that transitively
include a changed record via PDL `includes`.

Rules:
  - Only aspects (files with @Aspect annotation) are versioned
  - Default version if not specified is 1
  - Any change bumps to base_branch_version + 1
  - New files (not on base branch) already default to version 1; no write needed
  - If record A is included by aspect B and A changes, B is also bumped

Environment variables:
  PDL_ROOTS    Colon-separated list of PDL source roots to scan.
               Default: metadata-models/src/main/pegasus
  MAIN_BRANCH  Base branch fallback when origin/HEAD is not configured locally.
               Default: master

Usage:
    python3 scripts/bump_schema_versions.py
    python3 scripts/bump_schema_versions.py --base-branch master
    python3 scripts/bump_schema_versions.py --dry-run --verbose
    PDL_ROOTS="metadata-models/src/main/pegasus:other/src/main/pegasus" python3 scripts/bump_schema_versions.py
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict, deque
from pathlib import Path


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------


def detect_default_branch() -> str:
    """
    Detect the repo's default branch.

    Resolution order:
    1. git symbolic-ref refs/remotes/origin/HEAD  (fast, offline)
    2. MAIN_BRANCH environment variable
    3. Hard fallback to 'master'
    """
    result = subprocess.run(
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout.strip().split("/")[-1]

    if env_branch := os.environ.get("MAIN_BRANCH"):
        return env_branch

    return "master"


def get_changed_pdl_files(base_branch: str) -> list[str]:
    """Return repo-relative paths of PDL files that differ from base_branch."""
    try:
        result = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                "--diff-filter=ACM",
                base_branch,
                "--",
                "*.pdl",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return [f.strip() for f in result.stdout.strip().splitlines() if f.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def get_file_at_branch(filepath: str, branch: str) -> str | None:
    """Return file content at branch, or None if the file doesn't exist there."""
    result = subprocess.run(
        ["git", "show", f"{branch}:{filepath}"],
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


# ---------------------------------------------------------------------------
# PDL parsing
# ---------------------------------------------------------------------------

# Default PDL source root relative to the repo root
_DEFAULT_PDL_ROOT = Path("metadata-models/src/main/pegasus")


def get_pdl_roots() -> list[Path]:
    """
    Return the list of PDL source roots to scan.

    Reads the PDL_ROOTS environment variable (colon-separated paths).
    Falls back to the single default root if the variable is not set.
    """
    raw = os.environ.get("PDL_ROOTS")
    if raw:
        return [Path(p.strip()) for p in raw.split(":") if p.strip()]
    return [_DEFAULT_PDL_ROOT]


def _relative_to_any_root(path: Path, roots: list[Path]) -> Path | None:
    """Return path relative to the first matching root, or None if it matches none."""
    for root in roots:
        try:
            return path.relative_to(root)
        except ValueError:
            continue
    return None


def find_all_pdl_files() -> list[Path]:
    return [f for root in get_pdl_roots() for f in root.rglob("*.pdl")]


def fqn_to_path(fqn: str) -> Path | None:
    """Convert a fully-qualified PDL name (com.linkedin.X.Foo) to a file path."""
    return _DEFAULT_PDL_ROOT / Path(fqn.replace(".", "/")).with_suffix(".pdl")


def parse_pdl_header(content: str) -> tuple[str, dict[str, str]]:
    """
    Return (namespace, imports) where imports maps short name → FQN.
    e.g. ("com.linkedin.dataset", {"TimeseriesAspectBase": "com.linkedin.timeseries.TimeseriesAspectBase"})
    """
    ns_match = re.search(r"^\s*namespace\s+([\w.]+)", content, re.MULTILINE)
    namespace = ns_match.group(1) if ns_match else ""

    imports: dict[str, str] = {}
    for m in re.finditer(r"^\s*import\s+([\w.]+)", content, re.MULTILINE):
        fqn = m.group(1)
        short = fqn.rsplit(".", 1)[-1]
        imports[short] = fqn

    return namespace, imports


def parse_includes(content: str) -> list[str]:
    """
    Return the short names listed in all `record X includes A, B, C` clauses.
    """
    names: list[str] = []
    for match in re.finditer(r"\brecord\s+\w+\s+includes\s+([^{]+)", content):
        names.extend(name.strip() for name in match.group(1).split(",") if name.strip())
    return names


def resolve_includes(content: str) -> list[str]:
    """
    Return FQNs for all records this PDL file includes.
    Short names are resolved via import statements, with namespace as fallback.
    """
    namespace, imports = parse_pdl_header(content)
    short_names = parse_includes(content)
    fqns: list[str] = []
    for name in short_names:
        if name in imports:
            fqns.append(imports[name])
        elif namespace:
            fqns.append(f"{namespace}.{name}")
    return fqns


def find_aspect_annotation_bounds(content: str) -> tuple[int, int] | None:
    """Locate the @Aspect annotation object. Returns (start, end) or None."""
    match = re.search(r"@Aspect\s*=\s*(\{)", content)
    if not match:
        return None

    start = match.start(1)
    depth = 0
    for i in range(start, len(content)):
        if content[i] == "{":
            depth += 1
        elif content[i] == "}":
            depth -= 1
            if depth == 0:
                return (start, i + 1)
    return None


def parse_annotation(annotation_str: str) -> dict:
    """Parse a PDL annotation object (JSON with optional trailing commas)."""
    cleaned = re.sub(r",\s*([}\]])", r"\1", annotation_str)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Cannot parse @Aspect annotation: {e}\nContent: {annotation_str}"
        )


def get_schema_version(content: str) -> int:
    """Extract schemaVersion from PDL content. Returns 1 if not present."""
    bounds = find_aspect_annotation_bounds(content)
    if bounds is None:
        return 1
    try:
        data = parse_annotation(content[bounds[0] : bounds[1]])
        return int(data.get("schemaVersion", 1))
    except (ValueError, KeyError):
        return 1


def is_aspect(content: str) -> bool:
    """Return True if the PDL file defines an aspect."""
    return bool(re.search(r"@Aspect\s*=\s*\{", content))


def update_schema_version(content: str, new_version: int) -> str:
    """
    Update or insert schemaVersion in the @Aspect annotation.
    Preserves the original formatting style of the annotation block.
    """
    bounds = find_aspect_annotation_bounds(content)
    if bounds is None:
        return content

    start, end = bounds
    annotation = content[start:end]

    if re.search(r'"schemaVersion"\s*:', annotation):
        new_annotation = re.sub(
            r'"schemaVersion"\s*:\s*\d+',
            f'"schemaVersion": {new_version}',
            annotation,
        )
    else:
        indent_match = re.search(r"\n(\s+)", annotation)
        indent = indent_match.group(1) if indent_match else "  "

        close_pos = annotation.rfind("}")
        before_close = annotation[:close_pos]
        before_stripped = before_close.rstrip()
        if before_stripped and not before_stripped.endswith(","):
            before_stripped += ","

        new_annotation = (
            before_stripped
            + f'\n{indent}"schemaVersion": {new_version}\n'
            + annotation[close_pos:]
        )

    return content[:start] + new_annotation + content[end:]


# ---------------------------------------------------------------------------
# Include graph
# ---------------------------------------------------------------------------


def build_reverse_include_graph(
    all_pdl_files: list[Path],
) -> dict[str, set[str]]:
    """
    Build a reverse map: fqn → set of repo-relative file paths that include it.

    Given that A.pdl `includes` B and C, the graph will have:
      fqn(B) → { path(A.pdl) }
      fqn(C) → { path(A.pdl) }
    """
    reverse: dict[str, set[str]] = defaultdict(set)
    roots = get_pdl_roots()

    for pdl_path in all_pdl_files:
        try:
            content = pdl_path.read_text(encoding="utf-8")
        except OSError:
            continue

        rel = _relative_to_any_root(pdl_path, roots)
        if rel is None:
            continue

        for included_fqn in resolve_includes(content):
            reverse[included_fqn].add(str(pdl_path))

    return reverse


def find_transitively_affected_aspects(
    directly_changed: list[str],
    reverse_graph: dict[str, set[str]],
    all_pdl_files: list[Path],
) -> set[str]:
    """
    BFS from the set of directly changed files through the reverse include graph.
    Returns all *aspect* files (including the originals) that need a version bump.

    'directly changed' files don't need to be aspects themselves — a changed
    non-aspect record can trigger bumps in aspects that include it.
    """
    # Build fqn → path index for fast lookup
    roots = get_pdl_roots()
    fqn_to_file: dict[str, str] = {}
    for pdl_path in all_pdl_files:
        rel = _relative_to_any_root(pdl_path, roots)
        if rel is None:
            continue
        fqn = ".".join(rel.with_suffix("").parts)
        fqn_to_file[fqn] = str(pdl_path)

    # Also index by repo-relative path → fqn
    file_to_fqn: dict[str, str] = {v: k for k, v in fqn_to_file.items()}

    queue: deque[str] = deque(directly_changed)
    visited: set[str] = set(directly_changed)
    affected_aspects: set[str] = set()

    while queue:
        filepath = queue.popleft()
        path = Path(filepath)
        if not path.exists():
            continue

        content = path.read_text(encoding="utf-8")
        if is_aspect(content):
            affected_aspects.add(filepath)

        # Walk up the reverse graph: find everything that includes this file
        fqn = file_to_fqn.get(filepath)
        if fqn is None:
            continue

        for parent_path in reverse_graph.get(fqn, set()):
            if parent_path not in visited:
                visited.add(parent_path)
                queue.append(parent_path)

    return affected_aspects


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    default_branch = detect_default_branch()

    parser = argparse.ArgumentParser(
        description="Bump schemaVersion on changed PDL aspect files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--base-branch",
        default=default_branch,
        help=f"Branch to compare against (default: auto-detected '{default_branch}')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing files",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed per-file output",
    )
    args = parser.parse_args()

    directly_changed = get_changed_pdl_files(args.base_branch)

    if not directly_changed:
        print("No changed PDL files found.")
        return 0

    if args.verbose:
        print(f"Comparing against branch: {args.base_branch}")
        print(f"Found {len(directly_changed)} directly changed PDL file(s):")
        for f in directly_changed:
            print(f"  {f}")
        print()

    # Build the include graph once across all PDL files
    all_pdl_files = find_all_pdl_files()
    reverse_graph = build_reverse_include_graph(all_pdl_files)

    # Expand to all transitively affected aspects
    to_bump = find_transitively_affected_aspects(
        directly_changed, reverse_graph, all_pdl_files
    )

    if not to_bump:
        print("No aspect files affected.")
        return 0

    # Separate directly changed aspects from implicitly affected ones for logging
    directly_changed_set = set(directly_changed)

    bumped: list[str] = []
    skipped: list[str] = []
    errors: list[str] = []

    for filepath in sorted(to_bump):
        path = Path(filepath)
        current_content = path.read_text(encoding="utf-8")

        base_content = get_file_at_branch(filepath, args.base_branch)
        base_version = get_schema_version(base_content) if base_content else 0
        current_version = get_schema_version(current_content)
        new_version = base_version + 1

        if current_version >= new_version:
            if args.verbose:
                reason = (
                    "direct change"
                    if filepath in directly_changed_set
                    else "implicit (includes changed record)"
                )
                print(
                    f"SKIP  {filepath}  "
                    f"(already at v{current_version} >= v{new_version})  [{reason}]"
                )
            skipped.append(filepath)
            continue

        reason = (
            "direct change"
            if filepath in directly_changed_set
            else "implicit (includes changed record)"
        )

        try:
            new_content = update_schema_version(current_content, new_version)
        except Exception as e:
            print(f"ERROR {filepath}: {e}", file=sys.stderr)
            errors.append(filepath)
            continue

        if args.dry_run:
            print(
                f"BUMP  {filepath}  v{base_version} → v{new_version}"
                f"  [{reason}]  [dry-run]"
            )
            bumped.append(filepath)
        else:
            path.write_text(new_content, encoding="utf-8")
            print(f"BUMP  {filepath}  v{base_version} → v{new_version}  [{reason}]")
            bumped.append(filepath)

    print(
        f"\nSummary: {len(bumped)} bumped, {len(skipped)} skipped, {len(errors)} errors"
    )

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
