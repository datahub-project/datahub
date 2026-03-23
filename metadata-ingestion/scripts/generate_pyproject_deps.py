#!/usr/bin/env python3
"""
Generate pyproject.toml dependency sections from setup.py.

Resolves all Python set math (sql_common | postgres_common | aws_common) in
memory and outputs fully flattened, inlined dependency arrays per plugin.
No self-referencing extras — pyproject.toml is treated as compiled output.

setup.py remains the human-readable source of truth where developers use DRY
Python set composition and maintain comments (CVE refs, version history, etc.).

Usage:
    python scripts/generate_pyproject_deps.py
"""

import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

from packaging.requirements import Requirement
from packaging.version import Version

SCRIPT_DIR = Path(__file__).parent
METADATA_INGESTION_DIR = SCRIPT_DIR.parent

# Extras that create circular dependencies with uv lock
CIRCULAR_EXTRAS = {"airflow", "great-expectations"}


def load_setup_py_variables() -> Dict:
    """Load variables from setup.py by executing it in a controlled namespace."""
    setup_py_path = METADATA_INGESTION_DIR / "setup.py"
    namespace: Dict = {
        "__name__": "__not_main__",
        "__file__": str(setup_py_path),
    }
    with open(setup_py_path) as f:
        code = f.read()
    code = code.replace("setuptools.setup(", "_setup_args = dict(")
    exec(code, namespace)
    assert "_setup_args" in namespace, (
        "setup.py did not produce _setup_args — the setuptools.setup() replacement failed. "
        "Check if setup.py changed its calling convention."
    )
    return namespace


def _spec_sort_key(spec_str: str) -> Tuple[int, str]:
    """Sort specifiers: >= > == ~= != <= <"""
    order = {">=": 0, ">": 1, "==": 2, "~=": 3, "!=": 4, "<=": 5, "<": 6}
    for op in sorted(order.keys(), key=len, reverse=True):
        if spec_str.startswith(op):
            return (order[op], spec_str[len(op) :])
    return (99, spec_str)


def _parse_spec(spec_str: str) -> Tuple[str, str]:
    """Split a specifier string into (operator, version)."""
    for op in (">=", ">", "<=", "<", "!=", "==", "~="):
        if spec_str.startswith(op):
            return (op, spec_str[len(op) :])
    return ("", spec_str)


def _narrow_specifiers(specs: Set[str]) -> Set[str]:
    """Simplify redundant specifiers to the narrowest constraint.

    For upper bounds (<, <=): keep only the lowest (most restrictive).
    For lower bounds (>=, >): keep only the highest (most restrictive).
    For exclusions and pins (!=, ==, ~=): keep all unique.
    """
    by_op: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for spec_str in specs:
        op, ver = _parse_spec(spec_str)
        by_op[op].append((ver, spec_str))

    result: Set[str] = set()
    for op, entries in by_op.items():
        if op in (">=", ">"):
            # Keep highest version = most restrictive lower bound
            # Tiebreaker: prefer longer string (more explicit, e.g. 2.0.0 > 2.0)
            try:
                best = max(entries, key=lambda x: (Version(x[0]), len(x[0])))
                result.add(best[1])
            except Exception:
                result.update(s for _, s in entries)
        elif op in ("<=", "<"):
            # Keep lowest version = most restrictive upper bound
            # Tiebreaker: prefer longer string (more explicit, e.g. 3.0.0 > 3.0)
            try:
                best = min(entries, key=lambda x: (Version(x[0]), -len(x[0])))
                result.add(best[1])
            except Exception:
                result.update(s for _, s in entries)
        else:
            # !=, ==, ~=: keep all unique
            result.update(s for _, s in entries)

    return result


def merge_duplicate_deps(deps: Set[str]) -> Set[str]:
    """Merge duplicate package specifiers in a dependency set.

    When flattening setup.py's set math, the same package can appear multiple
    times with different specifiers or extras (e.g. from sql_common and aws_common).
    This merges them into a single entry per (package, marker) pair:
    - Union extras: smart-open[s3] + smart-open[azure] -> smart-open[azure,s3]
    - Narrow specifiers: pandas<2.2.0 + pandas<3.0.0 -> pandas<2.2.0
    - Entries with different environment markers are kept separate.
    """
    groups: Dict[Tuple[str, str], List[Requirement]] = defaultdict(list)
    originals: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for dep_str in deps:
        try:
            req = Requirement(dep_str)
        except Exception:
            # Unparseable — keep original string as-is
            key = ("__unparseable__", dep_str)
            groups[key].append(None)  # type: ignore[arg-type]
            originals[key].append(dep_str)
            continue

        marker_str = str(req.marker) if req.marker else ""
        key = (req.name.lower().replace("-", "_"), marker_str)
        groups[key].append(req)
        originals[key].append(dep_str)

    result: Set[str] = set()
    for key, reqs in groups.items():
        if key[0] == "__unparseable__" or len(reqs) == 1:
            result.add(originals[key][0])
            continue

        # Merge extras (union)
        merged_extras: Set[str] = set()
        for req in reqs:
            merged_extras |= req.extras

        # Collect all specifier clauses, then narrow to tightest constraint
        all_specs: Set[str] = set()
        for req in reqs:
            for spec in req.specifier:
                all_specs.add(str(spec))
        narrowed = _narrow_specifiers(all_specs)

        # Reconstruct using original package name casing
        pkg_name = reqs[0].name
        marker_str = key[1]

        parts = [pkg_name]
        if merged_extras:
            parts.append("[" + ",".join(sorted(merged_extras)) + "]")
        if narrowed:
            parts.append(",".join(sorted(narrowed, key=_spec_sort_key)))
        if marker_str:
            parts.append(" ; " + marker_str)

        result.add("".join(parts))

    return result


def sort_deps(deps: Set[str]) -> List[str]:
    merged = merge_duplicate_deps(deps)
    return sorted(merged, key=lambda x: x.lower())


def format_toml_list(items: List[str], indent: str = "    ") -> str:
    if not items:
        return "[]"
    lines = ["["]
    for item in items:
        escaped = item.replace('"', '\\"')
        lines.append(f'{indent}"{escaped}",')
    lines.append("]")
    return "\n".join(lines)


def generate_pyproject_toml() -> str:
    ns = load_setup_py_variables()

    base_requirements: Set[str] = ns["base_requirements"]
    framework_common: Set[str] = ns["framework_common"]
    plugins: Dict[str, Set[str]] = ns["plugins"]
    all_exclude_plugins: Set[str] = ns["all_exclude_plugins"]
    test_api_requirements: Set[str] = ns["test_api_requirements"]
    lint_requirements: Set[str] = ns["lint_requirements"]
    dev_requirements: Set[str] = ns["dev_requirements"]
    docs_requirements: Set[str] = ns["docs_requirements"]
    full_test_dev_requirements: Set[str] = ns["full_test_dev_requirements"]
    debug_requirements: Set[str] = ns["debug_requirements"]
    entry_points: Dict = ns["entry_points"]

    # framework_common deps are in [project].dependencies, so plugin extras
    # only need their unique deps (deps beyond the base).
    fw = frozenset(base_requirements | framework_common)

    output_lines: List[str] = []

    # Header
    output_lines.append("# ===== DO NOT EDIT dependency sections by hand =====")
    output_lines.append("#")
    output_lines.append("# This file is generated from setup.py by:")
    output_lines.append("#   python scripts/generate_pyproject_deps.py")
    output_lines.append("#")
    output_lines.append(
        "# setup.py is the human-readable source of truth for dependencies."
    )
    output_lines.append("# After regenerating, verify equivalence with setup.py:")
    output_lines.append("#   python scripts/verify_pyproject_equivalence.py")
    output_lines.append("")

    # Build system
    output_lines.append("[build-system]")
    output_lines.append('build-backend = "setuptools.build_meta"')
    output_lines.append('requires = ["setuptools>=78.1.1", "wheel"]')
    output_lines.append("")

    # Project metadata
    output_lines.append("[project]")
    output_lines.append('name = "acryl-datahub"')
    output_lines.append('dynamic = ["version"]')
    output_lines.append('description = "A CLI to work with DataHub metadata"')
    output_lines.append('readme = "README.md"')
    output_lines.append('license = "Apache-2.0"')
    output_lines.append('requires-python = ">=3.10"')
    output_lines.append("classifiers = [")
    for c in [
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ]:
        output_lines.append(f'    "{c}",')
    output_lines.append("]")
    output_lines.append("")

    # Base dependencies (base_requirements + framework_common)
    base_deps = sort_deps(base_requirements | framework_common)
    output_lines.append("dependencies = " + format_toml_list(base_deps))
    output_lines.append("")

    # Project URLs
    output_lines.append("[project.urls]")
    output_lines.append('Homepage = "https://docs.datahub.com/"')
    output_lines.append('Documentation = "https://docs.datahub.com/docs/"')
    output_lines.append('Source = "https://github.com/datahub-project/datahub"')
    output_lines.append(
        'Changelog = "https://github.com/datahub-project/datahub/releases"'
    )
    output_lines.append('Releases = "https://github.com/acryldata/datahub/releases"')
    output_lines.append("")

    # === Optional dependencies (fully flattened) ===
    output_lines.append("[project.optional-dependencies]")
    output_lines.append("")

    # base extra: empty marker referenced in Docker builds
    output_lines.append("base = []")
    output_lines.append("")

    # Plugin extras — each plugin's deps are fully inlined (no self-references)
    output_lines.append("# airflow and great-expectations excluded (circular deps).")
    output_lines.append(
        "# Install acryl-datahub-airflow-plugin / acryl-datahub-gx-plugin directly."
    )
    output_lines.append("")

    for plugin_name in sorted(plugins.keys()):
        if plugin_name in CIRCULAR_EXTRAS:
            continue
        plugin_deps = plugins[plugin_name]
        unique_deps = plugin_deps - fw
        output_lines.append(
            f"{plugin_name} = " + format_toml_list(sort_deps(unique_deps))
        )
        output_lines.append("")

    # "all" extra
    output_lines.append(
        "# All plugins (excluding: " + ", ".join(sorted(all_exclude_plugins)) + ")"
    )
    all_deps: Set[str] = set()
    for plugin_name, plugin_deps in plugins.items():
        if plugin_name not in all_exclude_plugins:
            all_deps |= plugin_deps
    all_unique = all_deps - fw
    output_lines.append("all = " + format_toml_list(sort_deps(all_unique)))
    output_lines.append("")

    # Meta extras: cloud, dev, docs, lint, testing-utils, integration-tests, debug
    output_lines.append('cloud = ["acryl-datahub-cloud"]')
    output_lines.append("")
    output_lines.append("dev = " + format_toml_list(sort_deps(dev_requirements)))
    output_lines.append("")
    output_lines.append("docs = " + format_toml_list(sort_deps(docs_requirements)))
    output_lines.append("")
    output_lines.append("lint = " + format_toml_list(sort_deps(lint_requirements)))
    output_lines.append("")
    output_lines.append(
        "testing-utils = " + format_toml_list(sort_deps(test_api_requirements))
    )
    output_lines.append("")
    output_lines.append(
        "integration-tests = " + format_toml_list(sort_deps(full_test_dev_requirements))
    )
    output_lines.append("")
    output_lines.append("debug = " + format_toml_list(sort_deps(debug_requirements)))
    output_lines.append("")

    # Entry points - scripts
    output_lines.append("[project.scripts]")
    for script in entry_points.get("console_scripts", []):
        name, target = script.split(" = ")
        output_lines.append(f'{name} = "{target}"')
    output_lines.append("")

    # Entry points - plugins
    for ep_group, ep_list in entry_points.items():
        if ep_group == "console_scripts":
            continue
        if not ep_list:
            continue
        output_lines.append(f'[project.entry-points."{ep_group}"]')
        for ep in ep_list:
            name, target = ep.split(" = ")
            if "." in name:
                name = f'"{name}"'
            output_lines.append(f'{name} = "{target}"')
        output_lines.append("")

    # Setuptools config
    output_lines.append("[tool.setuptools.dynamic]")
    output_lines.append('version = {attr = "datahub._version.__version__"}')
    output_lines.append("")
    output_lines.append("[tool.setuptools.packages.find]")
    output_lines.append('where = ["src"]')
    output_lines.append("")
    package_data: Dict = ns["_setup_args"].get("package_data", {})
    output_lines.append("[tool.setuptools.package-data]")
    for pkg_name in sorted(package_data.keys()):
        patterns = package_data[pkg_name]
        patterns_toml = ", ".join(f'"{p}"' for p in patterns)
        if "." in pkg_name:
            output_lines.append(f'"{pkg_name}" = [{patterns_toml}]')
        else:
            output_lines.append(f"{pkg_name} = [{patterns_toml}]")
    output_lines.append("")

    # End of generated section marker
    output_lines.append("# ===== END OF GENERATED SECTION =====")
    output_lines.append("")

    return "\n".join(output_lines)


MANUAL_SECTION_MARKER = "# ===== TOOL CONFIGURATION"


def read_manual_sections(pyproject_path: Path) -> str:
    """Read the manually maintained [tool.*] sections from existing pyproject.toml."""
    if not pyproject_path.exists():
        return ""
    content = pyproject_path.read_text()
    for i, line in enumerate(content.splitlines()):
        if line.startswith(MANUAL_SECTION_MARKER):
            return "\n".join(content.splitlines()[i:]) + "\n"
    return ""


def main():
    try:
        pyproject_path = METADATA_INGESTION_DIR / "pyproject.toml"
        generated = generate_pyproject_toml()
        manual = read_manual_sections(pyproject_path)
        combined = generated + "\n" + manual if manual else generated + "\n"
        pyproject_path.write_text(combined)
        print(f"Wrote {pyproject_path}")
    except Exception as e:
        print(f"Error generating pyproject.toml: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
