#!/usr/bin/env python3
"""
Verify that pyproject.toml is equivalent to setup.py.

Loads both files, resolves self-referencing extras in pyproject.toml,
and compares every dependency set, entry point, and metadata field.

Usage:
    python scripts/verify_pyproject_equivalence.py
"""

import re
import sys
from pathlib import Path
from typing import Dict, Set

import toml

SCRIPT_DIR = Path(__file__).parent
METADATA_INGESTION_DIR = SCRIPT_DIR.parent

CIRCULAR_EXTRAS = {"airflow", "great-expectations"}
SELF_REF_PATTERN = re.compile(r"^acryl-datahub\[(.+)\]$")


def load_setup_py_variables() -> Dict:
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


def load_pyproject() -> Dict:
    return toml.load(METADATA_INGESTION_DIR / "pyproject.toml")


def resolve_extra(
    extra_name: str,
    optional_deps: Dict[str, list],
    cache: Dict[str, Set[str]],
) -> Set[str]:
    """Recursively resolve a pyproject.toml extra, expanding self-references."""
    if extra_name in cache:
        return cache[extra_name]

    # Guard against infinite recursion
    cache[extra_name] = set()

    deps: Set[str] = set()
    for dep in optional_deps.get(extra_name, []):
        m = SELF_REF_PATTERN.match(dep)
        if m:
            for ref in m.group(1).split(","):
                ref = ref.strip()
                deps |= resolve_extra(ref, optional_deps, cache)
        else:
            deps.add(dep)

    cache[extra_name] = deps
    return deps


def normalize_entry_point(ep: str) -> tuple:
    """Normalize 'name = target' string to (name, target) tuple."""
    parts = ep.split(" = ", 1) if " = " in ep else ep.split("=", 1)
    return (parts[0].strip(), parts[1].strip())


def compare_sets(label: str, setup_set: Set[str], pyproject_set: Set[str]) -> bool:
    if setup_set == pyproject_set:
        print(f"  {label}... OK ({len(setup_set)} deps)")
        return True

    only_setup = setup_set - pyproject_set
    only_pyproject = pyproject_set - setup_set
    print(f"  {label}... MISMATCH")
    if only_setup:
        print(f"    In setup.py only: {sorted(only_setup)}")
    if only_pyproject:
        print(f"    In pyproject.toml only: {sorted(only_pyproject)}")
    return False


def check_entry_points(entry_points: Dict, pyproject: Dict) -> tuple:
    """Check entry points match between setup.py and pyproject.toml."""
    total = 0
    mismatches = 0
    pyproject_scripts = pyproject.get("project", {}).get("scripts", {})
    pyproject_entry_points = pyproject.get("project", {}).get("entry-points", {})

    for group, ep_list in sorted(entry_points.items()):
        total += 1
        setup_eps = {normalize_entry_point(ep) for ep in ep_list}

        if group == "console_scripts":
            pyproject_eps = {(k, v) for k, v in pyproject_scripts.items()}
        elif group == "datahub.custom_packages":
            pyproject_eps = set()
            if not ep_list:
                print(f"  {group}... OK (empty)")
                continue
        else:
            group_dict = pyproject_entry_points.get(group, {})
            pyproject_eps = {(k, v) for k, v in group_dict.items()}

        if setup_eps == pyproject_eps:
            print(f"  {group}... OK ({len(setup_eps)} entries)")
        else:
            only_setup = setup_eps - pyproject_eps
            only_pyproject = pyproject_eps - setup_eps
            print(f"  {group}... MISMATCH")
            if only_setup:
                print(f"    In setup.py only: {sorted(only_setup)}")
            if only_pyproject:
                print(f"    In pyproject.toml only: {sorted(only_pyproject)}")
            mismatches += 1

    return total, mismatches


def main():
    ns = load_setup_py_variables()
    pyproject = load_pyproject()

    base_requirements: Set[str] = ns["base_requirements"]
    framework_common: Set[str] = ns["framework_common"]
    plugins: Dict[str, Set[str]] = ns["plugins"]
    all_exclude_plugins: Set[str] = ns["all_exclude_plugins"]
    entry_points: Dict = ns["entry_points"]

    optional_deps = pyproject.get("project", {}).get("optional-dependencies", {})
    resolve_cache: Dict[str, Set[str]] = {}

    mismatches = 0
    total = 0

    # --- Base dependencies ---
    print("=== Base dependencies ===")
    setup_base = base_requirements | framework_common
    pyproject_base = set(pyproject.get("project", {}).get("dependencies", []))
    total += 1
    if not compare_sets("dependencies", setup_base, pyproject_base):
        mismatches += 1

    # --- Plugin extras ---
    print("\n=== Plugin extras ===")
    for plugin_name in sorted(plugins.keys()):
        if plugin_name in CIRCULAR_EXTRAS:
            continue
        total += 1
        setup_total = setup_base | framework_common | plugins[plugin_name]
        pyproject_resolved = resolve_extra(plugin_name, optional_deps, resolve_cache)
        pyproject_total = pyproject_base | pyproject_resolved
        if not compare_sets(plugin_name, setup_total, pyproject_total):
            mismatches += 1

    # --- Meta extras ---
    print("\n=== Meta extras ===")
    total += 1
    setup_all = setup_base | framework_common.union(
        *(deps for name, deps in plugins.items() if name not in all_exclude_plugins)
    )
    pyproject_all = pyproject_base | resolve_extra("all", optional_deps, resolve_cache)
    if not compare_sets("all", setup_all, pyproject_all):
        mismatches += 1

    # "base" extra: deps are already in [project].dependencies, so
    # pyproject.toml has base = []. Just verify the extra exists.
    total += 1
    if "base" in optional_deps:
        print("  base... OK (empty marker, deps already in dependencies)")
    else:
        print("  base... MISSING (required by Docker builds)")
        mismatches += 1

    # "cloud" extra
    total += 1
    setup_cloud = {"acryl-datahub-cloud"}
    pyproject_cloud = resolve_extra("cloud", optional_deps, resolve_cache)
    if not compare_sets("cloud", setup_cloud, pyproject_cloud):
        mismatches += 1

    # Non-plugin meta extras are standalone sets
    meta_extras = {
        "dev": ns["dev_requirements"],
        "docs": ns["docs_requirements"],
        "lint": ns["lint_requirements"],
        "testing-utils": ns["test_api_requirements"],
        "integration-tests": ns["full_test_dev_requirements"],
        "debug": ns["debug_requirements"],
    }
    for extra_name, setup_deps in sorted(meta_extras.items()):
        total += 1
        pyproject_deps = resolve_extra(extra_name, optional_deps, resolve_cache)
        if not compare_sets(extra_name, setup_deps, pyproject_deps):
            mismatches += 1

    # --- Entry points ---
    print("\n=== Entry points ===")
    ep_total, ep_mismatches = check_entry_points(entry_points, pyproject)
    total += ep_total
    mismatches += ep_mismatches

    # --- Project metadata ---
    print("\n=== Project metadata ===")
    project = pyproject.get("project", {})
    total += 1
    meta_ok = True
    if project.get("name") != "acryl-datahub":
        print("  name... MISMATCH")
        meta_ok = False
    if project.get("requires-python") != ">=3.10":
        print(f"  requires-python... MISMATCH: {project.get('requires-python')}")
        meta_ok = False
    if meta_ok:
        print("  metadata... OK")
    else:
        mismatches += 1

    # --- Circular extras ---
    print("\n=== Intentionally excluded (circular) ===")
    for name in sorted(CIRCULAR_EXTRAS):
        in_setup = name in plugins
        in_pyproject = name in optional_deps
        status = "present" if in_pyproject else "excluded"
        print(
            f"  {name}: setup.py={'yes' if in_setup else 'no'}, pyproject.toml={status}"
        )

    # --- Summary ---
    passed = total - mismatches
    print(f"\nResult: {passed}/{total} checks passed, {mismatches} mismatches")
    sys.exit(1 if mismatches else 0)


if __name__ == "__main__":
    main()
