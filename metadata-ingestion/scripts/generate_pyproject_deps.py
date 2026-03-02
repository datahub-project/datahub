#!/usr/bin/env python3
"""
Generate pyproject.toml dependencies from setup.py.

Uses self-referencing extras (PEP 621) to avoid duplicating shared dependency
sets across plugins. Shared sets like sql_common, aws_common, etc. become
their own extras, and plugins reference them via acryl-datahub[sql-common].

Usage:
    python scripts/generate_pyproject_deps.py > pyproject_deps.toml
    # Then manually merge into pyproject.toml
"""

import sys
from pathlib import Path
from typing import Dict, FrozenSet, List, Set, Tuple

SCRIPT_DIR = Path(__file__).parent
METADATA_INGESTION_DIR = SCRIPT_DIR.parent

# Shared dependency sets from setup.py that become their own extras.
# Order matters: larger/composite sets first so greedy matching works correctly.
SHARED_SET_NAMES: List[str] = [
    # Composite sets (contain other shared sets)
    "sql_common",
    "mysql_common",
    "looker_common",
    "s3_base",
    "delta_lake",
    "notion_common",
    "confluence_common",
    "unstructured_lib",
    # Medium shared sets
    "snowflake_common",
    "bigquery_common",
    "redshift_common",
    "iceberg_common",
    "databricks_common",
    "databricks",
    "abs_base",
    "data_lake_profiling",
    "aws_common",
    "kafka_common",
    "kafka_protobuf",
    "pyhive_common",
    "mssql_common",
    "postgres_common",
    "clickhouse_common",
    "dataplex_common",
    "superset_common",
    "embedding_common",
    # Small shared sets
    "sqlglot_lib",
    "sqlalchemy_lib",
    "classification_lib",
    "great_expectations_lib",
    "dbt_common",
    "usage_common",
    "cachetools_lib",
    "pyarrow_common",
    "path_spec_common",
    "rest_common",
    "microsoft_common",
    "threading_timeout_common",
    "azure_data_factory",
    "powerbi_report_server",
    "slack",
    "trino",
    "mysql",
    "sac",
]

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


def sort_deps(deps) -> List[str]:
    return sorted(deps, key=lambda x: x.lower())


def to_extra_name(var_name: str) -> str:
    """Convert Python variable name to PEP 621 extra name."""
    return var_name.replace("_", "-")


def format_toml_list(items: List[str], indent: str = "    ") -> str:
    if not items:
        return "[]"
    lines = ["["]
    for item in items:
        escaped = item.replace('"', '\\"')
        lines.append(f'{indent}"{escaped}",')
    lines.append("]")
    return "\n".join(lines)


def decompose_plugin(
    plugin_deps: FrozenSet[str],
    framework_common: FrozenSet[str],
    shared_sets: List[Tuple[str, FrozenSet[str]]],
) -> Tuple[List[str], List[str]]:
    """Decompose a plugin's deps into self-references + unique deps.

    Returns (extra_refs, unique_deps) where extra_refs are like
    "acryl-datahub[sql-common]" and unique_deps are individual packages.
    """
    plugin_unique = plugin_deps - framework_common
    remaining = set(plugin_unique)
    refs: List[str] = []

    for var_name, shared_deps in shared_sets:
        overlap = shared_deps - framework_common
        # Shared set's deps must all be within the plugin's full dep set
        # (don't reference a shared set that would add unwanted deps).
        # But only require it to contribute at least one dep still in remaining
        # (other deps may already be covered by an earlier shared set reference).
        if overlap and overlap <= plugin_unique:
            contribution = overlap & remaining
            if contribution:
                refs.append(to_extra_name(var_name))
                remaining -= contribution

    extra_refs = []
    if refs:
        extra_refs = [f"acryl-datahub[{','.join(sorted(refs))}]"]
    return extra_refs, sort_deps(remaining)


def generate_pyproject_toml():
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

    fw_frozen = frozenset(framework_common)

    # Load shared sets from setup.py namespace, skipping names that
    # collide with plugin names (those are handled as plugins directly)
    plugin_extra_names = {p.replace("_", "-") for p in plugins}
    shared_sets: List[Tuple[str, FrozenSet[str]]] = []
    for var_name in SHARED_SET_NAMES:
        extra_name = to_extra_name(var_name)
        if extra_name in plugin_extra_names:
            continue
        if var_name in ns and isinstance(ns[var_name], set):
            shared_sets.append((var_name, frozenset(ns[var_name])))

    output_lines: List[str] = []

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

    # Base dependencies
    base_deps = sort_deps(base_requirements | framework_common)
    output_lines.append("dependencies = " + format_toml_list(base_deps))
    output_lines.append("")

    # Project URLs (must come after all [project] keys)
    output_lines.append("[project.urls]")
    output_lines.append('Homepage = "https://docs.datahub.com/"')
    output_lines.append('Documentation = "https://docs.datahub.com/docs/"')
    output_lines.append('Source = "https://github.com/datahub-project/datahub"')
    output_lines.append(
        'Changelog = "https://github.com/datahub-project/datahub/releases"'
    )
    output_lines.append('Releases = "https://github.com/acryldata/datahub/releases"')
    output_lines.append("")

    # === Optional dependencies ===
    output_lines.append("[project.optional-dependencies]")
    output_lines.append("")

    # base extra: referenced in Docker builds (e.g., [base,datahub-rest,...]).
    # Its deps are already in [project].dependencies, so this is an empty marker.
    output_lines.append("base = []")
    output_lines.append("")

    # Shared dependency sets as extras (enables self-referencing)
    output_lines.append("# --- Shared dependency sets ---")
    output_lines.append(
        "# These mirror the named sets in setup.py (sql_common, aws_common, etc.)."
    )
    output_lines.append(
        "# Plugins reference them via acryl-datahub[sql-common] instead of"
    )
    output_lines.append("# duplicating every dependency.")
    output_lines.append("")

    for var_name, shared_deps in shared_sets:
        extra_name = to_extra_name(var_name)
        # Exclude self from decomposition targets to avoid self-matching
        other_shared = [(n, d) for n, d in shared_sets if n != var_name]
        own_refs, own_unique = decompose_plugin(shared_deps, fw_frozen, other_shared)
        all_items = own_refs + own_unique
        if all_items:
            output_lines.append(f"{extra_name} = " + format_toml_list(all_items))
            output_lines.append("")

    # Plugin extras
    output_lines.append("# --- Plugin extras ---")
    output_lines.append("# airflow and great-expectations excluded (circular deps).")
    output_lines.append(
        "# Use acryl-datahub-airflow-plugin / acryl-datahub-gx-plugin directly."
    )
    output_lines.append("")

    for plugin_name in sorted(plugins.keys()):
        if plugin_name in CIRCULAR_EXTRAS:
            continue
        plugin_deps = frozenset(plugins[plugin_name])
        refs, unique = decompose_plugin(plugin_deps, fw_frozen, shared_sets)
        all_items = refs + unique
        if all_items:
            output_lines.append(f"{plugin_name} = " + format_toml_list(all_items))
        else:
            output_lines.append(f"{plugin_name} = []")
        output_lines.append("")

    # "all" extra
    output_lines.append(
        "# All plugins (excluding: " + ", ".join(sorted(all_exclude_plugins)) + ")"
    )
    all_deps = framework_common.copy()
    for plugin_name, plugin_deps in plugins.items():
        if plugin_name not in all_exclude_plugins:
            all_deps |= plugin_deps
    all_frozen = frozenset(all_deps)
    all_refs, all_unique = decompose_plugin(all_frozen, fw_frozen, shared_sets)
    output_lines.append("all = " + format_toml_list(all_refs + all_unique))
    output_lines.append("")

    # Cloud, dev, docs, lint, testing-utils, integration-tests, debug
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
    output_lines.append("[tool.setuptools.package-data]")
    output_lines.append('datahub = ["py.typed"]')
    output_lines.append('"datahub.metadata" = ["schema.avsc"]')
    output_lines.append('"datahub.metadata.schemas" = ["*.avsc"]')
    output_lines.append(
        '"datahub.ingestion.source.powerbi" = ["powerbi-lexical-grammar.rule"]'
    )
    output_lines.append('"datahub.ingestion.autogenerated" = ["*.json"]')
    output_lines.append("")

    return "\n".join(output_lines)


def main():
    try:
        output = generate_pyproject_toml()
        print(output)
    except Exception as e:
        print(f"Error generating pyproject.toml: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
