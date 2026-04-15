#!/usr/bin/env python3
"""
Selective CI checks for DataHub metadata-ingestion.
Modelled after Apache Airflow's selective_checks.py.

Outputs a JSON object consumed by GitHub Actions downstream jobs.
Only files under source/{connector}/ get selective treatment.
Everything else that triggers the workflow -> run_all_integration=true.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
import typing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TypedDict

import yaml

# The only path that gets selective treatment.
# Everything else that triggers the workflow -> run_all_integration=true.
CONNECTOR_SOURCE_PREFIX = "metadata-ingestion/src/datahub/ingestion/source/"

# Integration test path prefix — changes here trigger that connector's tests
INTEGRATION_TEST_PREFIX = "metadata-ingestion/tests/integration/"

# Paths under metadata-ingestion/ that are safe to ignore
# (no integration tests needed)
SAFE_PREFIXES = [
    "metadata-ingestion/tests/unit/",
    "metadata-ingestion/tests/performance/",
    "metadata-ingestion/tests/conftest",
    "metadata-ingestion/scripts/",
    "metadata-ingestion/docs/",
]

IGNORED_SOURCE_EXTENSIONS = (".md",)

# Prefix for all metadata-ingestion-relative paths in repo-root form
MI_PREFIX = "metadata-ingestion/"

# Relative prefix for integration test directory paths (within metadata-ingestion/)
MI_TEST_INTEGRATION_PREFIX = "tests/integration/"


def _source_path_to_module(path: str) -> str:
    """Convert a metadata-ingestion-relative source path to a dotted module name.

    Examples:
        src/datahub/ingestion/source/sql       -> datahub.ingestion.source.sql
        src/datahub/ingestion/source/feast.py  -> datahub.ingestion.source.feast
        src/datahub/ingestion/source/sql/clickhouse.py
                                               -> datahub.ingestion.source.sql.clickhouse
    """
    module = path.replace("/", ".").removeprefix("src.")
    if path.endswith(".py"):
        module = module.removesuffix(".py")
    return module


def _find_source_dir(module: str, sorted_registry: list[ConnectorEntry]) -> str | None:
    """Return the source dir whose module prefix best matches (longest match wins)."""
    for connector in sorted_registry:
        prefix = _source_path_to_module(connector.source_dir)
        if module == prefix or module.startswith(prefix + "."):
            return connector.source_dir
    return None


def _register_plugin_variants(mapping: dict[str, str], name: str, module: str) -> None:
    """Register all lookup variants for a plugin entry-point name.

    Registers the original name, hyphen<->underscore swaps, and all three forms
    with the "datahub-" prefix stripped — so test dir names like "clickhouse"
    match entry points like "datahub-clickhouse" or "clickhouse".
    """
    variants = {name, name.replace("-", "_"), name.replace("_", "-")}
    if name.startswith("datahub-"):
        short = name.removeprefix("datahub-")
        variants |= {short, short.replace("-", "_"), short.replace("_", "-")}
    for v in variants:
        mapping[v] = module


class TestMatrixEntry(TypedDict):
    connector: str
    test_path: str


@dataclass(frozen=True)
class ConnectorEntry:
    """A discovered connector in the registry."""

    source_dir: str
    is_single_file: bool = False
    test_path: str | None = None
    extra_test_paths: list[str] = field(default_factory=list)
    extra_source_paths: list[str] = field(default_factory=list)
    test_paths: list[str] = field(default_factory=list)  # legacy, validate-only


@dataclass(frozen=True)
class CIDecisions:
    test_matrix: list[TestMatrixEntry] = field(default_factory=list)
    run_all_integration: bool = False

    def __post_init__(self) -> None:
        if self.run_all_integration and self.test_matrix:
            raise ValueError(
                "run_all_integration=True and non-empty test_matrix are mutually exclusive"
            )

    @classmethod
    def run_all(cls) -> "CIDecisions":
        return cls(run_all_integration=True)

    @classmethod
    def run_selected(cls, tests: list[TestMatrixEntry]) -> "CIDecisions":
        if not tests:
            return cls()
        return cls(test_matrix=tests)

    @classmethod
    def run_none(cls) -> "CIDecisions":
        return cls()


def get_changed_files(base_ref: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"origin/{base_ref}...HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"ERROR: git diff failed (exit {result.returncode}).\n"
            f"  stderr: {result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)
    return [f.strip() for f in result.stdout.splitlines() if f.strip()]


def load_connector_registry(repo_root: Path) -> list[ConnectorEntry]:
    """
    Auto-discover connectors using convention (source dir -> test dir match)
    plus explicit connector.yaml overrides for exceptions.
    """
    mi = repo_root / "metadata-ingestion"
    src = mi / "src" / "datahub" / "ingestion" / "source"
    tests = mi / "tests" / "integration"

    if not src.is_dir():
        print(f"ERROR: Source directory {src} does not exist.", file=sys.stderr)
        sys.exit(1)

    registry: list[ConnectorEntry] = []

    for source_dir in sorted(src.iterdir()):
        if not source_dir.is_dir() or source_dir.name.startswith("_"):
            continue

        rel_source = str(source_dir.relative_to(mi))
        connector_yaml = source_dir / "connector.yaml"

        # Load explicit config if present, otherwise use convention
        if connector_yaml.exists():
            try:
                raw = yaml.safe_load(connector_yaml.read_text())
            except (yaml.YAMLError, OSError) as e:
                print(
                    f"ERROR: Failed to load {connector_yaml}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
            if raw is None:
                data: dict = {}
            elif not isinstance(raw, dict):
                print(
                    f"ERROR: {connector_yaml} must be a YAML mapping, got {type(raw).__name__}",
                    file=sys.stderr,
                )
                sys.exit(1)
            else:
                data = raw
        else:
            data = {}

        # Convention: test_path defaults to tests/integration/{source_dir_name}/
        test_path = data.get("test_path")
        if test_path is None:
            default_test_dir = tests / source_dir.name
            if default_test_dir.exists():
                test_path = f"{MI_TEST_INTEGRATION_PREFIX}{source_dir.name}/"

        registry.append(
            ConnectorEntry(
                source_dir=rel_source,
                test_path=test_path,
                extra_test_paths=data.get("extra_test_paths", []),
                extra_source_paths=data.get("extra_source_paths", []),
                test_paths=data.get("test_paths", []),
            )
        )

    # Also register single-file connectors (source/*.py — no subdirectory).
    for source_file in sorted(src.glob("*.py")):
        if source_file.name.startswith("_"):
            continue
        rel_path = str(source_file.relative_to(mi))
        name = source_file.stem
        single_test_path: str | None = None
        if (tests / name).exists():
            single_test_path = f"{MI_TEST_INTEGRATION_PREFIX}{name}/"
        registry.append(
            ConnectorEntry(
                source_dir=rel_path,
                is_single_file=True,
                test_path=single_test_path,
            )
        )

    return registry


def _resolve_import_deps(
    imported_modules: list[str],
    module_to_source_dir: dict[str, str],
    exclude_dir: str,
) -> set[str]:
    """Resolve a list of imported module names to their source directories."""
    deps: set[str] = set()
    for imported_module in imported_modules:
        for module_prefix, dep_src_dir in module_to_source_dir.items():
            if dep_src_dir == exclude_dir:
                continue
            if imported_module == module_prefix or imported_module.startswith(
                module_prefix + "."
            ):
                deps.add(dep_src_dir)
    return deps


def build_import_graph(
    repo_root: Path, registry: list[ConnectorEntry]
) -> dict[str, set[str]]:
    """
    Scan Python imports to build: source_dir -> set of source_dirs it imports from.

    For each connector, parse its .py files with ast and extract import targets.
    Map those imports back to known source directories in the registry.
    """
    mi_root = repo_root / "metadata-ingestion"

    # Build a lookup: Python module prefix -> source_dir
    module_to_source_dir: dict[str, str] = {}
    for connector in registry:
        module_prefix = _source_path_to_module(connector.source_dir)
        module_to_source_dir[module_prefix] = connector.source_dir

    graph: dict[str, set[str]] = {}

    for connector in registry:
        src_dir = connector.source_dir
        deps: set[str] = set()
        connector_path = mi_root / src_dir

        py_files: list[Path]
        if connector.is_single_file:
            py_files = [connector_path] if connector_path.is_file() else []
        else:
            py_files = list(connector_path.rglob("*.py"))

        for py_file in py_files:
            try:
                source_text = py_file.read_text()
            except (OSError, UnicodeDecodeError) as e:
                print(
                    f"  WARNING: Could not read {py_file} (connector: {src_dir}): {e}",
                    file=sys.stderr,
                )
                continue

            try:
                tree = ast.parse(source_text)
            except (SyntaxError, ValueError) as e:
                print(
                    f"  WARNING: Could not parse {py_file} (connector: {src_dir}): {e}, skipping",
                    file=sys.stderr,
                )
                continue

            for node in ast.walk(tree):
                imported_modules: list[str] = []
                if isinstance(node, ast.ImportFrom) and node.module:
                    imported_modules.append(node.module)
                elif isinstance(node, ast.Import):
                    imported_modules.extend(alias.name for alias in node.names)
                else:
                    continue

                deps.update(
                    _resolve_import_deps(
                        imported_modules, module_to_source_dir, src_dir
                    )
                )

        graph[src_dir] = deps

    return graph


def build_source_to_test_dirs(
    repo_root: Path, registry: list[ConnectorEntry]
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """
    Use setup.py entry points to map each integration test dir to its source dir,
    then return the reverse: source_dir -> set of test dirs.

    This covers thin-wrapper connectors whose source lives inside a shared base
    directory (e.g. clickhouse inside source/sql/) and thus can't be found by
    convention alone. The test dir name is used as the plugin/entry-point name.

    Example:
        tests/integration/clickhouse/ -> entry point "clickhouse"
          -> datahub.ingestion.source.sql.clickhouse
          -> source dir src/datahub/ingestion/source/sql
    """
    mi = repo_root / "metadata-ingestion"

    # Parse entry points from setup.py: plugin_name -> source_module
    ep_pattern = re.compile(
        r'"([a-z][a-z0-9_-]*)\s*=\s*(datahub\.ingestion\.source\.[^:]+):[^"]+"'
    )
    try:
        text = (mi / "setup.py").read_text()
    except OSError as e:
        print(
            f"  WARNING: Could not read setup.py at {mi / 'setup.py'}: {e}. "
            f"Entry-point-derived test narrowing will be disabled.",
            file=sys.stderr,
        )
        return {}, {}

    # Store both the original name and hyphen<->underscore variants for lookup.
    # Also strip the "datahub-" prefix (e.g. "datahub-business-glossary" -> "business-glossary")
    # so test dir names that omit the prefix are matched correctly.
    plugin_to_module: dict[str, str] = {}
    for name, module in ep_pattern.findall(text):
        _register_plugin_variants(plugin_to_module, name, module)

    # Build module-prefix -> source_dir lookup; longest match wins
    sorted_registry = sorted(registry, key=lambda c: len(c.source_dir), reverse=True)

    # For each test dir, resolve to a source dir and build the reverse map.
    # Also keep ep_module -> test_dir for fine-grained per-file narrowing.
    source_to_tests: dict[str, set[str]] = {}
    ep_module_to_test: dict[str, str] = {}
    tests_dir = mi / "tests" / "integration"

    if not tests_dir.is_dir():
        print(
            f"  WARNING: Integration tests directory {tests_dir} does not exist. "
            f"Entry-point-derived test mapping will be empty.",
            file=sys.stderr,
        )
        return source_to_tests, ep_module_to_test

    for test_dir in sorted(tests_dir.iterdir()):
        if not test_dir.is_dir() or test_dir.name.startswith("_"):
            continue

        test_rel = f"{MI_TEST_INTEGRATION_PREFIX}{test_dir.name}/"
        module = plugin_to_module.get(test_dir.name)
        if not module:
            continue

        ep_module_to_test[module] = test_rel
        src_dir = _find_source_dir(module, sorted_registry)
        if src_dir:
            source_to_tests.setdefault(src_dir, set()).add(test_rel)

    return source_to_tests, ep_module_to_test


def _narrow_ep_tests(
    source_dir: str,
    source_files: list[str],
    ep_module_to_test: dict[str, str],
) -> set[str] | None:
    """For files changed inside a shared-base source dir, return only the entry-point
    test dirs that map to those specific sub-modules.

    Returns None if any changed file has no entry-point match (shared utility) — the
    caller should fall back to the full source_to_test_dirs set in that case.
    Returns None also if no files in this dir are in source_files (nothing to narrow).

    Example: sql/clickhouse.py -> module datahub.ingestion.source.sql.clickhouse
             -> ep module datahub.ingestion.source.sql.clickhouse -> clickhouse tests only
             sql/sql_common.py -> no ep match -> returns None -> caller runs all SQL tests
    """
    mi_prefix = f"{MI_PREFIX}{source_dir}/"
    files_in_dir = [f for f in source_files if f.startswith(mi_prefix)]
    if not files_in_dir:
        return None

    narrowed: set[str] = set()
    for f in files_in_dir:
        module = _source_path_to_module(f.removeprefix(MI_PREFIX))
        matched: set[str] = set()
        for ep_module, test_dir in ep_module_to_test.items():
            # File is the ep module itself, or a sub-module, or a parent package of it
            if (
                module == ep_module
                or module.startswith(ep_module + ".")
                or ep_module.startswith(module + ".")
            ):
                matched.add(test_dir)
        if not matched:
            return None  # shared utility file — can't narrow
        narrowed.update(matched)

    return narrowed


def _match_source_files(
    source_files: list[str], registry: list[ConnectorEntry]
) -> tuple[set[str], list[str]]:
    """Return (changed_source_dirs, known_source_prefixes) for the given source files."""
    changed_source_dirs: set[str] = set()
    known_source_prefixes: list[str] = []

    for connector in registry:
        source_dir = connector.source_dir
        is_single_file = connector.is_single_file
        all_source_paths = [source_dir] + connector.extra_source_paths

        for p in all_source_paths:
            prefix = f"{MI_PREFIX}{p.rstrip('/')}"
            known_source_prefixes.append(prefix if is_single_file else prefix + "/")

        full_paths = [f"{MI_PREFIX}{p.rstrip('/')}" for p in all_source_paths]
        if is_single_file:
            matched = any(f in full_paths for f in source_files)
        else:
            matched = any(
                f.startswith(p + "/") for f in source_files for p in full_paths
            )
        if matched:
            changed_source_dirs.add(source_dir)

    return changed_source_dirs, known_source_prefixes


def classify(changed_files: list[str], repo_root: Path) -> CIDecisions:
    if not changed_files:
        return CIDecisions.run_none()

    # Classify each changed file into: connector source, integration test, safe, or unknown
    for f in changed_files:
        if f.startswith(CONNECTOR_SOURCE_PREFIX):
            continue  # handled below (connector source change)
        if f.startswith(INTEGRATION_TEST_PREFIX):
            continue  # handled below (integration test / golden file change)
        if any(f.startswith(p) for p in SAFE_PREFIXES):
            continue  # unit tests, scripts, docs — no integration tests needed
        if f.startswith("metadata-ingestion/") or f.startswith("metadata-models/"):
            return CIDecisions.run_all()

    # Load connector registry, import graph, and entry-point-derived test dir map
    registry = load_connector_registry(repo_root)
    import_graph = build_import_graph(repo_root, registry)
    source_to_test_dirs, ep_module_to_test = build_source_to_test_dirs(
        repo_root, registry
    )

    # Find which connector source directories were directly changed.
    # Any file in a connector source dir should trigger tests, except
    # documentation files (.md) which don't affect runtime behavior.
    source_files = [
        f
        for f in changed_files
        if f.startswith(CONNECTOR_SOURCE_PREFIX)
        and not any(f.endswith(ext) for ext in IGNORED_SOURCE_EXTENSIONS)
    ]

    changed_source_dirs, known_source_prefixes = _match_source_files(
        source_files, registry
    )

    # Check for .py files under source/ not covered by any connector -> full suite.
    # known_source_prefixes contains paths with trailing "/" for dirs and exact paths
    # for single-file connectors; startswith handles both cases correctly.
    for f in source_files:
        if not any(f.startswith(p) for p in known_source_prefixes):
            return CIDecisions.run_all()

    # For source dirs that host multiple thin-wrapper connectors (e.g. source/sql/),
    # try to narrow changed_source_dirs to only the specific sub-connector files that
    # changed. When narrowing succeeds, the dir is removed from changed_source_dirs so
    # it does NOT propagate through the import graph to unrelated connectors.
    # Example: sql/clickhouse.py -> add clickhouse tests, remove source/sql/ from
    #   changed_source_dirs so redshift/bigquery/etc. are not triggered.
    # Example: sql/sql_common.py -> narrowing fails, source/sql/ stays in
    #   changed_source_dirs, import graph propagates to all sql dependents correctly.
    pre_resolved_tests: set[str] = set()
    # Any source dir with entry-point-derived tests is a candidate for narrowing.
    narrowable = set(source_to_test_dirs.keys())
    for source_dir in list(changed_source_dirs & narrowable):
        narrowed = _narrow_ep_tests(source_dir, source_files, ep_module_to_test)
        if narrowed is not None:
            pre_resolved_tests.update(narrowed)
            changed_source_dirs.discard(source_dir)

    # Resolve import-derived dependencies and build test matrix
    test_paths: set[str] = set(pre_resolved_tests)

    # Integration test / golden file changes → add that test dir directly
    for f in changed_files:
        if f.startswith(INTEGRATION_TEST_PREFIX):
            # Extract: metadata-ingestion/tests/integration/{connector_name}/...
            rest = f[len(INTEGRATION_TEST_PREFIX) :]
            if "/" in rest:
                test_dir_name = rest.split("/")[0]
                test_paths.add(f"{MI_TEST_INTEGRATION_PREFIX}{test_dir_name}/")
            else:
                # File directly in tests/integration/ (e.g. conftest.py) — shared infra
                return CIDecisions.run_all()

    for connector in registry:
        source_dir = connector.source_dir
        deps = import_graph.get(source_dir, set())

        if source_dir in changed_source_dirs or deps & changed_source_dirs:
            if connector.test_path:
                test_paths.add(connector.test_path)
            test_paths.update(connector.extra_test_paths)
            test_paths.update(source_to_test_dirs.get(source_dir, set()))

    # Safety net: if we found changed source dirs but produced no tests,
    # something is wrong — fall back to running everything.
    if changed_source_dirs and not test_paths:
        print(
            f"  WARNING: Changed source dirs {sorted(changed_source_dirs)} produced no "
            f"test matrix entries. Falling back to run_all_integration=true.",
            file=sys.stderr,
        )
        return CIDecisions.run_all()

    return CIDecisions.run_selected(
        sorted(
            [
                TestMatrixEntry(connector=Path(tp).name, test_path=tp)
                for tp in test_paths
            ],
            key=lambda x: x["connector"],
        )
    )


def validate(repo_root: Path) -> list[str]:
    """Validate connector.yaml files and cross-check shared base coverage."""
    errors: list[str] = []
    warnings: list[str] = []
    registry = load_connector_registry(repo_root)
    source_to_test_dirs, _ = build_source_to_test_dirs(repo_root, registry)

    # 1. Every test_path declared must exist on disk
    mi = repo_root / "metadata-ingestion"
    for c in registry:
        all_paths = [c.test_path or ""] + c.extra_test_paths + c.test_paths
        for tp in all_paths:
            if tp and not (mi / tp).exists():
                errors.append(f"{c.source_dir}: test path '{tp}' does not exist")

    # 2. Warn about test dirs not covered by any connector
    # A test dir is covered if: listed in registry test_path/extra_test_paths/test_paths,
    # or matches a source dir by convention, or is in entry-point-derived mapping.
    ep_covered_tests: set[str] = set()
    for test_dirs in source_to_test_dirs.values():
        ep_covered_tests.update(test_dirs)

    tests_dir = mi / "tests" / "integration"
    if tests_dir.exists():
        for test_dir in sorted(tests_dir.iterdir()):
            if not test_dir.is_dir() or test_dir.name.startswith("_"):
                continue
            test_path = f"tests/integration/{test_dir.name}/"
            registry_covered = any(
                test_path == c.test_path
                or test_path in c.extra_test_paths
                or test_path in c.test_paths
                for c in registry
            )
            convention_covered = (mi / test_path).exists() and any(
                c.source_dir.endswith("/" + test_dir.name)
                or c.source_dir == f"src/datahub/ingestion/source/{test_dir.name}"
                for c in registry
            )
            if (
                not registry_covered
                and not convention_covered
                and test_path not in ep_covered_tests
            ):
                warnings.append(
                    f"tests/integration/{test_dir.name}/ is not covered by any "
                    f"connector — it will only run when run_all_integration=true"
                )

    # Print warnings to stderr
    for w in warnings:
        print(f"  WARNING: {w}", file=sys.stderr)

    return errors


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="Selective CI checks for metadata-ingestion"
    )
    parser.add_argument(
        "--base-ref", default="master", help="Base branch to diff against"
    )
    parser.add_argument(
        "--output", choices=["json", "gha"], default="gha", help="Output format"
    )
    parser.add_argument(
        "--validate", action="store_true", help="Validate connector.yaml mappings"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without setting outputs",
    )
    parser.add_argument(
        "--force-all", action="store_true", help="Force run_all_integration=true"
    )
    args = parser.parse_args()

    # Script lives at metadata-ingestion/scripts/selective_ci_checks.py
    repo_root = Path(__file__).resolve().parent.parent.parent
    if not (repo_root / "metadata-ingestion").is_dir():
        print(
            f"ERROR: Computed repo root {repo_root} does not contain metadata-ingestion/. "
            f"Is this script in the expected location?",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.validate:
        errors = validate(repo_root)
        if errors:
            print("Validation errors:", file=sys.stderr)
            for e in errors:
                print(f"  - {e}", file=sys.stderr)
            sys.exit(1)
        else:
            print("All connector mappings valid.")
            sys.exit(0)

    if args.force_all:
        decisions = CIDecisions.run_all()
    else:
        changed = get_changed_files(args.base_ref)
        decisions = classify(changed, repo_root)

    decisions_dict = asdict(decisions)

    # Always write the full JSON to a file for artifact upload / debugging
    json_path = Path("ci-decisions.json")
    json_path.write_text(json.dumps(decisions_dict, indent=2) + "\n")
    print(f"Wrote decisions to {json_path.resolve()}", file=sys.stderr)

    if args.dry_run or args.output == "json":
        print(json.dumps(decisions_dict, indent=2))
    else:
        # Write key=value pairs to GITHUB_OUTPUT
        output_file = os.environ.get("GITHUB_OUTPUT")
        fh: typing.IO[str]
        if output_file:
            try:
                fh = open(output_file, "a")
            except OSError as e:
                print(
                    f"ERROR: Cannot write to GITHUB_OUTPUT '{output_file}': {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            fh = sys.stdout
        try:
            for key, value in decisions_dict.items():
                if isinstance(value, list):
                    fh.write(f"{key}={json.dumps(value)}\n")
                else:
                    fh.write(f"{key}={str(value).lower()}\n")
        finally:
            if fh is not sys.stdout:
                fh.close()

        # Write a human-readable summary to GITHUB_STEP_SUMMARY
        summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_file:
            try:
                with open(summary_file, "a") as sf:
                    sf.write("## Selective CI Decisions\n\n")
                    if decisions.run_all_integration:
                        sf.write("**Mode:** Run all integration tests\n\n")
                    elif decisions.test_matrix:
                        sf.write(
                            f"**Mode:** Selective — {len(decisions.test_matrix)} connector(s)\n\n"
                        )
                        sf.write("| Connector | Test Path |\n")
                        sf.write("|-----------|----------|\n")
                        for entry in decisions.test_matrix:
                            sf.write(
                                f"| `{entry['connector']}` | `{entry['test_path']}` |\n"
                            )
                    else:
                        sf.write("**Mode:** No integration tests needed\n\n")
                    sf.write(
                        "\n<details><summary>Full JSON</summary>\n\n"
                        f"```json\n{json.dumps(decisions_dict, indent=2)}\n```\n\n"
                        "</details>\n"
                    )
            except OSError as e:
                print(
                    f"WARNING: Cannot write to GITHUB_STEP_SUMMARY: {e}",
                    file=sys.stderr,
                )
