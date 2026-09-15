# Selective CI for Integration Tests

The selective CI system runs only the integration tests relevant to the connectors changed in a PR, instead of running the full integration test suite every time.

## How it works

The `scripts/selective_ci_checks.py` script analyzes the list of changed files in a PR and decides which integration tests to run:

1. **If any file outside `source/{connector}/` changed** (e.g. `api/`, `emitter/`, `setup.py`, `metadata-models/`), it runs the full test suite — these are core changes that could affect any connector.

2. **If only connector source files changed**, it builds a targeted test matrix:

   - Each changed connector's integration tests are included.
   - If a connector imports from another connector (e.g. `bigquery_v2` imports from `sql/`), changing `sql/` also triggers `bigquery_v2` tests. Dependencies are resolved automatically by parsing Python imports with `ast`.

3. **Integration test or golden file changes** trigger that specific connector's tests (e.g. editing `tests/integration/powerbi/golden.json` runs PowerBI tests). Files directly in `tests/integration/` (not in a subdirectory) trigger the full suite since they are shared test infrastructure.

4. **Script-only, doc-only, or unit-test-only changes** skip integration tests entirely.

## Connector discovery

Connectors are discovered automatically through two complementary mechanisms:

### 1. Convention-based discovery

If `source/{name}/` exists and `tests/integration/{name}/` exists, the connector is registered automatically — no configuration needed.

For single-file connectors (`source/feast.py`, etc.), the corresponding `tests/integration/feast/` directory is matched with exact-path semantics.

### 2. Entry-point-based discovery

For connectors whose source lives inside a **shared directory** (e.g. `source/sql/`, `source/usage/`), the system reads the `datahub.ingestion.source.plugins` entry points from `setup.py` to map each integration test directory to its source module. This enables **per-file narrowing**: changing `source/sql/clickhouse.py` runs only `tests/integration/clickhouse/`, not all other SQL connector tests.

When a file in a shared directory has no entry-point match (e.g. `source/sql/sql_common.py`, which is a shared utility), narrowing is disabled for that change and the import graph is consulted instead — so all connectors that import from `sql/` get their tests triggered.

## When you need `connector.yaml`

A `connector.yaml` file in the source directory is only required for exceptions:

### Non-matching directory names

If the test directory name differs from the source directory name:

```yaml
# Example: src/datahub/ingestion/source/myconnector/connector.yaml
# (hypothetical — only needed when the test dir name doesn't match the source dir name)
test_path: tests/integration/my-connector-tests/
```

### Extra source paths

If a connector's source spans multiple directories:

```yaml
# src/datahub/ingestion/source/powerbi/connector.yaml
extra_source_paths:
  - src/datahub/ingestion/source/powerbi_report_server/
```

### Extra test paths

If a connector's tests span multiple directories:

```yaml
# Example: src/datahub/ingestion/source/myconnector/connector.yaml
# (hypothetical — only needed when a connector has more than one test directory)
extra_test_paths:
  - tests/integration/myconnector-extra/
```

Connectors that live inside a shared source directory (like `source/sql/`) do **not** need a `connector.yaml` — their tests are discovered automatically via entry-point analysis.

## Adding a new connector

### If your test directory name matches your source directory name

Nothing to do. Create `source/myconnector/` and `tests/integration/myconnector/`, and the selective CI system will discover it automatically.

### If your test directory name differs

Create a `connector.yaml` in your source directory:

```yaml
# src/datahub/ingestion/source/myconnector/connector.yaml
test_path: tests/integration/my-connector-tests/
```

### If your connector lives inside a shared source directory (e.g. `source/sql/`)

Nothing to do. Register the connector as an entry point in `setup.py` — the selective CI system reads entry points automatically to map your connector to its test directory. When your connector's source file changes, only its tests run. When a shared utility (like `sql_common.py`) changes, the full set of SQL connector tests runs via import-graph analysis.

## Running the script locally

```bash
# See what would run for a given set of changes
# Requires a git checkout with origin/<base-ref> reachable (i.e. git fetch first)
cd metadata-ingestion
python scripts/selective_ci_checks.py --dry-run --base-ref master

# Validate all connector.yaml mappings
python scripts/selective_ci_checks.py --validate

# Force full suite (used by the run-all-ingestion-tests label)
python scripts/selective_ci_checks.py --force-all
```

## How import analysis works

The `build_import_graph()` function:

1. Scans every `.py` file in each connector's source directory
2. Parses `import` and `from ... import` statements using Python's `ast` module
3. Maps imported modules back to known connector source directories
4. Builds a dependency graph: `connector_a → {connector_b, connector_c}`

When `connector_b` changes, the graph is consulted to find all connectors that import from it, and their tests are added to the matrix.

This catches non-obvious dependencies like `dbt` importing `sql/sql_types.py` — something a human maintaining a manual dependency list would likely miss.

## Safe defaults

- **Unknown files** under `source/` that don't belong to any known connector trigger the full suite.
- **Non-connector files** (api/, emitter/, setup.py, metadata-models/) always trigger the full suite.
- **Shared test infrastructure** (files directly in `tests/integration/`, e.g. `conftest.py`) triggers the full suite.
- **Documentation files** (`.md`) inside connector source directories are ignored — they don't affect runtime behavior. All other file types trigger tests.
- The `run-all-ingestion-tests` label on a PR forces the full suite regardless of changes.
- If the detection script crashes, the gate job fails the PR — it never silently passes.
- **Safety net**: if changed source directories produce no test matrix entries (e.g. a source dir with no test path and no entry-point coverage), the full suite runs with a warning — changed code is never silently untested.

## `connector.yaml` reference

| Field                | Required | Description                                                                                   |
| -------------------- | -------- | --------------------------------------------------------------------------------------------- |
| `test_path`          | No       | Override the default test directory (default: `tests/integration/{source_dir_name}/`)         |
| `extra_source_paths` | No       | Additional source directories covered by this connector's tests                               |
| `extra_test_paths`   | No       | Additional test directories for this connector                                                |
| `test_paths`         | No       | Legacy list form of test directories (validate-only; prefer `test_path` + `extra_test_paths`) |

Shared base directories (like `source/sql/`, `source/usage/`) no longer need a `connector.yaml` — entry-point analysis handles test discovery for all connectors within them automatically.
