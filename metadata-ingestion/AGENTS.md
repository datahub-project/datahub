# DataHub Metadata Ingestion Development Guide

## Build and Test Commands

**Using Gradle (slow but reliable):**

```bash
# Development setup from metadata-ingestion/
../gradlew :metadata-ingestion:installDev   # Setup Python environment
source venv/bin/activate                    # Activate virtual environment

# Linting and formatting
../gradlew :metadata-ingestion:lint         # Run ruff + mypy
../gradlew :metadata-ingestion:lintFix      # Auto-fix linting issues

# Testing
../gradlew :metadata-ingestion:testQuick                           # Fast unit tests
../gradlew :metadata-ingestion:testFull                            # All tests
../gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit/test_file.py  # Single test
```

**Direct Python commands (when venv is activated):**

```bash
# Testing
pytest -vv                                 # Run all tests
pytest -m 'not integration'                # Unit tests only
pytest -m 'integration'                    # Integration tests
pytest tests/path/to/file.py               # Single test file
pytest tests/path/to/file.py::TestClass    # Single test class
pytest tests/path/to/file.py::TestClass::test_method  # Single test
```

## Environment Variables

**Build configuration:**

- `DATAHUB_VENV_USE_COPIES`: Set to `true` to use `--copies` flag when creating Python virtual environments. This copies the Python binary instead of creating a symlink. Useful for Nix environments, immutable filesystems, Windows, or container environments where symlinks don't work correctly. Increases disk usage and setup time, so only enable if needed.

  ```bash
  export DATAHUB_VENV_USE_COPIES=true
  ../gradlew :metadata-ingestion:installDev
  ```

## Directory Structure

- `src/datahub/`: Source code for the DataHub CLI and ingestion framework
- `tests/`: All tests (NOT in `src/` directory)
- `tests/unit/`: Unit tests
- `tests/integration/`: Integration tests
- `scripts/`: Build and development scripts
- `examples/`: Example ingestion configurations
- `developing.md`: Detailed development environment information

## Code Style Guidelines

- **Formatting**: Uses ruff, 88 character line length
- **Imports**: Sorted with ruff.lint.isort, no relative imports, Imports are always put at the top of the file, just after any module comments and docstrings, and before module globals and constants.
- **Types**: Always use type annotations, prefer Protocol for interfaces
  - Avoid `Any` type - use specific types (`Dict[str, int]`, `TypedDict`, or typevars)
  - Use `isinstance` checks instead of `hasattr`
  - Prefer `assert isinstance(...)` over `cast`
- **Data Structures**: Use dataclasses/pydantic for internal data representation
  - Return dataclasses instead of tuples from methods
  - Centralize utility functions to avoid code duplication
- **Naming**: Descriptive names, match source system terminology in configs
- **Error Handling**: Validators throw only ValueError/TypeError/AssertionError
  - Add robust error handling with layers of protection for known failure points
- **Code Quality**: Avoid global state, use named arguments, don't re-export in `__init__.py`
- **Documentation**: All configs need descriptions
- **Dependencies**: Avoid version pinning, use ranges with comments
- **Architecture**: Avoid tall inheritance hierarchies, prefer mixins

## Testing Conventions

- **Location**: Tests go in `tests/` directory alongside `src/`, NOT in `src/`
- **Structure**: Test files should mirror the source directory structure
- **Framework**: Use pytest, not unittest
- **Assertions**: Use `assert` statements, not `self.assertEqual()` or `self.assertIsNone()`
  - Boolean: `assert func()` or `assert not func()`, not `assert func() is True/False`
  - None: `assert result is None` (correct), not `assert result == None`
  - Keep tests concise, avoid verbose repetitive patterns
- **Classes**: Use regular classes, not `unittest.TestCase`
- **Imports**: Import `pytest` in test files
- **Naming**: Test files should be named `test_*.py`
- **Categories**:
  - Unit tests: `tests/unit/` - fast, no external dependencies
  - Integration tests: `tests/integration/` - may use Docker/external services

## Configuration Guidelines (Pydantic)

- **Naming**: Match terminology of the source system (e.g., `account_id` for Snowflake, not `host_port`)
- **Descriptions**: All configs must have descriptions
- **Patterns**: Use AllowDenyPatterns for filtering, named `*_pattern`
- **Defaults**: Set reasonable defaults, avoid config-driven filtering that should be automatic
- **Validation**: Single pydantic validator per validation concern
- **Security**: Use `SecretStr` for passwords, auth tokens, etc.
- **Deprecation**: Use `pydantic_removed_field` helper for field deprecations

## Connector Conventions

- **Structure**: Split connector code by duty: `constants.py`, `models.py`, `config.py`, `client.py`, `source.py`, plus `lineage.py`, `mapper.py`, and `usage.py` as needed. Match the quality and structure of existing connectors like Power BI, Airbyte, Redshift, BigID, and Grafana.
- **Constants**: Avoid double-quoted string literals in connector code. Hoist magic strings into module-level constants, and keep regex patterns pre-compiled in the constants file.
- **Comments**: No top-of-file docstrings in connector modules. Keep comments and docstrings only where they add real context.
- **Imports**: Do not use `TYPE_CHECKING` in connector code. Lazy imports are fine only for opt-in features the connector does not always depend on.
- **Syntax**: Do not use the walrus operator in connector code.
- **Types**: Prefer `Dict` and `List` from `typing` over builtin `dict` and `list` in connector type annotations.
- **Reporting**: Prefer `self.report.warning(...)` and report counters over bare `logger` warnings for skips, inaccessible objects, and other operator-visible edge cases.
- **SQL lineage**: Use `SqlParsingAggregator` via `create_lineage_from_sql_statements` with a platform map instead of setting sqlglot dialects per connector. Mirror existing connectors for known-URN mapping, platform instance / env handling, casing, multi-part names, and temp-table handling.
- **Column lineage**: Resolve upstream and downstream schemas from the DataHub graph when available, load known URNs from the platform / platform instance / env mapping, and match columns case-insensitively.
- **Progress**: Surface ingestion progress and use explicit ingestion stages, following connectors like Dremio and Snowflake.
- **New connectors**: A connector usually needs more than Python code: source logo, integrations-page logo, UI form pieces, a `datahub.json` update, entry-point registration in `setup.py`, `../gradlew :metadata-ingestion:updateLockFile`, and shared subtypes instead of connector-local subtype definitions.
- **XML**: Avoid Python's stdlib `xml` parser. Use a safe XML library, following the HANA-related code path.
- **Validation workflow**: Before committing a new connector, run ingestion locally in debug mode to a local file and inspect the full logs. When testing against customer environments, keep secrets only in temporary paths such as `/tmp/*.env`.
- **PR scope**: Keep each connector in its own PR. Split shared framework changes such as sqlglot helpers into a separate PR, and keep the PR title and description focused on that connector only.

## Dependencies and Entry Points

`setup.py` is the **source of truth** for all dependencies, extras, and entry points. `pyproject.toml`, `uv.lock`, and `constraints.txt` are **generated from it** and must never be edited manually.

**After any change to `setup.py`** (new dependency, new entry point, version bump), run:

```bash
../gradlew :metadata-ingestion:updateLockFile
```

This regenerates the full chain: `setup.py` → `pyproject.toml` → `uv.lock` → `constraints.txt`.

CI runs `checkLockFile` which calls `scripts/verify_pyproject_equivalence.py` to verify these files are in sync. If you edit `setup.py` without running `updateLockFile`, CI will fail.

**Adding a new ingestion source entry point:**

1. Add the entry to `setup.py` under `entry_points["datahub.ingestion.source.plugins"]`
2. Run `../gradlew :metadata-ingestion:updateLockFile`
3. Verify with `../gradlew :metadata-ingestion:checkLockFile`

## Key Files

- `src/datahub/emitter/mcp_builder.py`: Examples of defining various aspect types
- `setup.py`: Source of truth for dependencies, extras, and entry points
- `pyproject.toml`: Generated from `setup.py` — do not edit manually
- `setup.cfg`: Code style configuration
- `scripts/generate_pyproject_deps.py`: Generates `pyproject.toml` from `setup.py`
- `scripts/verify_pyproject_equivalence.py`: Validates `setup.py` and `pyproject.toml` are in sync
- `.github/workflows/metadata-ingestion.yml`: CI workflow configuration

## Connector Documentation

Connector documentation follows a structured format with specific file types and heading conventions.

**Authoring guidelines**: See `docs/sources/AGENTS.md` in this directory for:

- File structure (`_pre.md`, `_post.md`, `_recipe.yml`)
- Heading level rules (H3 baseline, H2 reserved for module headings)
- Section ordering (Overview → Concept Mapping → Prerequisites)
- Content placement guidelines
- Style conventions and formatting requirements

**Quick reference:**

```bash
# Regenerate documentation after changes
../gradlew :metadata-ingestion:docGen

# Preview locally
../gradlew :docs-website:yarnStart  # Opens localhost:3001

# Format markdown before committing
../gradlew :datahub-web-react:mdPrettierWrite
```
