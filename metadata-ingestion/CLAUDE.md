# DataHub Metadata Ingestion Development Guide

## Build and Test Commands

**Using Gradle (slow but reliable):**

```bash
# Development setup from repository root
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
# Linting
ruff format src/ tests/
ruff check src/ tests/
mypy src/ tests/

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

## Key Files

- `src/datahub/emitter/mcp_builder.py`: Examples of defining various aspect types
- `setup.py`, `pyproject.toml`, `setup.cfg`: Code style and dependency configuration
- `.github/workflows/metadata-ingestion.yml`: CI workflow configuration

## Recent Work: Redshift Sigma Computing Lineage Fixes (Feb 2025)

### Problem
Sigma Computing generates malformed SQL queries that fail lineage parsing, and Redshift's stl_scan-based lineage extraction was missing column-level lineage.

### Issues Fixed

#### 1. Sigma SQL Malformation Patterns
Sigma Computing generates SQL with missing spaces between keywords:

| Pattern | Example | Fix |
|---------|---------|-----|
| `or<identifier>` | `orif_542 is null` | `or if_542 is null` |
| `selectstatus` | `selectstatus from...` | `select status from...` |
| `select<ident>,` | `selectcol1, col2` | `select col1, col2` |
| `from<ident>` | `from"schema"."table"` | `from "schema"."table"` |
| `as<ident>` | `ascol_alias` | `as col_alias` |
| `end thencase` | `end thencase when` | `end then case when` |
| `end else<func>` | `end elsedateadd(` | `end else dateadd(` |
| `AS<TYPE>` | `CAST(1 ASINT4)` | `CAST(1 AS INT4)` |
| `group by<ident>` | `group byrep_name` | `group by rep_name` |
| `<alias>on` | `) q6on coalesce` | `) q6 on coalesce` |

**Implementation**: `src/datahub/sql_parsing/sqlglot_lineage.py`
- `_SIGMA_SQL_FIX_PATTERNS`: List of regex patterns to fix malformed SQL
- `_preprocess_query_for_sigma()`: Applies all patterns to fix SQL before parsing

#### 2. Column-Level Lineage for Known Queries (stl_scan)
Redshift lineage from `stl_scan`/`stl_insert` system tables was missing column-level lineage.

**Root Cause**: `add_known_query_lineage()` accepted `column_lineage` parameter but didn't parse SQL when not provided.

**Fix**: Added SQL parsing fallback in `add_known_query_lineage()`:
```python
# If column_lineage is not provided, try to parse it from the query text.
if not column_lineage and known_query_lineage.query_text and self._need_schemas:
    parsed = self._run_sql_parser(query=known_query_lineage.query_text, ...)
    if parsed.column_lineage:
        column_lineage = parsed.column_lineage
```

**File**: `src/datahub/sql_parsing/sql_parsing_aggregator.py:add_known_query_lineage()`

#### 3. AWS DMS UPDATE Query Preprocessing
AWS DMS CDC generates UPDATE queries that reference staging tables (`awsdms_*`) in SET expressions without a FROM clause:
```sql
UPDATE target SET col1 = "schema"."awsdms_staging".col1 WHERE ...
-- Missing: FROM "schema"."awsdms_staging"
```

Sqlglot doesn't detect these implicit table references.

**Fix**: Added `_preprocess_dms_update_query()` to inject FROM clause:
```python
def _preprocess_dms_update_query(query: str) -> str:
    """Add FROM clause for DMS UPDATE queries with implicit staging table refs."""
    # Detects "schema"."awsdms_*" patterns in SET expressions
    # Injects FROM clause before WHERE (or at end)
```

**File**: `src/datahub/sql_parsing/sqlglot_lineage.py`

### Key Files Changed
- `src/datahub/sql_parsing/sql_parsing_aggregator.py` - Column lineage parsing, preprocessing calls
- `src/datahub/sql_parsing/sqlglot_lineage.py` - Sigma patterns, DMS preprocessing
- `src/datahub/ingestion/source/redshift/lineage.py` - Per-row error handling
- `tests/unit/sql_parsing/test_sql_aggregator.py` - Unit tests

### Commits
- `e17d81d7aa` - fix(redshift): parse column lineage for known query lineage
- `325e0e787e` - fix(redshift): DMS UPDATE preprocessing and additional Sigma patterns

#### 4. Per-Row Error Handling in Lineage Extraction
Previously, if one query failed to parse during QUERY_SCAN lineage extraction, it would stop the entire phase. This caused the "Placeholder object is not iterable" error to break all lineage extraction.

**Fix**: Added try-except around each row processing in `_populate_lineage_agg()`:
- Each row is now processed independently
- Parsing failures are logged at DEBUG level but don't stop the phase
- Other queries continue to be processed

**File**: `src/datahub/ingestion/source/redshift/lineage.py`

### Known Limitations
- **Query truncation**: Redshift truncates queries at ~4000 characters. Truncated queries cannot be fixed by preprocessing and will fail parsing.
- **Complex Sigma patterns**: Some edge cases may still fail; add patterns to `_SIGMA_SQL_FIX_PATTERNS` as discovered.

### Testing
```bash
# Run SQL aggregator tests
pytest tests/unit/sql_parsing/test_sql_aggregator.py -v

# Run sqlglot lineage tests
pytest tests/unit/sql_parsing/test_sqlglot_lineage.py -v
```
