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

## Recent Work: Redshift Sigma/DMS Lineage Fixes (Feb 2026)

### Problem
Redshift lineage extraction was failing for queries from:
1. **Sigma Computing** - generates SQL with missing spaces (e.g., `casewhen`, `whenq11.col`, `groupby`)
2. **AWS DMS** - UPDATE queries lack FROM clause, password redaction merges column names

### Solution
Created `src/datahub/sql_parsing/redshift_preprocessing.py` with SQL preprocessing:

**Sigma Fixes (40+ patterns in 5 categories):**
- Type Cast: `::timestamptzcast_` → `::timestamptz cast_`
- Compound Keywords: `casewhen` → `case when`, `groupby` → `group by`
- Alias Patterns (whitelist): `whenq11.` → `when q11.` (only q1-q999, t1-t999)
- Identifier Patterns (blacklist): `whenarr_down` → `when arr_down` (excludes "whenever", "android")
- Function Patterns: `ormax(` → `or max(`

**DMS Fixes:**
- `preprocess_dms_update_query()`: Injects missing FROM clause for UPDATE queries
- `preprocess_dms_password_redaction()`: Splits `'***'next_col` → `", "next_col"`

**Sigma Temp Table Detection** in `lineage.py`:
- `_is_sigma_temp_table()`: Detects `sigma.t_mat_*` and `sigma.t_*_<timestamp>` patterns
- Filters these from lineage to avoid noise

### Key Design Decisions
1. **Whitelist for aliases**: Only match Sigma's predictable patterns (q1, t1) - safe from false positives
2. **Blacklist with safe words for identifiers**: Filter out "whenever", "android", "selection" etc.
3. **Two-level triggers**: Quick substring check before running regex (performance)
4. **ReDoS prevention**: All identifier patterns use `{1,100}` length limits
5. **Redshift-specific**: All preprocessing in Redshift code, not shared sql_parsing
6. **Applied to all Redshift queries**: Preprocessing runs on all queries but uses early-exit optimization - clean queries only do ~30 substring checks (microseconds), no regex execution

### Production Results
- Parsing errors: 41,922 → 516 (99% reduction)
- Column-level lineage: 40 → 59 entries (48% improvement)

### Files Changed
- `src/datahub/sql_parsing/redshift_preprocessing.py` - New preprocessing module (526 lines)
- `src/datahub/ingestion/source/redshift/lineage.py` - Sigma temp table detection, preprocessing integration, permission error tracking
- `src/datahub/ingestion/source/redshift/report.py` - Added CLL parse failure counter and permission denied tracking
- `tests/unit/redshift/test_redshift_lineage.py` - Comprehensive tests (19 new tests)

### Test Coverage
- `TestSigmaSqlPreprocessing`: 8 tests for Sigma patterns + false positive prevention
- `TestDmsPasswordRedaction`: 4 tests for DMS password handling
- `TestSigmaTempTableDetection`: 7 tests for temp table detection

### Root Cause Discovery (Feb 2026)

**Status:** Investigation complete, fix pending

Code review revealed the "missing spaces" are NOT from Sigma generating invalid SQL, but from our own RTRIM logic in `query.py` stripping meaningful trailing spaces during query reconstruction.

**The Bug:**
- Redshift stores query text in `character(200)` fixed-width columns (STL_QUERYTEXT)
- We use `RTRIM()` on each segment before LISTAGG concatenation
- When SQL splits at exactly 200 chars after a meaningful space, RTRIM removes it
- Example: `"...CASE WHEN "` (200 chars) → RTRIM → `"...CASE WHEN"` → concat → `"...CASE WHENx > 0"`

**Current (Buggy):**
```sql
LISTAGG(CASE WHEN LEN(RTRIM(text)) = 0 THEN text ELSE RTRIM(text) END, '')
  WITHIN GROUP (ORDER BY sequence)
```

**Proposed Fix:**
```sql
RTRIM(LISTAGG(text, '') WITHIN GROUP (ORDER BY sequence))
```

**Impact if Fixed:**
- Most Sigma preprocessing patterns become unnecessary
- Keep: Sigma temp table detection, DMS fixes (if verified as real issues)
- Remove: All "missing space" patterns

**Documentation:** See `docs/redshift-rtrim-investigation.md` for full analysis
