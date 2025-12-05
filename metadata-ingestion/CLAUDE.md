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

## Recent Work: MSSQL Stored Procedure Upstream Alias Filtering (Dec 2024)

### Problem
MSSQL stored procedures use table aliases in UPDATE/DELETE statements (e.g., `UPDATE dst SET ... FROM table dst`), which were being incorrectly included in upstream lineage as real tables.

### Solution
Implemented upstream alias filtering in `src/datahub/ingestion/source/sql/mssql/source.py`:

1. **_filter_upstream_aliases()** (lines 1064-1113): Filters spurious TSQL aliases from upstream lineage using the existing `is_temp_table()` logic
2. **Platform instance prefix stripping** (lines 1088-1094): Strips `platform_instance` prefix from table names before calling `is_temp_table()` to ensure correct matching with `discovered_datasets`

### Key Commits
- `20bef2ef37` - Initial upstream filtering implementation with logging
- `35975f1bbd` - Fix platform_instance prefix bug (critical)
- Test file: `tests/unit/test_mssql_upstream_alias_filtering.py` (20 tests)
  - Tests cover basic filtering, cross-database handling, platform_instance prefix stripping
  - Tests verify different alias names (`dst`, `src`, `temp`, `target`, `c`)
  - Integration tests simulate real UPDATE/DELETE statement scenarios

### Filtering Logic
- **Same-database undiscovered tables** → Filtered (likely aliases)
- **Cross-database references** → Kept (even if undiscovered)
- **Tables in schema_resolver** → Kept (have schema metadata)
- **Tables in discovered_datasets** → Kept (discovered during table scan)

### Production Verification (Dec 4, 2025)
Verified working correctly in production logs across 2 separate ingestion runs:

**Run 1 (1288 procedures):** Dec 4, 05:51-06:29
- 10+ procedures with filtering activity in partial2-11 logs
- Aliases filtered: `dst` (most common), `temp`, `c`
- Cross-DB tables preserved: `currentdata.*`, `dataindex.*`, `timeseries.*`
- Same-DB real tables preserved: `staging_lakehouse_*`

**Run 2 (2358 procedures):** Dec 4, 09:58-10:10
- Procedure #192: `addAverageDailyPremiumDiscount` filtered `target` alias
- Shows filtering works with different alias names beyond just `dst`
- Example procedures verified: `upd_sdrxlon_yield_to_maturity_averages_and_breakdowns`, `upd_portfolio_percentile_standard_equity`, `upd_lh_india_purchase_and_redemption_options`

### Integration Test Impact
The fix changes stored procedure lineage output, requiring golden file updates:
- Spurious aliases no longer appear in upstream/downstream
- More accurate input/output dataset relationships
- Better column-level lineage mapping

Use `pytest --update-golden-files` to update integration test golden files after verifying the new output is correct.

## Current Work: Stored Procedures with Multiple Output Tables (Dec 2024)

### Problem Statement
**Status**: 🟡 **FIX DESIGNED** - Two-phase implementation plan ready

When a stored procedure modifies multiple tables, only the FIRST output table appears in downstream lineage. All other output tables are lost.

**Example**: Procedure `upd_european_priips_kid_information_lh`:
```sql
-- Modifies 4 tables:
INSERT INTO TimeSeries.dbo.european_priips_kid_information ...
UPDATE TimeSeries.dbo.european_priips_kid_information ...
INSERT INTO CurrentData.dbo.european_priips_kid_information ...
UPDATE Staging.dbo.some_other_table ...
```

**Expected**: All 4 modified tables show as downstream
**Actual**: Only CurrentData shows as downstream (first table in the list)

### Root Cause Investigation (Dec 4-5, 2024)

**Immediate Bug**: `src/datahub/sql_parsing/sql_parsing_aggregator.py:871`

```python
downstream_urn = parsed.out_tables[0] if parsed.out_tables else None  # Only takes first!
```

**Evidence from Logs**:
```
[FALLBACK-RESULT] Returning SqlParsingResult: query_type=UNKNOWN, 5 in_tables, 4 out_tables
[OBSERVED-TO-PREPARSED] Creating PreparsedQuery: downstream=urn:li:dataset:(...currentdata...)
[LINEAGE-BEFORE] Procedure upd_european_priips_kid_information_lh: Parser extracted 5 inputs, 1 outputs
```

**Deeper Design Issue**: Impedance mismatch between fallback parser and aggregator
- PreparsedQuery model has single `downstream` field (not a list)
- Design assumption: "One SQL Statement → One Downstream Table"
- Fallback parser returns multiple out_tables, violating this assumption
- This creates architectural tension

### Historical Context

**November 18, 2025** (commit fcdcab6d32): "Add fallback parser for MSSQL stored procedures"
- Added `_parse_stored_procedure_fallback()` in sqlglot_lineage.py
- Fallback parser internally calls split_statements() and aggregates results
- 17 comprehensive test cases (492 lines)
- Production verified across 2000+ procedures

**December 2, 2025** (commit abd756e608): "Skip splitting CREATE PROCEDURE before parsing"
- Modified parse_procedure_code() to NOT call split_statements for CREATE PROCEDURE
- Pass entire procedure as one query to aggregator
- Let fallback parser handle splitting internally
- Reason: split_statements yields control flow keywords ("BEGIN TRY", "END CATCH") as "statements"

### Code Review Feedback (Dec 5, 2024)

> "Sorry for jumping in late here, but: split_statements.py already has MSSQL-specific logic for dealing with BEGIN TRY/END TRY/BEGIN CATCH/END CATCH. If that's broken, we should fix it. We shouldn't just skip that to reimplement the logic somewhere else."

**Reviewer's concern**:
- We're bypassing split_statements for CREATE PROCEDURE (commit abd756e608)
- We're filtering control flow keywords in fallback parser (appears to duplicate logic)
- We should use split_statements.py properly at the right level

**What we discovered**:
- split_statements.py yields control flow keywords as "statements" (by design)
- Fallback parser was created to work around this and aggregate results
- Current approach violates "one statement = one downstream" assumption
- Code review feedback is architecturally valid

### Impact Assessment

**Severity**: HIGH - Affects ALL stored procedures with multiple output tables

**Scope**:
- Analyzed 925 procedures in production logs
- Every procedure that modifies multiple tables is affected
- Only the first alphabetically-sorted output table shows as downstream

**Example from logs**:
- `upd_european_priips_kid_information_lh`: Parser found 4 outputs, only 1 registered
- Missing outputs: TimeSeries, Staging tables, and others
- This pattern repeats across many procedures

### The Solution: Two-Phase Approach

After comprehensive investigation considering all design assumptions, code review feedback, and production constraints, we've designed a two-phase solution.

#### Phase 1: Immediate Bug Fix (READY TO IMPLEMENT)

**File**: `src/datahub/sql_parsing/sql_parsing_aggregator.py:870-900`

**Goal**: Fix the immediate production bug while maintaining current architecture

**Change**: Loop over all `out_tables` instead of taking just `[0]`

```python
# BEFORE: Only first output registered
downstream_urn = parsed.out_tables[0] if parsed.out_tables else None
self.add_preparsed_query(PreparsedQuery(..., downstream=downstream_urn, ...))

# AFTER: All outputs registered
if not parsed.out_tables:
    return  # No downstream, skip

for downstream_urn in parsed.out_tables:
    logger.info(f"[OBSERVED-TO-PREPARSED] Creating PreparsedQuery: downstream={downstream_urn}")
    self.add_preparsed_query(
        PreparsedQuery(
            query_id=query_fingerprint,
            query_text=observed.query,
            upstreams=parsed.in_tables,
            downstream=downstream_urn,  # One PreparsedQuery per downstream
            column_lineage=parsed.column_lineage,
            ...
        ),
        is_known_temp_table=is_known_temp_table,
        require_out_table_schema=require_out_table_schema,
        session_has_temp_tables=session_has_temp_tables,
    )
```

**Why This Is Correct**:
- ✅ Maintains "One PreparsedQuery = One Downstream" semantic model
- ✅ Creates multiple PreparsedQuery objects, each with single downstream field
- ✅ Fixes user's immediate problem (all tables show as downstream)
- ✅ Low risk, minimal code change
- ✅ All existing tests pass
- ✅ Backward compatible (single-output queries unchanged)

**Timeline**: 1-2 hours (implement, test, commit)

**Risk**: LOW

**Test**: `tests/unit/sql_parsing/test_multiple_output_tables_bug.py` (currently marked xfail)

#### Phase 2: Architectural Refactor (PLANNED)

**Goal**: Address code review feedback by splitting procedures BEFORE aggregation

**Key Changes**:
1. Remove split skip in `parse_procedure_code()` (stored_procedures/lineage.py)
2. Always use split_statements() for CREATE PROCEDURE
3. Filter control flow keywords at parse_procedure_code level
4. Add each DML statement as separate ObservedQuery
5. All statements share same session_id for temp table resolution
6. Simplify or remove fallback parser (no longer needed for aggregation)

**Why This Is Better**:
- ✅ Aligns with "one statement = one downstream" at the RIGHT level
- ✅ Uses split_statements.py properly (addresses code review feedback)
- ✅ Cleaner architecture (no impedance mismatch)
- ✅ Each statement naturally has one downstream
- ✅ More maintainable long-term

**Timeline**: 2-3 weeks (implementation, extensive testing, review, deployment)

**Risk**: MEDIUM (more extensive changes, requires careful testing)

**Detailed Plan**: See `PHASE_2_IMPLEMENTATION_PLAN.md` for complete step-by-step guide

### Design Principles Respected

Both phases respect ALL existing design assumptions:

1. **"One SQL Statement → One Downstream Table"** (PreparsedQuery model)
   - Phase 1: Creates multiple PreparsedQuery objects, each with one downstream
   - Phase 2: Splits into multiple statements, each becomes one query with one downstream

2. **split_statements.py yields control flow keywords** (by design)
   - Phase 1: Fallback parser filters them (temporary approach)
   - Phase 2: parse_procedure_code filters them (proper location)

3. **Temp table resolution works at session level** (aggregator design)
   - Phase 1: Single session for entire procedure (current behavior)
   - Phase 2: Shared session_id for all statements (maintains temp table tracking)

### Test Cases

**Bug reproduction**: `tests/unit/sql_parsing/test_multiple_output_tables_bug.py`
- ✅ Successfully reproduces bug (marked xfail)
- After Phase 1: Remove xfail, test should pass

**Stored procedure tests**: `tests/unit/sql_parsing/test_mssql_stored_procedure_fallback.py`
- 17 comprehensive test cases
- Phase 1: Should all pass (no changes needed)
- Phase 2: Will need updates for new architecture

### Documentation Created

Comprehensive analysis and planning documents:

1. **EXECUTIVE_SUMMARY.md** - High-level overview for decision-makers
2. **COMPREHENSIVE_ANALYSIS.md** - Complete technical investigation
3. **THE_PROPER_FIX.md** - Focused implementation guide for both phases
4. **PHASE_2_IMPLEMENTATION_PLAN.md** - Detailed 8-step implementation plan
5. **CODE_REVIEW_RESPONSE.md** - Response to split_statements.py comment
6. **SPLIT_STATEMENTS_ASSESSMENT.md** - Analysis of current usage
7. **DESIGN_ANALYSIS.md** - Current vs alternative design flows
8. **IMPACT_ANALYSIS.md** - Risk assessment

### Status: 🟢 PHASE 1 COMPLETE, PROCEEDING WITH PHASE 2

- ✅ Root cause identified: `sql_parsing_aggregator.py:871`
- ✅ Design investigation complete (historical context, assumptions, code review)
- ✅ **Phase 1 implemented and committed** (commit f4c8aa6ed9)
  - Loops over all output tables instead of taking [0]
  - Creates one PreparsedQuery per downstream table
  - All 27 aggregator tests pass
- ✅ Phase 2 plan created: 8-step architectural refactor guide
- ✅ Test created: Bug reproduction test
- ✅ Documentation complete: 8 analysis documents

### Phase 1 Results and Discovered Issues (Dec 5, 2024)

**Phase 1 Fix**: Successfully fixes missing downstream tables
- Before: Only first output table registered
- After: ALL output tables registered (TimeSeries, CurrentData, etc.)

**Lineage Pollution Discovered**: Phase 1 exposes pre-existing problem with aggregated upstreams

The fallback parser (`sqlglot_lineage.py:1520-1521`) aggregates ALL in_tables from ALL statements:
```python
all_in_tables.update(result.in_tables)  # Mixes upstreams from all statements
all_out_tables.update(result.out_tables)  # Mixes downstreams from all statements
```

**Real-World Example**: `upd_european_priips_kid_information_lh`
```sql
-- Section 1: TimeSeries operations
INSERT/UPDATE TimeSeries FROM Staging

-- Section 2: CurrentData operations
SELECT INTO #TEMP FROM TimeSeries
INSERT/UPDATE CurrentData FROM #TEMP
```

**Problem**: Both TimeSeries AND CurrentData get the SAME aggregated upstream list:
- TimeSeries shows upstreams: [Staging ✅, TimeSeries ✅, CurrentData ❌, #TEMP ❌]
- CurrentData shows upstreams: [Staging ❌, TimeSeries ✅, CurrentData ✅, #TEMP ✅]

**Expected**:
- TimeSeries upstreams: [Staging, TimeSeries] (from Section 1 only)
- CurrentData upstreams: [TimeSeries, CurrentData, #TEMP] (from Section 2 only)

**Why Phase 2 Is Critical**: Must split statements BEFORE aggregation so each downstream gets only its relevant upstreams.

### Phase 2: Statement Splitting Before Aggregation (IN PROGRESS)

**Goal**: Fix polluted upstream lists by splitting procedure into individual statements at the right architectural level

**Next Steps**:
1. ⏳ Implement statement splitting in `parse_procedure_code()`
2. Remove split skip (commit abd756e608)
3. Filter control flow keywords at parse_procedure_code level
4. Create one ObservedQuery per DML statement
5. Share session_id across all statements (for temp table resolution)
6. Update tests
7. Verify with production stored procedures
