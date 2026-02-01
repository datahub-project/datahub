# Redshift & Sigma Computing Lineage Extraction Fixes

**Date**: February 2025
**Branch**: `dev/sigma-lineage-fix`
**Commits**: `e17d81d7aa`, `325e0e787e`

## Summary

This document describes fixes for Redshift lineage extraction issues, specifically:
1. Missing column-level lineage from stl_scan/stl_insert queries
2. AWS DMS UPDATE queries with implicit table references
3. Sigma Computing malformed SQL patterns

## Background

### How Redshift Lineage Works

DataHub extracts lineage from Redshift using two methods:

1. **Query Log Parsing** (`_run_sql_parser`)
   - Parses SQL from `stl_query` system table
   - Extracts table-level and column-level lineage
   - Main code path in `SqlParsingAggregator`

2. **STL Scan Lineage** (`_process_stl_scan_lineage`)
   - Uses `stl_scan` + `stl_insert` system tables
   - Provides table-level lineage based on scan/insert operations
   - Passes lineage to `add_known_query_lineage()`

### The Problem

Before these fixes:
- STL scan lineage had **no column-level lineage** (only table-level)
- Sigma Computing SQL frequently **failed parsing** due to malformed syntax
- AWS DMS UPDATE queries had **missing upstream references**

## Fix 1: Column-Level Lineage for Known Queries

### Problem

`add_known_query_lineage()` accepts a `column_lineage` parameter, but when called from `_process_stl_scan_lineage()`, it was always `None`. The method stored this `None` value without attempting to parse the query.

### Solution

Modified `add_known_query_lineage()` to parse column lineage from `query_text` when not provided:

```python
# File: src/datahub/sql_parsing/sql_parsing_aggregator.py

def add_known_query_lineage(self, known_query_lineage: KnownQueryLineageInfo, ...) -> None:
    # ...existing code...

    # NEW: If column_lineage is not provided, try to parse it from the query text.
    column_lineage = known_query_lineage.column_lineage
    if not column_lineage and known_query_lineage.query_text and self._need_schemas:
        try:
            parsed = self._run_sql_parser(
                query=known_query_lineage.query_text,
                default_db=None,
                default_schema=None,
                schema_resolver=self._schema_resolver,
                session_id=known_query_lineage.session_id or _MISSING_SESSION_ID,
                timestamp=known_query_lineage.timestamp,
            )
            if parsed.column_lineage:
                column_lineage = parsed.column_lineage
                logger.debug(
                    f"Parsed column lineage for known query: "
                    f"{len(column_lineage)} column mappings found"
                )
        except Exception as e:
            logger.debug(f"Failed to parse column lineage for known query: {e}", exc_info=True)
```

### Impact

- INSERT queries discovered via stl_scan now have column-level lineage
- Column-level lineage appears in DataHub UI (clickable columns)
- No breaking changes to existing behavior

## Fix 2: AWS DMS UPDATE Query Preprocessing

### Problem

AWS DMS (Database Migration Service) generates UPDATE queries for CDC (Change Data Capture) that reference staging tables in SET expressions without a FROM clause:

```sql
-- DMS-generated UPDATE query
UPDATE "schema"."target_table"
SET col1 = "schema"."awsdms_staging_table".col1,
    col2 = "schema"."awsdms_staging_table".col2
WHERE id = "schema"."awsdms_staging_table".id
```

Sqlglot doesn't detect `"schema"."awsdms_staging_table"` as an upstream because it's not in a FROM clause.

### Solution

Added `_preprocess_dms_update_query()` to inject missing FROM clause:

```python
# File: src/datahub/sql_parsing/sqlglot_lineage.py

_DMS_STAGING_TABLE_PATTERN = re.compile(
    r'"([^"]+)"\.\"(awsdms_[^"]+)\"', re.IGNORECASE
)

def _preprocess_dms_update_query(query: str) -> str:
    """Preprocess AWS DMS UPDATE queries to add missing FROM clause."""
    # Only process UPDATE queries without existing FROM clause
    if not query.strip().upper().startswith("UPDATE"):
        return query
    if re.search(r"\bFROM\b", query, re.IGNORECASE):
        return query

    # Find all awsdms_* table references
    matches = _DMS_STAGING_TABLE_PATTERN.findall(query)
    staging_tables = {(schema, table) for schema, table in matches
                      if table.lower().startswith("awsdms_")}

    if not staging_tables:
        return query

    # Build and inject FROM clause
    from_parts = [f'"{schema}"."{table}"' for schema, table in staging_tables]
    from_clause = " FROM " + ", ".join(from_parts)

    # Insert before WHERE or append at end
    where_match = re.search(r"\bWHERE\b", query, re.IGNORECASE)
    if where_match:
        return query[:where_match.start()] + from_clause + " " + query[where_match.start():]
    return query + from_clause
```

### Impact

- DMS staging tables (`awsdms_*`) now appear in upstream lineage
- Column-level lineage correctly traces from staging to target tables

## Fix 3: Sigma Computing SQL Malformation Patterns

### Problem

Sigma Computing generates malformed SQL with missing spaces between keywords:

```sql
-- Actual Sigma-generated SQL (malformed)
selectstatus, count(*) from table orif_542 is null

-- Should be
select status, count(*) from table or if_542 is null
```

### Solution

Added regex patterns to `_SIGMA_SQL_FIX_PATTERNS` in `sqlglot_lineage.py`:

```python
_SIGMA_SQL_FIX_PATTERNS: List[Tuple[re.Pattern, str]] = [
    # Existing patterns...

    # NEW: or<identifier starting with if_> -> or <identifier>
    (re.compile(r"\bor(if_[a-z0-9_]+)", re.IGNORECASE), r"or \1"),

    # NEW: selectstatus -> select status
    (re.compile(r"\bselectstatus\b", re.IGNORECASE), "select status"),

    # NEW: select<identifier> followed by comma -> select <identifier>
    (re.compile(r"\bselect([a-z][a-z0-9_]*)\s*,", re.IGNORECASE), r"select \1,"),
]
```

### Full Pattern List

| Pattern | Before | After |
|---------|--------|-------|
| `from"` | `from"schema"` | `from "schema"` |
| `as<ident>` | `ascol_alias` | `as col_alias` |
| `or<ident>` | `orsome_col` | `or some_col` |
| `orif_*` | `orif_542` | `or if_542` |
| `selectstatus` | `selectstatus` | `select status` |
| `select<ident>,` | `selectcol,` | `select col,` |
| `end thencase` | `end thencase when` | `end then case when` |
| `end else<func>` | `end elsedateadd(` | `end else dateadd(` |
| `AS<TYPE>` | `CAST(1 ASINT4)` | `CAST(1 AS INT4)` |
| `group by<ident>` | `group byrep_name` | `group by rep_name` |
| `<alias>on` | `) q6on coalesce` | `) q6 on coalesce` |

### Impact

- Previously failing Sigma queries now parse successfully
- Lineage extracted for Sigma-created views and dashboards

## Fix 4: Per-Row Error Handling in Lineage Extraction

### Problem

Previously, if one query failed to parse during QUERY_SCAN lineage extraction, it would stop the entire phase. The error "'Placeholder' object is not iterable" was causing the entire lineage extraction to fail.

### Solution

Added try-except around each row processing in `_populate_lineage_agg()`:

```python
# File: src/datahub/ingestion/source/redshift/lineage.py

def _populate_lineage_agg(self, ...):
    with timer:
        for lineage_row in RedshiftDataDictionary.get_lineage_rows(...):
            try:
                processor(lineage_row)
            except Exception as e:
                # Log per-row errors but continue processing other rows.
                logger.debug(
                    f"Failed to process {lineage_type.name} lineage row "
                    f"for {lineage_row.target_schema}.{lineage_row.target_table}: {e}",
                    exc_info=True,
                )
```

### Impact

- Parsing failures for individual queries no longer stop the entire phase
- More lineage is extracted even when some queries fail to parse
- Errors are logged at DEBUG level for investigation

## Integration with SqlParsingAggregator

All preprocessing is applied in `_run_sql_parser()`:

```python
# File: src/datahub/sql_parsing/sql_parsing_aggregator.py

def _run_sql_parser(self, ...) -> SqlParsingResult:
    # Apply preprocessing for Redshift platform.
    if self.platform.platform_name == "redshift":
        # Fix Sigma Computing malformed SQL (missing spaces between keywords).
        query = _preprocess_query_for_sigma(query)
        # Add FROM clause for DMS UPDATE queries (implicit table references).
        query = _preprocess_dms_update_query(query)

    return sqlglot_lineage(query, ...)
```

## Known Limitations

### Query Truncation

Redshift truncates queries at ~4000 characters in system tables. Truncated queries:
- Cannot be fixed by preprocessing
- Will fail parsing (incomplete SQL)
- No workaround available

Example truncated query:
```sql
INSERT INTO blog_attribution (col1, col2, ...)
SELECT ... FROM table1 JOIN table2 ON ... WHERE ... -- truncated at 4000 chars
```

### Edge Cases

Some Sigma patterns may still fail. To add new patterns:

1. Find the malformed SQL in logs
2. Add pattern to `_SIGMA_SQL_FIX_PATTERNS` in `sqlglot_lineage.py`
3. Add test case to `tests/unit/sql_parsing/test_sqlglot_lineage.py`

## Testing

### Run Unit Tests

```bash
# SQL aggregator tests (includes column lineage test)
pytest tests/unit/sql_parsing/test_sql_aggregator.py -v

# Sqlglot lineage tests (includes Sigma preprocessing)
pytest tests/unit/sql_parsing/test_sqlglot_lineage.py -v

# Specific test for known query column lineage
pytest tests/unit/sql_parsing/test_sql_aggregator.py::test_add_known_query_lineage_parses_column_lineage -v
```

### Production Verification

After deployment, check logs for:
- `Parsed column lineage for known query:` - indicates successful column lineage extraction
- `Failed to parse column lineage for known query:` - indicates parsing failure (check query)

## Files Changed

| File | Changes |
|------|---------|
| `src/datahub/sql_parsing/sql_parsing_aggregator.py` | Column lineage parsing, preprocessing integration |
| `src/datahub/sql_parsing/sqlglot_lineage.py` | Sigma patterns, DMS preprocessing function |
| `src/datahub/ingestion/source/redshift/lineage.py` | Per-row error handling in lineage extraction |
| `tests/unit/sql_parsing/test_sql_aggregator.py` | New test for column lineage parsing |

## Questions?

Contact the Data Platform team or check the CLAUDE.md file in the metadata-ingestion directory for additional context.
