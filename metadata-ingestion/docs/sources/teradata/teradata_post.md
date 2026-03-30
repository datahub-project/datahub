### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Large-scale Deployment Tuning

For Teradata installations with thousands of tables the following options can significantly reduce ingestion time.

**Incremental column extraction**

The connector compares each table's `LastAlterTimeStamp` against a watermark and skips column extraction for tables that have not changed. Only altered tables and tables with no recorded alter timestamp are re-extracted. At 13 000 tables where ~200 change per day this typically reduces a multi-hour run to minutes.

Two mutually exclusive options control the watermark (setting both raises a validation error at startup):

- **`column_extraction_days_back`** — recommended for scheduled pipelines. Set once and never update the recipe. A value of 3 covers up to two missed daily runs with no gap risk.

  ```yaml
  column_extraction_days_back: 3
  ```

- **`column_extraction_watermark`** — for stateful pipelines that track the exact timestamp of the last successful run programmatically.

  ```yaml
  column_extraction_watermark: "2024-06-01T00:00:00Z"
  ```

**Faster view column fetching**

By default the connector uses Teradata `HELP` statements for every view to ensure derived expression columns (e.g. `col1 + col2`) have correct types. Set `use_dbc_columns_for_views: true` to attempt a bulk `dbc.ColumnsV` fetch first and fall back to `HELP` only for views where any column has an unknown type. This can reduce `HELP` calls by 80–90 % on installations where most view columns have explicit types.

**Profiling at scale**

Profiling all tables in a large installation is impractical. Use `profiling.limit` (part of the standard `GEProfilingConfig`) to cap how many tables are profiled per run. You can also combine it with `profile_pattern` to restrict profiling to specific schemas or tables.

```yaml
profiling:
  enabled: true
  limit: 500
profile_pattern:
  allow:
    - "high_priority_db\\..*"
```

**Lineage query scope**

When `databases` is not set the connector automatically scopes `DBC.QryLogV` queries to the databases discovered during metadata extraction, filtered by `database_pattern`. This avoids scanning the entire audit log. You can further restrict the scope with an explicit `databases` list.

**Connection timeouts**

Use `request_timeout_ms` and `connect_timeout_ms` to tune the Teradata driver timeouts. Increase `request_timeout_ms` (default: 120 000 ms) if lineage queries against large `DBC.QryLogV` tables time out silently.

### Limitations

- `use_dbc_columns_for_views` falls back to `HELP` for any view that contains derived expression columns. Views with _only_ explicit-type columns benefit most from this option.
- `column_extraction_watermark` must be managed manually — set it to the start time of the previous successful run. Use `column_extraction_days_back` instead if you want a self-maintaining schedule-relative window.
- `column_extraction_watermark` and `column_extraction_days_back` are mutually exclusive. Setting both raises a validation error at startup.
- Profiling capped by `profiling.limit` does not prioritise tables — they are profiled in the order they are returned by `dbc.TablesV`. Use `profile_pattern` to target specific schemas if order matters.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

If lineage queries fail silently and return no results, increase `request_timeout_ms`. The default 2-minute timeout can be insufficient for `DBC.QryLogV` on busy systems with large audit logs.
