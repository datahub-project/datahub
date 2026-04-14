### Capabilities

#### Lineage

When `include_lineage` is enabled the connector extracts:

- **Dashboard → Report**: Dashboards link to their constituent reports via `DashboardInfo.charts`
- **Report → Dataset/Cube**: Reports link to their data sources via `ChartInfo.inputs`

Optional **warehouse (physical table) lineage** for Intelligent Cubes: set
`include_warehouse_lineage: true`. The warehouse platform is auto-detected by querying
`/api/datasources` — no manual platform configuration is needed. Set
`warehouse_lineage_database` and `warehouse_lineage_schema` to qualify bare table names so
dataset URNs match your warehouse ingestion.

**Column-level lineage** from warehouse tables to cube columns is available when
`include_column_lineage: true`, `include_warehouse_lineage: true`, and
`include_cube_view_sql: true` are all set. The connector parses cube SQL view output using
DataHub's `SqlParsingAggregator`.

#### Parallel Prefetch

Cube metadata (SQL view + schema) is pre-fetched in parallel using a thread pool. The
`max_workers` config controls parallelism (default: 4). Set to `1` to disable for easier
debugging.

#### Stateful Deletion

Enable `stateful_ingestion.enabled: true` and `remove_stale_metadata: true` to automatically
soft-delete dashboards, reports, and cubes removed from MicroStrategy.

### Limitations

#### Column-Level Lineage from Cube to Chart

Column-level lineage from cubes to dashboard visualizations (cube field → chart column) is not
currently supported. Use table-level lineage for impact analysis.

#### Large Environments

Large MicroStrategy environments (>2,000 entities) may take 45–60+ minutes for initial
ingestion due to API pagination. Use `project_pattern`, `dashboard_pattern`, and similar
filters to scope ingestion, and enable stateful ingestion for incremental runs.

#### Project Availability

By default only projects with `status: 0` (loaded on IServer) are ingested. Set
`include_unloaded_projects: true` to include idle/unloaded projects, which may trigger
IServer ERR001-style errors.

### Troubleshooting

#### Connection Issues

**Problem**: Unable to connect to MicroStrategy REST API.

**Solution**: Verify `base_url` is reachable from your DataHub environment and that
`/MicroStrategyLibrary/api/status` returns HTTP 200. Check firewall rules and that the
MicroStrategy Library REST API is enabled in your instance configuration.

#### Authentication Failures

**Problem**: Authentication fails with "Invalid credentials" or "Unauthorized".

**Solution**: Verify credentials and confirm the service account has the required Browse/Read
permissions on target projects. For anonymous access, verify the instance supports guest
access.

#### Missing Dashboards or Reports

**Problem**: Some objects are not appearing in DataHub.

**Solution**: Check `dashboard_pattern` and `report_pattern` allow filters. Verify the
service account has read access to the missing objects. Review ingestion logs for filtering
messages or API errors.

#### Empty Lineage

**Problem**: No lineage edges are appearing in DataHub.

**Solution**: Confirm `include_lineage: true` is set. Verify dashboards actually reference
cubes or datasets. Check the service account can call `/api/v2/cubes/{id}/sqlView` — a 403
response means warehouse lineage will be skipped.
