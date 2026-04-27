# Hex V2 Connector — Planning Document

**Created**: 2026-04-27
**Status**: IN_PROGRESS

---

## Overview

The `hex-v2` connector is a full superset replacement for the existing `hex` connector. It uses the **Hex CLI** (`hex` binary) as its primary data source rather than calling the Hex REST API directly, which gives access to the project YAML export endpoint — the richest data source available and the key unlock for direct SQL-based lineage.

### Headline Value Proposition

**v1 lineage** requires a warehouse ingestion (Snowflake, BigQuery, etc.) to have *already* run with `use_queries_v2: true`, captured Hex SQL queries with embedded metadata comments, and stored those as DataHub Query entities. Without that prior setup, `hex` produces zero lineage.

**v2 lineage** works on day one. By exporting each project's YAML, parsing the SQL source from every SQL cell, and resolving the cell's `dataConnectionId` to a DataHub platform URN (via the connections API), the connector builds complete upstream lineage with no dependency on warehouse ingestion at all. Coverage is total: every project with a SQL cell gets lineage, whether it ran yesterday or never.

---

## Coexistence Strategy

`hex-v2` ships alongside `hex` as a separate, independent connector:

| Attribute         | `hex`                                                | `hex-v2`                                                  |
| ----------------- | ---------------------------------------------------- | --------------------------------------------------------- |
| Entry point       | `hex`                                                | `hex-v2`                                                  |
| Source directory  | `src/datahub/ingestion/source/hex/`                  | `src/datahub/ingestion/source/hex_v2/`                    |
| Config class      | `HexSourceConfig`                                    | `HexV2SourceConfig`                                       |
| Integration tests | `tests/integration/hex/`                             | `tests/integration/hex_v2/`                               |
| Unit tests        | `tests/unit/hex/`                                    | `tests/unit/hex_v2/`                                      |
| Deprecation plan  | Document `hex` as deprecated in its docs; users migrate when ready |

---

## Research Summary

### Source Classification

- **Type**: API-Based
- **Source Category**: BI Tools
- **Interface**: Hex CLI (`hex` binary), hex v1.2.2+
- **Standards**: `standards/source_types/bi_tools.md`, `standards/api.md`, `standards/lineage.md`
- **Documentation**: https://learn.hex.tech/docs/api-integrations/cli

### Similar DataHub Connectors

| Connector   | Relevance | Key Patterns                               |
| ----------- | --------- | ------------------------------------------ |
| `hex`       | Direct    | Entity mapping, mapper pattern, report structure |
| `tableau`   | High      | BI tool with SQL-based lineage, project export pattern |
| `looker`    | High      | Workspace → Container → Dashboard → Chart hierarchy |
| `metabase`  | Medium    | Simple BI tool with API-based lineage      |

### CLI Endpoints Used

| CLI Command                                      | Purpose                                      | Auth Level  |
| ------------------------------------------------ | -------------------------------------------- | ----------- |
| `hex auth login --token-from-env`                | Bootstrap auth before any other call         | N/A         |
| `hex project list --json --limit N --after <c>`  | Enumerate all projects + components          | Standard    |
| `hex project get <id> --json`                    | Richer project metadata (description, owner) | Standard    |
| `hex project export <id> --version draft`        | Full YAML with SQL cells + connections       | Standard    |
| `hex connection list --json`                     | Build `connectionId → platform` map          | Standard    |
| `hex run list <id> --json`                       | Last run time + status per project           | Standard    |
| `hex cell list <id> --json --after <c>`          | Cell enumeration (fallback if export fails)  | Standard    |

---

## Entity Mapping

| Source Concept     | DataHub Entity | Subtype      | URN Pattern                                                           | Notes                                        |
| ------------------ | -------------- | ------------ | --------------------------------------------------------------------- | -------------------------------------------- |
| Hex Workspace      | Container      | —            | `urn:li:container:<hash(workspace)>`                                  | Top-level container                          |
| Hex Project        | Dashboard      | HexProject   | `urn:li:dashboard:(urn:li:dataPlatform:hex,<workspace>.<id>,ENV)`     | Same as v1                                   |
| Hex Component      | Dashboard      | HexComponent | `urn:li:dashboard:(urn:li:dataPlatform:hex,<workspace>.<id>,ENV)`     | Same as v1                                   |
| Hex Data Connection| (internal)     | —            | Not emitted as DataHub entity; used to resolve platform URNs          | Informs lineage resolution                   |
| Upstream Table     | Dataset        | —            | `urn:li:dataset:(urn:li:dataPlatform:<conn_type>,<db.schema.table>,ENV)` | Resolved from SQL parsing + connection type |
| Hex EXPLORE Cell   | Chart          | HexChart     | `urn:li:chart:(urn:li:dataPlatform:hex,<workspace>.<project_id>.<cell_id>,ENV)` | **Phase 3, optional**                |

---

## Architecture Decisions

### Base Class

`StatefulIngestionSourceBase` — same as v1. No SQL/JDBC interface; all data comes from CLI subprocess calls.

### CLI as System Dependency

The `hex` binary must be installed and accessible in `PATH`. This is a **documented prerequisite** — the connector does not install it. Users install it via the Hex CLI releases page.

**Auth bootstrap**: The connector calls `hex auth login --token-from-env --insecure-storage --profile <profile>` automatically on startup, injecting `HEX_CLI_LOGIN_TOKEN=<token>` from the config. This is safe for CI/CD environments (plaintext storage in a temp profile is acceptable because the token is already passed as a secret). The profile is scoped per ingestion run using a generated name (`datahub-<workspace>`) to avoid polluting the user's system keyring.

### Module Structure

```
src/datahub/ingestion/source/hex_v2/
├── __init__.py
├── hex_v2.py          # HexV2Source, HexV2SourceConfig, HexV2Report
├── cli_client.py      # HexCliClient — all subprocess calls, auth, pagination
├── yaml_parser.py     # Parse project YAML → extract SQL cells + connections
├── lineage_builder.py # SQL cells → sqlglot_lineage → UpstreamLineage aspects
├── mapper.py          # HexV2Mapper — same responsibility as v1 mapper, extended
├── model.py           # HexV2 domain models (superset of v1 models)
└── constants.py       # Platform name, URN helpers, connection_type → platform map
```

### Config Structure

`HexV2SourceConfig` is a strict superset of v1 config fields. Every v1 config key works unchanged in v2 with the same semantics.

```yaml
source:
  type: hex-v2
  config:
    # --- Auth (v1-compatible) ---
    workspace_name: my-workspace
    token: ${HEX_TOKEN}            # written to HEX_CLI_LOGIN_TOKEN on startup

    # --- CLI (new) ---
    hex_cli_path: hex              # default: "hex" (from PATH)
    cli_timeout_seconds: 120       # default: 120s per subprocess call

    # --- Lineage mode (replaces include_lineage) ---
    lineage_mode: yaml_export      # "yaml_export" (new default) | "datahub_queries" (v1 compat) | "both" | "none"

    # --- v1-compatible fields (all preserved) ---
    include_components: true
    page_size: 100
    patch_metadata: false
    collections_as_tags: true
    status_as_tag: true
    categories_as_tags: true
    project_title_pattern:
      allow: [".*"]
    component_title_pattern:
      allow: [".*"]
    set_ownership_from_email: true
    category_pattern:
      allow: [".*"]
    base_url: https://app.hex.tech/api/v1   # kept for datahub_queries lineage mode only

    # --- v1 lineage fields (kept for datahub_queries mode) ---
    lineage_start_time: null
    lineage_end_time: null
    datahub_page_size: 100

    # --- New v2 fields ---
    include_run_history: true      # Emit last run time as operational metadata
    include_connections: true      # Fetch connections to resolve lineage platform URNs
    connection_platform_map: {}    # Override: {"<connection_id>": "snowflake"}
    export_version: draft          # "draft" | "published" | specific version number
    sql_parsing_platform_default: snowflake  # fallback dialect if connection type unknown

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
```

### Category Filtering Semantics (Preserved from v1)

v1 has a deliberate behavior: projects with **no categories** are always allowed through regardless of `category_pattern`. v2 preserves this exactly:

```python
# Projects with no categories bypass category_pattern — same as v1
if project.categories and (
    any(category_pattern.denied(c.name) for c in project.categories)
    or not any(category_pattern.allowed(c.name) for c in project.categories)
):
    continue
```

This is intentional: category pattern is "if you have categories, at least one must be allowed".

### Direct SQL Lineage (New — `yaml_export` mode)

The core new capability. Processing flow per project:

```
1. hex connection list --json
   → build Map<connectionId, {name, type}>

2. hex project export <id> --version draft
   → parse YAML (PyYAML)
   → extract SQL cells: [{cellId, cellLabel, source (SQL), dataConnectionId}]

3. For each SQL cell:
   a. Lookup connection: connectionId → {name, connection_type}
   b. Map connection_type → DataHub platform dialect
      (snowflake→snowflake, bigquery→bigquery, redshift→redshift, etc.)
   c. sqlglot_lineage(sql, dialect=dialect) → in_tables
   d. For each in_table: build DatasetUrn(platform, db.schema.table, env)

4. Aggregate all DatasetUrns across all SQL cells
   → emit UpstreamLineage on the Project's Dashboard URN

5. Optionally: emit EXPLORE cell → SQL cell lineage (Phase 3)
```

**Connection type → platform dialect map** (in `constants.py`):
```python
CONNECTION_TYPE_TO_DATAHUB_PLATFORM = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "spark": "spark",
}
```

**Per-cell connection resolution**: Each SQL cell in the YAML has its own `dataConnectionId`. A project can span multiple connections (e.g., some cells hit Snowflake, others hit BigQuery). The lineage builder resolves each cell independently and produces upstream entries from multiple platforms when needed.

**Fallback**: If a `dataConnectionId` is unknown (not returned by `connection list`), use `sql_parsing_platform_default` config value as the dialect and log a warning. If SQL parsing fails on a cell, warn and continue — never hard-fail ingestion.

### Run History (New)

```bash
hex run list <project_id> --json
```

The most recent completed run is emitted as an `OperationClass` aspect on the project's Dashboard URN:
- `operationType: RUN`
- `lastUpdatedTimestamp`: `start_time` of most recent run
- `actor`: `urn:li:corpuser:datahub` (system)

Only the latest run is emitted (not history). Controlled by `include_run_history: true`.

### Capabilities

```python
@capability(SourceCapability.DESCRIPTIONS, "Supported by default")
@capability(SourceCapability.OWNERSHIP, "Supported by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "Supported by default", subtype_modifier=[SourceCapabilityModifier.HEX_PROJECT])
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default via project YAML export and SQL parsing (yaml_export mode). Also supports DataHub query-based lineage (datahub_queries mode).")
@capability(SourceCapability.TAGS, "Supported: status, categories, collections as tags")
```

---

## Known Limitations

| Limitation | Impact | Workaround |
| --- | --- | --- |
| `hex` binary must be on PATH | Breaks if not installed | Document as prerequisite; check at startup with helpful error |
| Collections API requires elevated permissions (`Forbidden` for standard users) | Collections not available as tags for standard accounts | Graceful fallback: omit collections tags, warn in report |
| Users/Groups API requires workspace admin | No ownership resolution from user list | Use `creator.email` / `owner.email` from project get (same as v1) |
| Column-level lineage | SQL parsing may not resolve columns without schema info | Table-level lineage only; column-level is Phase 3 |
| Many-to-many Project–Component relationships | No API support | Same limitation as v1; document in `_post.md` |
| SQL parsing accuracy | Complex CTEs / dialects may fail | Warn and continue; table-level coverage is best-effort |
| `hex project export` may be slow for large projects | Long per-project latency | Configurable `cli_timeout_seconds`; run history/export are skippable |
| Multi-workspace ingestion | Each workspace = separate recipe | Use `--profile` isolation (built into config) |

---

## Implementation Plan

### Phase 1: CLI Client + Auth + Project Listing (v1 parity via CLI)

- [ ] `hex_v2/constants.py` — platform name, URN helpers, `CONNECTION_TYPE_TO_DATAHUB_PLATFORM`
- [ ] `hex_v2/model.py` — superset of v1 models; add `DataConnection`, `SqlCell`, `RunRecord`
- [ ] `hex_v2/cli_client.py` — `HexCliClient`:
  - `__init__`: validate `hex` binary exists; bootstrap auth
  - `list_projects()`: paginated `hex project list --json`
  - `get_project(id)`: `hex project get <id> --json`
  - `export_project(id, version)`: `hex project export <id> --version <v>` → parsed YAML
  - `list_connections()`: `hex connection list --json`
  - `list_runs(project_id)`: `hex run list <id> --json`
- [ ] `hex_v2/mapper.py` — `HexV2Mapper` (extend v1 mapper; same work unit structure)
- [ ] `hex_v2/hex_v2.py` — `HexV2Source` + `HexV2SourceConfig` + `HexV2Report`
  - Config: all v1 fields + new v2 fields
  - Core loop: list projects → filter → get → emit (same as v1)
  - Stale entity removal (same as v1)
- [ ] Register entry point `hex-v2` in `setup.py`
- [ ] Run `../gradlew :metadata-ingestion:updateLockFile`
- [ ] Unit tests: `tests/unit/hex_v2/test_cli_client.py`, `test_mapper.py`
- [ ] Integration test: `tests/integration/hex_v2/` with mock CLI binary serving JSON fixtures

### Phase 2: YAML Export + SQL Lineage (new capability)

- [ ] `hex_v2/yaml_parser.py` — `HexYamlParser`:
  - Parse YAML bytes → extract `SqlCell` list (cellId, label, source SQL, dataConnectionId)
  - Extract `sharedAssets.dataConnections` (workspace-level connection references)
  - Handle schema version differences gracefully
- [ ] `hex_v2/lineage_builder.py` — `HexLineageBuilder`:
  - Accept `List[SqlCell]` + `Dict[str, DataConnection]` (connections map)
  - Per-cell: resolve `dataConnectionId → platform`; call `sqlglot_lineage`
  - Aggregate `DatasetUrn` set across cells → return upstream list
  - Warn on: unknown connection type, SQL parse failure; never raise
- [ ] Wire into `HexV2Source.get_workunits_internal()` under `lineage_mode: yaml_export`
- [ ] Extend `HexV2Report` with lineage-specific metrics:
  - `projects_with_lineage`, `projects_without_sql_cells`
  - `sql_cells_parsed`, `sql_cells_failed`, `upstream_datasets_found`
  - `unknown_connection_types` (set)
- [ ] Unit tests: `test_yaml_parser.py`, `test_lineage_builder.py` with fixture YAML files
- [ ] Update integration golden files with lineage

### Phase 3: Run History + Connections Metadata (operational metadata)

- [ ] Emit `OperationClass` for most recent run per project (if `include_run_history`)
- [ ] Extend `HexV2Report` with run history metrics
- [ ] Unit + integration tests

### Phase 4: v1 Lineage Mode Preservation (`datahub_queries` mode)

- [ ] Port `HexQueryFetcher` logic into `hex_v2` (or import from `hex` — keep as internal dependency, not public API)
- [ ] `lineage_mode: datahub_queries` triggers the old DataHub-query-search path
- [ ] `lineage_mode: both` runs both paths and merges upstream lists (deduplicates by URN)
- [ ] Config validation: `datahub_queries` and `both` modes require `ctx.graph` (same as v1)

### Phase 5: Docs + Registration

- [ ] `docs/sources/hex_v2/hex_v2_pre.md` — overview, prerequisites (hex CLI install, token setup)
- [ ] `docs/sources/hex_v2/hex_v2_post.md` — capabilities, lineage modes, limitations
- [ ] `docs/sources/hex_v2/hex_v2_recipe.yml` — minimal recipe
- [ ] Add `hex-v2` to the platform registry (icon, display name) if not already present
- [ ] Run `./gradlew :metadata-ingestion:lintFix` — all Python must pass
- [ ] Run `./gradlew :datahub-web-react:mdPrettierWrite` — docs formatting

### Phase 6: Optional — Cell-Level Chart Entities

- [ ] Model EXPLORE cells as `Chart` entities (subtype `HexChart`)
- [ ] Emit `ChartInfo` with reference to upstream SQL cells' result variable names
- [ ] Emit `DashboardInfo.charts` on the parent Project linking to chart URNs
- [ ] Lineage: `DatasetUrn → Chart → Dashboard` instead of `DatasetUrn → Dashboard`

---

## File Checklist (new files only)

```
metadata-ingestion/src/datahub/ingestion/source/hex_v2/
  __init__.py
  hex_v2.py
  cli_client.py
  yaml_parser.py
  lineage_builder.py
  mapper.py
  model.py
  constants.py

metadata-ingestion/tests/unit/hex_v2/
  __init__.py
  conftest.py
  test_cli_client.py
  test_yaml_parser.py
  test_lineage_builder.py
  test_mapper.py
  test_hex_v2.py
  test_data/                     (fixture JSONs + YAML exports)

metadata-ingestion/tests/integration/hex_v2/
  __init__.py
  test_hex_v2.py
  docker/
    mock_hex_cli.py              (mock binary or HTTP server for CLI calls)
  golden/
    hex_v2_mce_golden.json
    hex_v2_mce_with_lineage_golden.json

metadata-ingestion/docs/sources/hex_v2/
  hex_v2_pre.md
  hex_v2_post.md
  hex_v2_recipe.yml
```

---

## Approval

- [ ] User approved this plan
- [ ] Approval message: _pending_
