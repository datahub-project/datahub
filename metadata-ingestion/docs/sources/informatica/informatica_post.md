### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Filtering

Three filter layers can be combined, applied in order:

1. **Tag-based** (`tag_filter_names`, recommended for large orgs) — an allowlist of IDMC tags; only tagged objects are ingested.
2. **Path-based** (`project_pattern`, `folder_pattern`) — regex allow/deny on project and folder names.
3. **Name-based** (`mapping_pattern`, `taskflow_pattern`) — regex allow/deny on mapping and taskflow names.

#### Connection Type Mapping

When emitting lineage, each IDMC connection is mapped to a DataHub platform (e.g. `Snowflake_Cloud_Data_Warehouse → snowflake`). The mapping is driven by `connParams["Connection Type"]`. If IDMC returns an unknown type (or a customer-specific connector), set `connection_type_overrides` to map that connection ID to a DataHub platform name. The connector will warn about unknown platforms at config-parse time.

#### External Dataset Stubs

A dataset referenced by mapping lineage that DataHub has no `Status` for receives a minimal `Status` aspect. Without this stub, DataHub treats URNs that no other connector has ingested as non-existent and `searchAcrossLineage` filters them out of results — which would leave the left-side chevron on a Mapping Task's `transform` DataJob unable to expand upstream datasets. The stub does not override Schema, Ownership, or other metadata written by the source platform's own connector when it runs.

These datasets belong to other connectors, so this source never soft-deletes them:

- A dataset that already has a `Status` gets no stub. A dataset another connector soft-deleted stays deleted, and is counted in `external_datasets_soft_deleted` in the report.
- If the source cannot read the datasets' status from DataHub, it skips their stubs and reports a `Could not check external dataset status` warning.
- Without a DataHub graph (for example with a file sink), every referenced dataset gets the stub.

#### Stale Metadata Removal

With `stateful_ingestion.remove_stale_metadata` enabled, objects that a run did not see are soft-deleted. The source only removes objects it knows are gone:

- If listing projects, folders, Taskflows or Mapping Tasks fails, the run fails and stale metadata removal is skipped for that run.
- If a single folder fails to process, the run reports a warning and keeps the containers from the previous run.
- If a single Taskflow fails to process, the run reports a warning and keeps its DataFlow and `orchestrate` DataJob from the previous run.
- If a Taskflow's definition cannot be exported, its `orchestrate` DataJob from the previous run is kept. A Taskflow that exports with no steps is treated as having none.

### Limitations

- **No column-level lineage** — the v3 export gives us transformation-level source/target tables but not column mappings.
- **No execution history** — the connector does not ingest Activity Monitor runs as DataProcessInstances.
- **Taskflow step DAG requires `Asset - export`** — Taskflow step ordering lives in a `taskflowModel` XML document fetched via the v3 Export API. For a Taskflow the user can't export, the Taskflow is still emitted as a DataFlow, but no step chain or `orchestrate` DataJob is emitted; the `orchestrate` DataJob from the previous run is kept instead. The report includes `taskflows_with_steps` so you can confirm coverage.
- **Single-user auth only** — service-principal / federated SSO login is not supported; use a native IDMC user.
- **v2 API endpoints are not paginated** — `/api/v2/mapping` and `/api/v2/connection` return all records in a single response; the IDMC v2 API does not honour `limit`, `skip`, or `maxRecordsCount` parameters (verified against a live instance). For orgs with very large numbers of mappings (>10k) or connections (>1k), the single call may exceed `request_timeout_secs` or produce a very large response. Mitigations: raise `request_timeout_secs`, or use `tag_filter_names` to scope ingestion to a tagged subset of objects.

### Troubleshooting

#### `IDMC login failed` at startup

The connector raises this when the v2 login endpoint returns non-200 or a body without `icSessionId`/`serverUrl`. Common causes:

- Wrong `login_url` for your pod (see the region table in the Prerequisites section).
- Service account locked out, MFA-protected, or password-expired. Use a dedicated IDMC user without interactive MFA.
- Firewall blocking egress to `*.informaticacloud.com`.

The raised error includes the HTTP status, a truncated response body, and the `login_url` used.

#### `connections_unresolved` entries in the report

The connector resolves lineage dataset URNs by matching the mapping's `connectionId` (e.g. `saas:@fed-xyz`) against the IDMC connection catalog. If a connection cannot be mapped to a DataHub platform, the lineage edge is dropped and the connection is recorded in `connections_unresolved`. Two typical causes:

1. The connection uses a type not in the built-in `CONNECTION_TYPE_MAP` (e.g. a custom connector). Add it to `connection_type_overrides` with the connection ID → DataHub platform.
2. The `Connection - read` privilege is missing from the service account, so `list_connections` fetches an empty or partial catalog.

#### `Failed to list IDMC mapping tasks` failure

Listing Mapping Tasks is often restricted to specific roles. If it fails, the run fails: Mapping Tasks are missing from the output, and stale metadata removal is skipped so they are not soft-deleted. Grant `Asset - read` on Mapping Tasks to the service account.

The same applies to `Failed to list IDMC projects`, `Failed to list IDMC folders` and `Failed to list IDMC taskflows`: fix the permission or API error named in the report.

#### Export job timed out

The v3 Export API is asynchronous; for very large orgs, the default `export_poll_timeout_secs: 300` may be too short. Try:

- Reduce `export_batch_size` (default 1000) — smaller batches finish faster individually.
- Raise `export_poll_timeout_secs` (max 3600).
- Use `tag_filter_names` to scope the export to tagged mappings only.

The connector emits a report warning titled "IDMC export job timed out" for each timed-out batch and records it under `export_jobs_failed`.

Taskflow exports are slower than mapping exports, and `export_batch_size` applies to both. A timed-out Taskflow batch also reports "IDMC Taskflow batch export timed out": those Taskflows are still ingested, without their step DAGs. If you see it, lower `export_batch_size` (for example to 100). The connector doesn't retry the Taskflows in a failed batch one at a time, so a timeout doesn't turn into one export job per Taskflow.

#### Add-On Bundles showing up

IDMC ships several marketplace bundles (e.g. Cloud Data Integration templates). The connector filters these out automatically by checking `path.startswith("Add-On Bundles/")` or `updated_by == "bundle-license-notifier"`. If you see bundle mappings leaking through, open an issue with the offending object's path.
