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

#### `Failed to fetch mapping tasks` warning

Mapping Tasks live at `/api/v2/mttask`, which is often restricted to specific roles. The connector treats this as a warning (not a failure) because mapping and lineage ingestion can still complete without it. Grant `Asset - read` on mapping tasks if you need them.

#### Export job timed out

The v3 Export API is asynchronous; for very large orgs, the default `export_poll_timeout_secs: 300` may be too short. Try:

- Reduce `export_batch_size` (default 1000) — smaller batches finish faster individually.
- Raise `export_poll_timeout_secs` (max 3600).
- Use `tag_filter_names` to scope the export to tagged mappings only.

The connector emits a report warning titled "IDMC export job timed out" for each timed-out batch and records it under `export_jobs_failed`.

#### Add-On Bundles showing up

IDMC ships several marketplace bundles (e.g. Cloud Data Integration templates). The connector filters these out automatically by checking `path.startswith("Add-On Bundles/")` or `updated_by == "bundle-license-notifier"`. If you see bundle mappings leaking through, open an issue with the offending object's path.

### Known Limitations

- **No column-level lineage** — the v3 export gives us transformation-level source/target tables but not column mappings.
- **No execution history** — the connector does not ingest Activity Monitor runs as DataProcessInstances.
- **Tag extraction not yet wired** — the `extract_tags` flag is accepted for forward compatibility but tags are not emitted in this release.
- **Single-user auth only** — service-principal / federated SSO login is not supported; use a native IDMC user.
