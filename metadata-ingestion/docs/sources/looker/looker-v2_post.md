### Capabilities

See the **Important Capabilities** table above for what's enabled by default and what needs
extra configuration.

A few things worth calling out:

- Column-level lineage runs via Looker's `generate_sql_query` API, which requires the
  `develop` permission. If your API client doesn't have it, set `enable_api_sql_lineage: false`.
- LookML view extraction (`extract_views: true`) needs access to the LookML files themselves,
  either via `base_folder` (a local checkout) or `git_info` (a remote Git repo).
- Usage statistics come from Looker's system activity history. The default window is 30 days,
  configurable via `extract_usage_history_for_interval`.

### Limitations

- `extract_looks: true` requires `stateful_ingestion.enabled: true`. This is enforced at
  startup — the pipeline will refuse to run without it.
- SQL lineage uses Looker's `generate_sql_query` API, which can fail on large field sets.
  `api_sql_lineage_field_chunk_size` (default: 100) splits large views into smaller API calls.
  Setting `api_sql_lineage_individual_field_fallback: true` retries failing fields one at a time.
- LookML view extraction does not support all LookML syntax. If a view fails to parse, the
  view is skipped with a warning in the ingestion report.

### Troubleshooting

**"disabled due to missing dependency: liquid"**
Install the plugin with all its dependencies: `pip install 'acryl-datahub[looker-v2]'`.

**Missing explores**
Check `emit_used_explores_only` (default: `true`) and `explore_pattern`. By default, only
explores referenced by at least one dashboard or look are emitted.

**Stateful ingestion required**
Set `stateful_ingestion.enabled: true` in your config when using `extract_looks: true`.

**Lineage is missing or incomplete**
If `enable_api_sql_lineage: true` and lineage is still missing, confirm the API client has
the `develop` permission. Also check the ingestion report for per-view SQL generation errors.
