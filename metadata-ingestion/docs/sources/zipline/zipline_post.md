### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Feature naming

`MLFeature` names mirror Chronon's own backfill output columns, following the `{input_column}_{operation}_{window}` convention (for example `purchase_amount_sum_3d`). Bucketed aggregations append `_by_{bucket}`, and derivations are applied on top of aggregation outputs. Feature data types are inferred from the aggregation operation; derived columns fall back to `UNKNOWN`.

#### Tags and ownership

Tag extraction (`enable_tag_extraction`) reads tag bags stored in each object's `MetaData.customJson` (`groupby_tags`, `join_tags`, and per-column `column_tags`). Owner extraction (`enable_owner_extraction`) requires `owner_mappings` to map each Chronon team to a DataHub owner URN.

#### Reading from a local directory or Git

Point `path` at a locally-available compiled output directory, or set `git_info` to shallow-clone the repository at ingestion time (authenticated with an SSH deploy key). When `git_info` is set, `path` is interpreted relative to the checkout (e.g. `path: compiled`) and may be omitted to scan from the repository root. This lets you ingest directly from a GitHub/GitLab repository of compiled Chronon output instead of pre-fetching it.

Both compiled-output layouts are auto-detected when `path` points at a repository root: `production/` (produced by OSS Chronon's `compile.py`) and `compiled/` (produced by the newer `zipline compile` CLI). To ingest a canary environment, point `path` at `compiled_canary/` explicitly.

### Limitations

- The connector reads the **compiled** repository, not the Python config source. Run ingestion after `compile.py` so metadata reflects the latest changes.
- `JoinSource` GroupBys (whose source is another Join's output) are reported and skipped, because resolving them requires the parent join's compiled output.
- Primary key data types are not carried in the compiled config and are emitted as `UNKNOWN`.

### Troubleshooting

If ingestion fails, first confirm that `path` points at a compiled output directory containing `group_bys/`. Review the ingestion report for unmapped source namespaces (extend `source_platform_map`), unparseable files, and StagingQuery SQL parse failures.
