### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Stateful Ingestion

Enabling `stateful_ingestion` unlocks three incremental capabilities that reduce API load on repeated runs:

| Feature                   | Config key                                | What it does                                                                                                                                                          |
| ------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stale entity removal      | `stateful_ingestion.enabled: true`        | Removes entities from DataHub that no longer exist in Dremio                                                                                                          |
| Time-window deduplication | `enable_stateful_time_window: true`       | Advances the query lineage start time to the previous run's end time, so `SYS.JOBS_RECENT` is never re-processed                                                      |
| Incremental profiling     | `profiling.profile_if_updated_since_days` | Skips re-profiling tables that DataHub profiled within the configured window (compared against last-profiled time, since Dremio has no table modification timestamps) |

```yaml
stateful_ingestion:
  enabled: true

enable_stateful_time_window: true # only process new job history each run

profiling:
  enabled: true
  profile_if_updated_since_days: 1 # re-profile at most once per day
```

#### Preserving Manual Edits Between Runs

By default, each ingestion run overwrites the full `DatasetProperties` aspect, which resets any
descriptions or custom properties edited in the DataHub UI. Set `incremental_properties: true` to
emit properties as PATCH operations instead, so only the fields the connector knows about are
updated and your manual edits are preserved.

This is particularly useful for Dremio, which often acts as a semantic layer on top of raw sources
that teams re-document in DataHub.

```yaml
incremental_properties: true
```

Set `incremental_lineage: true` to emit lineage as PATCH operations, so manually-curated
lineage edges added in the DataHub UI are not removed on the next run. Defaults to `false`
(full-overwrite) for consistency with the standard `IncrementalLineageConfigMixin` used by other
connectors; PATCH emission requires a GMS that supports patch aspects.

#### Probe support

`datahub recipe probe` checks a Dremio recipe against the live server before a run. It connects with the recipe's own settings (Dremio Cloud or Software, personal access token or password, TLS) through the connector's API client, and returns metadata only: names and paths, never rows or view SQL.

| Command   | Parameters           | Returns                                                                                                                   |
| --------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `sources` | `limit`              | Each source's name                                                                                                        |
| `spaces`  | `limit`              | Each space's name, home spaces (`@<user>`) included with `home: true`                                                     |
| `folders` | `container`, `limit` | Folders under one source or space, or under all of them, by full dotted path, with their `root`                           |
| `tables`  | `limit`              | Tables by full dotted path, with their `schema`, the server's `edition` and `has_columns`, read from `INFORMATION_SCHEMA` |
| `views`   | `limit`              | Views, named and described as `tables` are                                                                                |

`schema_pattern` filters sources, spaces and folders, and `dataset_pattern` filters tables and views. Every listed name is the full dotted path that ingestion's filters read, and `probe filter` applies ingestion's rules to it:

- A source or space passes `schema_pattern` on its name, or as the first segment of a dotted allow entry such as `space.folder`.
- A folder is reached only under a source or space that passes, and is then judged on its lower-cased path, or as a prefix of an allow entry that ends in `.*`.
- Ingestion applies `schema_pattern` to datasets inside its dataset query, where Dremio's `REGEXP_LIKE` finds the pattern anywhere in the upper-cased schema, not only at its start. On Community edition that is the dataset's schema. On Enterprise and Cloud it is the whole path, the dataset's own name included. A deny entry can therefore drop the datasets in a folder that ingestion still emits as a container.
- `dataset_pattern` matches the lower-cased `schema.dataset` path. Dremio Reflections, under `_accelerator_`, are never emitted.
- Ingestion's dataset query also needs Dremio to hold column metadata for a dataset. A source table that nothing has queried yet can have none (`has_columns: false`), and is reported as excluded by `no_column_metadata`.

The dataset rule depends on the edition, so judge datasets from a saved listing, which records it:

```shell
datahub recipe probe run views --recipe recipe.yml --report-to views.json
datahub recipe probe filter --recipe recipe.yml --kind View --from-run views.json
datahub recipe probe run folders --recipe recipe.yml --container "my_space"
```

With `--name` alone, a dataset's `schema_pattern` rule is judged only on Dremio Cloud, where the edition is known from the recipe; elsewhere the result warns that it was skipped.

`profile_pattern` is not a probe filter: it decides which datasets are profiled, not which are ingested.

A home space is named after its user. When the recipe does not ingest one, the probe counts it, and the folders and datasets in it, in a warning without listing them.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
