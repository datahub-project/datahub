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

`incremental_lineage: true` (the default) emits lineage as PATCH operations, so manually-curated
lineage edges added in the DataHub UI are not removed on the next run.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
