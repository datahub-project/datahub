### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Large sites

Each app is opened in the Engine without its data. `max_workers` (default 4) sets how many apps are read at the same time. Each worker holds one Engine session. `max_workers` applies to Qlik Cloud too, where rate-limited requests are retried with back-off.

With `deployment: on_prem`, `skip_unchanged_apps: true` and stateful ingestion enabled, later runs only open apps that changed in the Engine. An app is read again when the app or any of its sheets, stories or master items is saved, when it is reloaded, published or moves stream, when its owner changes, when the recipe or a data connection changes, or after a connector upgrade. It is also read again if the last read of it failed or its metadata is no longer in DataHub. Entities of a skipped app stay in place and are not soft-deleted. `skip_unchanged_apps` has no effect on Qlik Cloud.

Column-level lineage is resolved against the upstream schemas in DataHub at read time. After ingesting an upstream source for the first time, set `stateful_ingestion.ignore_old_state: true` for one run so every app is read again.

```yaml
max_workers: 4
skip_unchanged_apps: true
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
