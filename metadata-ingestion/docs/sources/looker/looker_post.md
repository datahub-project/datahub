### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Usage Statistics

When `extract_usage_history` is enabled, the `looker` module extracts usage from Looker's [System Activity](https://cloud.google.com/looker/docs/system-activity) `history` explore and attaches it to the corresponding DataHub entities:

- **Dashboards** and **Looks / Charts** — view counts and per-user usage.
- **Explores** — query counts and per-user usage, emitted as dataset usage statistics on the explore's dataset URN.

Usage is aggregated per day over the window set by `extract_usage_history_for_interval`.

:::note

Explore usage is attached only to explores that were actually ingested in the same run. Unlike dashboards and looks, Looker exposes no absolute usage snapshot for an explore, so only the per-day time-series (with per-user counts) is emitted for explores. View-level (LookML view) usage is not derivable at the explore-query grain in System Activity.

:::

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
