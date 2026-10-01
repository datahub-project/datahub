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

#### Probing a Looker recipe

`datahub recipe probe` lists what this recipe's credential can see and judges
it the way ingestion will, before you run an ingestion. Probe output is
metadata only: no owners, user emails, personal-folder names, query filters or
SQL.

| Command                   | Lists                                    | Judge with `probe filter`   |
| ------------------------- | ---------------------------------------- | --------------------------- |
| `dashboards`              | dashboards by id, including deleted ones | `--kind Dashboard`          |
| `charts --dashboard <id>` | a dashboard's elements by element id     | `--kind Look` (with parent) |
| `looks`                   | saved looks by look id                   | `--kind Look` (no parent)   |
| `models`                  | LookML models                            | `--kind "LookML Model"`     |
| `explores --model <name>` | a model's explores                       | `--kind Explore`            |
| `permissions`             | granted and missing API permissions      | n/a                         |

Ingestion filters dashboards on more than the id: `skip_personal_folders`,
`folder_path_pattern` and `include_deleted` also apply. Save the listing and
judge it from the file, so those facts are used:

```shell
datahub recipe probe run dashboards --recipe looker.yml --report-to dashboards.json
datahub recipe probe filter --recipe looker.yml --from-run dashboards.json
```

With `emit_used_explores_only: true` (the default), an explore, and its LookML
model, is ingested only when a chart or look that ingestion keeps queries it.
A name alone cannot show that, so `probe filter` reports such explores as not
ingested and says the verdict is undetermined. Add `--trace-charts` to
`explores` or `models` to record whether each is `used`, or to `looks` to
record whether each is `on_kept_dashboard`. The trace lists dashboards and
reads each one `dashboard_pattern` keeps, skipping personal-folder dashboards
under `skip_personal_folders`, as ingestion does. It does not apply
`folder_path_pattern`: ingestion counts a dashboard's explores and looks before
it drops the dashboard by folder. For `explores` and `models` with
`extract_independent_looks`, it also reads each standalone look's query.
Besides the listing calls, that is one read per dashboard plus one per
standalone look, and it stops after 1000 reads; whatever it could not read is
left undetermined.

```shell
datahub recipe probe run explores --model <model> --trace-charts --recipe looker.yml --report-to explores.json
datahub recipe probe filter --recipe looker.yml --from-run explores.json
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
