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

#### LookML projects ingested under other platform instances

Explores link to the LookML views they read. Those view URNs must match the ones the `lookml` recipes emit, which include each recipe's `platform_instance`. By default the `looker` recipe builds every view URN with its own `platform_instance` and `env`. When LookML projects are ingested by separate `lookml` recipes with different platform instances, for example one recipe per git repository, map each project to its recipe's instance:

```yaml
source:
  type: looker
  config:
    platform_instance: looker_prod
    project_to_platform_instance:
      sales_models:
        platform_instance: lookml_sales
      finance_models:
        platform_instance: lookml_finance
        env: DEV # optional, defaults to the top-level env
```

Keys are LookML project names. A view from an imported project is keyed by the imported project's name. Projects that are not listed, and fields an entry leaves out, keep the top-level `platform_instance` and `env`. Dashboard, chart and explore URNs are unchanged; only the view URNs in explore lineage follow the mapping.

The `lookml` source accepts the same option for imported projects that another `lookml` recipe ingests. A recipe does not emit views of a project mapped to a different `platform_instance` or `env` than its own, because the recipe for that project owns them. They are listed under `views_owned_elsewhere` in the run report. Ingest each project with its own recipe, and keep the recipe's own project out of the map.

#### Probe support

`datahub recipe probe` lists what this recipe's credential can see and judges
it the way ingestion will, before you run an ingestion. Probe output is
metadata only: no owners, user emails, personal-folder names, query filters or
SQL. Under `skip_personal_folders`, dashboards and looks in personal folders are
listed by id with their title withheld, since ingestion does not emit them.

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
standalone look, up to a fixed bound; whatever it could not read is left
undetermined, and the result says whether the bound or a failed read stopped it.

A listing Looker refuses (HTTP 403 or 404) comes back empty with a warning.
When a command reads many objects, one by one, a read that is refused or
fails (a 5xx or gateway page, a rate limit, a timeout) leaves only that
object undetermined, and is counted in one warning per kind of failure. A
listing that fails otherwise ends the command, as does a Looker that cannot be
reached at all.

```shell
datahub recipe probe run explores --model <model> --trace-charts --recipe looker.yml --report-to explores.json
datahub recipe probe filter --recipe looker.yml --from-run explores.json
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
