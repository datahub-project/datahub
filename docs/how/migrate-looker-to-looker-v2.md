# Migrating from `looker` / `lookml` to `looker-v2`

The `looker` and `lookml` ingestion sources are deprecated. The `looker-v2` source replaces
both — one pipeline handles dashboards, charts, explores, and LookML views.

## Why migrate

Running `looker` and `lookml` as separate pipelines meant coordinating two configs, two
schedules, and two sources of truth for the same platform. `looker-v2` combines them. It also
adds improvements that weren't backported to the old sources: better SQL lineage via
`generate_sql_query`, multi-project LookML dependencies, and stateful deletion of Looks.

The old sources still work, but they emit deprecation warnings in the ingestion report and
in CLI logs. They won't receive new features.

## Install the plugin

```bash
pip install 'acryl-datahub[looker-v2]'
```

## Config migration

### From `looker`

Most fields carry over with the same names. A few were removed or renamed:

| Old field (`looker`)               | New field (`looker-v2`)                     | Notes                          |
| ---------------------------------- | ------------------------------------------- | ------------------------------ |
| `extract_independent_looks`        | `extract_looks`                             | Renamed                        |
| `use_api_for_view_lineage`         | `enable_api_sql_lineage`                    | Renamed; also covers PDT graph |
| `field_threshold_for_splitting`    | `api_sql_lineage_field_chunk_size`          | Renamed                        |
| `enable_individual_field_fallback` | `api_sql_lineage_individual_field_fallback` | Renamed                        |
| `max_threads`                      | `max_concurrent_requests`                   | Renamed                        |
| `explore_browse_pattern`           | _(removed)_                                 | No longer supported            |
| `view_browse_pattern`              | _(removed)_                                 | No longer supported            |

Everything else (`base_url`, `client_id`, `client_secret`, `dashboard_pattern`,
`chart_pattern`, `folder_path_pattern`, `skip_personal_folders`, `extract_owners`,
`extract_usage_history`, `platform_instance`, etc.) works the same way.

### From `lookml`

| Old field (`lookml`)        | New field (`looker-v2`)  | Notes                                                                              |
| --------------------------- | ------------------------ | ---------------------------------------------------------------------------------- |
| `api`                       | _(merged into root)_     | API credentials (`base_url`, `client_id`, etc.) are top-level in `looker-v2`       |
| `emit_reachable_views_only` | `emit_unreachable_views` | Logic inverted; set `emit_unreachable_views: true` to get the old `false` behavior |
| `explore_browse_pattern`    | _(removed)_              | No longer supported                                                                |
| `view_browse_pattern`       | _(removed)_              | No longer supported                                                                |

Fields that carry over unchanged: `base_folder`, `git_info`, `project_name`,
`project_dependencies`, `model_pattern`, `view_pattern`, `connection_to_platform_map`,
`liquid_variables`, `lookml_constants`, `looker_environment`, `process_refinements`,
`platform_instance`, `stateful_ingestion`.

## Minimal example

A config that covers what previously required two separate sources:

```yaml
source:
  type: looker-v2
  config:
    base_url: "https://<company>.cloud.looker.com"
    client_id: ${LOOKER_CLIENT_ID}
    client_secret: ${LOOKER_CLIENT_SECRET}

    # Dashboard + chart extraction (replaces `looker`)
    extract_dashboards: true
    extract_looks: false # set to true + enable stateful_ingestion if needed

    # Explore extraction
    extract_explores: true

    # LookML view extraction (replaces `lookml`)
    extract_views: true
    base_folder: /path/to/lookml # or use git_info for remote repos


    # Stateful ingestion (required if extract_looks: true)
    # stateful_ingestion:
    #   enabled: true
```

## URN compatibility

By default, `looker-v2` generates the same URNs as the old `looker` source, so existing
dashboards and charts keep their identities in DataHub.

If your old `looker` config had `platform_instance` set, also set
`include_platform_instance_in_urns: true` in `looker-v2` to maintain the same URN format.

## Removing old pipeline recipes

Once you've verified `looker-v2` is ingesting correctly:

1. Disable or delete your old `looker` recipe
2. Disable or delete your old `lookml` recipe
3. If you were using stateful ingestion on either, the first `looker-v2` run will produce a
   fresh checkpoint — you may see some entities re-ingested once
