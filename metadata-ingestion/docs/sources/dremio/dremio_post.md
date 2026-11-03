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

#### Cross-Platform Lineage (`source_mappings`)

Each Dremio table in a mapped source gets a COPY upstream on the source platform, for example
`mssql`. The upstream URN has to match the URN the source's own connector ingests:

- SQL Server, MySQL, Snowflake and Teradata sources list databases as folders, so the database
  comes from the Dremio path. For sources that list only schemas (Postgres, Redshift, Synapse,
  ...), the database from the Dremio source config is prepended, in the case it is configured. A
  custom type from `source_type_mappings` can declare `database_in_path: true` or `false`;
  otherwise the database is prepended unless the path already starts with it.
- Casing follows the target connector's default: lowercase for Snowflake, ALL-UPPERCASE names
  lowercased for Oracle and SAP HANA, an uppercased database for Db2, Dremio's case otherwise.
  Object storage keys keep their case. A Dremio-to-Dremio source maps to `dremio.<path>`,
  lowercased. If you ingested the target with non-default
  casing, set `convert_urns_to_lowercase` on the mapping, for example `true` for SQL Server
  ingested with `convert_urns_to_lowercase: true` (which that connector recommends for lineage),
  or `false` for Snowflake ingested with `convert_urns_to_lowercase: false`. The option applies to
  database sources only; object storage and Dremio-to-Dremio mappings ignore it.
- Set `platform_instance` and `env` on the mapping to the values the target connector uses.
- `schema_pattern` must allow the source itself, not only paths below it. Otherwise the source
  is filtered out and its tables get no upstream. Use, for example, `^my_src$` together with
  `^my_src\.my_db(\..*)?$`, or `^my_src($|\..*)`. A `source_mappings` entry that matches no
  ingested source is reported as a warning.
- The ingestion report lists one sample upstream URN per mapped source under
  `source_mapping_sample_upstream_urns`. Compare it with a URN of the target connector.

```yaml
source_mappings:
  - source_name: my_sqlserver
    platform: mssql
    platform_instance: my_instance
    env: PROD
    convert_urns_to_lowercase: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
