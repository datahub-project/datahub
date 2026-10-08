### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage

The Grafana source can extract lineage information between charts and their data sources. You can control lineage extraction using these configuration options:

```yaml
source:
  type: grafana
  config:
    url: "https://grafana.company.com"
    service_account_token: "your_token"

    # Lineage extraction (default: true)
    include_lineage: true

    # Column-level lineage from SQL queries (default: true)
    # Only applicable when include_lineage is true
    include_column_lineage: true

    # Platform mappings for lineage extraction
    connection_to_platform_map:
      postgres_datasource_uid:
        platform: postgres
        platform_instance: my_postgres
        env: PROD
        database: analytics
        database_schema: public
```

**Lineage Features:**

- **Dataset-level lineage**: Links charts to their underlying data sources
- **Column-level lineage**: Extracts field-to-field relationships from SQL queries
- **Platform mapping**: Maps Grafana data sources to their actual platforms for accurate lineage
- **SQL parsing**: Supports parsing of SQL queries for detailed lineage extraction

**Performance Note:** Lineage extraction can be disabled (`include_lineage: false`) to improve ingestion performance when lineage information is not needed.

#### Ownership

The Grafana source extracts dashboard ownership from the dashboard creator and assigns them as a Technical Owner.

```yaml
source:
  type: grafana
  config:
    url: "https://grafana.company.com"
    service_account_token: "your_token"

    # Ownership extraction (default: true)
    ingest_owners: true

    # Email suffix removal like @acryl.io (default: true)
    remove_email_suffix: true
```

**Ownership Features:**

- **Technical Owner assignment**: Dashboard creators are automatically assigned as Technical Owners
- **Email suffix control**: Configure how user email addresses are converted to DataHub user URNs via `remove_email_suffix`
- **Disable ownership**: Set `ingest_owners: false` to skip ownership extraction entirely

#### Probe support

`datahub recipe probe` checks a recipe against Grafana without running ingestion. It offers
`folders` and `dashboards`:

```shell
datahub recipe probe methods --recipe grafana_recipe.yml
datahub recipe probe run folders --recipe grafana_recipe.yml
datahub recipe probe run dashboards --recipe grafana_recipe.yml --report-to dashboards.json
datahub recipe probe filter --recipe grafana_recipe.yml --from-run dashboards.json
```

The probe signs in with the recipe's `service_account_token` and `verify_ssl`, as ingestion does,
and reads the same endpoints: `/api/folders` for top-level folders and `/api/search` for
dashboards. A Viewer service account is enough. Both listings include what the recipe would
exclude, so `probe filter` can say why. `folder_pattern` and `dashboard_pattern` are matched on
titles. A dashboard is judged on its own title only: one inside a folder that `folder_pattern`
excludes is still ingested. With `basic_mode`, ingestion emits no folders, and its untyped
`/api/search` returns folders too, which it emits as dashboards; the probe lists the same results.
The probe never reads dashboard JSON, so it sees no panel queries.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
