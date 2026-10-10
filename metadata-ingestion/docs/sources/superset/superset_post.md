If you were using `database_alias` in one of your other ingestions to rename your databases to something else based on business needs you can rename them in superset also

```yml
source:
  type: superset
  config:
    # Coordinates
    connect_uri: http://localhost:8088

    # Credentials
    username: user
    password: pass
    provider: ldap
    database_alias:
      example_name_1: business_name_1
      example_name_2: business_name_2

sink:
  # sink configs
```

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Probe support

`datahub recipe probe` checks a recipe against the Superset instance without running ingestion.
It logs in as ingestion does and offers four listings, each including what the recipe would
exclude: `dashboards`, `charts`, `datasets` and `databases`, each with a `--limit`:

```shell
datahub recipe probe methods --recipe superset_recipe.yml
datahub recipe probe run datasets --recipe superset_recipe.yml --report-to datasets.json
datahub recipe probe filter --recipe superset_recipe.yml --from-run datasets.json
```

`probe filter` judges each kind the way ingestion does: a dashboard by `dashboard_pattern` on its
title, a chart by `chart_pattern` on its name, and a dataset by `dataset_pattern` on its table name
and then by `database_pattern` on its database. Judge a `datasets` listing with
`probe filter --from-run`, which carries each dataset's database, or pass the database as
`--parent`; a bare dataset name is judged on `dataset_pattern` alone, with a warning.
`database_pattern` never drops a chart or a dashboard, and `ingest_dashboards`, `ingest_charts`
and `ingest_datasets` switch a whole kind off (`ingest_datasets` is off by default).

The probe reads the list endpoints ingestion reads, and a dataset's detail only when its list record
lacks the database. Records carry names, ids, schemas and database names; owners and the users who
last changed an object are never returned. A listing the credential's role may not read (HTTP 403 or 404) comes back empty with a warning; a refused login, any other HTTP error or an unreachable host
fails the command with exit code 3.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
