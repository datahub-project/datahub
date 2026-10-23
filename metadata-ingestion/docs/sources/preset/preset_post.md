### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Platform instance

If you ingest from more than one Preset workspace, set `platform_instance` to keep them apart. Dashboards, charts and datasets then get the instance in their URNs and in a `dataPlatformInstance` aspect, so DataHub's Navigate panel lists each workspace under its own instance instead of "Default".

```yml
source:
  type: preset
  config:
    platform_instance: analytics_prod
```

#### Database alias

If you were using `database_alias` in one of your other ingestions to rename your databases to something else based on business needs you can rename them in superset also

```yml
source:
  type: preset
  config:
    # Coordinates
    connect_uri: Preset workspace URL
    manager_uri: https://api.app.preset.io

    # Credentials
    api_key: API key
    api_secret: API secret
    database_alias:
      example_name_1: business_name_1
      example_name_2: business_name_2

sink:
  # sink configs
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
