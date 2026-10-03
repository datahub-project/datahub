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

#### Platform instance

If you ingest from more than one Superset deployment, set `platform_instance` to keep them apart. Dashboards and charts get the instance in their URNs and are emitted with a `dataPlatformInstance` aspect. DataHub's Navigate panel groups by that aspect, so each deployment appears under its own instance instead of everything landing under "Default".

```yml
source:
  type: superset
  config:
    # Coordinates
    connect_uri: http://localhost:8088
    platform_instance: analytics_prod

    # Credentials
    username: user
    password: pass
    provider: ldap

sink:
  # sink configs
```

Datasets do not receive the aspect yet because their URNs do not carry the instance; that follows in a later change together with database and schema containers.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
