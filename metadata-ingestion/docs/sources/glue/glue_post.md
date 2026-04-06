### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

#### JDBC Upstream Lineage

When a Glue job reads from a JDBC source (e.g. PostgreSQL, MySQL, Redshift, Oracle, SQL Server), the plugin automatically extracts upstream lineage to the referenced tables. This works for both:

- **Direct JDBC connections** specified inline in the job script (via `connection_type` and `connection_options`)
- **Named Glue connections** configured in the Glue console and referenced by `connectionName`

Supported JDBC platforms: PostgreSQL, MySQL, MariaDB, Redshift, Oracle, SQL Server.

The plugin resolves table references from either the `dbtable` parameter or by parsing SQL from the `query` parameter.

#### Aligning URNs with Target Platform Connectors

If you also ingest the JDBC source separately (e.g. using the `postgres` or `mysql` connector) and that connector uses a `platform_instance` or a different `env`, you should configure `target_platform_configs` so the URNs match:

```yaml
source:
  type: glue
  config:
    target_platform_configs:
      postgres:
        platform_instance: prod-postgres
        env: PROD
      mysql:
        platform_instance: prod-mysql
```

When this is configured, dataset URNs produced by the Glue connector will include the same `platform_instance` and `env` as the target platform's connector, ensuring entities merge correctly in DataHub. If the target platform connector does not use a `platform_instance`, no configuration is needed — URNs will match by default.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

JDBC upstream lineage from SQL queries (`query` parameter) does not currently apply `target_platform_configs`. Only the `dbtable` code path uses the configured `platform_instance` and `env`.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
