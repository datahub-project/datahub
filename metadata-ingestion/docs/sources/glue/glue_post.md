### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Cross-Account Lineage

When ingesting Glue metadata from different AWS accounts, the connector can establish lineage relationships across account boundaries. This is particularly useful for tracking data flows in multi-account AWS environments.

**How it works:**

When you ingest a Glue catalog that references storage (e.g., S3 buckets) or other resources in different accounts, the connector creates lineage edges to those resources. For example:

- Account A has a Glue table pointing to an S3 bucket in Account A
- Account B has shared access to Account A's Glue catalog (using `catalog_id`)
- When you ingest from Account B, DataHub creates lineage from the Glue table to the S3 bucket

**URN resolution and entity merging:**

The connector's behavior depends on your `platform_instance` configuration:

- **Without platform instance**: If you ingest the same Glue catalog from different accounts without setting `platform_instance`, DataHub recognizes them as the same entities and creates a single dataset. Lineage to S3 (or other storage) is preserved regardless of which account performed the ingestion.

  ```yaml
  # Account A ingestion
  source:
    type: glue
    config:
      aws_config:
        aws_region: us-east-1
  ```

  ```yaml
  # Account B ingestion (using catalog_id)
  source:
    type: glue
    config:
      catalog_id: "111111111111" # Account A's ID
      aws_config:
        aws_region: us-east-1
  ```

  Result: Single Glue table entity with lineage to S3.

- **With platform instance**: Using different `platform_instance` values creates separate dataset entities with distinct URNs. This is useful for tracking the same data through different access paths or maintaining separate views per account.

  ```yaml
  # Account A ingestion
  source:
    type: glue
    config:
      platform_instance: account-a
      aws_config:
        aws_region: us-east-1
  ```

  ```yaml
  # Account B ingestion
  source:
    type: glue
    config:
      catalog_id: "111111111111" # Account A's ID
      platform_instance: account-b
      aws_config:
        aws_region: us-east-1
  ```

  Result: Two separate Glue table entities (one per platform instance), each with its own lineage edges.

**Storage lineage:**

To see lineage between Glue tables and their underlying S3 locations, you must:

1. Set `emit_storage_lineage: true` in the Glue connector config
2. Ingest S3 separately using the [S3 connector](https://docs.datahub.com/docs/generated/ingestion/sources/s3)

The S3 connector creates dataset entities for the storage locations, and the Glue connector creates lineage edges connecting Glue tables to those S3 datasets. This works seamlessly across account boundaries when both connectors are configured correctly.

For cross-account S3 access, refer to the [S3 connector's cross-account documentation](https://docs.datahub.com/docs/generated/ingestion/sources/s3#cross-account-access).

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
