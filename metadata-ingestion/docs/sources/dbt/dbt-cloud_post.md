### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

Semantic models and the `metrics:` block are ingested from dbt Cloud. Measures with `create_metric: true` are also emitted as metrics.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
