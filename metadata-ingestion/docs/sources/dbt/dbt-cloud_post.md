### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

**Top-level dbt metrics are not available from dbt Cloud.** The Discovery API this source reads does not expose the `metrics:` block - metric definitions live in dbt's separate Semantic Layer GraphQL API, which needs its own endpoint and a token with the "Semantic Layer Only" permission set. With `emit_semantic_model_entities` on, dbt Cloud therefore emits metrics only for measures with `create_metric: true`, which the `semanticModels` query does return. Use the dbt Core source (reading `manifest.json`) if you need the `metrics:` block. The ingestion report states this on every affected run.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
