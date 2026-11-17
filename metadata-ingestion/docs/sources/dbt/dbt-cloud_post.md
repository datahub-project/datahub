### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

**dbt Cloud metrics come from the environment definition.** The job query has no `semanticModels` or `metrics` field. Semantic models are read from `environment.definition.semanticModels`, and the `metrics:` block from `environment.definition.metrics`. Measures with `create_metric: true` are also emitted as metrics. The Semantic Layer GraphQL API, which runs metric queries, is a separate endpoint and is not used here.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
