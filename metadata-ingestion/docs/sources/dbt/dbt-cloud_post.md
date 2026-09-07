### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Semantic models and metrics

`emit_semantic_model_entities` works the same way here as for dbt Core — see
[Semantic Models and Metrics](/docs/generated/ingestion/sources/dbt#semantic-models-and-metrics)
for what it emits. Two dbt Cloud specifics:

- The project name in the `SemanticModel` and `Metric` URNs comes from the semantic models'
  `packageName`, since there is no manifest metadata to read it from. Set
  `semantic_model_project_name` to pin it.
- Only metrics from measures with `create_metric: true` are ingested. Metrics declared in a
  `metrics:` block are not, because the Discovery API does not expose the semantic graph — those
  definitions live in the separate
  [Semantic Layer GraphQL API](https://docs.getdbt.com/docs/dbt-apis/sl-graphql), which requires its
  own endpoint and a service token carrying the `Semantic Layer Only` permission set. Use the dbt
  Core source if you need the `metrics:` block.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
