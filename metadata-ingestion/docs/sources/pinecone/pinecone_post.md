### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- Schema inference is based on sampled vectors and may not capture all metadata fields present in the full dataset.
- The `describe_namespace()` API is only available for serverless indexes; pod-based indexes use `describe_index_stats()` for namespace discovery.
- Vector values themselves are not ingested, only metadata fields and statistics.

### Troubleshooting

If ingestion fails, validate your API key, check network connectivity to the Pinecone API, and review ingestion logs for source-specific errors. If schema inference is slow, reduce `schema_sampling_size` or set `enable_schema_inference: false`.
