### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported metadata, lineage, and usage features for this module.

### Limitations

- Coverage depends on metadata visibility in both Presto and the Hive Metastore integration.
- Lineage and usage fidelity depend on query access and parsing compatibility.

### Troubleshooting

- Verify catalog connectivity and metadata permissions in Presto.
- Confirm metastore access and namespace/table visibility for the ingestion principal.
- Check ingestion logs for query parsing errors or permission-denied responses.
