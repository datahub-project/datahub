### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::note Version Compatibility

This connector requires an MLflow server version **1.28.0 or later**.  
If you're using an earlier version, ingestion of **Experiments** and **Runs** will be skipped.

:::

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

### Auth Configuration

You can configure the MLflow source to authenticate with the MLflow server using the `username` and `password` configuration options.

```yaml
source:
  type: mlflow
  config:
    tracking_uri: "http://127.0.0.1:5000"
    username: <username>
    password: <password>
```

### Dataset Lineage

You can map MLflow run datasets to specific DataHub platforms using the `source_mapping_to_platform` configuration option. This allows you to specify which DataHub platform should be associated with datasets from different MLflow engines.

Example:

```yaml
source_mapping_to_platform:
  huggingface: snowflake # Maps Hugging Face datasets to Snowflake platform
  http: s3 # Maps HTTP data sources to s3 platform
```

**Default behavior:** Links to existing datasets by platform and name; does not create new datasets.

To create datasets automatically, enable `materialize_dataset_inputs`:

```yaml
materlize_dataset_inputs: true # Creates new datasets if they don't exist
```

You can configure these options independently:

```yaml
# Only map to existing datasets
materlize_dataset_inputs: false
source_mapping_to_platform:
    huggingface: snowflake  # Maps Hugging Face datasets to Snowflake platform
    pytorch: snowflake      # Maps PyTorch datasets to Snowflake platform

# Create new datasets and map platforms
materlize_dataset_inputs: true
source_mapping_to_platform:
    huggingface: snowflake
    pytorch: snowflake
```
