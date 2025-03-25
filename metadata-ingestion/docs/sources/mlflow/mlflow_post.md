You can map MLflow run datasets to specific DataHub platforms using the `source_mapping_to_platform` configuration option. This allows you to specify which DataHub platform should be associated with datasets from different MLflow engines.

Example:
```yaml
source_mapping_to_platform:
    huggingface: snowflake  # Maps Hugging Face datasets to Snowflake platform
```

By default, DataHub will attempt to connect lineage with existing datasets based on the platform and name, but will not create new datasets if they don't exist.

To enable automatic dataset creation and lineage mapping, use the `materlize_dataset_inputs` option:

```yaml
materlize_dataset_inputs: true  # Creates new datasets if they don't exist
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