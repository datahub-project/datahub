# Feast

**Note: Feast ingestion requires Docker to be installed.**

To install this plugin, run `pip install 'acryl-datahub[feast]'`.

This plugin extracts the following:

- List of feature tables (modeled as [`MLFeatureTable`](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/ml/metadata/MLFeatureTableProperties.pdl)s),
  features ([`MLFeature`](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/ml/metadata/MLFeatureProperties.pdl)s),
  and entities ([`MLPrimaryKey`](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/ml/metadata/MLPrimaryKeyProperties.pdl)s)
- Column types associated with each feature and entity

Note: this uses a separate Docker container to extract Feast's metadata into a JSON file, which is then
parsed to DataHub's native objects. This was done because of a dependency conflict in the `feast` module.

```yml
source:
  type: feast
  config:
    core_url: localhost:6565 # default
    env: "PROD" # Optional, default is "PROD"
    use_local_build: False # Whether to build Feast ingestion image locally, default is False
```
