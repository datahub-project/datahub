# Feast

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

**Note: Feast ingestion requires Docker to be installed.**

To install this plugin, run `pip install 'acryl-datahub[feast]'`.

## Capabilities

This plugin extracts the following:

- List of feature tables (modeled as [`MLFeatureTable`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/ml/metadata/MLFeatureTableProperties.pdl)s),
  features ([`MLFeature`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/ml/metadata/MLFeatureProperties.pdl)s),
  and entities ([`MLPrimaryKey`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/ml/metadata/MLPrimaryKeyProperties.pdl)s)
- Column types associated with each feature and entity

Note: this uses a separate Docker container to extract Feast's metadata into a JSON file, which is then
parsed to DataHub's native objects. This separation was performed because of a dependency conflict in the `feast` module.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: feast
  config:
    # Coordinates
    core_url: "localhost:6565"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field             | Required | Default            | Description                                             |
| ----------------- | -------- | ------------------ | ------------------------------------------------------- |
| `core_url`        |          | `"localhost:6565"` | URL of Feast Core instance.                             |
| `env`             |          | `"PROD"`           | Environment to use in namespace when constructing URNs. |
| `use_local_build` |          | `False`            | Whether to build Feast ingestion Docker image locally.  |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
