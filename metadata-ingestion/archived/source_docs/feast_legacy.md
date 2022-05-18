# Feast (Legacy)

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

This source is designed for Feast 0.9 core services.

As of version 0.10+, Feast has changed the architecture from a stack of services to SDK/CLI centric application. Please refer to [Feast 0.9 vs Feast 0.10+](https://docs.feast.dev/project/feast-0.9-vs-feast-0.10+) for further details.

See [Feast](feast.md) source for something compatible with the latest Feast versions.

## Setup

**Note: Feast ingestion requires Docker to be installed.**

To install this plugin, run `pip install 'acryl-datahub[feast-legacy]'`.

## Capabilities

This plugin extracts the following:

- Entities as [`MLPrimaryKey`](https://datahubproject.io/docs/graphql/objects#mlprimarykey)
- Features as [`MLFeature`](https://datahubproject.io/docs/graphql/objects#mlfeature)
- Feature tables as [`MLFeatureTable`](https://datahubproject.io/docs/graphql/objects#mlfeaturetable)
- Batch and stream source details as [`Dataset`](https://datahubproject.io/docs/graphql/objects#dataset)
- Column types associated with each entity and feature

Note: this uses a separate Docker container to extract Feast's metadata into a JSON file, which is then
parsed to DataHub's native objects. This separation was performed because of a dependency conflict in the `feast` module.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: feast-legacy
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

This source is compatible with [Feast (0.10.5)](https://github.com/feast-dev/feast/releases/tag/v0.10.5) and earlier versions.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
