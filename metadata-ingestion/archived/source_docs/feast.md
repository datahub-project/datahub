# Feast

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

This source is designed for Feast 10+ repositories.

As of version 0.10+, Feast has changed the architecture from a stack of services to SDK/CLI centric application. Please refer to [Feast 0.9 vs Feast 0.10+](https://docs.feast.dev/project/feast-0.9-vs-feast-0.10+) for further details.

For compatibility with pre-0.10 Feast, see [Feast Legacy](feast_legacy.md) source.

:::note

This source is only compatible with Feast 0.18.0
:::

## Setup

To install this plugin, run `pip install 'acryl-datahub[feast]'`.

## Capabilities

This plugin extracts:

- Entities as [`MLPrimaryKey`](https://datahubproject.io/docs/graphql/objects#mlprimarykey)
- Features as [`MLFeature`](https://datahubproject.io/docs/graphql/objects#mlfeature)
- Feature views and on-demand feature views as [`MLFeatureTable`](https://datahubproject.io/docs/graphql/objects#mlfeaturetable)
- Batch and stream source details as [`Dataset`](https://datahubproject.io/docs/graphql/objects#dataset)
- Column types associated with each entity and feature

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yaml
source:
  type: "feast"
  config:
    # Coordinates
    path: "/path/to/repository/"
    # Options
    environment: "PROD"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field         | Required | Default | Description                                |
| ------------- | -------- | ------- | ------------------------------------------ |
| `path`        | ✅       |         | Path to Feast repository.                  |
| `environment` |          | `PROD`  | Environment to use when constructing URNs. |

## Compatibility

This source is compatible with [Feast (==0.18.0)](https://github.com/feast-dev/feast/releases/tag/v0.18.0).

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
