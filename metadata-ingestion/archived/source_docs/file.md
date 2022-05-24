# File

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

This plugin pulls metadata from a previously generated file. The [file sink](../sink_docs/file.md)
can produce such files, and a number of samples are included in the
[examples/mce_files](../examples/mce_files) directory.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: file
  config:
    # Coordinates
    filename: ./path/to/mce/file.json

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field      | Required | Default | Description             |
| ---------- | -------- | ------- | ----------------------- |
| `filename` | ✅       |         | Path to file to ingest. |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
