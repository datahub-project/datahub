# Console

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

Simply prints each metadata event to stdout. Useful for experimentation and debugging purposes.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  # source configs

sink:
  type: "console"
```

## Config details

None!
