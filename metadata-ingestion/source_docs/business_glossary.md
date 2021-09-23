# Business Glossary

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

This plugin pulls business glossary metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](../examples/bootstrap_data/business_glossary.yml).

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: datahub-business-glossary
  config:
    # Coordinates
    file: /path/to/business_glossary_yaml

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field      | Required | Default | Description             |
| ---------- | -------- | ------- | ----------------------- |
| `file` | ✅       |         | Path to business glossary file to ingest. |

### Business Glossary File Format

The business glossary file format should be pretty easy to understand using the sample business glossary checked in [here](../examples/bootstrap_data/business_glossary.yml)

## Compatibility

Compatible with version 1 of business glossary format. 
The source will be evolved as we publish newer versions of this format.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
