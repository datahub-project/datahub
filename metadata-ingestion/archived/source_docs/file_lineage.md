# File Based Lineage

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

This plugin pulls lineage metadata from a yaml-formatted file. An example of one such file is located in the examples
directory [here](../examples/bootstrap_data/file_lineage.yml).

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration
options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: datahub-lineage-file
  config:
    # Coordinates
    file: /path/to/file_lineage.yml
    # Whether we want to query datahub-gms for upstream data
    preserve_upstream: False

sink:
# sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field               | Required | Default | Description                                                                                                                                                                                                                    |
|---------------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `file`              | ✅        |         | Path to lineage file to ingest.                                                                                                                                                                                                    |
| `preserve_upstream` |          | `True`  | Whether we want to query datahub-gms for upstream data. `False` means it will hard replace upstream data for a given entity. `True` means it will query the backend for existing upstreams and include it in the ingestion run |

### Lineage File Format

The lineage source file should be a `.yml` file with the following top-level keys:

**version**: the version of lineage file config the config conforms to. Currently, the only version released
is `1`.

**lineage**: the top level key of the lineage file containing a list of **EntityNodeConfig** objects

**EntityNodeConfig**:

- **entity**: **EntityConfig** object
- **upstream**: (optional) list of child **EntityNodeConfig** objects

**EntityConfig**:

- **name** : name of the entity
- **type**: type of the entity (only `dataset` is supported as of now)
- **env**: the environment of this entity. Should match the values in the
  table [here](https://datahubproject.io/docs/graphql/enums/#fabrictype)
- **platform**: a valid platform like kafka, snowflake, etc..
- **platform_instance**: optional string specifying the platform instance of this entity

You can also view an example lineage file checked in [here](../examples/bootstrap_data/file_lineage.yml)

## Compatibility

Compatible with version 1 of lineage format. The source will be evolved as we publish newer versions of this
format.

## Questions

If you've got any questions on configuring this source, feel free to ping us
on [our Slack](https://slack.datahubproject.io/)!