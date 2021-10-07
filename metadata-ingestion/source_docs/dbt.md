# dbt

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

This plugin pulls metadata from dbt's artifact files:

- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
- [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
  - This file contains metadata for sources with freshness checks.
  - We transfer dbt's freshness checks to DataHub's last-modified fields.
  - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.
- target_platform:
  - The data platform you are enriching with dbt metadata.
  - [data platforms](https://github.com/linkedin/datahub/blob/master/metadata-service/restli-impl/src/main/resources/DataPlatformInfo.json)
- load_schemas:
  - Load schemas from dbt catalog file, not necessary when the underlying data platform already has this data.
- use_identifiers:
  - Use model [identifier](https://docs.getdbt.com/reference/resource-properties/identifier) instead of model name if defined (if not, default to model name).
- tag_prefix:
  - Prefix added to tags during ingestion.
- node_type_pattern:
  - Use this filter to exclude and include node types using allow or deny method

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "dbt"
  config:
    # Coordinates
    manifest_path: "./path/dbt/manifest_file.json"
    catalog_path: "./path/dbt/catalog_file.json"
    sources_path: "./path/dbt/sources_file.json"

    # Options
    target_platform: "my_target_platform_id"
    load_schemas: True # note: if this is disabled

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                     | Required | Default  | Description                                                                                                                                           |
| ------------------------- | -------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `manifest_path`           | ✅       |          | Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json                                                              |
| `catalog_path`            | ✅       |          | Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json                                                                |
| `sources_path`            |          |          | Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. If not specified, last-modified fields will not be populated. |
| `env`                     |          | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                               |
| `target_platform`         | ✅       |          | The platform that dbt is loading onto.                                                                                                                |
| `load_schemas`            | ✅       |          | Whether to load database schemas. If set to `False`, table schema details (e.g. columns) will not be ingested.                                        |
| `use_identifiers`         |         | `False`   | Whether to use model identifiers instead of names, if defined (if not, default to names)                                                             |
| `tag_prefix`              |         | `dbt:`    | Prefix added to tags during ingestion. |
| `node_type_pattern.allow` |          |          | List of regex patterns for dbt nodes to include in ingestion.                                                                                                  |
| `node_type_pattern.deny`  |          |          | List of regex patterns for dbt nodes to exclude from ingestion.                                                                                                |
| `node_type_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
