# dbt

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

This plugin pulls metadata from dbt's artifact files and generates:
- dbt Tables: for nodes in the dbt manifest file that are models materialized as tables
- dbt Views: for nodes in the dbt manifest file that are models materialized as views
- dbt Ephemeral: for nodes in the dbt manifest file that are ephemeral models
- dbt Sources: for nodes that are sources on top of the underlying platform tables
- dbt Seed: for seed entities
- dbt Test: for dbt test entities

Note:
1. It also generates lineage between the `dbt` nodes (e.g. ephemeral nodes that depend on other dbt sources) as well as lineage between the `dbt` nodes and the underlying (target) platform nodes (e.g. BigQuery Table -> dbt Source, dbt View -> BigQuery View).
2. The previous version of this source (`acryl_datahub<=0.8.16.2`) did not generate `dbt` entities and lineage between `dbt` entities and platform entities. For backwards compatibility with the previous version of this source, there is a config flag `disable_dbt_node_creation` that falls back to the old behavior.
3. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.

The artifacts used by this source are:
- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
- [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
  - This file contains metadata for sources with freshness checks.
  - We transfer dbt's freshness checks to DataHub's last-modified fields.
  - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.

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
    target_platform: "my_target_platform_id" # e.g. bigquery/postgres/etc.
    load_schemas: True # note: if this is disabled

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                          | Required | Default  | Description                                                                                                                                                                                                                                                                                                                                                                                |
|--------------------------------| -------- |----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `manifest_path`                | ✅       |          | Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json  Note this can be a local file or a URI.                                                                                                                                                                                                                                                          |
| `catalog_path`                 | ✅       |          | Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json    Note this can be a local file or a URI.                                                                                                                                                                                                                                                          |
| `sources_path`                 |          |          | Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. If not specified, last-modified fields will not be populated. Note this can be a local file or a URI.                                                                                                                                                                                              |
| `env`                          |          | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                                                                                                                                                                                                                    |
| `target_platform`              | ✅       |          | The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)                                                                                                                                                                                                                                                                                                          |
| `use_identifiers`              |         | `False`  | Use model [identifier](https://docs.getdbt.com/reference/resource-properties/identifier) instead of model name if defined (if not, default to model name).                                                                                                                                                                                                                                 |
| `tag_prefix`                   |         | `dbt:`   | Prefix added to tags during ingestion.                                                                                                                                                                                                                                                                                                                                                     |
| `node_type_pattern.allow`      |          |          | List of regex patterns for dbt nodes to include in ingestion.                                                                                                                                                                                                                                                                                                                              |
| `node_type_pattern.deny`       |          |          | List of regex patterns for dbt nodes to exclude from ingestion.                                                                                                                                                                                                                                                                                                                            |
| `node_type_pattern.ignoreCase` |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                                                                                                                                |
| `node_name_pattern.allow`      |          |          | List of regex patterns for dbt model names to include in ingestion.                                                                                                                                                                                                                                                                                                                        |
| `node_name_pattern.deny`       |          |          | List of regex patterns for dbt model names to exclude from ingestion.                                                                                                                                                                                                                                                                                                                      |
| `node_name_pattern.ignoreCase` |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                                                                                                                                |
| `disable_dbt_node_creation`    |          | `False`  | Whether to suppress `dbt` dataset metadata creation. When set to `True`, this flag applies the dbt metadata to the `target_platform` entities (e.g. populating schema and column descriptions from dbt into the postgres / bigquery table metadata in DataHub) and generates lineage between the platform entities.                                                                        |
| `load_schemas`                 |      | `True`   | This flag is only consulted when `disable_dbt_node_creation` is set to `True`. Load schemas for `target_platform` entities from dbt catalog file, not necessary when you are already ingesting this metadata from the data platform directly. If set to `False`, table schema details (e.g. columns) will not be ingested.                                                                 |
| `meta_mapping`                 |       |          | mapping rules that will be executed against dbt meta properties. Refer to the section below on dbt meta automated mappings.                                                                                                                                                                                                                                                                |
| `enable_meta_mapping`          |       | `False`  | When enabled, applies the mappings that are defined through the `meta_mapping` directives.                                                                                                                                                                                                                                                                                                 |
| `write_semantics`              |       | `PATCH`  | Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE"                                                                                                                                                                                                                    |
| `strip_user_ids_from_email`    |     | `FALSE`  | Whether or not to strip email id while adding owners using dbt meta actions.                                                                                                                                                                                                                                                                                                               |
| `owner_extraction_pattern`     |     | `None`    | Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r"(?P<owner>(.*)): (\w+) (\w+)"` will extract `jdoe` as the owner from `"jdoe: John Doe"` (2) `r"@(?P<owner>(.*))"` will extract `alice` as the owner from `"@alice"`. |

### dbt meta automated mappings
dbt allows authors to define meta properties for datasets. Checkout this link to know more - [dbt meta](https://docs.getdbt.com/reference/resource-configs/meta). Our dbt source allows users to define
actions such as add a tag, term or owner. For example if a dbt model has a meta config ```"has_pii": True```, we can define an action
that evaluates if the property is set to true and add, lets say, a ```pii``` tag.
To leverage this feature we require users to define mappings as part of the recipe. Following is how mappings can be defined -
```json
            "meta_mapping": {
                    "business_owner": {
                        "match": ".*",
                        "operation": "add_owner",
                        "config": {"owner_type": "user"},
                    },
                    "has_pii": {
                        "match": True,
                        "operation": "add_tag",
                        "config": {"tag": "has_pii_test"},
                    },
                    "int_property": {
                        "match": 1,
                        "operation": "add_tag",
                        "config": {"tag": "int_meta_property"},
                    },
                    "double_property": {
                        "match": 2.5,
                        "operation": "add_term",
                        "config": {"term": "double_meta_property"},
                    },
                    "data_governance.team_owner": {
                        "match": "Finance",
                        "operation": "add_term",
                        "config": {"term": "Finance_test"},
                    },
                }
```
We support the below actions -
1. add_tag - Requires ```tag``` property in config.
2. add_term - Requires ```term``` property in config.
3. add_owner - Requires ```owner_type``` property in config which can be either user or group.

Note:
1. Currently, dbt meta mapping is only supported for meta configs defined at the top most level or a node in manifest file. If that is not preset we will look for meta in the config section of the node.
2. For string based meta properties we support regex matching.
## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
