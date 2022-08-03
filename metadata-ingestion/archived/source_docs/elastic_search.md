# Elastic Search

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[elasticsearch]'`.

## Capabilities

This plugin extracts the following:

- Metadata for indexes
- Column types associated with each index field

| Capability | Status | Details | 
| -----------| ------ | ---- |
| Platform Instance | ✔️ | [link](../../docs/platform-instances.md) |


## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "elasticsearch"
  config:
    # Coordinates
    host: 'localhost:9200'

    # Credentials
    username: user # optional
    password: pass # optional

    # Options
    url_prefix: "" # optional url_prefix
    env: "PROD"
    index_pattern:
      allow: [".*some_index_name_pattern*"]
      deny: [".*skip_index_name_pattern*"]
    ingest_index_templates: False
    index_template_pattern:
      allow: [".*some_index_template_name_pattern*"]

sink:
# sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.


| Field                          | Required | Default            | Description                                                                                                                                                                                                                                 |
|--------------------------------| -------- |--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `host`                         | ✅       | `"localhost:9092"` | The elastic search host URI.                                                                                                                                                                                                                |
| `username`                     |          | None               | The username credential.                                                                                                                                                                                                                    |
| `password`                     |          | None               | The password credential.                                                                                                                                                                                                                    |
| `url_prefix`                   |          | ""                 | There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters. |
| `env`                          |          | `"PROD"`           | Environment to use in namespace when constructing URNs.                                                                                                                                                                                     |
| `platform_instance`            |          | None               | The Platform instance to use while constructing URNs.                                                                                                                                                                                       |
| `index_pattern.allow`          |          |                    | List of regex patterns for indexes to include in ingestion.                                                                                                                                                                                 |
| `index_pattern.deny`           |          |                    | List of regex patterns for indexes to exclude from ingestion.                                                                                                                                                                               |
| `index_pattern.ignoreCase`     |          | `True`             | Whether regex matching should ignore case or not                                                                                                                                                                                            |
| `ingest_index_templates`       |          | `False`            | Whether index templates should be ingested                                                                                                                                                                                                  |
| `index_template_pattern.allow` |          |                    | List of regex patterns for index templates to include in ingestion.                                                                                                                                                                         |
| `index_template_pattern.deny`  |          |                    | List of regex patterns for index templates to exclude from ingestion.                                                                                                                                                                       |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
