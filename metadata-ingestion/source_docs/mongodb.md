# MongoDB

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[mongodb]'`.

## Capabilities

This plugin extracts the following:

- Databases and associated metadata
- Collections in each database and schemas for each collection (via schema inference)

By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "mongodb"
  config:
    # Coordinates
    connect_uri: "mongodb://localhost"

    # Credentials
    username: admin
    password: password
    authMechanism: "DEFAULT"

    # Options
    enableSchemaInference: True
    useRandomSampling: True
    maxSchemaSize: 300

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default                 | Description                                                                                                              |
| -------------------------- | -------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `connect_uri`              |          | `"mongodb://localhost"` | MongoDB connection URI.                                                                                                  |
| `username`                 |          |                         | MongoDB username.                                                                                                        |
| `password`                 |          |                         | MongoDB password.                                                                                                        |
| `authMechanism`            |          |                         | MongoDB authentication mechanism. See https://pymongo.readthedocs.io/en/stable/examples/authentication.html for details. |
| `options`                  |          |                         | Additional options to pass to `pymongo.MongoClient()`.                                                                   |
| `enableSchemaInference`    |          | `True`                  | Whether to infer schemas.                                                                                                |
| `schemaSamplingSize`       |          | `1000`                  | Number of documents to use when inferring schema size. If set to `0`, all documents will be scanned.                     |
| `maxSchemaSize`       |          | `300`                  | Maximum number of fields to include in the schema.                     |
| `useRandomSampling`        |          | `True`                  | If documents for schema inference should be randomly selected. If `False`, documents will be selected from start.        |
| `env`                      |          | `"PROD"`                | Environment to use in namespace when constructing URNs.                                                                  |
| `database_pattern.allow`   |          |                         | List of regex patterns for databases to include in ingestion.                                                                     |
| `database_pattern.deny`    |          |                         | List of regex patterns for databases to exclude from ingestion.                                                                   |
| `database_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `collection_pattern.allow` |          |                         | List of regex patterns for collections to include in ingestion.                                                                   |
| `collection_pattern.deny`  |          |                         | List of regex patterns for collections to exclude from ingestion.                                                                 |
| `collection_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
