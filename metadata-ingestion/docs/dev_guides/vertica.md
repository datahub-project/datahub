# Vertica

<!-- Set Support Status -->
<!-- ![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen) -->
<!-- ![Incubating](https://img.shields.io/badge/support%20status-incubating-blue) -->
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

This plugin extracts metadata for Tables and Views on Vertica.

This plugin is in beta and has only been tested on sample data on the Vertica database.

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrevant DataHub Concepts -->

| Source Concept | DataHub Concept                                                    | Notes |
| -------------- | ------------------------------------------------------------------ | ----- |
| `Vertica`      | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |       |
| Table          | [Dataset](docs/generated/metamodel/entities/dataset.md)            |       |
| View           | [Dataset](docs/generated/metamodel/entities/dataset.md)            |       |

### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability              | Status | Notes                    |
| ----------------------- | :----: | ------------------------ |
| Data Container          |   ✅    | Enabled by default       |
| Detect Deleted Entities |   ❌    | Not applicable to source |
| Data Domain             |   ❌    | Requires transformer     |
| Dataset Profiling       |   ❌    | Not applicable to source |
| Dataset Usage           |   ❌    | Not applicable to source |
| Extract Descriptions    |   ❌    | Not applicable to source |
| Extract Lineage         |   ❌    | Not applicable to source |
| Extract Ownership       |   ❌    | Not applicable to source |
| Extract Tags            |   ❌    | Requires transformer     |
| Partition Support       |   ❌    | Not applicable to source |
| Platform Instance       |   ❌    | Not applicable to source |
| View Definition         |   ✅    | Enabled by default       |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from Vertica, you will need:

- Python Version 3.6+
- Vertica Server Version 10.1.1-0 and avobe. It may also work for older versions.
- Vertica Credentials (Username/Password)

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[vertica]'`

### Configure the Ingestion Recipe(s)

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

_For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes)._

```yml
source:
  type: vertica
  config:
    # Coordinates
    host_port: localhost:5433
    database: DATABASE_NAME

    # Credentials
    username: "${VERTICA_USER}"
    password: "${VERTICA_PASSWORD}"

sink:
  # sink configs
```


### Config details

Like all SQL-based sources, the Vertica integration supports:
- Stale Metadata Deletion: See [here](./stateful_ingestion.md) for more details on configuration.
- SQL Profiling: See [here](./sql_profiles.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                       | Required | Default                                                                    | Description                                                                                                                                                                             |
| --------------------------- | -------- | -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`                  |          |                                                                            | Vertica username.                                                                                                                                                                       |
| `password`                  |          |                                                                            | Vertica password.                                                                                                                                                                       |
| `host_port`                 | ✅        |                                                                            | Vertica host URL.                                                                                                                                                                       |
| `database`                  |          |                                                                            | Vertica database to connect.                                                                                                                                                            |
| `sqlalchemy_uri`            |          |                                                                            | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.                                |
| `env`                       |          | `"PROD"`                                                                   | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `platform_instance`         |          | None                                                                       | The Platform instance to use while constructing URNs.                                                                                                                                   |
| `options.<option>`          |          |                                                                            | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`       |          |                                                                            | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`        |          |                                                                            | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`  |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`      |          |                                                                            | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`       |          |                                                                            | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase` |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`        |          |                                                                            | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`         |          |                                                                            | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`   |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`            |          | `True`                                                                     | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`             |          | `True`                                                                     | Whether views should be ingested.                                                                                                                                                       |
| `include_table_lineage`     |          | `True`                                                                     | Whether table lineage should be ingested.                                                                                                                                               |
| `profiling`                 |          | See the defaults for [profiling config](./sql_profiles.md#Config-details). | See [profiling config](./sql_profiles.md#Config-details).                                                                                                                               |


## Troubleshooting

### [Common Issue]

[Provide description of common issues with this integration and steps to resolve]
