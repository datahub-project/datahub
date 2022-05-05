# LookML

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[lookml]'`.

Note! This plugin uses a package that requires Python 3.7+!

## Capabilities

This plugin extracts the following:

- LookML views from model files in a project
- Name, upstream table names, metadata for dimensions, measures, and dimension groups attached as tags
- If API integration is enabled (recommended), resolves table and view names by calling the Looker API, otherwise supports offline resolution of these names.

**_NOTE_:** To get complete Looker metadata integration (including Looker dashboards and charts and lineage to the underlying Looker views, you must ALSO use the Looker source. Documentation for that is [here](./looker.md)

| Capability | Status | Details |
| -----------| ------ | ---- |
| Platform Instance | Partial (Lineage only) | Platform instances are supported for lineage edges between Looker and external data platforms like BigQuery, Snowflake etc. Not supported for Looker entities themselves. [link](../../docs/platform-instances.md) |


### Configuration Notes

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.
You need to ensure that the API key is attached to a user that has Admin privileges. If that is not possible, read the configuration section to provide an offline specification of the `connection_to_platform_map` and the `project_name`.


## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "lookml"
  config:
    # Coordinates
    base_folder: /path/to/model/files

    # Options
    api:
      # Coordinates for your looker instance
      base_url: https://YOUR_INSTANCE.cloud.looker.com

      # Credentials for your Looker connection (https://docs.looker.com/reference/api-and-integration/api-auth)
      client_id: client_id_from_looker
      client_secret: client_secret_from_looker

    # Alternative to API section above if you want a purely file-based ingestion with no api calls to Looker or if you want to provide platform_instance ids for your connections
    # project_name: PROJECT_NAME # See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what is your project name
    # connection_to_platform_map:
    #   connection_name_1:
    #     platform: snowflake # bigquery, hive, etc
    #     default_db: DEFAULT_DATABASE. # the default database configured for this connection
    #     default_schema: DEFAULT_SCHEMA # the default schema configured for this connection
    #     platform_instance: snow_warehouse # optional
    #     platform_env: PROD  # optional
    #   connection_name_2:
    #     platform: bigquery # snowflake, hive, etc
    #     default_db: DEFAULT_DATABASE. # the default database configured for this connection
    #     default_schema: DEFAULT_SCHEMA # the default schema configured for this connection
    #     platform_instance: bq_warehouse # optional
    #     platform_env: DEV  # optional

    github_info:
       repo: org/repo-name


sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                          | Required | Default    | Description                                                             |
| ---------------------------------------------- | -------- | ---------- | ----------------------------------------------------------------------- |
| `base_folder`                                  | ✅       |            | Where the `*.model.lkml` and `*.view.lkml` files are stored.            |
| `api.base_url`                                 | ❓ if using api |            | Url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar. |
| `api.client_id`                                | ❓ if using api |            | Looker API3 client ID.                                 |
| `api.client_secret`                            | ❓ if using api |            | Looker API3 client secret. |
| `project_name` | ❓ if NOT using api         |           | The project name within with all the model files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what the Looker project name should be. The simplest way to see your projects is to click on `Develop` followed by `Manage LookML Projects` in the Looker application. |
| `connection_to_platform_map.<connection_name>` |          |            | Mappings between connection names in the model files to platform, database and schema values |
| `connection_to_platform_map.<connection_name>.platform` | ❓ if NOT using api         |           | Mappings between connection name in the model files to platform name (e.g. snowflake, bigquery, etc) |
| `connection_to_platform_map.<connection_name>.platform_instance` | ❓ if NOT using api or needing platform instances        |           | Mappings between connection name in the model files to platform instance. [link](../../docs/platform-instances.md) |
| `connection_to_platform_map.<connection_name>.platform_env` | ❓ if NOT using api or needing platform instances        |           | Mappings between connection name in the model files to env (fabric) that the platform belongs to. [link](../../docs/platform-instances.md) |
| `connection_to_platform_map.<connection_name>.default_db` | ❓ if NOT using api         |           | Mappings between connection name in the model files to default database configured for this platform on Looker |
| `connection_to_platform_map.<connection_name>.default_schema` | ❓ if NOT using api         |           | Mappings between connection name in the model files to default schema configured for this platform on Looker |
| `platform_name`                                |          | `"looker"` | Platform to use in namespace when constructing URNs.                    |
| `model_pattern.allow`                          |          |            | List of regex patterns for models to include in ingestion.                       |
| `model_pattern.deny`                           |          |            | List of regex patterns for models to exclude from ingestion.                     |
| `model_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `view_pattern.allow`                           |          |            | List of regex patterns for views to include in ingestion.                        |
| `view_pattern.deny`                            |          |            | List of regex patterns for views to exclude from ingestion.                      |
| `view_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `view_naming_pattern` |   | `{project}.view.{name}` | Pattern for providing dataset names to views. Allowed variables are `{project}`, `{model}`, `{name}` |
| `view_browse_pattern` |   | `/{env}/{platform}/{project}/views/{name}` | Pattern for providing browse paths to views. Allowed variables are `{project}`, `{model}`, `{name}`, `{platform}` and `{env}` |
| `env`                                          |          | `"PROD"`   | Environment to use in namespace when constructing URNs.                 |
| `parse_table_names_from_sql`                   |          | `False`    | See note below.                                                         |
| `tag_measures_and_dimensions`   |          | `True`    | When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column. |
| `github_info`                   |          | Empty.    | When provided, will annotate views with github urls. See config variables below. |
| `github_info.repo`              |  ✅   if providing `github_info`        |    |  Your github repository in `org/repo` form. e.g. `datahub-project/datahub` |
| `github_info.branch`            |          | `main` | The default branch in your repo that you want urls to point to. Typically `main` or `master` |
| `github_info.base_url`          |          | `https://github.com` | The base url for your github coordinates |
| `sql_parser`                                   |          | `datahub.utilities.sql_parser.DefaultSQLParser`    | See note below.                                                         |
| `transport_options`                                |          |        |  Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client |

Note! The integration can use an SQL parser to try to parse the tables the views depends on. This parsing is disabled by default,
but can be enabled by setting `parse_table_names_from_sql: True`.  The default parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
As this package doesn't officially support all the SQL dialects that Looker supports, the result might not be correct. You can, however, implement a
custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
