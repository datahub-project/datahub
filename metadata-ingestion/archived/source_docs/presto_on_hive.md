# Presto on Hive

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[presto-on-hive]'`.

## Capabilities

This plugin extracts the following:

- Metadata for Presto views and Hive tables (external / managed)
- Column types associated with each table / view
- Detailed table / view property info


## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: presto-on-hive
  config:
    # Hive metastore DB connection 
    host_port: localhost:5432
    database: metastore
   
    # specify the schema where metastore tables reside
    schema_pattern:
      allow:
        - "^public"

    # credentials
    username: user # optional
    password: pass # optional

    #scheme: 'postgresql+psycopg2' # set this if metastore db is using postgres
    #scheme: 'mysql+pymysql' # set this if metastore db is using mysql, default if unset

    # set this to have advanced filters on what to ingest
    #views_where_clause_suffix: AND d."name" in ('db1') 
    #tables_where_clause_suffix: AND d."name" in ('db1')
    
sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.


| Field                        | Required | Default  | Description                                                                                                                                                                             |
| ---------------------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`                   |          |          | Database username.                                                                                                                                                                      |
| `password`                   |          |          | Database password.                                                                                                                                                                      |
| `host_port`                  | ✅       |          | Host URL and port to connect to.                                                                                                                                                        |
| `database`                   |          |          | Database to ingest.                                                                                                                                                                     |
| `env`                        |          | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`           |          |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `schema_pattern.allow`       |          |          | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.ignoreCase`  |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_views`              |          | `True`   | Whether Presto views should be ingested.                                                                                                                                                |
| `include_tables`             |          | `True`   | Whether Hive tables should be ingested.                                                                                                                                                 |
| `views_where_clause_suffix`  |          | `""`     | Where clause to specify what Presto views should be ingested.                                                                                                                           |
| `tables_where_clause_suffix` |          | `""`     | Where clause to specify what Hive tables should be ingested.                                                                                                                            |
| `schemas_where_clause_suffix`|          | `""`     | Where clause to specify what Hive schemas should be ingested.                                                                                                                           |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
