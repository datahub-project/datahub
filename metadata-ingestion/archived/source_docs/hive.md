# Hive

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[hive]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Detailed table and storage information
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)

| Capability        | Status | Details                                  | 
|-------------------|--------|------------------------------------------|
| Platform Instance | ✔️     | [link](../../docs/platform-instances.md) |
| Data Containers   | ✔️     |                                          |
| Data Domains      | ✔️     | [link](../../docs/domains.md)            |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: hive
  config:
    # Coordinates
    host_port: localhost:10000
    database: DemoDatabase # optional, if not specified, ingests from all databases

    # Credentials
    username: user # optional
    password: pass # optional

    # For more details on authentication, see the PyHive docs:
    # https://github.com/dropbox/PyHive#passing-session-configuration.
    # LDAP, Kerberos, etc. are supported using connect_args, which can be
    # added under the `options` config parameter.
    #options:
    #  connect_args:
    #    auth: KERBEROS
    #    kerberos_service_name: hive
    #scheme: 'hive+http' # set this if Thrift should use the HTTP transport
    #scheme: 'hive+https' # set this if Thrift should use the HTTP with SSL transport
    #scheme: 'sparksql' # set this for Spark Thrift Server

sink:
  # sink configs
```

Ingestion with

<details>
  <summary>Azure HDInsight</summary>

```yml
# Connecting to Microsoft Azure HDInsight using TLS.
source:
  type: hive
  config:
    # Coordinates
    host_port: <cluster_name>.azurehdinsight.net:443

    # Credentials
    username: admin
    password: password

    # Options
    options:
      connect_args:
        http_path: "/hive2"
        auth: BASIC

sink:
  # sink configs
```

</details>

<details>
  <summary>Databricks </summary>

Ensure that databricks-dbapi is installed. If not, use ```pip install databricks-dbapi``` to install.

Use the ```http_path``` from your Databricks cluster in the following recipe. See [here](https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html#get-server-hostname-port-http-path-and-jdbc-url) for instructions to find ```http_path```.

```yml
source:
  type: hive
  config:
    host_port: <databricks workspace URL>:443
    username: token
    password: <api token>
    scheme: 'databricks+pyhive'

    options:
      connect_args:
        http_path: 'sql/protocolv1/o/xxxyyyzzzaaasa/1234-567890-hello123'

sink:
  # sink configs
```
</details>

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

As a SQL-based service, the Athena integration is also supported by our SQL profiler. See [here](./sql_profiles.md) for more details on configuration.

| Field                          | Required | Default  | Description                                                                                                                                                                             |
|--------------------------------|----------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                     |          |          | Database username.                                                                                                                                                                      |
| `password`                     |          |          | Database password.                                                                                                                                                                      |
| `host_port`                    | ✅        |          | Host URL and port to connect to.                                                                                                                                                        |
| `database`                     |          |          | Database to ingest.                                                                                                                                                                     |
| `database_alias`               |          |          | Alias to apply to database when ingesting. Use `platform_instance` instead of this for supporting multiple Hive instances.                                                              |
| `env`                          |          | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `platform_instance`            |          | None     | The Platform instance to use while constructing URNs.                                                                                                                                   |
| `options.<option>`             |          |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`          |          |          | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`           |          |          | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`     |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`         |          |          | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`          |          |          | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase`    |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`           |          |          | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`            |          |          | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`      |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`               |          | `True`   | Whether tables should be ingested.                                                                                                                                                      |
| `domain.domain_key.allow`      |          |          | List of regex patterns for tables/schemas to set domain_key domain key (domain_key can be any string like `sales`. There can be multiple domain key specified.                          |
| `domain.domain_key.deny`       |          |          | List of regex patterns for tables/schemas to not assign domain_key. There can be multiple domain key specified.                                                                         |
| `domain.domain_key.ignoreCase` |          | `True`   | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                  |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
