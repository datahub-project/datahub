# DataHub Metadata Ingestion

![Python version 3.6+](https://img.shields.io/badge/python-3.6%2B-blue)

This module hosts an extensible Python-based metadata ingestion system for DataHub.
This supports sending data to DataHub using Kafka or through the REST API.
It can be used through our CLI tool, with an orchestrator like Airflow, or as a library.

## Getting Started

### Prerequisites

Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. If you are trying this out locally, the easiest way to do that is through [quickstart Docker images](../docker).

### Install from PyPI

The folks over at [Acryl Data](https://www.acryl.io/) maintain a PyPI package for DataHub metadata ingestion.

```shell
# Requires Python 3.6+
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip uninstall datahub acryl-datahub || true  # sanity check - ok if it fails
python3 -m pip install --upgrade acryl-datahub
datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
```

If you run into an error, try checking the [_common setup issues_](./developing.md#Common-setup-issues).

#### Installing Plugins

We use a plugin architecture so that you can install only the dependencies you actually need.

| Plugin Name   | Install Command                                            | Provides                            |
| ------------- | ---------------------------------------------------------- | ----------------------------------- |
| file          | _included by default_                                      | File source and sink                |
| console       | _included by default_                                      | Console sink                        |
| athena        | `pip install 'acryl-datahub[athena]'`                      | AWS Athena source                   |
| bigquery      | `pip install 'acryl-datahub[bigquery]'`                    | BigQuery source                     |
| glue          | `pip install 'acryl-datahub[glue]'`                        | AWS Glue source                     |
| hive          | `pip install 'acryl-datahub[hive]'`                        | Hive source                         |
| mssql         | `pip install 'acryl-datahub[mssql]'`                       | SQL Server source                   |
| mysql         | `pip install 'acryl-datahub[mysql]'`                       | MySQL source                        |
| oracle        | `pip install 'acryl-datahub[oracle]'`                      | Oracle source                       |
| postgres      | `pip install 'acryl-datahub[postgres]'`                    | Postgres source                     |
| redshift      | `pip install 'acryl-datahub[redshift]'`                    | Redshift source                     |
| sqlalchemy    | `pip install 'acryl-datahub[sqlalchemy]'`                  | Generic SQLAlchemy source           |
| snowflake     | `pip install 'acryl-datahub[snowflake]'`                   | Snowflake source                    |
| superset      | `pip install 'acryl-datahub[superset]'`                    | Supserset source                    |
| mongodb       | `pip install 'acryl-datahub[mongodb]'`                     | MongoDB source                      |
| ldap          | `pip install 'acryl-datahub[ldap]'` ([extra requirements]) | LDAP source                         |
| looker        | `pip install 'acryl-datahub[looker]'`                      | Looker source                       |
| lookml        | `pip install 'acryl-datahub[lookml]'`                      | LookML source, requires Python 3.7+ |
| kafka         | `pip install 'acryl-datahub[kafka]'`                       | Kafka source                        |
| druid         | `pip install 'acryl-datahub[druid]'`                       | Druid Source                        |
| dbt           | _no additional dependencies_                               | DBT source                          |
| datahub-rest  | `pip install 'acryl-datahub[datahub-rest]'`                | DataHub sink over REST API          |
| datahub-kafka | `pip install 'acryl-datahub[datahub-kafka]'`               | DataHub sink over Kafka             |

These plugins can be mixed and matched as desired. For example:

```shell
pip install 'acryl-datahub[bigquery,datahub-rest]'
```

You can check the active plugins:

```shell
datahub check plugins
```

[extra requirements]: https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites

#### Basic Usage

```shell
pip install 'acryl-datahub[datahub-rest]'  # install the required plugin
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml
```

### Install using Docker

[![Docker Hub](https://img.shields.io/docker/pulls/linkedin/datahub-ingestion?style=plastic)](https://hub.docker.com/r/linkedin/datahub-ingestion)
[![datahub-ingestion docker](https://github.com/linkedin/datahub/actions/workflows/docker-ingestion.yml/badge.svg)](https://github.com/linkedin/datahub/actions/workflows/docker-ingestion.yml)

If you don't want to install locally, you can alternatively run metadata ingestion within a Docker container.
We have prebuilt images available on [Docker hub](https://hub.docker.com/r/linkedin/datahub-ingestion). All plugins will be installed and enabled automatically.

_Limitation: the datahub_docker.sh convenience script assumes that the recipe and any input/output files are accessible in the current working directory or its subdirectories. Files outside the current working directory will not be found, and you'll need to invoke the Docker image directly._

```shell
./scripts/datahub_docker.sh ingest -c ./examples/recipes/example_to_datahub_rest.yml
```

### Install from source

If you'd like to install from source, see the [developer guide](./developing.md).

## Recipes

A recipe is a configuration file that tells our ingestion scripts where to pull data from (source) and where to put it (sink).
Here's a simple example that pulls metadata from MSSQL and puts it into datahub.

```yaml
# A sample recipe that pulls metadata from MSSQL and puts it into DataHub
# using the Rest API.
source:
  type: mssql
  config:
    username: sa
    password: ${MSSQL_PASSWORD}
    database: DemoData

transformers:
  - type: "fully-qualified-class-name-of-transformer"
    config:
      some_property: "some.value"

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

We automatically expand environment variables in the config,
similar to variable substitution in GNU bash or in docker-compose files. For details, see
https://docs.docker.com/compose/compose-file/compose-file-v2/#variable-substitution.

Running a recipe is quite easy.

```shell
datahub ingest -c ./examples/recipes/mssql_to_datahub.yml
```

A number of recipes are included in the examples/recipes directory.

## Sources

### Kafka Metadata `kafka`

Extracts:

- List of topics - from the Kafka broker
- Schemas associated with each topic - from the schema registry

```yml
source:
  type: "kafka"
  config:
    connection:
      bootstrap: "broker:9092"
      consumer_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serde-consumer
      schema_registry_url: http://localhost:8081
      schema_registry_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.SchemaRegistryClient
```

For a full example with a number of security options, see this [example recipe](./examples/recipes/secured_kafka_to_console.yml).

### MySQL Metadata `mysql`

Extracts:

- List of databases and tables
- Column types and schema associated with each table

```yml
source:
  type: mysql
  config:
    username: root
    password: example
    database: dbname
    host_port: localhost:3306
    table_pattern:
      deny:
        # Note that the deny patterns take precedence over the allow patterns.
        - "performance_schema"
      allow:
        - "schema1.table2"
      # Although the 'table_pattern' enables you to skip everything from certain schemas,
      # having another option to allow/deny on schema level is an optimization for the case when there is a large number
      # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
      # them out afterwards via the table_pattern.
    schema_pattern:
      deny:
        - "garbage_schema"
      allow:
        - "schema1"
```

### Microsoft SQL Server Metadata `mssql`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: mssql
  config:
    username: user
    password: pass
    host_port: localhost:1433
    database: DemoDatabase
    table_pattern:
      deny:
        - "^.*\\.sys_.*" # deny all tables that start with sys_
      allow:
        - "schema1.table1"
        - "schema1.table2"
    options:
      # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
      # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
      # Many of these options are specific to the underlying database driver, so that library's
      # documentation will be a good reference for what is supported. To find which dialect is likely
      # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
      charset: "utf8"
```

### Hive `hive`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table
- Detailed table and storage information

```yml
source:
  type: hive
  config:
    # For more details on authentication, see the PyHive docs:
    # https://github.com/dropbox/PyHive#passing-session-configuration.
    # LDAP, Kerberos, etc. are supported using connect_args, which can be
    # added under the `options` config parameter.
    #scheme: 'hive+http' # set this if Thrift should use the HTTP transport
    #scheme: 'hive+https' # set this if Thrift should use the HTTP with SSL transport
    username: user # optional
    password: pass # optional
    host_port: localhost:10000
    database: DemoDatabase # optional, defaults to 'default'
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

<details>
  <summary>Example: using ingestion with Azure HDInsight</summary>

```yml
# Connecting to Microsoft Azure HDInsight using TLS.
source:
  type: hive
  config:
    scheme: "hive+https"
    host_port: <cluster_name>.azurehdinsight.net:443
    username: admin
    password: "<password>"
    options:
      connect_args:
        http_path: "/hive2"
        auth: BASIC
    # table_pattern/schema_pattern is same as above
```

</details>

### PostgreSQL `postgres`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table
- Also supports PostGIS extensions

```yml
source:
  type: postgres
  config:
    username: user
    password: pass
    host_port: localhost:5432
    database: DemoDatabase
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

### Redshift `redshift`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table
- Also supports PostGIS extensions

```yml
source:
  type: redshift
  config:
    username: user
    password: pass
    host_port: localhost:5432
    database: DemoDatabase
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

### Snowflake `snowflake`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: snowflake
  config:
    username: user
    password: pass
    host_port: account_name
    database: db_name
    warehouse: "COMPUTE_WH" # optional
    role: "sysadmin" # optional
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

### Superset `superset`

Extracts:

- List of charts and dashboards

```yml
source:
  type: superset
  config:
    username: user
    password: pass
    provider: db | ldap
    connect_uri: http://localhost:8088
```

See documentation for superset's `/security/login` at https://superset.apache.org/docs/rest-api for more details on superset's login api.

### Oracle `oracle`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: oracle
  config:
    # For more details on authentication, see the documentation:
    # https://docs.sqlalchemy.org/en/14/dialects/oracle.html#dialect-oracle-cx_oracle-connect and
    # https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html#connection-strings.
    username: user
    password: pass
    host_port: localhost:5432
    database: dbname
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

### Google BigQuery `bigquery`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: bigquery
  config:
    project_id: project # optional - can autodetect from environment
    options: # options is same as above
      # See https://github.com/mxmzdlv/pybigquery#authentication for details.
      credentials_path: "/path/to/keyfile.json" # optional
    # table_pattern/schema_pattern is same as above
```

### AWS Athena `athena`

Extracts:

- List of databases and tables
- Column types associated with each table

```yml
source:
  type: athena
  config:
    username: aws_access_key_id # Optional. If not specified, credentials are picked up according to boto3 rules.
    # See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    password: aws_secret_access_key # Optional.
    database: database # Optional, defaults to "default"
    aws_region: aws_region_name # i.e. "eu-west-1"
    s3_staging_dir: s3_location # "s3://<bucket-name>/prefix/"
    # The s3_staging_dir parameter is needed because Athena always writes query results to S3.
    # See https://docs.aws.amazon.com/athena/latest/ug/querying.html
    # However, the athena driver will transparently fetch these results as you would expect from any other sql client.
    work_group: athena_workgroup # "primary"
    # table_pattern/schema_pattern is same as above
```

### AWS Glue `glue`

Extracts:

- List of tables
- Column types associated with each table
- Table metadata, such as owner, description and parameters

```yml
source:
  type: glue
  config:
    aws_region: aws_region_name # i.e. "eu-west-1"
    env: environment used for the DatasetSnapshot URN, one of "DEV", "EI", "PROD" or "CORP". # Optional, defaults to "PROD".
    database_pattern: # Optional, to filter databases scanned, same as schema_pattern above.
    table_pattern: # Optional, to filter tables scanned, same as table_pattern above.
    aws_access_key_id # Optional. If not specified, credentials are picked up according to boto3 rules.
    # See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    aws_secret_access_key # Optional.
    aws_session_token # Optional.
    aws_role # Optional (Role chaining supported by using a sorted list).
```

### Druid `druid`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

**Note** It is important to define a explicitly define deny schema pattern for internal druid databases (lookup & sys)
if adding a schema pattern otherwise the crawler may crash before processing relevant databases.
This deny pattern is defined by default but is overriden by user-submitted configurations

```yml
source:
  type: druid
  config:
    # Point to broker address
    host_port: localhost:8082
    schema_pattern:
      deny:
        - "^(lookup|sys).*"
    # options is same as above
```

### Other databases using SQLAlchemy `sqlalchemy`

The `sqlalchemy` source is useful if we don't have a pre-built source for your chosen
database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/)
defined elsewhere. In order to use this, you must `pip install` the required dialect packages yourself.

Extracts:

- List of schemas and tables
- Column types associated with each table

```yml
source:
  type: sqlalchemy
  config:
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    connect_uri: "dialect+driver://username:password@host:port/database"
    options: {} # same as above
    schema_pattern: {} # same as above
    table_pattern: {} # same as above
```

### MongoDB `mongodb`

Extracts:

- List of databases
- List of collections in each database and infers schemas for each collection

By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.

Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

```yml
source:
  type: "mongodb"
  config:
    # For advanced configurations, see the MongoDB docs.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    connect_uri: "mongodb://localhost"
    username: admin
    password: password
    authMechanism: "DEFAULT"
    options: {}
    database_pattern: {}
    collection_pattern: {}
    enableSchemaInference: True
    schemaSamplingSize: 1000
    # database_pattern/collection_pattern are similar to schema_pattern/table_pattern from above
```

### LDAP `ldap`

Extracts:

- List of people
- Names, emails, titles, and manager information for each person

```yml
source:
  type: "ldap"
  config:
    ldap_server: ldap://localhost
    ldap_user: "cn=admin,dc=example,dc=org"
    ldap_password: "admin"
    base_dn: "dc=example,dc=org"
    filter: "(objectClass=*)" # optional field
```

### LookML `lookml`

Note! This plugin uses a package that requires Python 3.7+!

Extracts:

- LookML views from model files
- Name, upstream table names, dimensions, measures, and dimension groups

```yml
source:
  type: "lookml"
  config:
    base_folder: /path/to/model/files # Where the *.model.lkml and *.view.lkml files are stored.
    connection_to_platform_map: # mapping between connection names in the model files to platform names.
      my_snowflake_conn: snowflake
    platform_name: looker_views # Optional, default is "looker_views"
    actor: "urn:li:corpuser:etl" # Optional, "urn:li:corpuser:etl"
    model_pattern: {}
    view_pattern: {}
    env: "PROD" # Optional, default is "PROD"
    parse_table_names_from_sql: False # See note below.
```

Note! The integration can use [`sql-metadata`](https://pypi.org/project/sql-metadata/) to try to parse the tables the
views depends on. As these SQL's can be complicated, and the package doesn't official support all the SQL dialects that
Looker support, the result might not be correct. This parsing is disables by default, but can be enabled by setting
`parse_table_names_from_sql: True`.

### Looker dashboards `looker`

Extracts:

- Looker dashboards and dashboard elements (charts)
- Names, descriptions, URLs, chart types, input view for the charts

```yml
source:
  type: "looker"
  config:
    client_id: str # Your Looker API client ID. As your Looker admin
    client_secret: str # Your Looker API client secret. As your Looker admin
    base_url: str # The url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar.
    platform_name: str = "looker" # Optional, default is "looker"
    view_platform_name: str = "looker_views" # Optional, default is "looker_views". Should be the same `platform_name` in the `lookml` source, if that source is also run.
    actor: str = "urn:li:corpuser:etl" # Optional, "urn:li:corpuser:etl"
    dashboard_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    env: str = "PROD" # Optional, default is "PROD"
```

### File `file`

Pulls metadata from a previously generated file. Note that the file sink
can produce such files, and a number of samples are included in the
[examples/mce_files](examples/mce_files) directory.

```yml
source:
  type: file
  config:
    filename: ./path/to/mce/file.json
```

### DBT `dbt`

Pull metadata from DBT output files:

- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - DBT does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
- target_platform: 
  - The data platform you are enriching with DBT metadata.
  - [data platforms](https://github.com/linkedin/datahub/blob/master/gms/impl/src/main/resources/DataPlatformInfo.json)
- load_schema:
  - Load schemas from dbt catalog file, not necessary when the underlying data platform already has this data.

```yml
source:
  type: "dbt"
  config:
    manifest_path: "./path/dbt/manifest_file.json"
    catalog_path: "./path/dbt/catalog_file.json"
    target_platform: "postgres" # optional eg postgres, snowflake etc. 
    load_schema: True / False
```

## Sinks

### DataHub Rest `datahub-rest`

Pushes metadata to DataHub using the GMA rest API. The advantage of the rest-based interface
is that any errors can immediately be reported.

```yml
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

### DataHub Kafka `datahub-kafka`

Pushes metadata to DataHub by publishing messages to Kafka. The advantage of the Kafka-based
interface is that it's asynchronous and can handle higher throughput. This requires the
Datahub mce-consumer container to be running.

```yml
sink:
  type: "datahub-kafka"
  config:
    connection:
      bootstrap: "localhost:9092"
      producer_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#serializingproducer
```

### Console `console`

Simply prints each metadata event to stdout. Useful for experimentation and debugging purposes.

```yml
sink:
  type: "console"
```

### File `file`

Outputs metadata to a file. This can be used to decouple metadata sourcing from the
process of pushing it into DataHub, and is particularly useful for debugging purposes.
Note that the file source can read files generated by this sink.

```yml
sink:
  type: file
  config:
    filename: ./path/to/mce/file.json
```

## Transformations

Beyond basic ingestion, sometimes there might exist a need to modify the source data before passing it on to the sink.
Example use cases could be to add ownership information, add extra tags etc.

In such a scenario, it is possible to configure a recipe with a list of transformers.

```yml
transformers:
  - type: "fully-qualified-class-name-of-transformer"
    config:
      some_property: "some.value"
```

A transformer class needs to inherit from [`Transformer`](./src/datahub/ingestion/api/transform.py).

### `simple_add_dataset_ownership`

Adds a set of owners to every dataset.

```yml
transformers:
  - type: "simple_add_dataset_ownership"
    config:
      owner_urns:
        - "urn:li:corpuser:username1"
        - "urn:li:corpuser:username2"
        - "urn:li:corpGroup:groupname"
```

:::tip

If you'd like to add more complex logic for assigning ownership, you can use the more generic [`AddDatasetOwnership` transformer](./src/datahub/ingestion/transformer/add_dataset_ownership.py), which calls a user-provided function to determine the ownership of each dataset.

:::

## Using as a library

In some cases, you might want to construct the MetadataChangeEvents yourself but still use this framework to emit that metadata to DataHub. In this case, take a look at the emitter interfaces, which can easily be imported and called from your own code.

- [DataHub emitter via REST](./src/datahub/emitter/rest_emitter.py) (same requirements as `datahub-rest`). Basic usage [example](./examples/library/lineage_emitter_rest.py).
- [DataHub emitter via Kafka](./src/datahub/emitter/kafka_emitter.py) (same requirements as `datahub-kafka`). Basic usage [example](./examples/library/lineage_emitter_kafka.py).

## Lineage with Airflow

There's a couple ways to get lineage information from Airflow into DataHub.

:::note Running ingestion on a schedule

If you're simply looking to run ingestion on a schedule, take a look at these sample DAGs:

- [`generic_recipe_sample_dag.py`](./src/datahub_provider/example_dags/generic_recipe_sample_dag.py) - reads a DataHub ingestion recipe file and runs it
- [`mysql_sample_dag.py`](./src/datahub_provider/example_dags/mysql_sample_dag.py) - runs a MySQL metadata ingestion pipeline using an inlined configuration.

:::

### Using Datahub's Airflow lineage backend (recommended)

:::caution

The Airflow lineage backend is only supported in Airflow 1.10.15+ and 2.0.2+.

:::

1. First, you must configure an Airflow hook for Datahub. We support both a Datahub REST hook and a Kafka-based hook, but you only need one.

   ```shell
   # For REST-based:
   airflow connections add  --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host 'http://localhost:8080'
   # For Kafka-based (standard Kafka sink config can be passed via extras):
   airflow connections add  --conn-type 'datahub_kafka' 'datahub_kafka_default' --conn-host 'broker:9092' --conn-extra '{}'
   ```

2. Add the following lines to your `airflow.cfg` file. You might need to
   ```ini
   [lineage]
   backend = datahub_provider.lineage.datahub.DatahubLineageBackend
   datahub_kwargs = {
       "datahub_conn_id": "datahub_rest_default",
       "capture_ownership_info": true,
       "capture_tags_info": true,
       "graceful_exceptions": true }
   # The above indentation is important!
   ```
   Configuration options:
   - `datahub_conn_id` (required): Usually `datahub_rest_default` or `datahub_kafka_default`, depending on what you named the connection in step 1.
   - `capture_ownership_info` (defaults to true): If true, the owners field of the DAG will be capture as a DataHub corpuser.
   - `capture_tags_info` (defaults to true): If true, the tags field of the DAG will be captured as DataHub tags.
   - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall task to fail. Note that configuration issues will still throw exceptions.
3. Configure `inlets` and `outlets` for your Airflow operators. For reference, look at the sample DAG in [`lineage_backend_demo.py`](./src/datahub_provider/example_dags/lineage_backend_demo.py).
4. [optional] Learn more about [Airflow lineage](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html), including shorthand notation and some automation.

### Emitting lineage via a separate operator

Take a look at this sample DAG:

- [`lineage_emission_dag.py`](./src/datahub_provider/example_dags/lineage_emission_dag.py) - emits lineage using the DatahubEmitterOperator.

In order to use this example, you must first configure the Datahub hook. Like in ingestion, we support a Datahub REST hook and a Kafka-based hook. See step 1 above for details.

## Developing

See the [developing guide](./developing.md).