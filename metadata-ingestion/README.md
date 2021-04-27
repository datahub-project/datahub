# DataHub Metadata Ingestion

![Python version 3.6+](https://img.shields.io/badge/python-3.6%2B-blue)

This module hosts an extensible Python-based metadata ingestion system for DataHub.
This supports sending data to DataHub using Kafka or through the REST API.
It can be used through our CLI tool, with an orchestrator like Airflow, or as a library.

## Getting Started

### Prerequisites

Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. If you are trying this out locally, the easiest way to do that is through [quickstart Docker images](../docker).

### Install from PyPI

The folks over at [Acryl](https://www.acryl.io/) maintain a PyPI package for DataHub metadata ingestion.

```sh
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

| Plugin Name   | Install Command                                            | Provides                   |
| ------------- | ---------------------------------------------------------- | -------------------------- |
| file          | _included by default_                                      | File source and sink       |
| console       | _included by default_                                      | Console sink               |
| athena        | `pip install 'acryl-datahub[athena]'`                      | AWS Athena source          |
| bigquery      | `pip install 'acryl-datahub[bigquery]'`                    | BigQuery source            |
| glue          | `pip install 'acryl-datahub[glue]'`                        | AWS Glue source            |
| hive          | `pip install 'acryl-datahub[hive]'`                        | Hive source                |
| mssql         | `pip install 'acryl-datahub[mssql]'`                       | SQL Server source          |
| mysql         | `pip install 'acryl-datahub[mysql]'`                       | MySQL source               |
| oracle        | `pip install 'acryl-datahub[oracle]'`                      | Oracle source              |
| postgres      | `pip install 'acryl-datahub[postgres]'`                    | Postgres source            |
| sqlalchemy    | `pip install 'acryl-datahub[sqlalchemy]'`                  | Generic SQLAlchemy source  |
| snowflake     | `pip install 'acryl-datahub[snowflake]'`                   | Snowflake source           |
| superset      | `pip install 'acryl-datahub[superset]'`                    | Supserset source           |
| mongodb       | `pip install 'acryl-datahub[mongodb]'`                     | MongoDB source             |
| ldap          | `pip install 'acryl-datahub[ldap]'` ([extra requirements]) | LDAP source                |
| kafka         | `pip install 'acryl-datahub[kafka]'`                       | Kafka source               |
| druid         | `pip install 'acryl-datahub[druid]'`                       | Druid Source               |
| dbt           | _no additional dependencies_                               | DBT source                 |
| datahub-rest  | `pip install 'acryl-datahub[datahub-rest]'`                | DataHub sink over REST API |
| datahub-kafka | `pip install 'acryl-datahub[datahub-kafka]'`               | DataHub sink over Kafka    |

These plugins can be mixed and matched as desired. For example:

```sh
pip install 'acryl-datahub[bigquery,datahub-rest]'
```

You can check the active plugins:

```sh
datahub check plugins
```

[extra requirements]: https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites

#### Basic Usage

```sh
pip install 'acryl-datahub[datahub-rest]'  # install the required plugin
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml
```

### Install using Docker

[![Docker Hub](https://img.shields.io/docker/pulls/linkedin/datahub-ingestion?style=plastic)](https://hub.docker.com/r/linkedin/datahub-ingestion)
[![datahub-ingestion docker](https://github.com/linkedin/datahub/actions/workflows/docker-ingestion.yml/badge.svg)](https://github.com/linkedin/datahub/actions/workflows/docker-ingestion.yml)

If you don't want to install locally, you can alternatively run metadata ingestion within a Docker container.
We have prebuilt images available on [Docker hub](https://hub.docker.com/r/linkedin/datahub-ingestion). All plugins will be installed and enabled automatically.

_Limitation: the datahub_docker.sh convenience script assumes that the recipe and any input/output files are accessible in the current working directory or its subdirectories. Files outside the current working directory will not be found, and you'll need to invoke the Docker image directly._

```sh
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

```sh
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
      schema_registry_url: http://localhost:8081
      consumer_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#deserializingconsumer
```

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

```yml
source:
  type: hive
  config:
    username: user
    password: pass
    host_port: localhost:10000
    database: DemoDatabase
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

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

See documentation for superset's `/security/login` at  https://superset.apache.org/docs/rest-api for more details on superset's login api.

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
- List of collections in each database

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

### File `file`

Pulls metadata from a previously generated file. Note that the file sink
can produce such files, and a number of samples are included in the
[examples/mce_files](examples/mce_files) directory.

```yml
source:
  type: file
  filename: ./path/to/mce/file.json
```

### DBT `dbt`

Pull metadata from DBT output files:

- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - DBT does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models

```yml
source:
  type: "dbt"
  config:
    manifest_path: "./path/dbt/manifest_file.json"
    catalog_path: "./path/dbt/catalog_file.json"
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

A transformer class needs to inherit from [`Transformer`](./src/datahub/ingestion/api/transform.py)
At the moment there are no built-in transformers.

## Using as a library

In some cases, you might want to construct the MetadataChangeEvents yourself but still use this framework to emit that metadata to DataHub. In this case, take a look at the emitter interfaces, which can easily be imported and called from your own code.

- [DataHub emitter via REST](./src/datahub/emitter/rest_emitter.py) (same requirements as `datahub-rest`). Basic usage [example](./examples/library/lineage_emitter_rest.py).
- [DataHub emitter via Kafka](./src/datahub/emitter/kafka_emitter.py) (same requirements as `datahub-kafka`). Basic usage [example](./examples/library/lineage_emitter_kafka.py).

## Usage with Airflow

There's a couple ways to integrate DataHub with Airflow.

### Running ingestion on a schedule

Take a look at these sample DAGs:

- [`generic_recipe_sample_dag.py`](./examples/airflow/generic_recipe_sample_dag.py) - a simple Airflow DAG that picks up a DataHub ingestion recipe configuration and runs it.
- [`mysql_sample_dag.py`](./examples/airflow/mysql_sample_dag.py) - an Airflow DAG that runs a MySQL metadata ingestion pipeline using an inlined configuration.

### Emitting lineage via a separate operator

Take a look at this sample DAG:

- [`lineage_emission_dag.py`](./examples/airflow/lineage_emission_dag.py) - emits lineage using the DatahubEmitterOperator.

In order to use this example, you must first configure the Datahub hook. Like in ingestion, we support a Datahub REST hook and a Kafka-based hook.

```sh
# For REST-based:
airflow connections add  --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host 'http://localhost:8080'
# For Kafka-based (standard Kafka sink config can be passed via extras):
airflow connections add  --conn-type 'datahub_kafka' 'datahub_kafka_default' --conn-host 'broker:9092' --conn-extra '{}'
```

### Using Datahub's Airflow lineage backend

_Note: The Airflow lineage backend is only supported in Airflow 1.10.15+ and 2.0.2+._

1. First, you must configure the Airflow hooks. See above for details.
2. Add the following lines to your `airflow.cfg` file. You might need to
   ```ini
   [lineage]
   backend = datahub.integrations.airflow.DatahubAirflowLineageBackend
   datahub_conn_id = datahub_rest_default  # or datahub_kafka_default - whatever you named the connection in step 1
   ```
3. Configure `inlets` and `outlets` for your Airflow operators. For reference, look at the sample DAG in [`lineage_backend_demo.py`](./examples/airflow/lineage_backend_demo.py).
4. [optional] Learn more about [Airflow lineage](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html), including shorthand notation and some automation.

## Developing

See the [developing guide](./developing.md).
