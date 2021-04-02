# Metadata Ingestion

![Python version 3.6+](https://img.shields.io/badge/python-3.6%2B-blue)

This module hosts an extensible Python-based metadata ingestion system for DataHub.
This supports sending data to DataHub using Kafka or through the REST api.
It can be used through our CLI tool or as a library e.g. with an orchestrator like Airflow.

### Architecture

![metadata ingestion framework layout](../docs/imgs/datahub-metadata-ingestion-framework.png)

The architecture of this metadata ingestion framework is heavily inspired by [Apache Gobblin](https://gobblin.apache.org/) (also originally a LinkedIn project!). We have a standardized format - the MetadataChangeEvent - and sources and sinks which respectively produce and consume these objects. The sources pull metadata from a variety of data systems, while the sinks are primarily for moving this metadata into DataHub.

## Getting Started

### Prerequisites

Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. If you are trying this out locally, the easiest way to do that is through [quickstart Docker images](../docker).

<!-- You can run this ingestion framework by building from source or by running docker images. -->

### Install from Source

#### Requirements

1. Python 3.6+ must be installed in your host environment.
2. You also need to build the `mxe-schemas` module as below.
   ```
   (cd .. && ./gradlew :metadata-events:mxe-schemas:build)
   ```
   This is needed to generate `MetadataChangeEvent.avsc` which is the schema for the `MetadataChangeEvent_v4` Kafka topic.
3. On MacOS: `brew install librdkafka`
4. On Debian/Ubuntu: `sudo apt install librdkafka-dev python3-dev python3-venv`
5. On Fedora (if using LDAP source integration): `sudo yum install openldap-devel`

#### Set up your Python environment

```sh
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip==20.2.4 wheel setuptools
pip uninstall datahub || true ; rm -r src/*.egg-info || true
pip install -e .
./scripts/codegen.sh
```

Common issues (click to expand):

<details>
  <summary>Wheel issues e.g. "Failed building wheel for avro-python3" or "error: invalid command 'bdist_wheel'"</summary>

This means Python's `wheel` is not installed. Try running the following commands and then retry.

```sh
pip install --upgrade pip wheel setuptools
pip cache purge
```

</details>

<details>
  <summary>Failure to install confluent_kafka: "error: command 'x86_64-linux-gnu-gcc' failed with exit status 1"</summary>

This sometimes happens if there's a version mismatch between the Kafka's C library and the Python wrapper library. Try running `pip install confluent_kafka==1.5.0` and then retrying.

</details>

<details>
  <summary>Failure to install avro-python3: "distutils.errors.DistutilsOptionError: Version loaded from file: avro/VERSION.txt does not comply with PEP 440"</summary>

The underlying `avro-python3` package is buggy. In particular, it often only installs correctly when installed from a pre-built "wheel" but not when from source. Try running the following commands and then retry.

```sh
pip uninstall avro-python3  # sanity check, ok if this fails
pip install --upgrade pip wheel setuptools
pip cache purge
pip install avro-python3
```

</details>

#### Installing Plugins

We use a plugin architecture so that you can install only the dependencies you actually need.

| Plugin Name   | Install Command                                   | Provides                   |
| ------------- | ------------------------------------------------- | -------------------------- |
| file          | _included by default_                             | File source and sink       |
| console       | _included by default_                             | Console sink               |
| athena        | `pip install -e '.[athena]'`                      | AWS Athena source          |
| bigquery      | `pip install -e '.[bigquery]'`                    | BigQuery source            |
| hive          | `pip install -e '.[hive]'`                        | Hive source                |
| mssql         | `pip install -e '.[mssql]'`                       | SQL Server source          |
| mysql         | `pip install -e '.[mysql]'`                       | MySQL source               |
| postgres      | `pip install -e '.[postgres]'`                    | Postgres source            |
| snowflake     | `pip install -e '.[snowflake]'`                   | Snowflake source           |
| mongodb       | `pip install -e '.[mongodb]'`                     | MongoDB source             |
| ldap          | `pip install -e '.[ldap]'` ([extra requirements]) | LDAP source                |
| kakfa         | `pip install -e '.[kafka]'`                       | Kafka source               |
| druid         | `pip install -e '.[druid]'`                       | Druid Source               |
| dbt           | no additional dependencies                        | DBT source                 |
| datahub-rest  | `pip install -e '.[datahub-rest]'`                | DataHub sink over REST API |
| datahub-kafka | `pip install -e '.[datahub-kafka]'`               | DataHub sink over Kafka    |

These plugins can be mixed and matched as desired. For example:

```sh
pip install -e '.[bigquery,datahub-rest]
```

You can check the active plugins:

```sh
datahub ingest-list-plugins
```

[extra requirements]: https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites

#### Basic Usage

```sh
pip install -e '.[datahub-rest]'  # install the required plugin
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

### Usage within Airflow

We have also included a couple [sample DAGs](./examples/airflow) that can be used with [Airflow](https://airflow.apache.org/).

- `generic_recipe_sample_dag.py` - a simple Airflow DAG that picks up a DataHub ingestion recipe configuration and runs it.
- `mysql_sample_dag.py` - an Airflow DAG that runs a MySQL metadata ingestion pipeline using an inlined configuration.

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
      # See https://docs.sqlalchemy.org/en/14/core/engines.html for details.
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
    dataset: dataset_name
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
  filename: ./path/to/mce/file.json
```

## Using as a library

In some cases, you might want to construct the MetadataChangeEvents yourself but still use this framework to emit that metadata to DataHub. In this case, take a look at the emitter interfaces, which can easily be imported and called from your own code.

- [DataHub emitter via REST](./src/datahub/emitter/rest_emitter.py) (same requirements as `datahub-rest`)
- [DataHub emitter via Kafka](./src/datahub/emitter/kafka_emitter.py) (same requirements as `datahub-kafka`)

## Migrating from the old scripts

If you were previously using the `mce_cli.py` tool to push metadata into DataHub: the new way for doing this is by creating a recipe with a file source pointing at your JSON file and a DataHub sink to push that metadata into DataHub.
This [example recipe](./examples/recipes/example_to_datahub_rest.yml) demonstrates how to ingest the [sample data](./examples/mce_files/bootstrap_mce.json) (previously called `bootstrap_mce.dat`) into DataHub over the REST API.
Note that we no longer use the `.dat` format, but instead use JSON. The main differences are that the JSON uses `null` instead of `None` and uses objects/dictionaries instead of tuples when representing unions.

If you were previously using one of the `sql-etl` scripts: the new way for doing this is by using the associated source. See [above](#Sources) for configuration details. Note that the source needs to be paired with a sink - likely `datahub-kafka` or `datahub-rest`, depending on your needs.

## Contributing

Contributions welcome!

### Code layout

- The CLI interface is defined in [entrypoints.py](./src/datahub/entrypoints.py).
- The high level interfaces are defined in the [API directory](./src/datahub/ingestion/api).
- The actual [sources](./src/datahub/ingestion/source) and [sinks](./src/datahub/ingestion/sink) have their own directories. The registry files in those directories import the implementations.
- The metadata models are created using code generation, and eventually live in the `./src/datahub/metadata` directory. However, these files are not checked in and instead are generated at build time. See the [codegen](./scripts/codegen.sh) script for details.

### Testing

```sh
# Follow standard install procedure - see above.

# Install, including all dev requirements.
pip install -e '.[dev]'

# Run unit tests.
pytest tests/unit

# Run integration tests. Note that the integration tests require docker.
pytest tests/integration
```

### Sanity check code before committing

```sh
# Assumes: pip install -e '.[dev]'
black .
isort .
flake8 .
mypy .
pytest
```
