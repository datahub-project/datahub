# Metadata Ingestion

This module hosts an extensible Python-based metadata ingestion system for DataHub.
This supports sending data to DataHub using Kafka or through the REST api.
It can be used through our CLI tool or as a library e.g. with an orchestrator like Airflow.

### Architecture

![metadata ingestion framework layout](../docs/imgs/datahub-metadata-ingestion-framework.png)

The architecture of this metadata ingestion framework is heavily inspired by [Apache Gobblin](https://gobblin.apache.org/) (also originally a LinkedIn project!). We have a standardized format - the MetadataChangeEvent - and sources and sinks which respectively produce and consume these objects. The sources pull metadata from a variety of data systems, while the sinks are primarily for moving this metadata into DataHub.

### Pre-Requisites

Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. If you are trying this out locally, the easiest way to do that is through [quickstart Docker images](../docker).

<!-- You can run this ingestion framework by building from source or by running docker images. -->

### Migrating from the old scripts
If you were previously using the `mce_cli.py` tool to push metadata into DataHub: the new way for doing this is by creating a recipe with a file source pointing at your JSON file and a DataHub sink to push that metadata into DataHub.
This [example recipe](./examples/recipes/example_to_datahub_rest.yml) demonstrates how to ingest the [sample data](./examples/mce_files/bootstrap_mce.json) (previously called `bootstrap_mce.dat`) into DataHub over the REST API.
Note that we no longer use the `.dat` format, but instead use JSON. The main differences are that the JSON uses `null` instead of `None` and uses objects/dictionaries instead of tuples when representing unions.

If you were previously using one of the `sql-etl` scripts: the new way for doing this is by using the associated source. See [below](#Sources) for configuration details. Note that the source needs to be paired with a sink - likely `datahub-kafka` or `datahub-rest`, depending on your needs.

### Building from source:

#### Pre-Requisites
1. Python 3.6+ must be installed in your host environment.
2. You also need to build the `mxe-schemas` module as below.
   ```
   (cd .. && ./gradlew :metadata-events:mxe-schemas:build)
   ```
   This is needed to generate `MetadataChangeEvent.avsc` which is the schema for the `MetadataChangeEvent_v4` Kafka topic.
3. On MacOS: `brew install librdkafka`
4. On Debian/Ubuntu: `sudo apt install librdkafka-dev python3-dev python3-venv`

#### Set up your Python environment
```sh
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel
pip install -e .
./scripts/codegen.sh
```

Common issues:

<details>
  <summary>Wheel issues e.g. "Failed building wheel for avro-python3" or "error: invalid command 'bdist_wheel'"</summary>

  This means Python's `wheel` is not installed. Try running the following commands and then retry.
  ```sh
  pip install --upgrade pip wheel
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
  pip install --upgrade pip wheel
  pip cache purge
  pip install avro-python3
  ```
</details>

### Usage
```sh
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml
```

<!--
## Running Ingestion using Docker:

### Build the image
```sh
source docker/docker_build.sh
```

### Usage - with Docker
We have a simple script provided that supports mounting a local directory for input recipes and an output directory for output data:
```sh
source docker/docker_run.sh examples/recipes/file_to_file.yml
```
-->

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
    password: test!Password
    database: DemoData

sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
```

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
      consumer_config: {}  # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#deserializingconsumer
```

### MySQL Metadata `mysql`
Extracts:
- List of databases and tables
- Column types and schema associated with each table

Extra requirements: `pip install pymysql`

```yml
source:
  type: mysql
  config:
    username: root
    password: example
    database: dbname
    host_port: localhost:3306
    table_pattern:
      allow:
      - "schema1.table2"
      deny:
      - "performance_schema"
```

### Microsoft SQL Server Metadata `mssql`
Extracts:
- List of databases, schema, and tables
- Column types associated with each table

Extra requirements: `pip install sqlalchemy-pytds`

```yml
source:
  type: mssql
  config:
    username: user
    password: pass
    database: DemoDatabase
    table_pattern:
      allow:
      - "schema1.table1"
      - "schema1.table2"
      deny:
      - "^.*\\.sys_.*" # deny all tables that start with sys_
    options:
      # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
      # See https://docs.sqlalchemy.org/en/14/core/engines.html for details.
      charset: 'utf8'
```

### Hive `hive`
Extracts:
- List of databases, schema, and tables
- Column types associated with each table

Extra requirements: `pip install pyhive[hive]`

```yml
source:
  type: hive
  config:
    username: user
    password: pass
    host_port: localhost:10000
    database: DemoDatabase
    # table_pattern is same as above
    # options is same as above
```

### PostgreSQL `postgres`
Extracts:
- List of databases, schema, and tables
- Column types associated with each table

Extra requirements: `pip install psycopg2-binary` or `pip install psycopg2`

If you're using PostGIS extensions for Postgres, also use `pip install GeoAlchemy2`.

```yml
source:
  type: postgres
  config:
    username: user
    password: pass
    host_port: localhost:5432
    database: DemoDatabase
    # table_pattern is same as above
    # options is same as above
```

### Snowflake `snowflake`
Extracts:
- List of databases, schema, and tables
- Column types associated with each table

Extra requirements: `pip install snowflake-sqlalchemy`

```yml
source:
  type: snowflake
  config:
    username: user
    password: pass
    host_port: account_name
    # table_pattern is same as above
    # options is same as above
```

### Google BigQuery `bigquery`
Extracts:
- List of databases, schema, and tables
- Column types associated with each table

Extra requirements: `pip install pybigquery`

```yml
source:
  type: bigquery
  config:
    project_id: project  # optional - can autodetect from environment
    dataset: dataset_name
    options: # options is same as above
      # See https://github.com/mxmzdlv/pybigquery#authentication for details.
      credentials_path: "/path/to/keyfile.json"  # optional
    # table_pattern is same as above
```

### LDAP `ldap`
Extracts:
- List of people
- Names, emails, titles, and manager information for each person

Extra requirements: `pip install python-ldap>=2.4`. See that [library's docs](https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#pre-built-binaries) for extra build requirements.

```yml
source:
  type: "ldap"
  config:
    ldap_server: ldap://localhost
    ldap_user: "cn=admin,dc=example,dc=org"
    ldap_password: "admin"
    base_dn: "dc=example,dc=org"
    filter: "(objectClass=*)"  # optional field
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

## Sinks

### DataHub Rest `datahub-rest`
Pushes metadata to DataHub using the GMA rest API. The advantage of the rest-based interface
is that any errors can immediately be reported.

```yml
sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'
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
      producer_config: {}  # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#serializingproducer
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

## Contributing

Contributions welcome!

### Code layout

- The CLI interface is defined in [entrypoints.py](./src/datahub/entrypoints.py).
- The high level interfaces are defined in the [API directory](./src/datahub/ingestion/api).
- The actual [sources](./src/datahub/ingestion/source) and [sinks](./src/datahub/ingestion/sink) implementations have their own directories - the `__init__.py` files in those directories are used to register the short codes for use in recipes.
- The metadata models are created using code generation, and eventually live in the `./src/datahub/metadata` directory. However, these files are not checked in and instead are generated at build time. See the [codegen](./scripts/codegen.sh) script for details.

### Testing
```sh
# Follow standard install procedure - see above.

# Install requirements.
pip install -r test_requirements.txt

# Run unit tests.
pytest tests/unit

# Run integration tests.
# Note: the integration tests require docker.
pytest tests/integration
```

### Sanity check code before committing
```sh
# Requires test_requirements.txt to have been installed.
black --exclude 'datahub/metadata' -S -t py36 src tests
isort src tests
flake8 src tests
mypy -p datahub
pytest
```
