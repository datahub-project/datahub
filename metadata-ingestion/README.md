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
python3 -m pip install --upgrade acryl-datahub
datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
```

If you run into an error, try checking the [_common setup issues_](./developing.md#Common-setup-issues).

#### Installing Plugins

We use a plugin architecture so that you can install only the dependencies you actually need.

| Plugin Name     | Install Command                                            | Provides                            |
| --------------- | ---------------------------------------------------------- | ----------------------------------- |
| file            | _included by default_                                      | File source and sink                |
| console         | _included by default_                                      | Console sink                        |
| athena          | `pip install 'acryl-datahub[athena]'`                      | AWS Athena source                   |
| bigquery        | `pip install 'acryl-datahub[bigquery]'`                    | BigQuery source                     |
| bigquery-usage  | `pip install 'acryl-datahub[bigquery-usage]'`              | BigQuery usage statistics source    |
| feast           | `pip install 'acryl-datahub[feast]'`                       | Feast source                        |
| glue            | `pip install 'acryl-datahub[glue]'`                        | AWS Glue source                     |
| hive            | `pip install 'acryl-datahub[hive]'`                        | Hive source                         |
| mssql           | `pip install 'acryl-datahub[mssql]'`                       | SQL Server source                   |
| mysql           | `pip install 'acryl-datahub[mysql]'`                       | MySQL source                        |
| oracle          | `pip install 'acryl-datahub[oracle]'`                      | Oracle source                       |
| postgres        | `pip install 'acryl-datahub[postgres]'`                    | Postgres source                     |
| redshift        | `pip install 'acryl-datahub[redshift]'`                    | Redshift source                     |
| sagemaker       | `pip install 'acryl-datahub[sagemaker]'`                   | AWS SageMaker source                |
| sqlalchemy      | `pip install 'acryl-datahub[sqlalchemy]'`                  | Generic SQLAlchemy source           |
| snowflake       | `pip install 'acryl-datahub[snowflake]'`                   | Snowflake source                    |
| snowflake-usage | `pip install 'acryl-datahub[snowflake-usage]'`             | Snowflake usage statistics source   |
| superset        | `pip install 'acryl-datahub[superset]'`                    | Superset source                     |
| mongodb         | `pip install 'acryl-datahub[mongodb]'`                     | MongoDB source                      |
| ldap            | `pip install 'acryl-datahub[ldap]'` ([extra requirements]) | LDAP source                         |
| looker          | `pip install 'acryl-datahub[looker]'`                      | Looker source                       |
| lookml          | `pip install 'acryl-datahub[lookml]'`                      | LookML source, requires Python 3.7+ |
| kafka           | `pip install 'acryl-datahub[kafka]'`                       | Kafka source                        |
| druid           | `pip install 'acryl-datahub[druid]'`                       | Druid Source                        |
| dbt             | _no additional dependencies_                               | dbt source                          |
| datahub-rest    | `pip install 'acryl-datahub[datahub-rest]'`                | DataHub sink over REST API          |
| datahub-kafka   | `pip install 'acryl-datahub[datahub-kafka]'`               | DataHub sink over Kafka             |

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
# Assumes the DataHub repo is cloned locally.
./metadata-ingestion/scripts/datahub_docker.sh ingest -c ./examples/recipes/example_to_datahub_rest.yml
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
      producer_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.SerializingProducer
      schema_registry_url: "http://localhost:8081"
      schema_registry_config: {} # passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.SchemaRegistryClient
```

The options in the producer config and schema registry config are passed to the Kafka SerializingProducer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](./examples/recipes/secured_kafka.yml).

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

If you'd like to add more complex logic for assigning ownership, you can use the more generic [`add_dataset_ownership` transformer](./src/datahub/ingestion/transformer/add_dataset_ownership.py), which calls a user-provided function to determine the ownership of each dataset.

:::

### `simple_add_dataset_tags`

Adds a set of tags to every dataset.

```yml
transformers:
  - type: "simple_add_dataset_tags"
    config:
      tag_urns:
        - "urn:li:tag:NeedsDocumentation"
        - "urn:li:tag:Legacy"
```

:::tip

If you'd like to add more complex logic for assigning tags, you can use the more generic [`add_dataset_tags` transformer](./src/datahub/ingestion/transformer/add_dataset_tags.py), which calls a user-provided function to determine the tags for each dataset.

:::

## Using as a library

In some cases, you might want to construct the MetadataChangeEvents yourself but still use this framework to emit that metadata to DataHub. In this case, take a look at the emitter interfaces, which can easily be imported and called from your own code.

- [DataHub emitter via REST](./src/datahub/emitter/rest_emitter.py) (same requirements as `datahub-rest`). Basic usage [example](./examples/library/lineage_emitter_rest.py).
- [DataHub emitter via Kafka](./src/datahub/emitter/kafka_emitter.py) (same requirements as `datahub-kafka`). Basic usage [example](./examples/library/lineage_emitter_kafka.py).

## Lineage with Airflow

There's a couple ways to get lineage information from Airflow into DataHub.

:::note

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

2. Add the following lines to your `airflow.cfg` file.
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
   **Configuration options:**
   - `datahub_conn_id` (required): Usually `datahub_rest_default` or `datahub_kafka_default`, depending on what you named the connection in step 1.
   - `capture_ownership_info` (defaults to true): If true, the owners field of the DAG will be capture as a DataHub corpuser.
   - `capture_tags_info` (defaults to true): If true, the tags field of the DAG will be captured as DataHub tags.
   - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall task to fail. Note that configuration issues will still throw exceptions.
3. Configure `inlets` and `outlets` for your Airflow operators. For reference, look at the sample DAG in [`lineage_backend_demo.py`](./src/datahub_provider/example_dags/lineage_backend_demo.py), or reference [`lineage_backend_taskflow_demo.py`](./src/datahub_provider/example_dags/lineage_backend_taskflow_demo.py) if you're using the [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html).
4. [optional] Learn more about [Airflow lineage](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html), including shorthand notation and some automation.

### Emitting lineage via a separate operator

Take a look at this sample DAG:

- [`lineage_emission_dag.py`](./src/datahub_provider/example_dags/lineage_emission_dag.py) - emits lineage using the DatahubEmitterOperator.

In order to use this example, you must first configure the Datahub hook. Like in ingestion, we support a Datahub REST hook and a Kafka-based hook. See step 1 above for details.

## Developing

See the [developing guide](./developing.md) or the [adding a source guide](./adding-source.md).
