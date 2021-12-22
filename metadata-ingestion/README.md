# Metadata Ingestion

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

We use a plugin architecture so that you can install only the dependencies you actually need. Click the plugin name to learn more about the specific source recipe and any FAQs!

Sources:

| Plugin Name                                                     | Install Command                                            | Provides                            |
|-----------------------------------------------------------------|------------------------------------------------------------| ----------------------------------- |
| [file](./source_docs/file.md)                                   | _included by default_                                      | File source and sink                |
| [athena](./source_docs/athena.md)                               | `pip install 'acryl-datahub[athena]'`                      | AWS Athena source                   |
| [bigquery](./source_docs/bigquery.md)                           | `pip install 'acryl-datahub[bigquery]'`                    | BigQuery source                     |
| [bigquery-usage](./source_docs/bigquery.md)                     | `pip install 'acryl-datahub[bigquery-usage]'`              | BigQuery usage statistics source    |
| [datahub-business-glossary](./source_docs/business_glossary.md) | _no additional dependencies_                               | Business Glossary File source                         |
| [dbt](./source_docs/dbt.md)                                     | _no additional dependencies_                               | dbt source                          |
| [druid](./source_docs/druid.md)                                 | `pip install 'acryl-datahub[druid]'`                       | Druid Source                        |
| [feast](./source_docs/feast.md)                                 | `pip install 'acryl-datahub[feast]'`                       | Feast source                        |
| [glue](./source_docs/glue.md)                                   | `pip install 'acryl-datahub[glue]'`                        | AWS Glue source                     |
| [hive](./source_docs/hive.md)                                   | `pip install 'acryl-datahub[hive]'`                        | Hive source                         |
| [kafka](./source_docs/kafka.md)                                 | `pip install 'acryl-datahub[kafka]'`                       | Kafka source                        |
| [kafka-connect](./source_docs/kafka-connect.md)                 | `pip install 'acryl-datahub[kafka-connect]'`               | Kafka connect source                |
| [ldap](./source_docs/ldap.md)                                   | `pip install 'acryl-datahub[ldap]'` ([extra requirements]) | LDAP source                         |
| [looker](./source_docs/looker.md)                               | `pip install 'acryl-datahub[looker]'`                      | Looker source                       |
| [lookml](./source_docs/lookml.md)                               | `pip install 'acryl-datahub[lookml]'`                      | LookML source, requires Python 3.7+ |
| [metabase](./source_docs/metabase.md)                           | `pip install 'acryl-datahub[metabase]`                     | Metabase source                     |
| [mode](./source_docs/mode.md)                                   | `pip install 'acryl-datahub[mode]'`                        | Mode Analytics source               |
| [mongodb](./source_docs/mongodb.md)                             | `pip install 'acryl-datahub[mongodb]'`                     | MongoDB source                      |
| [mssql](./source_docs/mssql.md)                                 | `pip install 'acryl-datahub[mssql]'`                       | SQL Server source                   |
| [mysql](./source_docs/mysql.md)                                 | `pip install 'acryl-datahub[mysql]'`                       | MySQL source                        |
| [mariadb](./source_docs/mariadb.md)                             | `pip install 'acryl-datahub[mariadb]'`                     | MariaDB source                      |
| [openapi](./source_docs/openapi.md)                             | `pip install 'acryl-datahub[openapi]'`                     | OpenApi Source                      |
| [oracle](./source_docs/oracle.md)                               | `pip install 'acryl-datahub[oracle]'`                      | Oracle source                       |
| [postgres](./source_docs/postgres.md)                           | `pip install 'acryl-datahub[postgres]'`                    | Postgres source                     |
| [redash](./source_docs/redash.md)                               | `pip install 'acryl-datahub[redash]'`                      | Redash source                       |
| [redshift](./source_docs/redshift.md)                           | `pip install 'acryl-datahub[redshift]'`                    | Redshift source                     |
| [sagemaker](./source_docs/sagemaker.md)                         | `pip install 'acryl-datahub[sagemaker]'`                   | AWS SageMaker source                |
| [snowflake](./source_docs/snowflake.md)                         | `pip install 'acryl-datahub[snowflake]'`                   | Snowflake source                    |
| [snowflake-usage](./source_docs/snowflake.md)                   | `pip install 'acryl-datahub[snowflake-usage]'`             | Snowflake usage statistics source   |
| [sql-profiles](./source_docs/sql_profiles.md)                   | `pip install 'acryl-datahub[sql-profiles]'`                | Data profiles for SQL-based systems |
| [sqlalchemy](./source_docs/sqlalchemy.md)                       | `pip install 'acryl-datahub[sqlalchemy]'`                  | Generic SQLAlchemy source           |
| [superset](./source_docs/superset.md)                           | `pip install 'acryl-datahub[superset]'`                    | Superset source                     |
| [trino](./source_docs/trino.md)                                 | `pip install 'acryl-datahub[trino]`                        | Trino source                     |
| [starburst-trino-usage](./source_docs/trino.md)                 | `pip install 'acryl-datahub[starburst-trino-usage]'`       | Starburst Trino usage statistics source   |
| [nifi](./source_docs/nifi.md)                                   | `pip install 'acryl-datahub[nifi]'                         | Nifi source                         |

Sinks

| Plugin Name                             | Install Command                              | Provides                   |
| --------------------------------------- | -------------------------------------------- | -------------------------- |
| [file](./sink_docs/file.md)             | _included by default_                        | File source and sink       |
| [console](./sink_docs/console.md)       | _included by default_                        | Console sink               |
| [datahub-rest](./sink_docs/datahub.md)  | `pip install 'acryl-datahub[datahub-rest]'`  | DataHub sink over REST API |
| [datahub-kafka](./sink_docs/datahub.md) | `pip install 'acryl-datahub[datahub-kafka]'` | DataHub sink over Kafka    |

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

The `--dry-run` option of the `ingest` command performs all of the ingestion steps, except writing to the sink. This is useful to ensure that the
ingestion recipe is producing the desired workunits before ingesting them into datahub.

```shell
# Dry run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml --dry-run
# Short-form
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml -n
```

The `--preview` option of the `ingest` command performs all of the ingestion steps, but limits the processing to only the first 10 workunits produced by the source.
This option helps with quick end-to-end smoke testing of the ingestion recipe.

```shell
# Preview
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml --preview
# Preview with dry-run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml -n --preview
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

Running a recipe is quite easy.

```shell
datahub ingest -c ./examples/recipes/mssql_to_datahub.yml
```

A number of recipes are included in the [examples/recipes](./examples/recipes) directory. For full info and context on each source and sink, see the pages described in the [table of plugins](#installing-plugins).

### Handling sensitive information in recipes

We automatically expand environment variables in the config (e.g. `${MSSQL_PASSWORD}`),
similar to variable substitution in GNU bash or in docker-compose files. For details, see
https://docs.docker.com/compose/compose-file/compose-file-v2/#variable-substitution. This environment variable substitution should be used to mask sensitive information in recipe files. As long as you can get env variables securely to the ingestion process there would not be any need to store sensitive information in recipes.
## Transformations

If you'd like to modify data before it reaches the ingestion sinks – for instance, adding additional owners or tags – you can use a transformer to write your own module and integrate it with DataHub.

Check out the [transformers guide](./transformers.md) for more info!

## Using as a library

In some cases, you might want to construct the MetadataChangeEvents yourself but still use this framework to emit that metadata to DataHub. In this case, take a look at the emitter interfaces, which can easily be imported and called from your own code.

- [DataHub emitter via REST](./src/datahub/emitter/rest_emitter.py) (same requirements as `datahub-rest`).
- [DataHub emitter via Kafka](./src/datahub/emitter/kafka_emitter.py) (same requirements as `datahub-kafka`).

### Programmatic Pipeline
In some cases, you might want to configure and run a pipeline entirely from within your custom python script. Here is an example of how to do it.
 - [programmatic_pipeline.py](./examples/library/programatic_pipeline.py) - a basic mysql to REST programmatic pipeline.


## Developing

See the guides on [developing](./developing.md), [adding a source](./adding-source.md) and [using transformers](./transformers.md).

